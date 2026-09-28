//! ARC attaches a GitHub runner to one warm builder for the lifetime of its RPC.
use crate::{
    job_cgroup::Group,
    online_rpc::{Config, wire::*},
};
use anyhow::{Context, Result, ensure};
use runner_event::Kind;
use runner_pool_server::{RunnerPool, RunnerPoolServer};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::{
    fs,
    os::unix::{fs::PermissionsExt, process::ExitStatusExt},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use subtle::ConstantTimeEq;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::Command,
    sync::{OwnedSemaphorePermit, Semaphore, mpsc},
    task::JoinSet,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

const CONFIG: &str = "/run/distributed-nix-arc.json";
pub const POISON: &str = "/run/distributed-nix-runner-poisoned";

#[derive(Clone, Deserialize, Serialize)]
pub struct Runtime {
    pub program: PathBuf,
    pub cgroup: PathBuf,
    pub work: PathBuf,
    pub database: PathBuf,
    pub ready: PathBuf,
    pub poison: PathBuf,
    pub node: String,
}

pub fn prepare() -> Result<()> {
    let image: serde_json::Value =
        serde_json::from_slice(&fs::read("/etc/distributed-nix/runtime.json")?)?;
    let Some(program) = image["githubRunner"].as_str() else {
        return Ok(());
    };
    let cgroup = crate::job_cgroup::parent(Path::new("/run/distributed-nix-cgroups"))?;
    for path in [
        "/work/arc",
        "/work/cache",
        "/run/distributed-nix-runner/jobs",
    ] {
        fs::create_dir_all(path)?;
    }
    std::os::unix::fs::chown("/work/cache", Some(1001), Some(1001))?;
    let runtime = Runtime {
        program: program.into(),
        cgroup,
        work: "/work/arc".into(),
        database: "/var/lib/distributed-nix/runner-starts.sqlite".into(),
        ready: "/run/distributed-nix-ready".into(),
        poison: POISON.into(),
        node: std::env::var("DISTRIBUTED_NIX_NODE_NAME")?,
    };
    fs::write(CONFIG, serde_json::to_vec(&runtime)?)?;
    Ok(())
}

#[derive(Clone)]
pub struct Service {
    token: Arc<Vec<u8>>,
    runtime: Option<Runtime>,
    starts: Option<Arc<Mutex<Connection>>>,
    slots: Arc<Semaphore>,
    poisoned: Arc<AtomicBool>,
}
impl Service {
    pub fn new(token: &str, runtime: Option<Runtime>) -> Result<Self> {
        let starts = runtime.as_ref().map(|r| -> Result<_> {
            let db = Connection::open(&r.database)?;
            db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; CREATE TABLE IF NOT EXISTS starts (id TEXT PRIMARY KEY) STRICT;")?;
            Ok(Arc::new(Mutex::new(db)))
        }).transpose()?;
        Ok(Self {
            token: Arc::new(format!("Bearer {token}").into_bytes()),
            runtime,
            starts,
            slots: Arc::new(Semaphore::new(1)),
            poisoned: Arc::new(AtomicBool::new(false)),
        })
    }
    fn started(&self, id: &str) -> Result<(), Status> {
        self.starts
            .as_ref()
            .ok_or_else(|| Status::unimplemented("ARC runtime not installed"))?
            .lock()
            .map_err(|_| Status::internal("runner bookkeeping poisoned"))?
            .execute("INSERT INTO starts (id) VALUES (?1)", [id])
            .map_err(|e| {
                if e.sqlite_error_code() == Some(rusqlite::ErrorCode::ConstraintViolation) {
                    Status::already_exists("runner attempt already started")
                } else {
                    Status::internal("cannot persist runner attempt")
                }
            })?;
        Ok(())
    }
}
pub fn server(config: &Config) -> Result<RunnerPoolServer<Service>> {
    let runtime = if config.index != 0 && Path::new(CONFIG).try_exists()? {
        Some(serde_json::from_slice(&fs::read(CONFIG)?)?)
    } else {
        None
    };
    Ok(
        RunnerPoolServer::new(Service::new(&config.token()?, runtime)?)
            .max_decoding_message_size(128 * 1024)
            .max_encoding_message_size(128 * 1024),
    )
}
fn event(kind: Kind) -> RunnerEvent {
    RunnerEvent {
        kind: kind as i32,
        output: vec![],
        exit_code: 0,
    }
}
fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 64 && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
}

// An interrupted/panicked handler cannot make a dirty slot available again.
struct Slot {
    permit: Option<OwnedSemaphorePermit>,
    poisoned: Arc<AtomicBool>,
    poison: PathBuf,
}
impl Slot {
    fn release(mut self) {
        drop(self.permit.take());
    }
}
impl Drop for Slot {
    fn drop(&mut self) {
        if let Some(permit) = self.permit.take() {
            permit.forget();
            self.poisoned.store(true, Ordering::Release);
            let _ = fs::write(&self.poison, b"runner cleanup incomplete\n");
        }
    }
}

type Sender = mpsc::Sender<Result<RunnerEvent, Status>>;
async fn forward(mut input: impl AsyncRead + Unpin, sender: Sender) {
    let mut buffer = vec![0; 16384];
    while let Ok(size) = input.read(&mut buffer).await {
        if size == 0 {
            break;
        }
        let mut message = event(Kind::Output);
        message.output = buffer[..size].to_vec();
        if sender.send(Ok(message)).await.is_err() {
            break;
        }
    }
}

async fn execute(
    runtime: &Runtime,
    group: &Group,
    id: &str,
    start: RunnerStart,
    work: &Path,
    socket: &Path,
    sender: Sender,
) -> Result<i32> {
    let mut native = Command::new(std::env::current_exe()?);
    native
        .args(["node", "runner-daemon", "--notify-ready"])
        .env("DISTRIBUTED_NIX_POD_UID", format!("arc-{id}"))
        .env("DISTRIBUTED_NIX_SOCKET_PATH", socket)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .kill_on_drop(true);
    group.attach(&mut native, false)?;
    let mut native = native.spawn()?;
    let mut acknowledgement = [0];
    native
        .stdout
        .take()
        .context("native readiness pipe")?
        .read_exact(&mut acknowledgement)
        .await?;
    ensure!(
        acknowledgement == *b"R",
        "invalid native readiness acknowledgement"
    );

    let mut runner = Command::new(&runtime.program);
    runner.args(["run", "--jitconfig", &start.jit_config]).env_clear()
        .env("PATH", std::env::var("PATH")?).env("HOME", work).env("RUNNER_ROOT", work)
        .env("XDG_CACHE_HOME", "/work/cache")
        .env("USER", "runner").env("LOGNAME", "runner").env("LANG", "C.UTF-8")
        .env("SSL_CERT_FILE", "/etc/ssl/certs/ca-certificates.crt")
        .env("NIX_SSL_CERT_FILE", "/etc/ssl/certs/ca-certificates.crt")
        .env("ACTIONS_RUNNER_RETURN_VERSION_DEPRECATED_EXIT_CODE", "true")
        .env("GITHUB_ACTIONS_RUNNER_EXTRA_USER_AGENT", start.user_agent)
        .env("CIBOX_NODE", &runtime.node).env("CIBOX_POD_UID", id)
        .env("NIX_CONFIG", format!("store = unix://{}\nexperimental-features = nix-command flakes ca-derivations\nflake-registry =\nmax-jobs = 2\ncores = 0\n", socket.display()))
        .current_dir(work).stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped()).kill_on_drop(true);
    group.attach(&mut runner, true)?;
    let mut runner = runner.spawn()?;
    let mut output = JoinSet::new();
    output.spawn(forward(
        runner.stdout.take().context("runner stdout")?,
        sender.clone(),
    ));
    output.spawn(forward(
        runner.stderr.take().context("runner stderr")?,
        sender.clone(),
    ));
    sender
        .send(Ok(event(Kind::Started)))
        .await
        .map_err(|_| anyhow::anyhow!("ARC client disconnected"))?;
    let status = tokio::select! {
        result = runner.wait() => result?,
        result = native.wait() => anyhow::bail!("job's native Nix daemon exited: {}", result?),
    };
    Ok(status
        .code()
        .unwrap_or_else(|| 128 + status.signal().unwrap_or(1)))
}

#[tonic::async_trait]
impl RunnerPool for Service {
    type AttachStream = ReceiverStream<Result<RunnerEvent, Status>>;
    async fn attach(
        &self,
        request: Request<tonic::Streaming<RunnerInput>>,
    ) -> Result<Response<Self::AttachStream>, Status> {
        let supplied = request
            .metadata()
            .get("authorization")
            .map(|v| v.as_bytes())
            .unwrap_or_default();
        if !bool::from(supplied.ct_eq(&self.token)) {
            return Err(Status::unauthenticated("invalid runner token"));
        }
        let runtime = self
            .runtime
            .clone()
            .ok_or_else(|| Status::unimplemented("ARC runtime not installed"))?;
        if self.poisoned.load(Ordering::Acquire) || !runtime.ready.is_file() {
            return Err(Status::unavailable("builder not ready"));
        }
        let mut input = request.into_inner();
        let claim = match input.message().await?.and_then(|m| m.message) {
            Some(runner_input::Message::Claim(claim)) if valid_id(&claim.id) => claim,
            _ => {
                return Err(Status::invalid_argument(
                    "first message must claim a valid runner ID",
                ));
            }
        };
        let permit = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("builder occupied"))?;
        let (sender, receiver) = mpsc::channel(16);
        let service = self.clone();
        tokio::spawn(async move {
            // No executable starts until the client chooses this reservation.
            if sender.send(Ok(event(Kind::Reserved))).await.is_err() {
                return;
            }
            let start = tokio::select! {
                _ = sender.closed() => return,
                next = input.message() => match next {
                    Ok(Some(RunnerInput { message: Some(runner_input::Message::Start(start)) })) => start,
                    _ => return,
                }
            };
            if start.jit_config.is_empty()
                || start.jit_config.len() > 65536
                || !start
                    .jit_config
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"+/=".contains(&b))
                || start.user_agent.len() > 512
                || !start
                    .user_agent
                    .bytes()
                    .all(|b| b.is_ascii_graphic() || b == b' ')
            {
                let _ = sender
                    .send(Err(Status::invalid_argument(
                        "invalid runner configuration",
                    )))
                    .await;
                return;
            }
            if let Err(error) = service.started(&claim.id) {
                let _ = sender.send(Err(error)).await;
                return;
            }
            let slot = Slot {
                permit: Some(permit),
                poisoned: service.poisoned.clone(),
                poison: runtime.poison.clone(),
            };
            let work = runtime.work.join(&claim.id);
            let socket = PathBuf::from(format!(
                "/run/distributed-nix-runner/jobs/{}.sock",
                claim.id
            ));
            let group = match Group::new(&runtime.cgroup, &claim.id) {
                Ok(group) => group,
                Err(error) => {
                    let _ = sender
                        .send(Err(Status::failed_precondition(error.to_string())))
                        .await;
                    return;
                }
            };
            let roots = match crate::online::group(
                Path::new(crate::node::BASE),
                Path::new(crate::node::ROOT),
                Some(&format!("arc-{}", claim.id)),
            ) {
                Ok(roots) => roots,
                Err(error) => {
                    let _ = sender.send(Err(Status::internal(error.to_string()))).await;
                    return;
                }
            };
            let result = async {
                fs::create_dir(&work)?;
                fs::set_permissions(&work, fs::Permissions::from_mode(0o700))?;
                std::os::unix::fs::chown(&work, Some(1001), Some(1001))?;
                tokio::select! {
                    result = execute(&runtime, &group, &claim.id, start, &work, &socket, sender.clone()) => result,
                    _ = sender.closed() => anyhow::bail!("ARC client disconnected"),
                    _ = input.message() => anyhow::bail!("ARC attachment ended"),
                }
            }.await;
            let cleanup = async {
                group.stop().await?;
                if socket.try_exists()? {
                    fs::remove_file(&socket)?;
                }
                tokio::task::spawn_blocking(move || {
                    if work.exists() {
                        fs::remove_dir_all(work)
                    } else {
                        Ok(())
                    }
                })
                .await??;
                Ok::<_, anyhow::Error>(())
            }
            .await;
            if let Err(error) = cleanup {
                // Keep pins until this poisoned builder exits if descendants remain.
                std::mem::forget(roots);
                eprintln!("ARC cleanup failed: {error:#}");
                let _ = sender
                    .send(Err(Status::internal(
                        "builder cleanup failed; restarting required",
                    )))
                    .await;
                return;
            }
            drop(roots);
            slot.release();
            let reply = match result {
                Ok(code) => {
                    let mut message = event(Kind::Exited);
                    message.exit_code = code;
                    Ok(message)
                }
                Err(error) => Err(Status::failed_precondition(error.to_string())),
            };
            let _ = sender.send(reply).await;
        });
        Ok(Response::new(ReceiverStream::new(receiver)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use runner_pool_client::RunnerPoolClient;
    use tokio_stream::wrappers::TcpListenerStream;

    fn runtime(base: &Path) -> Runtime {
        Runtime {
            program: base.join("never-execute"),
            cgroup: base.join("no-cgroups"),
            work: base.join("work"),
            database: base.join("starts.sqlite"),
            ready: base.join("ready"),
            poison: base.join("poison"),
            node: "test-node".into(),
        }
    }

    #[tokio::test]
    async fn attachment_reserves_without_executing_and_disconnect_releases() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let runtime = runtime(dir.path());
        fs::write(&runtime.ready, b"")?;
        let service = Service::new("test-token", Some(runtime.clone()))?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server_service = service.clone();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(RunnerPoolServer::new(server_service))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
        });
        let mut client = RunnerPoolClient::connect(format!("http://{address}")).await?;
        let request = |token: &str, id: &str| {
            let (sender, receiver) = mpsc::channel(2);
            sender
                .try_send(RunnerInput {
                    message: Some(runner_input::Message::Claim(RunnerClaim { id: id.into() })),
                })
                .unwrap();
            let mut request = Request::new(ReceiverStream::new(receiver));
            request
                .metadata_mut()
                .insert("authorization", format!("Bearer {token}").parse().unwrap());
            (sender, request)
        };
        let (_unauthorized, req) = request("wrong", "first");
        assert_eq!(
            client.attach(req).await.unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
        let (_invalid, req) = request("test-token", "../escape");
        assert_eq!(
            client.attach(req).await.unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        let (input, req) = request("test-token", "first");
        let mut events = client.attach(req).await?.into_inner();
        assert_eq!(events.message().await?.unwrap().kind, Kind::Reserved as i32);
        assert!(!runtime.work.exists());
        assert_eq!(
            service.starts.as_ref().unwrap().lock().unwrap().query_row(
                "SELECT count(*) FROM starts",
                [],
                |r| r.get::<_, i64>(0)
            )?,
            0
        );
        let (_busy, req) = request("test-token", "second");
        assert_eq!(
            client.attach(req).await.unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );
        drop(input);
        assert!(events.message().await?.is_none());
        drop(service.slots.acquire().await?);
        let (input, req) = request("test-token", "second");
        let mut events = client.attach(req).await?.into_inner();
        assert_eq!(events.message().await?.unwrap().kind, Kind::Reserved as i32);
        input
            .send(RunnerInput {
                message: Some(runner_input::Message::Start(RunnerStart {
                    jit_config: "not base64!".into(),
                    user_agent: String::new(),
                })),
            })
            .await?;
        assert_eq!(
            events.message().await.unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        drop(service.slots.acquire().await?);
        assert!(!runtime.poison.exists());
        assert!(!runtime.work.exists());
        server.abort();
        Ok(())
    }

    #[test]
    fn attempt_ids_survive_restart_and_cleanup_failure_poison_is_fail_closed() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let runtime = runtime(dir.path());
        let service = Service::new("token", Some(runtime.clone()))?;
        service.started("first")?;
        drop(service);
        let service = Service::new("token", Some(runtime.clone()))?;
        assert_eq!(
            service.started("first").unwrap_err().code(),
            tonic::Code::AlreadyExists
        );
        service.started("second")?;
        let slot = Slot {
            permit: Some(service.slots.clone().try_acquire_owned()?),
            poisoned: service.poisoned.clone(),
            poison: runtime.poison.clone(),
        };
        slot.release();
        assert_eq!(service.slots.available_permits(), 1);
        let slot = Slot {
            permit: Some(service.slots.clone().try_acquire_owned()?),
            poisoned: service.poisoned.clone(),
            poison: runtime.poison.clone(),
        };
        drop(slot);
        assert!(service.poisoned.load(Ordering::Acquire));
        assert!(runtime.poison.is_file());
        assert_eq!(service.slots.available_permits(), 0);
        Ok(())
    }
}
