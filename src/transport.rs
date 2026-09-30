//! Authenticated store publication and bounded native Nix streams over gRPC.
use crate::{
    manifest::Manifest,
    node::Node,
    online_rpc::{Config, wire::*},
    util::{Lock, durable, read_json},
};
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
};
use store_request::Operation;
use store_transport_client::StoreTransportClient;
use store_transport_server::{StoreTransport, StoreTransportServer};
use subtle::ConstantTimeEq;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    sync::{Semaphore, mpsc},
    task::{JoinHandle, JoinSet},
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, transport::Channel};
const CHUNK: usize = 65536;
const MAX: usize = 64 * 1024 * 1024;
fn status(e: impl std::fmt::Display) -> Status {
    Status::failed_precondition(e.to_string().chars().take(4096).collect::<String>())
}

#[derive(Clone)]
pub struct Service {
    node: Node,
    config: Config,
    token: Arc<Vec<u8>>,
    sessions: Arc<Semaphore>,
    executable: PathBuf,
}
pub fn server(
    node: Node,
    config: Config,
    executable: PathBuf,
) -> Result<StoreTransportServer<Service>> {
    let token = Arc::new(format!("Bearer {}", config.token()?).into_bytes());
    Ok(StoreTransportServer::new(Service {
        node,
        config,
        token,
        sessions: Arc::new(Semaphore::new(32)),
        executable,
    })
    .max_decoding_message_size(MAX)
    .max_encoding_message_size(MAX))
}
impl Service {
    fn authorize<T>(&self, request: &Request<T>) -> Result<(), Status> {
        let supplied = request
            .metadata()
            .get("authorization")
            .map(|v| v.as_bytes())
            .unwrap_or_default();
        if !bool::from(supplied.ct_eq(&self.token)) {
            return Err(Status::unauthenticated("invalid controller token"));
        }
        Ok(())
    }
    fn operate(&self, request: StoreRequest) -> Result<Value> {
        let op = Operation::try_from(request.operation)?;
        let _gate = Lock::acquire(&self.node.base.join("maintenance.lock"), true)?;
        ensure!(
            self.node.base.join("ready").exists(),
            "store recovery incomplete"
        );
        let batch_file = || -> Result<PathBuf> {
            ensure!(
                request.batch.len() == 64 && request.batch.bytes().all(|c| c.is_ascii_hexdigit()),
                "invalid batch ID"
            );
            Ok(self
                .node
                .base
                .join("incoming")
                .join(format!("{}.json", request.batch)))
        };
        if matches!(
            op,
            Operation::DumpOrigin
                | Operation::Reserve
                | Operation::Commit
                | Operation::Canonical
                | Operation::Publication
        ) {
            ensure!(
                self.config.index == 0,
                "operation requires collection owner"
            );
        }
        Ok(match op {
            Operation::Info => {
                json!({"index":self.config.index,"ready":true,"members":self.config.nodes})
            }
            Operation::Release => {
                crate::gc::remove_file(&batch_file()?)?;
                Value::Null
            }
            Operation::CaReject => self
                .node
                .ca_reject(&serde_json::from_slice(&request.manifest)?)?,
            Operation::Receive => {
                let manifest = Manifest::parse(serde_json::from_slice(&request.manifest)?)?;
                let id = manifest.id()?;
                durable(
                    &self.node.base.join("incoming").join(format!("{id}.json")),
                    &manifest,
                )?;
                json!({"batch":id})
            }
            Operation::Dump | Operation::DumpOrigin => serde_json::to_value(crate::native::dump(
                if op == Operation::DumpOrigin {
                    &self.node.origin
                } else {
                    &self.node.root
                },
                &request.paths,
            )?)?,
            Operation::Admit => self.node.admit(&batch_file()?, false)?,
            Operation::Reserve | Operation::Commit => self
                .node
                .publication(&batch_file()?, op == Operation::Commit)?,
            Operation::Canonical => serde_json::to_value(crate::native::canonical_manifest(
                &self.node.origin,
                &Manifest::read(&batch_file()?)?,
            )?)?,
            Operation::Outbox => self.node.outbox()?,
            Operation::Acknowledge => self.node.acknowledge(&request.paths)?,
            Operation::CaOutbox => self.node.ca_outbox()?,
            Operation::CaAcknowledge => self.node.ca_acknowledge(&request.paths)?,
            Operation::Pin => self.node.pin(&request.paths)?,
            Operation::Conflicts => {
                let manifest = Manifest::read(&batch_file()?)?;
                let worker = crate::native::realisation_conflicts(&self.node.root, &manifest)?;
                let origin = if self.config.index == 0 {
                    crate::native::realisation_conflicts(&self.node.origin, &manifest)?
                } else {
                    json!([])
                };
                json!({"worker":worker,"origin":origin})
            }
            Operation::Publication => {
                batch_file()?;
                read_json(
                    &self
                        .node
                        .origin
                        .join(".distributed-nix-publications")
                        .join(format!("{}.json", request.batch)),
                )?
            }
        })
    }
}
#[tonic::async_trait]
impl StoreTransport for Service {
    async fn operate(
        &self,
        request: Request<StoreRequest>,
    ) -> Result<Response<StoreReply>, Status> {
        self.authorize(&request)?;
        let service = self.clone();
        let result = tokio::task::spawn_blocking(move || service.operate(request.into_inner()))
            .await
            .map_err(status)?
            .map_err(status)?;
        Ok(Response::new(StoreReply {
            json: serde_json::to_vec(&result).map_err(status)?,
        }))
    }
    type LeaseStream = ReceiverStream<Result<Empty, Status>>;
    async fn lease(&self, request: Request<Empty>) -> Result<Response<Self::LeaseStream>, Status> {
        self.authorize(&request)?;
        if self.config.index != 0 {
            return Err(Status::failed_precondition(
                "lease requires collection owner",
            ));
        }
        let base = self.node.base.clone();
        let lease = tokio::task::spawn_blocking(move || -> Result<Lock> {
            let lock = Lock::acquire(&base.join("publication.lock"), true)?;
            ensure!(
                !base.join("online-master.json").exists() && !base.join("online-gc.json").exists(),
                "online collection pending"
            );
            Ok(lock)
        })
        .await
        .map_err(status)?
        .map_err(status)?;
        let (sender, receiver) = mpsc::channel(1);
        sender.send(Ok(Empty {})).await.map_err(status)?;
        tokio::spawn(async move {
            let _lease = lease;
            sender.closed().await;
        });
        Ok(Response::new(ReceiverStream::new(receiver)))
    }
    type TransferStream = ReceiverStream<Result<Frame, Status>>;
    async fn transfer(
        &self,
        request: Request<tonic::Streaming<Frame>>,
    ) -> Result<Response<Self::TransferStream>, Status> {
        self.authorize(&request)?;
        let origin = request
            .metadata()
            .get("x-origin")
            .is_some_and(|v| v == "true");
        if origin && self.config.index != 0 {
            return Err(Status::failed_precondition("not collection owner"));
        }
        let permit = self
            .sessions
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("too many transfers"))?;
        let node = self.node.clone();
        let gate = tokio::task::spawn_blocking(move || -> Result<Lock> {
            let gate = Lock::acquire(
                &node.base.join(if origin {
                    "publication.lock"
                } else {
                    "admit.lock"
                }),
                true,
            )?;
            ensure!(
                node.base.join("ready").exists(),
                "store recovery incomplete"
            );
            if origin {
                ensure!(
                    !node.base.join("online-master.json").exists(),
                    "collection pending"
                );
            }
            Ok(gate)
        })
        .await
        .map_err(status)?
        .map_err(status)?;
        let root = if origin {
            &self.node.origin
        } else {
            &self.node.root
        };
        // Nix read-only mode opens SQLite as immutable, ignoring live WAL commits.
        let mut child = tokio::process::Command::new(&self.executable)
            .arg("native-transfer").arg(root).arg(if origin { "import" } else { "export" }).env("NIX_CONFIG", "experimental-features = nix-command flakes ca-derivations\nbuild-users-group =\nmax-jobs = 0\n")
            .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::inherit()).kill_on_drop(true).spawn().map_err(status)?;
        let mut input = child.stdin.take().unwrap();
        let mut output = child.stdout.take().unwrap();
        let mut incoming = request.into_inner();
        let mut writer = tokio::spawn(async move {
            while let Some(frame) = incoming.message().await? {
                if frame.data.len() > CHUNK {
                    return Err(Status::resource_exhausted("oversized transfer frame"));
                }
                input.write_all(&frame.data).await.map_err(status)?;
            }
            input.shutdown().await.map_err(status)
        });
        let (sender, receiver) = mpsc::channel(16);
        tokio::spawn(async move {
            let (_gate, _permit) = (gate, permit);
            let mut bytes = vec![0; CHUNK];
            let mut input_closed = false;
            loop {
                let read = tokio::select! {
                    _ = sender.closed() => { let _ = child.kill().await; break; }
                    result = &mut writer, if !input_closed => {
                        input_closed = true;
                        if !matches!(result, Ok(Ok(()))) { let _ = sender.send(Err(Status::aborted("transfer input failed"))).await; let _ = child.kill().await; break; }
                        continue;
                    }
                    read = output.read(&mut bytes) => read,
                };
                match read {
                    Ok(0) => break,
                    Ok(size) => {
                        if sender
                            .send(Ok(Frame {
                                data: bytes[..size].to_vec(),
                            }))
                            .await
                            .is_err()
                        {
                            let _ = child.kill().await;
                            break;
                        }
                    }
                    Err(error) => {
                        let _ = sender.send(Err(status(error))).await;
                        let _ = child.kill().await;
                        break;
                    }
                }
            }
            writer.abort();
            match child.wait().await {
                Ok(exit) if exit.success() => (),
                _ => {
                    let _ = sender
                        .send(Err(Status::internal("native transfer failed")))
                        .await;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(receiver)))
    }
}

#[derive(Clone)]
pub struct Client {
    inner: StoreTransportClient<Channel>,
    token: tonic::metadata::MetadataValue<tonic::metadata::Ascii>,
    origin: bool,
}
impl Client {
    pub fn new(endpoint: &str, token: &str, origin: bool) -> Result<Self> {
        let channel = Channel::from_shared(format!("http://{endpoint}"))?
            .connect_timeout(std::time::Duration::from_secs(5))
            .connect_lazy();
        Ok(Self {
            inner: StoreTransportClient::new(channel)
                .max_decoding_message_size(MAX)
                .max_encoding_message_size(MAX),
            token: format!("Bearer {token}").parse()?,
            origin,
        })
    }
    fn request<T>(&self, value: T) -> Request<T> {
        let mut request = Request::new(value);
        request
            .metadata_mut()
            .insert("authorization", self.token.clone());
        request.metadata_mut().insert(
            "x-origin",
            if self.origin { "true" } else { "false" }.parse().unwrap(),
        );
        request
    }
    pub async fn operate(&self, request: StoreRequest) -> Result<Value> {
        let reply = self
            .inner
            .clone()
            .operate(self.request(request))
            .await?
            .into_inner();
        Ok(serde_json::from_slice(&reply.json)?)
    }
    pub async fn lease(&self) -> Result<tonic::Streaming<Empty>> {
        let mut stream = self
            .inner
            .clone()
            .lease(self.request(Empty {}))
            .await?
            .into_inner();
        ensure!(
            stream.message().await?.is_some(),
            "lease closed before acknowledgement"
        );
        Ok(stream)
    }
    async fn bridge(&self, socket: UnixStream) -> Result<()> {
        let (mut read, mut write) = socket.into_split();
        let (sender, receiver) = mpsc::channel(16);
        let mut input = JoinSet::new();
        input.spawn(async move {
            let mut bytes = vec![0; CHUNK];
            loop {
                let size = read.read(&mut bytes).await?;
                if size == 0
                    || sender
                        .send(Frame {
                            data: bytes[..size].to_vec(),
                        })
                        .await
                        .is_err()
                {
                    break;
                }
            }
            Ok::<(), std::io::Error>(())
        });
        let result = async {
            let mut response = self
                .inner
                .clone()
                .transfer(self.request(ReceiverStream::new(receiver)))
                .await?
                .into_inner();
            while let Some(frame) = response.message().await? {
                ensure!(frame.data.len() <= CHUNK, "oversized transfer response");
                write.write_all(&frame.data).await?;
            }
            write.shutdown().await?;
            Ok::<(), anyhow::Error>(())
        }
        .await;
        input.abort_all();
        result
    }
    pub fn proxy(&self, path: &Path) -> Result<Proxy> {
        use std::os::unix::fs::PermissionsExt;
        let listener = UnixListener::bind(path)?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        let client = self.clone();
        let task = tokio::spawn(async move {
            let mut sessions = JoinSet::new();
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        let Ok((socket, _)) = result else {break}; let client = client.clone();
                        sessions.spawn(async move {if let Err(error) = client.bridge(socket).await {eprintln!("store transport: {error:#}");}});
                    }
                    _ = sessions.join_next(), if !sessions.is_empty() => (),
                }
            }
        });
        Ok(Proxy {
            task,
            path: path.to_path_buf(),
        })
    }
}
pub struct Proxy {
    task: JoinHandle<()>,
    path: PathBuf,
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
        let _ = fs::remove_file(&self.path);
    }
}
