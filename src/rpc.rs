use crate::{
    manifest::Manifest,
    native,
    node::{Node, roots_named},
    util::{durable, read_json, syncdir},
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    os::{fd::AsRawFd, unix::fs::PermissionsExt},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};
use subtle::ConstantTimeEq;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, UnixListener, UnixStream},
    sync::mpsc,
    task::{JoinHandle, JoinSet},
};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{
    Request, Response, Status,
    transport::{Channel, Server},
};

pub mod wire {
    tonic::include_proto!("distributed_nix.v1");
}
use wire::{
    Frame, Member, Prepared, Publication, Receipt, Snapshot,
    collection_client::CollectionClient,
    collection_server::{Collection, CollectionServer},
};
const CHUNK: usize = 64 * 1024;
const MAX_MESSAGE: usize = 64 * 1024 * 1024;

#[derive(Deserialize, Serialize)]
pub struct Config {
    pub root: PathBuf,
    pub listen: std::net::SocketAddr,
    pub token_file: PathBuf,
}

#[derive(Deserialize, Serialize)]
pub struct ClientConfig {
    pub endpoint: String,
    pub token_file: PathBuf,
    pub member: String,
}

impl ClientConfig {
    pub async fn connect(&self) -> Result<Client> {
        let token = fs::read_to_string(&self.token_file)?;
        Client::connect(&self.endpoint, token.trim(), &self.member).await
    }
}

fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 128 && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
}
fn status(error: impl std::fmt::Display) -> Status {
    Status::failed_precondition(error.to_string())
}

#[derive(Default)]
struct Registry {
    snapshot: Option<Snapshot>,
    sessions: BTreeMap<String, usize>,
}

pub struct Service {
    root: PathBuf,
    state: PathBuf,
    executable: PathBuf,
    token: Vec<u8>,
    registry: Mutex<Registry>,
    _lock: fs::File,
}
impl Service {
    pub fn new(root: PathBuf, token: Vec<u8>, executable: PathBuf) -> Result<Arc<Self>> {
        ensure!(root.is_absolute(), "collection root must be absolute");
        ensure!(
            token.len() >= 32 && token.len() <= 256,
            "collection token must contain 32–256 bytes"
        );
        let state = root.join(".distributed-nix-rpc");
        for directory in ["members", "pending"] {
            fs::create_dir_all(state.join(directory))?;
        }
        fs::set_permissions(&state, fs::Permissions::from_mode(0o700))?;
        let lock = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(state.join("server.lock"))?;
        ensure!(
            unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0,
            "collection already has an active coordinator"
        );
        fs::create_dir_all(root.join(".distributed-nix-catalogs"))?;
        Ok(Arc::new(Self {
            root,
            state,
            executable,
            token,
            registry: Mutex::new(Registry::default()),
            _lock: lock,
        }))
    }
    fn authorize<T>(&self, request: &Request<T>) -> std::result::Result<String, Status> {
        let supplied = request
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.strip_prefix("Bearer "))
            .ok_or_else(|| Status::unauthenticated("missing credentials"))?;
        if supplied.as_bytes().ct_eq(&self.token).unwrap_u8() != 1 {
            return Err(Status::unauthenticated("invalid credentials"));
        }
        let member = request
            .metadata()
            .get("x-member")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if !valid_id(member) {
            return Err(Status::invalid_argument("invalid member ID"));
        }
        Ok(member.to_string())
    }
    fn admitted(&self, member: &str) -> Result<()> {
        ensure!(
            !self.state.join("gc-active.json").exists(),
            "collection is paused for GC"
        );
        ensure!(
            self.state
                .join("members")
                .join(format!("{member}.json"))
                .is_file(),
            "member has not bootstrapped"
        );
        Ok(())
    }
    fn prune_snapshots(&self, registry: &Registry) -> Result<()> {
        let mut keep = BTreeSet::new();
        if let Some(snapshot) = &registry.snapshot {
            keep.insert(snapshot.generation.clone());
        }
        for entry in fs::read_dir(self.state.join("members"))? {
            let value = read_json(&entry?.path())?;
            keep.insert(
                value["generation"]
                    .as_str()
                    .context("member generation")?
                    .to_string(),
            );
        }
        let catalogs = self.root.join(".distributed-nix-catalogs");
        for entry in fs::read_dir(&catalogs)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with("g-")
                && valid_id(&name)
                && !keep.contains(&name)
                && entry.file_type()?.is_dir()
            {
                fs::remove_dir_all(entry.path())?;
            }
        }
        syncdir(&catalogs)
    }
    fn bootstrap(&self, member: &str) -> Result<Snapshot> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| anyhow::anyhow!("registry poisoned"))?;
        ensure!(
            !self.state.join("gc-active.json").exists(),
            "collection is paused for GC"
        );
        let lease = self.state.join("members").join(format!("{member}.json"));
        if lease.exists() {
            let value = read_json(&lease)?;
            return Ok(Snapshot {
                generation: value["generation"]
                    .as_str()
                    .context("member generation")?
                    .into(),
                paths: value["paths"].as_u64().context("member path count")?,
            });
        }
        let snapshot = if let Some(snapshot) = &registry.snapshot {
            snapshot.clone()
        } else {
            let generation = format!(
                "g-{}-{}",
                std::process::id(),
                SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
            );
            let destination = self
                .root
                .join(".distributed-nix-catalogs")
                .join(&generation);
            let metadata = native::snapshot(&self.root, &destination)?;
            for name in ["db.sqlite", "schema"] {
                fs::File::open(destination.join("nix/var/nix/db").join(name))?.sync_all()?;
            }
            syncdir(&destination.join("nix/var/nix/db"))?;
            durable(&destination.join("snapshot.json"), &metadata)?;
            let snapshot = Snapshot {
                generation,
                paths: metadata["paths"]
                    .as_array()
                    .context("snapshot paths")?
                    .len() as u64,
            };
            registry.snapshot = Some(snapshot.clone());
            snapshot
        };
        durable(
            &lease,
            &json!({"generation":snapshot.generation,"paths":snapshot.paths}),
        )?;
        self.prune_snapshots(&registry)?;
        Ok(snapshot)
    }
    fn prepare(&self, member: &str, candidate: Manifest) -> Result<Prepared> {
        let _registry = self
            .registry
            .lock()
            .map_err(|_| anyhow::anyhow!("registry poisoned"))?;
        self.admitted(member)?;
        let manifest = native::canonical_manifest(&self.root, &candidate)?;
        native::check(&self.root, &manifest)?;
        let id = manifest.id()?;
        roots_named(
            &self.root,
            &manifest.roots,
            &format!("distributed-nix-publications/{id}"),
        )?;
        durable(
            &self.state.join("pending").join(format!("{id}.json")),
            &manifest,
        )?;
        Ok(Prepared {
            id,
            manifest_json: serde_json::to_vec(&manifest)?,
        })
    }
    fn commit(&self, member: &str, manifest: Manifest) -> Result<Receipt> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| anyhow::anyhow!("registry poisoned"))?;
        self.admitted(member)?;
        let id = manifest.id()?;
        let prepared = Manifest::read(&self.state.join("pending").join(format!("{id}.json")))?;
        ensure!(prepared == manifest, "publication was not prepared");
        let check = native::check(&self.root, &manifest)?;
        ensure!(
            check["existing"]
                .as_array()
                .context("existing paths")?
                .len()
                == manifest.paths.len(),
            "publication has missing paths"
        );
        native::register(&self.root, &manifest)?;
        durable(
            &self
                .root
                .join(".distributed-nix-publications")
                .join(format!("{id}.json")),
            &manifest,
        )?;
        registry.snapshot = None;
        Ok(Receipt { id })
    }
    fn release(&self, member: &str) -> Result<Receipt> {
        let registry = self
            .registry
            .lock()
            .map_err(|_| anyhow::anyhow!("registry poisoned"))?;
        ensure!(
            registry.sessions.get(member).copied().unwrap_or(0) == 0,
            "member still has store sessions"
        );
        let lease = self.state.join("members").join(format!("{member}.json"));
        if lease.exists() {
            fs::remove_file(&lease)?;
            syncdir(lease.parent().unwrap())?;
        }
        self.prune_snapshots(&registry)?;
        Ok(Receipt { id: member.into() })
    }
}

struct SessionGuard {
    service: Arc<Service>,
    member: String,
}
impl Drop for SessionGuard {
    fn drop(&mut self) {
        if let Ok(mut registry) = self.service.registry.lock() {
            if let Some(count) = registry.sessions.get_mut(&self.member) {
                *count -= 1;
            }
        }
    }
}

#[tonic::async_trait]
impl Collection for Arc<Service> {
    async fn bootstrap(
        &self,
        request: Request<Member>,
    ) -> std::result::Result<Response<Snapshot>, Status> {
        let member = self.authorize(&request)?;
        if request.get_ref().id != member {
            return Err(Status::invalid_argument("member mismatch"));
        }
        let service = self.clone();
        let result = tokio::task::spawn_blocking(move || Service::bootstrap(&service, &member))
            .await
            .map_err(status)?
            .map_err(status)?;
        Ok(Response::new(result))
    }
    async fn prepare(
        &self,
        request: Request<Publication>,
    ) -> std::result::Result<Response<Prepared>, Status> {
        let member = self.authorize(&request)?;
        let input = request.into_inner();
        if input.member != member {
            return Err(Status::invalid_argument("member mismatch"));
        }
        let candidate =
            Manifest::parse(serde_json::from_slice(&input.manifest_json).map_err(status)?)
                .map_err(status)?;
        let service = self.clone();
        let result =
            tokio::task::spawn_blocking(move || Service::prepare(&service, &member, candidate))
                .await
                .map_err(status)?
                .map_err(status)?;
        Ok(Response::new(result))
    }
    async fn commit(
        &self,
        request: Request<Publication>,
    ) -> std::result::Result<Response<Receipt>, Status> {
        let member = self.authorize(&request)?;
        let input = request.into_inner();
        if input.member != member {
            return Err(Status::invalid_argument("member mismatch"));
        }
        let manifest =
            Manifest::parse(serde_json::from_slice(&input.manifest_json).map_err(status)?)
                .map_err(status)?;
        let service = self.clone();
        let result =
            tokio::task::spawn_blocking(move || Service::commit(&service, &member, manifest))
                .await
                .map_err(status)?
                .map_err(status)?;
        Ok(Response::new(result))
    }
    async fn release(
        &self,
        request: Request<Member>,
    ) -> std::result::Result<Response<Receipt>, Status> {
        let member = self.authorize(&request)?;
        if request.get_ref().id != member {
            return Err(Status::invalid_argument("member mismatch"));
        }
        let service = self.clone();
        let result = tokio::task::spawn_blocking(move || Service::release(&service, &member))
            .await
            .map_err(status)?
            .map_err(status)?;
        Ok(Response::new(result))
    }
    type StoreStream = ReceiverStream<std::result::Result<Frame, Status>>;
    async fn store(
        &self,
        request: Request<tonic::Streaming<Frame>>,
    ) -> std::result::Result<Response<Self::StoreStream>, Status> {
        let member = self.authorize(&request)?;
        {
            let mut registry = self.registry.lock().map_err(status)?;
            self.admitted(&member).map_err(status)?;
            if registry.sessions.values().sum::<usize>() >= 32 {
                return Err(Status::resource_exhausted("too many store sessions"));
            }
            *registry.sessions.entry(member.clone()).or_default() += 1;
        }
        let guard = SessionGuard {
            service: self.clone(),
            member,
        };
        let mut child = tokio::process::Command::new(&self.executable)
            .arg("native-collection").arg(&self.root)
            .env("NIX_CONFIG", "experimental-features = nix-command flakes ca-derivations\nbuild-users-group =\nmax-jobs = 0\n")
            .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::inherit()).kill_on_drop(true).spawn().map_err(status)?;
        let mut input = child
            .stdin
            .take()
            .ok_or_else(|| Status::internal("missing native input"))?;
        let mut output = child
            .stdout
            .take()
            .ok_or_else(|| Status::internal("missing native output"))?;
        let mut incoming = request.into_inner();
        let mut writer = tokio::spawn(async move {
            while let Some(frame) = incoming.message().await? {
                if frame.data.len() > CHUNK {
                    return Err(Status::resource_exhausted("oversized store frame"));
                }
                input.write_all(&frame.data).await.map_err(status)?;
            }
            input.shutdown().await.map_err(status)
        });
        let (sender, receiver) = mpsc::channel(16);
        tokio::spawn(async move {
            let _guard = guard;
            let mut bytes = vec![0; CHUNK];
            let mut input_closed = false;
            loop {
                let read = tokio::select! {
                    _ = sender.closed() => { let _ = child.kill().await; break; }
                    result = &mut writer, if !input_closed => {
                        input_closed = true;
                        match result {
                            Ok(Ok(())) => continue,
                            error => {
                                let _ = sender.send(Err(Status::aborted(format!("store input failed: {error:?}")))).await;
                                let _ = child.kill().await;
                                break;
                            }
                        }
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
                Ok(_) => {
                    let _ = sender
                        .send(Err(Status::internal("native store session failed")))
                        .await;
                }
                Err(error) => {
                    let _ = sender.send(Err(status(error))).await;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(receiver)))
    }
}

pub async fn serve(
    listener: TcpListener,
    service: Arc<Service>,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<()> {
    Server::builder()
        .add_service(
            CollectionServer::new(service)
                .max_decoding_message_size(MAX_MESSAGE)
                .max_encoding_message_size(MAX_MESSAGE),
        )
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
        .await?;
    Ok(())
}

#[derive(Clone)]
pub struct Client {
    inner: CollectionClient<Channel>,
    token: tonic::metadata::MetadataValue<tonic::metadata::Ascii>,
    member: String,
}
impl Client {
    pub async fn connect(endpoint: &str, token: &str, member: &str) -> Result<Self> {
        ensure!(valid_id(member), "invalid member ID");
        let channel = tonic::transport::Endpoint::from_shared(endpoint.to_string())?
            .connect_timeout(std::time::Duration::from_secs(5))
            .connect()
            .await?;
        Ok(Self {
            inner: CollectionClient::new(channel)
                .max_decoding_message_size(MAX_MESSAGE)
                .max_encoding_message_size(MAX_MESSAGE),
            token: format!("Bearer {token}").parse()?,
            member: member.into(),
        })
    }
    fn request<T>(&self, value: T) -> Request<T> {
        let mut request = Request::new(value);
        request
            .metadata_mut()
            .insert("authorization", self.token.clone());
        request.metadata_mut().insert(
            "x-member",
            self.member.parse().expect("validated member ID"),
        );
        request
    }
    pub async fn bootstrap(&self) -> Result<Snapshot> {
        Ok(self
            .inner
            .clone()
            .bootstrap(self.request(Member {
                id: self.member.clone(),
            }))
            .await?
            .into_inner())
    }
    pub async fn prepare(&self, manifest: &Manifest) -> Result<Manifest> {
        let result = self
            .inner
            .clone()
            .prepare(self.request(Publication {
                member: self.member.clone(),
                manifest_json: serde_json::to_vec(manifest)?,
            }))
            .await?
            .into_inner();
        let canonical = Manifest::parse(serde_json::from_slice(&result.manifest_json)?)?;
        ensure!(canonical.id()? == result.id, "publication ID mismatch");
        Ok(canonical)
    }
    pub async fn commit(&self, manifest: &Manifest) -> Result<()> {
        let result = self
            .inner
            .clone()
            .commit(self.request(Publication {
                member: self.member.clone(),
                manifest_json: serde_json::to_vec(manifest)?,
            }))
            .await?
            .into_inner();
        ensure!(
            result.id == manifest.id()?,
            "publication acknowledgement mismatch"
        );
        Ok(())
    }
    pub async fn publish(&self, root: &Path, manifest: &Manifest) -> Result<String> {
        self.prepare(manifest).await?;
        let temporary = tempfile::tempdir()?;
        let socket = temporary.path().join("store.sock");
        let _proxy = self.proxy(&socket)?;
        let source = root.to_path_buf();
        let paths = manifest.roots.clone();
        let target = format!("unix://{}", socket.display());
        tokio::task::spawn_blocking(move || native::copy(&source, &target, &paths)).await??;
        let canonical = self.prepare(manifest).await?;
        self.commit(&canonical).await?;
        canonical.id()
    }
    pub async fn flush(&self, node: &Node) -> Result<usize> {
        let source = node.clone();
        let ca = tokio::task::spawn_blocking(move || source.ca_outbox()).await??;
        let mut published = 0;
        if !ca.is_null() {
            let manifest = Manifest::parse(ca["manifest"].clone())?;
            self.publish(&node.root, &manifest).await?;
            let ready: Vec<String> = serde_json::from_value(ca["ready"].clone())?;
            node.ca_acknowledge(&ready)?;
            published += ready.len();
        }
        let source = node.clone();
        let paths: Vec<String> =
            serde_json::from_value(tokio::task::spawn_blocking(move || source.outbox()).await??)?;
        for batch in paths.chunks(128) {
            let source = node.root.clone();
            let roots = batch.to_vec();
            let manifest =
                tokio::task::spawn_blocking(move || native::dump(&source, &roots)).await??;
            self.publish(&node.root, &manifest).await?;
            node.acknowledge(batch)?;
            published += batch.len();
        }
        Ok(published)
    }
    pub async fn release(&self) -> Result<()> {
        self.inner
            .clone()
            .release(self.request(Member {
                id: self.member.clone(),
            }))
            .await?;
        Ok(())
    }
    async fn bridge(&self, socket: UnixStream) -> Result<()> {
        let (mut read, mut write) = socket.into_split();
        let (sender, receiver) = mpsc::channel(16);
        let mut input = JoinSet::new();
        input.spawn(async move {
            let mut bytes = vec![0; CHUNK];
            loop {
                let size = read.read(&mut bytes).await?;
                if size == 0 {
                    break;
                }
                if sender
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
                .store(self.request(ReceiverStream::new(receiver)))
                .await?
                .into_inner();
            while let Some(frame) = response.message().await? {
                ensure!(frame.data.len() <= CHUNK, "oversized store response");
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
        let listener = UnixListener::bind(path)?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        let client = self.clone();
        let task = tokio::spawn(async move {
            let mut sessions = JoinSet::new();
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        let Ok((socket, _)) = result else { break };
                        let client = client.clone();
                        sessions.spawn(async move { if let Err(error) = client.bridge(socket).await { eprintln!("collection transport: {error:#}"); } });
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
