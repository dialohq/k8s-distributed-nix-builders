//! Authenticated, typed GC coordination on the private cluster network.
use crate::{
    gc::{Plan, Snapshot, plan, remove_file},
    node::Node,
    rpc::wire::{
        GcReply, GcRequest,
        online_gc_client::OnlineGcClient,
        online_gc_server::{OnlineGc, OnlineGcServer},
    },
    util::{Lock, durable, failpoint, read_json},
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    fs,
    net::SocketAddr,
    path::PathBuf,
    process::Command,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use subtle::ConstantTimeEq;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{
    Request, Response, Status,
    transport::{Channel, Server},
};
const MAX: usize = 64 * 1024 * 1024;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub nodes: Vec<SocketAddr>,
    pub index: usize,
    pub token_file: PathBuf,
    pub cri_command: Vec<String>,
    pub namespace: String,
}
impl Config {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.nodes.len() == 3 && self.index < 3,
            "three GC endpoints required"
        );
        ensure!(
            self.nodes.iter().collect::<BTreeSet<_>>().len() == 3,
            "duplicate endpoints"
        );
        ensure!(
            !self.cri_command.is_empty() && self.cri_command[0].starts_with('/'),
            "absolute CRI command required"
        );
        Ok(())
    }
    fn token(&self) -> Result<String> {
        let token = fs::read_to_string(&self.token_file)?.trim().to_owned();
        ensure!((32..=256).contains(&token.len()), "invalid token length");
        Ok(token)
    }
    fn pods(&self) -> Result<(BTreeSet<String>, BTreeSet<String>)> {
        let reply =
            crate::util::json(Command::new(&self.cri_command[0]).args(&self.cri_command[1..]))?;
        let mut all = BTreeSet::new();
        let mut required = BTreeSet::new();
        for pod in reply["items"].as_array().context("CRI sandbox list")? {
            let uid = pod["metadata"]["uid"]
                .as_str()
                .context("sandbox UID")?
                .to_owned();
            if pod["metadata"]["namespace"] == self.namespace {
                required.insert(uid.clone());
            }
            all.insert(uid);
        }
        Ok((required, all))
    }
}

#[derive(Clone)]
pub struct Service {
    node: Node,
    config: Config,
    token: Arc<Vec<u8>>,
}
impl Service {
    pub fn new(node: Node, config: Config) -> Result<Self> {
        config.validate()?;
        let token = Arc::new(format!("Bearer {}", config.token()?).into_bytes());
        Ok(Self {
            node,
            config,
            token,
        })
    }
    async fn operation(
        &self,
        request: Request<GcRequest>,
        op: &'static str,
    ) -> Result<Response<GcReply>, Status> {
        let supplied = request
            .metadata()
            .get("authorization")
            .map(|v| v.as_bytes())
            .unwrap_or_default();
        if !bool::from(supplied.ct_eq(&self.token)) {
            return Err(Status::unauthenticated("invalid GC token"));
        }
        let service = self.clone();
        let request = request.into_inner();
        let result = tokio::task::spawn_blocking(move || -> Result<Value> {
            crate::gc::valid_id(&request.epoch)?;
            ensure!(
                !request.origin || service.config.index == 0,
                "origin role only exists on node zero"
            );
            let _operation =
                Lock::acquire(&service.node.base.join("online-operation.lock"), false)?;
            match op {
                "preflight" => {
                    let (required, all) = service.config.pods()?;
                    let mut report =
                        service
                            .node
                            .online_preflight(&request.epoch, &required, &all)?;
                    report["index"] = json!(service.config.index);
                    Ok(report)
                }
                "snapshot" => Ok(serde_json::to_value(
                    service.node.online_snapshot(request.origin)?,
                )?),
                "prepare" | "plan" => {
                    let plan: Plan = serde_json::from_slice(&request.plan_json)?;
                    ensure!(plan.id == request.epoch, "request epoch differs from plan");
                    if op == "prepare" {
                        service.node.online_prepare(&plan)
                    } else {
                        service.node.online_plan(&plan)
                    }
                }
                "sweep" => service.node.online_sweep(
                    &request.epoch,
                    if request.origin {
                        None
                    } else {
                        Some(service.config.index)
                    },
                ),
                "finish" => service.node.online_finish(&request.epoch),
                _ => unreachable!(),
            }
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .map_err(|e| Status::failed_precondition(format!("{e:#}")))?;
        Ok(Response::new(GcReply {
            json: serde_json::to_vec(&result).map_err(|e| Status::internal(e.to_string()))?,
        }))
    }
}
#[tonic::async_trait]
impl OnlineGc for Service {
    async fn preflight(&self, r: Request<GcRequest>) -> Result<Response<GcReply>, Status> {
        self.operation(r, "preflight").await
    }
    async fn snapshot(&self, r: Request<GcRequest>) -> Result<Response<GcReply>, Status> {
        self.operation(r, "snapshot").await
    }
    async fn prepare(&self, r: Request<GcRequest>) -> Result<Response<GcReply>, Status> {
        self.operation(r, "prepare").await
    }
    async fn plan(&self, r: Request<GcRequest>) -> Result<Response<GcReply>, Status> {
        self.operation(r, "plan").await
    }
    async fn sweep(&self, r: Request<GcRequest>) -> Result<Response<GcReply>, Status> {
        self.operation(r, "sweep").await
    }
    async fn finish(&self, r: Request<GcRequest>) -> Result<Response<GcReply>, Status> {
        self.operation(r, "finish").await
    }
}
pub async fn serve(
    listener: tokio::net::TcpListener,
    service: Service,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<()> {
    Server::builder()
        .add_service(
            OnlineGcServer::new(service)
                .max_decoding_message_size(MAX)
                .max_encoding_message_size(MAX),
        )
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
        .await?;
    Ok(())
}

struct Client {
    client: OnlineGcClient<Channel>,
    token: tonic::metadata::MetadataValue<tonic::metadata::Ascii>,
}
impl Client {
    async fn call(
        &mut self,
        op: &str,
        id: &str,
        origin: bool,
        plan: Option<&Plan>,
    ) -> Result<Value> {
        let mut request = Request::new(GcRequest {
            epoch: id.into(),
            origin,
            plan_json: plan
                .map(serde_json::to_vec)
                .transpose()?
                .unwrap_or_default(),
        });
        request
            .metadata_mut()
            .insert("authorization", self.token.clone());
        let reply = match op {
            "preflight" => self.client.preflight(request).await,
            "snapshot" => self.client.snapshot(request).await,
            "prepare" => self.client.prepare(request).await,
            "plan" => self.client.plan(request).await,
            "sweep" => self.client.sweep(request).await,
            "finish" => self.client.finish(request).await,
            _ => unreachable!(),
        }
        .with_context(|| format!("online GC {op}"))?
        .into_inner();
        Ok(serde_json::from_slice(&reply.json)?)
    }
}
async fn snapshots(clients: &mut [Client], id: &str) -> Result<Vec<Snapshot>> {
    let mut result = Vec::new();
    for client in clients.iter_mut() {
        result.push(serde_json::from_value(
            client.call("snapshot", id, false, None).await?,
        )?);
    }
    result.push(serde_json::from_value(
        clients[0].call("snapshot", id, true, None).await?,
    )?);
    Ok(result)
}

pub async fn collect(
    node: &Node,
    config: &Config,
    dry: bool,
    threshold: Option<u8>,
) -> Result<Value> {
    config.validate()?;
    ensure!(config.index == 0, "run coordinator on origin node");
    // These file locks are held by this dedicated CLI, not by the RPC servers.
    let _coordinator = Lock::acquire(&node.base.join("gc-coordinator.lock"), false)?;
    let _publication = Lock::acquire(&node.base.join("publication.lock"), false)?;
    ensure!(
        !node.base.join("gc-master.json").exists(),
        "resume offline GC first"
    );
    let file = node.base.join("online-master.json");
    let mut state = if file.exists() {
        ensure!(!dry, "resume active epoch before preview");
        read_json(&file)?
    } else {
        json!({"id":format!("{:032x}",SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()),"phase":"prepare"})
    };
    let id = state["id"].as_str().context("epoch")?.to_owned();
    let token: tonic::metadata::MetadataValue<tonic::metadata::Ascii> =
        format!("Bearer {}", config.token()?).parse()?;
    let mut clients = Vec::new();
    for address in &config.nodes {
        let endpoint = Channel::from_shared(format!("http://{address}"))?
            .connect_timeout(std::time::Duration::from_secs(5));
        clients.push(Client {
            client: OnlineGcClient::new(endpoint.connect().await?)
                .max_decoding_message_size(MAX)
                .max_encoding_message_size(MAX),
            token: token.clone(),
        });
    }
    // Finishing is idempotent even if a peer already removed its marker.
    if state["phase"] != "finish" {
        let mut pressure = false;
        for (index, client) in clients.iter_mut().enumerate() {
            let report = client.call("preflight", &id, false, None).await?;
            ensure!(
                report["index"] == index,
                "GC endpoint has the wrong node identity"
            );
            if let Some(percent) = threshold {
                ensure!(
                    (1..=100).contains(&percent),
                    "invalid disk pressure threshold"
                );
                pressure |= u128::from(report["available"].as_u64().context("available blocks")?)
                    * 100
                    < u128::from(report["blocks"].as_u64().context("total blocks")?)
                        * u128::from(percent);
            }
        }
        if threshold.is_some() && !pressure && !file.exists() {
            return Ok(json!({"skipped":"no shared-store disk pressure"}));
        }
    }
    if state.get("candidates").is_none() {
        let candidate = plan(&id, &snapshots(&mut clients, &id).await?)?;
        if dry {
            return Ok(json!({"dry_run":true,"plan":candidate}));
        }
        state["candidates"] = serde_json::to_value(candidate)?;
        durable(&file, &state)?;
    }
    if state.get("plan").is_none() {
        let candidate: Plan = serde_json::from_value(state["candidates"].clone())?;
        for client in &mut clients {
            client.call("prepare", &id, false, Some(&candidate)).await?;
        }
        failpoint("online-after-barriers");
        let mut final_plan = plan(&id, &snapshots(&mut clients, &id).await?)?;
        for (paths, first) in final_plan
            .workers
            .iter_mut()
            .chain([&mut final_plan.origin])
            .zip(candidate.workers.iter().chain([&candidate.origin]))
        {
            *paths = paths.intersection(first).cloned().collect();
        }
        final_plan.validate()?;
        state["plan"] = serde_json::to_value(final_plan)?;
        state["phase"] = json!("sweep");
        durable(&file, &state)?;
    }
    let plan: Plan = serde_json::from_value(state["plan"].clone())?;
    if state["phase"] == "sweep" {
        for client in &mut clients {
            client.call("plan", &id, false, Some(&plan)).await?;
        }
        let mut acks = Vec::new();
        for client in &mut clients {
            acks.push(client.call("sweep", &id, false, None).await?);
        }
        state["acks"] = json!(acks);
        durable(&file, &state)?;
        failpoint("online-after-worker-acks");
        state["origin_ack"] = clients[0].call("sweep", &id, true, None).await?;
        state["phase"] = json!("finish");
        durable(&file, &state)?;
    }
    for client in &mut clients {
        client.call("finish", &id, false, None).await?;
    }
    let bytes: u64 = state["acks"]
        .as_array()
        .context("acks")?
        .iter()
        .chain([&state["origin_ack"]])
        .map(|r| r["result"]["bytes_freed"].as_u64().unwrap_or(0))
        .sum();
    state["bytes_freed"] = json!(bytes);
    durable(&node.gc_epoch(&id)?.join("online-complete.json"), &state)?;
    remove_file(&file)?;
    Ok(state)
}
