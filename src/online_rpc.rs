//! Authenticated, typed GC coordination on the private cluster network.
use crate::{
    gc::{Plan, Policy, Snapshot, oldest_first, plan, remove_file},
    node::Node,
    online_rpc::wire::{
        GcReply, GcRequest,
        online_gc_client::OnlineGcClient,
        online_gc_server::{OnlineGc, OnlineGcServer},
    },
    util::{Lock, durable, failpoint, read_json},
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeSet, fs, path::PathBuf, sync::Arc};
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
    pub nodes: Vec<String>,
    pub index: usize,
    pub token_file: PathBuf,
    pub pod_uid: Option<String>,
}
impl Config {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.nodes.is_empty() && self.index < self.nodes.len(),
            "invalid participant endpoints"
        );
        ensure!(
            self.nodes.iter().collect::<BTreeSet<_>>().len() == self.nodes.len(),
            "duplicate endpoints"
        );
        for endpoint in &self.nodes {
            let uri: tonic::transport::Uri = format!("http://{endpoint}").parse()?;
            ensure!(
                uri.host().is_some() && uri.port_u16().is_some(),
                "endpoint requires host and port"
            );
        }
        Ok(())
    }
    pub fn token(&self) -> Result<String> {
        let token = fs::read_to_string(&self.token_file)?.trim().to_owned();
        ensure!((32..=256).contains(&token.len()), "invalid token length");
        Ok(token)
    }
    fn pods(&self) -> Result<(BTreeSet<String>, BTreeSet<String>)> {
        let pods: BTreeSet<_> = self.pod_uid.iter().cloned().collect();
        Ok((pods.clone(), pods))
    }
}

#[derive(Clone)]
pub struct Service {
    pub(crate) node: Node,
    pub(crate) config: Config,
    token: Arc<Vec<u8>>,
}
impl Service {
    pub fn new(node: Node, config: Config) -> Result<Self> {
        config.validate()?;
        let token = Arc::new(format!("Bearer {}", config.token()?).into_bytes());
        let _membership = Lock::acquire(&node.base.join("membership.lock"), false)?;
        let identity = json!({"nodes":config.nodes,"index":config.index});
        let file = node.base.join("membership.json");
        if file.exists() {
            ensure!(
                read_json(&file)? == identity,
                "participant membership differs from persistent volume; explicit retirement is required before resizing the pool"
            );
        } else {
            durable(&file, &identity)?;
        }
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
                "maintain" => service.node.maintain(&request.epoch),
                "preflight" => {
                    let (required, all) = service.config.pods()?;
                    let mut report =
                        service
                            .node
                            .online_preflight(&request.epoch, &required, &all)?;
                    report["index"] = json!(service.config.index);
                    report["members"] = json!(service.config.nodes);
                    Ok(report)
                }
                "snapshot" => Ok(serde_json::to_value(
                    service.node.online_snapshot(request.origin)?,
                )?),
                "prepare" | "plan" => {
                    let plan: Plan = serde_json::from_slice(&request.plan_json)?;
                    ensure!(plan.id == request.epoch, "request epoch differs from plan");
                    ensure!(
                        plan.workers.len() == service.config.nodes.len(),
                        "plan membership differs from service"
                    );
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
        .map_err(|e| {
            let text = format!("{e:#}");
            let tail: String = text
                .chars()
                .rev()
                .take(4096)
                .collect::<String>()
                .chars()
                .rev()
                .collect();
            eprintln!("online GC {op}: {tail}");
            Status::failed_precondition(tail)
        })?;
        Ok(Response::new(GcReply {
            json: serde_json::to_vec(&result).map_err(|e| Status::internal(e.to_string()))?,
        }))
    }
}
#[tonic::async_trait]
impl OnlineGc for Service {
    async fn maintain(&self, r: Request<GcRequest>) -> Result<Response<GcReply>, Status> {
        self.operation(r, "maintain").await
    }
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
        .http2_keepalive_interval(Some(std::time::Duration::from_secs(20)))
        .http2_keepalive_timeout(Some(std::time::Duration::from_secs(10)))
        .add_service(crate::runner::server(&service.config)?)
        .add_service(crate::transport::server(
            service.node.clone(),
            service.config.clone(),
            std::env::current_exe()?,
        )?)
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
            "maintain" => self.client.maintain(request).await,
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

fn new_epoch_id() -> Result<String> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|e| anyhow::anyhow!("GC epoch randomness: {e}"))?;
    Ok(format!("{:032x}", u128::from_le_bytes(bytes)))
}

fn reserve_epoch(node: &Node, id: &str) -> Result<()> {
    let epoch = node.gc_epoch(id)?;
    fs::create_dir_all(epoch.parent().unwrap())?;
    // Never reuse an old epoch's acknowledgements, even on a random-ID collision.
    fs::create_dir(&epoch).context("GC epoch already exists or cannot be reserved")?;
    crate::util::syncdir(epoch.parent().unwrap())
}

// The coordinator holds the exclusive publication lease; no copy can still
// own these reservations. Retries reserve again, and worker outboxes stay rooted.
fn discard_abandoned_publications(node: &Node) -> Result<()> {
    ensure!(
        !node.base.join("online-master.json").exists()
            && !node.base.join("online-gc.json").exists(),
        "finish the active collection before discarding reservations"
    );
    for file in crate::node::journals(&node.base.join("pending-publications"))? {
        remove_file(&file)?;
    }
    Ok(())
}

pub async fn collect(
    node: &Node,
    config: &Config,
    dry: bool,
    policy: Option<Policy>,
) -> Result<Value> {
    config.validate()?;
    ensure!(config.index == 0, "run coordinator on origin node");
    // These file locks are held by this dedicated CLI, not by the RPC servers.
    let _coordinator = Lock::acquire(&node.base.join("gc-coordinator.lock"), false)?;
    let _publication = Lock::acquire(&node.base.join("publication.lock"), false)?;
    let file = node.base.join("online-master.json");
    let mut state = if file.exists() {
        ensure!(!dry, "resume active epoch before preview");
        read_json(&file)?
    } else {
        json!({"id":new_epoch_id()?,"phase":"prepare", "members":config.nodes})
    };
    ensure!(
        state["members"] == json!(config.nodes),
        "active collection membership changed"
    );
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
    let mut goals = None;
    // Finishing is idempotent even if a peer already removed its marker.
    if state["phase"] != "finish" {
        let mut reports = Vec::new();
        for (index, client) in clients.iter_mut().enumerate() {
            let report = client.call("preflight", &id, false, None).await?;
            ensure!(
                report["members"] == json!(config.nodes),
                "participant membership mismatch"
            );
            ensure!(
                report["index"] == index,
                "GC endpoint has the wrong node identity"
            );
            reports.push(report);
        }
        if !dry && !file.exists() {
            for (index, client) in clients.iter_mut().enumerate() {
                client.call("maintain", &id, false, None).await?;
                reports[index] = client.call("preflight", &id, false, None).await?;
            }
        }
        if let Some(policy) = &policy {
            let (pressure, wanted) = policy.goals(&reports)?;
            if !pressure && !file.exists() {
                return Ok(json!({"skipped":"below cache budget and disk pressure thresholds"}));
            }
            goals = Some(wanted);
        }
    }
    if state.get("candidates").is_none() {
        if !dry && !file.exists() {
            discard_abandoned_publications(node)?;
        }
        let snapshots = snapshots(&mut clients, &id).await?;
        let mut candidate = plan(&id, &snapshots)?;
        if let Some(wanted) = goals {
            candidate = oldest_first(candidate, &snapshots, &wanted)?;
            state["requested_bytes"] = json!(wanted);
        }
        if dry {
            return Ok(json!({"dry_run":true,"plan":candidate}));
        }
        reserve_epoch(node, &id)?;
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

pub mod wire {
    tonic::include_proto!("distributed_nix.v1");
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn abandoned_reservations_do_not_retire_worker_or_committed_state() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let node = Node {
            base: directory.path().join("state"),
            origin: directory.path().join("origin"),
            ..Node::default()
        };
        let pending = node.base.join("pending-publications/batch.json");
        let committed = node.origin.join(".distributed-nix-publications/batch.json");
        let outbox = node.base.join("outbox/path.json");
        for file in [&pending, &committed, &outbox] {
            durable(file, &json!({"retained":true}))?;
        }
        let active = node.base.join("online-gc.json");
        durable(&active, &json!({"id":"active"}))?;
        ensure!(discard_abandoned_publications(&node).is_err());
        ensure!(pending.exists());
        remove_file(&active)?;
        let _publication = Lock::acquire(&node.base.join("publication.lock"), false)?;
        discard_abandoned_publications(&node)?;
        discard_abandoned_publications(&node)?;
        ensure!(!pending.exists());
        ensure!(read_json(&committed)? == json!({"retained":true}));
        ensure!(read_json(&outbox)? == json!({"retained":true}));
        Ok(())
    }

    #[test]
    fn epoch_reservation_never_reuses_previous_acknowledgements() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let node = Node {
            base: directory.path().into(),
            ..Node::default()
        };
        let id = "00000000000000000000000000000001";
        reserve_epoch(&node, id)?;
        let acknowledgement = node.gc_epoch(id)?.join("online-worker-swept.json");
        durable(&acknowledgement, &json!({"old":true}))?;
        ensure!(reserve_epoch(&node, id).is_err());
        ensure!(read_json(&acknowledgement)? == json!({"old":true}));
        crate::gc::valid_id(&new_epoch_id()?)?;
        Ok(())
    }
}
