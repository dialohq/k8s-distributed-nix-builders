//! Publication coordination over typed gRPC and native Nix transfer streams.
use crate::{
    manifest::{Manifest, valid_path},
    node::BASE,
    online_rpc::{
        Config,
        wire::{StoreRequest, store_request::Operation},
    },
    transport::Client,
    util::*,
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};
#[derive(Clone)]
pub struct Cluster {
    pub repo: PathBuf,
    runtime: Arc<tokio::runtime::Runtime>,
    clients: Vec<Client>,
    origin: Client,
}
impl Cluster {
    pub fn new() -> Result<Self> {
        let path = std::env::var_os("DISTRIBUTED_NIX_ONLINE_CONFIG")
            .map(PathBuf::from)
            .unwrap_or_else(|| "/etc/distributed-nix/online-gc.json".into());
        let config: Config = serde_json::from_slice(&std::fs::read(path)?)?;
        config.validate()?;
        let runtime = Arc::new(tokio::runtime::Runtime::new()?);
        let token = config.token()?;
        let (clients, origin) = runtime.block_on(async {
            Ok::<_, anyhow::Error>((
                config
                    .nodes
                    .iter()
                    .map(|address| Client::new(address, &token, false))
                    .collect::<Result<Vec<_>>>()?,
                Client::new(&config.nodes[0], &token, true)?,
            ))
        })?;
        Ok(Self {
            repo: PathBuf::from(BASE).join("cluster"),
            runtime,
            clients,
            origin,
        })
    }
    pub fn status(&self) -> Result<Value> {
        Ok(json!(self.parallel(|index| self.request(
            index,
            Operation::Info,
            &[],
            "",
            vec![]
        ))?))
    }
    pub fn len(&self) -> usize {
        self.clients.len()
    }
    fn request(
        &self,
        node: usize,
        operation: Operation,
        paths: &[String],
        batch: &str,
        manifest: Vec<u8>,
    ) -> Result<Value> {
        let client = self.clients.get(node).context("unknown participant")?;
        self.runtime.block_on(client.operate(StoreRequest {
            operation: operation as i32,
            paths: paths.to_vec(),
            batch: batch.into(),
            manifest,
        }))
    }
    pub fn call_json(&self, node: usize, args: &[String]) -> Result<Value> {
        let op = arg(args, 0)?;
        let operation = match op {
            "outbox" => Operation::Outbox,
            "acknowledge" => Operation::Acknowledge,
            "ca-outbox" => Operation::CaOutbox,
            "ca-acknowledge" => Operation::CaAcknowledge,
            "realisation-conflicts" => Operation::Conflicts,
            "canonical-manifest" => Operation::Canonical,
            "reserve" => Operation::Reserve,
            "commit" => Operation::Commit,
            "pin-local" => Operation::Pin,
            "admit" => Operation::Admit,
            _ => anyhow::bail!("unknown publication operation"),
        };
        let batch = matches!(
            operation,
            Operation::Conflicts
                | Operation::Canonical
                | Operation::Reserve
                | Operation::Commit
                | Operation::Admit
        );
        self.request(
            node,
            operation,
            if batch { &[] } else { &args[1..] },
            if batch { arg(args, 1)? } else { "" },
            vec![],
        )
    }
    pub fn receive(&self, node: usize, manifest: &Manifest) -> Result<String> {
        let value = self.request(
            node,
            Operation::Receive,
            &[],
            "",
            serde_json::to_vec(manifest)?,
        )?;
        Ok(value["batch"].as_str().context("batch ID")?.into())
    }
    pub fn dump(&self, node: usize, paths: &[String], origin: bool) -> Result<Manifest> {
        ensure!(
            !paths.is_empty() && paths.iter().all(|p| valid_path(p)),
            "invalid dump paths"
        );
        Manifest::parse(self.request(
            node,
            if origin {
                Operation::DumpOrigin
            } else {
                Operation::Dump
            },
            paths,
            "",
            vec![],
        )?)
    }
    pub fn admit(&self, node: usize, manifest: &Manifest) -> Result<Value> {
        let id = self.receive(node, manifest)?;
        self.call_json(node, &["admit".into(), id])
    }
    pub fn parallel<T: Send>(&self, f: impl Fn(usize) -> Result<T> + Sync) -> Result<Vec<T>> {
        std::thread::scope(|scope| {
            let jobs: Vec<_> = (0..self.len())
                .map(|i| {
                    let f = &f;
                    scope.spawn(move || f(i))
                })
                .collect();
            let results: Vec<_> = jobs
                .into_iter()
                .map(|job| {
                    job.join()
                        .map_err(|_| anyhow::anyhow!("publication thread panicked"))
                        .and_then(|r| r)
                })
                .collect();
            results.into_iter().collect()
        })
    }
    fn lease(&self) -> Result<tonic::Streaming<crate::online_rpc::wire::Empty>> {
        self.runtime.block_on(self.origin.lease())
    }
    fn copy(&self, source: usize, paths: &[String]) -> Result<()> {
        let directory = tempfile::tempdir()?;
        let source_socket = directory.path().join("source.sock");
        let destination_socket = directory.path().join("origin.sock");
        let (_source, _destination) = self.runtime.block_on(async {
            Ok::<_, anyhow::Error>((
                self.clients[source].proxy(&source_socket)?,
                self.origin.proxy(&destination_socket)?,
            ))
        })?;
        crate::native::copy(
            Path::new(&format!("unix://{}", source_socket.display())),
            &format!("unix://{}", destination_socket.display()),
            paths,
        )?;
        Ok(())
    }
    pub fn bootstrap(&self, node: &crate::node::Node) -> Result<Value> {
        let _publication = self.lease()?;
        let known: std::collections::BTreeSet<String> = {
            let _admit = Lock::acquire(&node.base.join("admit.lock"), false)?;
            crate::admissions::Admissions::open(&node.base.join("admissions"))?
                .ids()?
                .into_iter()
                .collect()
        };
        let mut count = 0;
        for file in crate::node::journals(&node.lower.join(".distributed-nix-publications"))? {
            let id = file
                .file_stem()
                .and_then(|s| s.to_str())
                .context("publication name")?;
            if !known.contains(id) {
                node.admit(&file, false)?;
                count += 1;
            }
        }
        Ok(json!({"admitted_batches":count}))
    }
    pub fn publish_to(&self, node: usize, paths: &[String], targets: &[usize]) -> Result<Value> {
        let _lease = self.lease()?;
        self.publish_inner(node, paths, targets)
    }
    fn publish_inner(&self, node: usize, paths: &[String], targets: &[usize]) -> Result<Value> {
        ensure!(
            node < self.len() && targets.iter().all(|n| *n < self.len()),
            "invalid node"
        );
        ensure!(
            !paths.is_empty() && paths.iter().all(|p| valid_path(p)),
            "invalid publication paths"
        );
        let start = Instant::now();
        let mut pin = vec!["pin-local".into()];
        pin.extend_from_slice(paths);
        self.call_json(node, &pin)?;
        let m = self.dump(node, paths, false)?;
        self.publish_manifest_inner(node, &m, targets, start)
    }
    pub fn publish_manifest(&self, node: usize, m: &Manifest, targets: &[usize]) -> Result<Value> {
        let _lease = self.lease()?;
        self.publish_manifest_inner(node, m, targets, Instant::now())
    }
    fn publish_manifest_inner(
        &self,
        node: usize,
        m: &Manifest,
        targets: &[usize],
        start: Instant,
    ) -> Result<Value> {
        ensure!(
            node < self.len() && targets.iter().all(|n| *n < self.len()),
            "invalid node"
        );
        m.validate()?;
        let candidate = self.receive(0, m)?;
        let canonical = self.call_json(0, &["canonical-manifest".into(), candidate])?;
        let canonical = Manifest::parse(canonical)?;
        let canonicalized: Vec<_> = m
            .paths
            .iter()
            .filter_map(|(p, info)| {
                canonical
                    .paths
                    .get(p)
                    .filter(|stored| stored["narHash"] != info["narHash"])
                    .map(|_| p.clone())
            })
            .collect();
        let m = &canonical;
        let paths = &m.roots;
        let id = m.id()?;
        let file = self.receive(0, &m)?;
        self.call_json(0, &["reserve".into(), file.clone()])?;
        self.copy(node, paths)?;
        self.call_json(0, &["commit".into(), file])?;
        let publish_seconds = start.elapsed().as_secs_f64();
        let results = std::thread::scope(|s| {
            let jobs: Vec<_> = targets
                .iter()
                .map(|&n| {
                    let m = &m;
                    s.spawn(move || match self.admit(n, m) {
                        Ok(v) => json!({"node":n,"ack":v}),
                        Err(e) => json!({"node":n,"error":format!("{e:#}")}),
                    })
                })
                .collect();
            jobs.into_iter()
                .map(|j| {
                    j.join()
                        .map_err(|_| anyhow::anyhow!("admission thread panicked"))
                })
                .collect::<Result<Vec<_>>>()
        })?;
        let result = json!({"batch":id,"paths":paths,"records":m.paths.len(),"published_paths":m.paths.keys().collect::<Vec<_>>(),"canonicalized_paths":canonicalized,"origin_committed":true,"publish_seconds":publish_seconds,"total_seconds":start.elapsed().as_secs_f64(),"admissions":results});
        durable(
            &self
                .repo
                .join("results/distributed-nix")
                .join(format!("publication-{id}.json")),
            &result,
        )?;
        ensure!(
            results.iter().all(|v| v.get("error").is_none()),
            "origin committed batch {id}; admission incomplete. Run `distributed-nix reconcile {id}`. Details: {results:?}"
        );
        Ok(result)
    }
    pub fn publish(&self, node: usize, paths: &[String]) -> Result<Value> {
        self.publish_to(node, paths, &(0..self.len()).collect::<Vec<_>>())
    }
    pub fn reconcile(&self, id: &str) -> Result<Value> {
        let _lease = self.lease()?;
        ensure!(
            id.len() == 64 && id.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid batch ID"
        );
        let m = Manifest::parse(self.request(0, Operation::Publication, &[], id, vec![])?)?;
        ensure!(m.id()? == id, "publication identity mismatch");
        let rows = self.parallel(|node| Ok(json!({"node":node,"ack":self.admit(node,&m)?})))?;
        let result = json!({"batch":id,"admissions":rows});
        durable(
            &self
                .repo
                .join("results/distributed-nix")
                .join(format!("reconcile-{id}.json")),
            &result,
        )?;
        Ok(result)
    }
}
