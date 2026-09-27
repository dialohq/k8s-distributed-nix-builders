//! Fixed-membership cluster transport; native Nix copies newly published closures.
use crate::{
    manifest::{Manifest, valid_path},
    node::{BASE, BIN, ORIGIN},
    util::*,
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    process::{Command, Output},
    time::Instant,
};

#[derive(Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    pub nodes: [std::net::Ipv4Addr; 3],
    pub identity_file: PathBuf,
    pub known_hosts_file: PathBuf,
}
impl ClusterConfig {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.nodes.iter().all(|ip| ip.is_private()),
            "cluster nodes must use private IPv4 addresses"
        );
        ensure!(
            self.nodes
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                == 3,
            "duplicate cluster node"
        );
        ensure!(
            self.identity_file.is_absolute() && self.known_hosts_file.is_absolute(),
            "SSH paths must be absolute"
        );
        Ok(())
    }
}

#[derive(Clone)]
pub struct Cluster {
    pub repo: PathBuf,
    config: ClusterConfig,
}
impl Cluster {
    pub fn new() -> Result<Self> {
        let path = std::env::var_os("DISTRIBUTED_NIX_CONFIG")
            .map(PathBuf::from)
            .unwrap_or_else(|| "/etc/distributed-nix/cluster.json".into());
        let config: ClusterConfig = serde_json::from_slice(&std::fs::read(path)?)?;
        config.validate()?;
        Ok(Self {
            repo: PathBuf::from(BASE).join("cluster"),
            config,
        })
    }
    fn ssh_options(&self) -> Vec<String> {
        vec![
            "-i".into(),
            self.config.identity_file.to_string_lossy().into_owned(),
            "-o".into(),
            format!(
                "UserKnownHostsFile={}",
                self.config.known_hosts_file.display()
            ),
            "-o".into(),
            "StrictHostKeyChecking=yes".into(),
            "-o".into(),
            "BatchMode=yes".into(),
            "-o".into(),
            "ConnectTimeout=5".into(),
            "-o".into(),
            "ServerAliveInterval=2".into(),
            "-o".into(),
            "ServerAliveCountMax=3".into(),
        ]
    }
    fn ssh(&self, node: usize, cmd: &str) -> Result<Command> {
        ensure!(node < 3, "node must be 0, 1 or 2");
        let mut c = Command::new("ssh");
        c.args(self.ssh_options())
            .arg(format!("root@{}", self.config.nodes[node]))
            .arg(cmd);
        Ok(c)
    }
    pub fn remote(&self, node: usize, cmd: &str) -> Result<Output> {
        output(&mut self.ssh(node, cmd)?)
    }
    pub fn call(&self, node: usize, args: &[String]) -> Result<Output> {
        let mut words = vec![BIN.into(), "node".into()];
        words.extend_from_slice(args);
        self.remote(node, &join(&words))
    }
    pub fn call_json(&self, node: usize, args: &[String]) -> Result<Value> {
        Ok(serde_json::from_slice(&self.call(node, args)?.stdout)?)
    }
    pub fn receive(&self, node: usize, m: &Manifest) -> Result<String> {
        let o = input(
            &mut self.ssh(node, &format!("{BIN} node receive"))?,
            &serde_json::to_vec(m)?,
        )?;
        Ok(serde_json::from_slice::<Value>(&o.stdout)?["file"]
            .as_str()
            .context("receive file")?
            .into())
    }
    pub fn dump(&self, node: usize, paths: &[String], origin: bool) -> Result<Manifest> {
        ensure!(
            !paths.is_empty() && paths.iter().all(|p| valid_path(p)),
            "invalid dump paths"
        );
        let mut args = vec![if origin {
            "dump-origin".into()
        } else {
            "dump".into()
        }];
        args.extend_from_slice(paths);
        Manifest::parse(self.call_json(node, &args)?)
    }

    pub fn admit(&self, node: usize, m: &Manifest) -> Result<Value> {
        let file = self.receive(node, m)?;
        self.call_json(node, &["admit".into(), file])
    }
    pub fn parallel<T: Send>(&self, f: impl Fn(usize) -> Result<T> + Sync) -> Result<Vec<T>> {
        std::thread::scope(|s| {
            let tasks: Vec<_> = (0..3)
                .map(|i| {
                    let f = &f;
                    s.spawn(move || f(i))
                })
                .collect();
            // Always join every operation, even when a peer fails.
            let results: Vec<_> = tasks
                .into_iter()
                .map(|h| {
                    h.join()
                        .map_err(|_| anyhow::anyhow!("cluster thread panicked"))
                        .and_then(|r| r)
                })
                .collect();
            results.into_iter().collect()
        })
    }
    fn nix(&self) -> Command {
        let mut c = Command::new("nix");
        c.env("NIX_SSHOPTS", join(&self.ssh_options()));
        c
    }
    fn uri(&self, node: usize, origin: bool) -> String {
        format!(
            "ssh-ng://root@{}?remote-program={BASE}/{}-stdio",
            self.config.nodes[node],
            if origin { "origin" } else { "worker" }
        )
    }
    pub fn publish_to(&self, node: usize, paths: &[String], targets: &[usize]) -> Result<Value> {
        let _lease = self.lease()?;
        self.publish_inner(node, paths, targets)
    }
    fn publish_inner(&self, node: usize, paths: &[String], targets: &[usize]) -> Result<Value> {
        ensure!(node < 3 && targets.iter().all(|n| *n < 3), "invalid node");
        ensure!(
            !paths.is_empty() && paths.iter().all(|p| valid_path(p)),
            "invalid publication paths"
        );
        let start = Instant::now();
        let mut pin = vec!["pin-local".into()];
        pin.extend_from_slice(paths);
        self.call(node, &pin)?;
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
        ensure!(node < 3 && targets.iter().all(|n| *n < 3), "invalid node");
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
        self.call(0, &["reserve".into(), file.clone()])?;
        let copied = output(
            self.nix()
                .args([
                    "copy",
                    "--from",
                    &self.uri(node, false),
                    "--to",
                    &self.uri(0, true),
                    "--no-check-sigs",
                ])
                .args(paths),
        )?;
        self.call(0, &["commit".into(), file])?;
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
        let result = json!({"batch":id,"paths":paths,"records":m.paths.len(),"canonicalized_paths":canonicalized,"origin_committed":true,"publish_seconds":publish_seconds,"total_seconds":start.elapsed().as_secs_f64(),"admissions":results,"copy_stderr":String::from_utf8_lossy(&copied.stderr)});
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
        self.publish_to(node, paths, &[0, 1, 2])
    }
    pub fn reconcile(&self, id: &str) -> Result<Value> {
        let _lease = self.lease()?;
        ensure!(
            id.len() == 64 && id.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid batch ID"
        );
        let m = Manifest::parse(serde_json::from_slice(
            &self
                .remote(
                    0,
                    &format!("cat {ORIGIN}/.distributed-nix-publications/{id}.json"),
                )?
                .stdout,
        )?)?;
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

/// An open SSH channel holds a kernel lock. Dropping it releases the lease;
/// publication cannot overlap an online collection epoch.
struct RemoteLease {
    child: std::process::Child,
}
impl Drop for RemoteLease {
    fn drop(&mut self) {
        drop(self.child.stdin.take());
        let _ = self.child.wait();
    }
}
impl Cluster {
    fn lease(&self) -> Result<RemoteLease> {
        use std::io::BufRead;
        use std::process::Stdio;
        let mut child = self
            .ssh(0, &format!("{BIN} node lease"))?
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let mut line = String::new();
        std::io::BufReader::new(child.stdout.as_mut().context("lease stdout")?)
            .read_line(&mut line)?;
        if line.trim() != "leased" {
            let _ = child.wait();
            anyhow::bail!("origin lease refused; GC may be paused");
        }
        Ok(RemoteLease { child })
    }
}
