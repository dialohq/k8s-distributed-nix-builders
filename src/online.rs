//! Pod-lifetime Nix roots and resumable retirement of unused shared paths.
use crate::{
    manifest::valid_path,
    node::{BASE, Node},
    util::{Lock, durable, read_json, syncdir},
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    fs::{self, OpenOptions},
    os::{fd::AsRawFd, unix::fs::symlink},
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
    time::Duration,
};

static ROOT_GROUP: OnceLock<String> = OnceLock::new();
static PINNED: Mutex<BTreeSet<String>> = Mutex::new(BTreeSet::new());
const ROOTS: &str = "nix/var/nix/gcroots/distributed-nix-clients";

pub struct RootGroup {
    name: String,
    _lease: Lock,
}

fn valid_group(name: &str) -> bool {
    !name.is_empty()
        && name.len() < 128
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
}

pub fn group(base: &Path, root: &Path, pod: Option<&str>) -> Result<RootGroup> {
    let _gate = Lock::acquire(&base.join("online-roots.lock"), true)?;
    let groups = base.join("client-roots");
    fs::create_dir_all(&groups)?;
    let (name, directory) = if let Some(pod) = pod {
        ensure!(valid_group(pod), "invalid pod UID");
        let name = format!("pod-{pod}");
        let directory = groups.join(&name);
        fs::create_dir_all(&directory)?;
        (name, directory)
    } else {
        let directory = tempfile::Builder::new()
            .prefix("client-")
            .tempdir_in(&groups)?
            .keep();
        (
            directory
                .file_name()
                .unwrap()
                .to_str()
                .context("group name")?
                .to_owned(),
            directory,
        )
    };
    let lease = Lock::acquire(&directory.join("lease"), true)?;
    let info = json!({"version":1,"pod":pod});
    let file = directory.join("info.json");
    if file.exists() {
        ensure!(read_json(&file)? == info, "root group identity changed");
    } else {
        durable(&file, &info)?;
    }
    fs::create_dir_all(root.join(ROOTS).join(&name))?;
    Ok(RootGroup {
        name,
        _lease: lease,
    })
}

impl RootGroup {
    pub fn activate(&self) -> Result<()> {
        ROOT_GROUP
            .set(self.name.clone())
            .map_err(|_| anyhow::anyhow!("root group already initialized"))
    }
}

pub fn native_group(root: &Path, runner: bool) -> Result<RootGroup> {
    let pod = if runner {
        Some(std::env::var("CIBOX_POD_UID").context("runner requires CIBOX_POD_UID")?)
    } else {
        None
    };
    group(Path::new(BASE), root, pod.as_deref())
}

fn retiring(base: &Path, paths: &[String]) -> Result<bool> {
    for path in paths {
        ensure!(valid_path(path), "invalid root path");
        let file = base
            .join("retiring")
            .join(Path::new(path).file_name().unwrap());
        match file.try_exists() {
            Ok(true) => return Ok(true),
            Ok(false) => (),
            Err(error) => return Err(error.into()),
        }
    }
    Ok(false)
}

pub fn guard(base: &Path, paths: &[String], wait: bool) -> Result<Lock> {
    loop {
        let gate = Lock::acquire(&base.join("online-roots.lock"), true)?;
        if !retiring(base, paths)? {
            return Ok(gate);
        }
        drop(gate);
        ensure!(
            wait,
            "publication intersects online GC retirement; retry after this epoch"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

pub fn pin(base: &Path, root: &Path, name: &str, paths: &[String]) -> Result<()> {
    ensure!(valid_group(name), "invalid root group");
    let _gate = guard(base, paths, true)?;
    let directory = root.join(ROOTS).join(name);
    ensure!(directory.is_dir(), "root group disappeared");
    for path in paths {
        let destination = directory.join(Path::new(path).file_name().unwrap());
        match symlink(path, &destination) {
            Ok(()) => (),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                ensure!(
                    fs::read_link(destination)? == Path::new(path),
                    "root target changed"
                );
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

pub fn pin_runtime(paths: &[String]) -> Result<Value> {
    let name = ROOT_GROUP
        .get()
        .context("native root group not initialized")?;
    let mut pinned = PINNED
        .lock()
        .map_err(|_| anyhow::anyhow!("root cache poisoned"))?;
    let missing: Vec<_> = paths
        .iter()
        .filter(|p| !pinned.contains(*p))
        .cloned()
        .collect();
    if !missing.is_empty() {
        pin(
            Path::new("/run/distributed-nix"),
            Path::new("/"),
            name,
            &missing,
        )?;
        pinned.extend(missing);
    }
    Ok(Value::Null)
}

impl Node {
    pub fn prune_client_roots(&self, pods: &BTreeSet<String>) -> Result<()> {
        let _gate = Lock::acquire(&self.base.join("online-roots.lock"), false)?;
        let directory = self.base.join("client-roots");
        fs::create_dir_all(&directory)?;
        for entry in fs::read_dir(&directory)? {
            let entry = entry?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| anyhow::anyhow!("root group name"))?;
            ensure!(valid_group(&name), "invalid root group directory");
            let metadata = entry.path().join("info.json");
            if metadata.exists() {
                let info = read_json(&metadata)?;
                ensure!(info["version"] == 1, "unsupported root group version");
                if info["pod"].as_str().is_some_and(|pod| pods.contains(pod)) {
                    continue;
                }
            }
            let lease = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(entry.path().join("lease"))?;
            if unsafe { libc::flock(lease.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
                let error = std::io::Error::last_os_error();
                if error.kind() == std::io::ErrorKind::WouldBlock {
                    continue;
                }
                return Err(error.into());
            }
            let roots = self.root.join(ROOTS).join(&name);
            if roots.exists() {
                fs::remove_dir_all(roots)?;
            }
            fs::remove_dir_all(entry.path())?;
        }
        Ok(())
    }

    pub fn client_roots(&self) -> Result<BTreeSet<String>> {
        let mut roots = BTreeSet::new();
        let directory = self.root.join(ROOTS);
        if !directory.exists() {
            return Ok(roots);
        }
        for group in fs::read_dir(directory)? {
            for entry in fs::read_dir(group?.path())? {
                let target = fs::read_link(entry?.path())?
                    .to_str()
                    .context("root path")?
                    .to_owned();
                ensure!(valid_path(&target), "invalid client root");
                roots.insert(target);
            }
        }
        Ok(roots)
    }
}

use crate::{
    admissions::Admissions,
    gc::{Plan, Snapshot, remove_file, rename, valid_id},
    manifest::{Manifest, realisation_path},
    node::{Kind, journals, mountpoint, physical, present},
    util::{failpoint, run},
};
use std::process::Command;

pub fn candidates(plan: &Plan) -> BTreeSet<String> {
    plan.workers
        .iter()
        .chain([&plan.origin])
        .flat_map(|s| s.iter().cloned())
        .collect()
}

impl Node {
    pub(crate) fn online_restore_checkpoint(&self) -> Result<()> {
        let active = self.base.join("online-gc.json");
        if active.exists() {
            let state = read_json(&active)?;
            let epoch = self.gc_epoch(state["id"].as_str().context("online epoch")?)?;
            let admissions = self.base.join("admissions");
            if epoch.join("old-admissions").exists() && !admissions.exists() {
                rename(&epoch.join("new-admissions"), &admissions)?;
            }
        }
        Ok(())
    }

    pub fn online_preflight(
        &self,
        id: &str,
        pods: &BTreeSet<String>,
        all_pods: &BTreeSet<String>,
    ) -> Result<Value> {
        valid_id(id)?;
        ensure!(!self.gc_active().exists(), "offline GC is active");
        ensure!(
            self.base.join("ready").exists(),
            "store recovery incomplete"
        );
        let active = self.base.join("online-gc.json");
        if active.exists() {
            ensure!(read_json(&active)?["id"] == id, "different online epoch");
        }
        for pod in pods {
            ensure!(valid_group(pod), "invalid pod UID");
            let info = self
                .base
                .join("client-roots")
                .join(format!("pod-{pod}"))
                .join("info.json");
            ensure!(
                info.exists() && read_json(&info)?["version"] == 1,
                "ARC pod {pod} has no online GC roots yet"
            );
        }
        self.prune_client_roots(all_pods)?;
        let path = std::ffi::CString::new(self.root.as_os_str().as_encoded_bytes())?;
        let mut usage: libc::statvfs = unsafe { std::mem::zeroed() };
        ensure!(
            unsafe { libc::statvfs(path.as_ptr(), &mut usage) } == 0,
            "store filesystem usage"
        );
        Ok(json!({"ready":true,"blocks":usage.f_blocks,"available":usage.f_bavail}))
    }

    pub fn online_snapshot(&self, origin: bool) -> Result<Snapshot> {
        let _admit = Lock::acquire(&self.base.join("admit.lock"), false)?;
        self.online_restore_checkpoint()?;
        let root = if origin { &self.origin } else { &self.root };
        let mut snapshot: Snapshot =
            serde_json::from_value(self.gc_native(root, "online-snapshot", &Value::Null)?)?;
        if !origin {
            snapshot.live.extend(self.client_roots()?);
            snapshot
                .live
                .extend(serde_json::from_value::<Vec<String>>(self.outbox()?)?);
            let ca = self.ca_outbox()?;
            if !ca.is_null() {
                snapshot
                    .live
                    .extend(Manifest::parse(ca["manifest"].clone())?.paths.into_keys());
            }
            Admissions::open(&self.base.join("admissions"))?.for_each(|journal| {
                if matches!(journal.status, crate::node::Status::Pending) {
                    snapshot.live.extend(journal.manifest.paths.keys().cloned());
                }
                for value in journal.manifest.realisations.values() {
                    if let Some(edges) = snapshot
                        .graph
                        .get_mut(&realisation_path(&value["outPath"])?)
                    {
                        for dependency in value["dependentRealisations"]
                            .as_object()
                            .context("dependencies")?
                            .values()
                        {
                            edges.insert(realisation_path(dependency)?);
                        }
                    }
                }
                Ok(())
            })?;
        } else {
            for file in journals(&self.base.join("pending-publications"))? {
                if !self
                    .origin
                    .join(".distributed-nix-publications")
                    .join(file.file_name().unwrap())
                    .exists()
                {
                    snapshot
                        .live
                        .extend(Manifest::read(&file)?.paths.into_keys());
                }
            }
        }
        Ok(snapshot)
    }

    pub fn online_prepare(&self, plan: &Plan) -> Result<Value> {
        plan.validate()?;
        let _gate = Lock::acquire(&self.base.join("online-roots.lock"), false)?;
        ensure!(!self.gc_active().exists(), "offline GC active");
        let epoch = self.gc_epoch(&plan.id)?;
        let active = self.base.join("online-gc.json");
        if active.exists() {
            ensure!(
                read_json(&active)?["id"] == plan.id,
                "different online GC epoch"
            );
        }
        let saved = epoch.join("candidates.json");
        if saved.exists() {
            ensure!(
                read_json(&saved)? == serde_json::to_value(plan)?,
                "candidates changed"
            );
        } else {
            durable(&saved, plan)?;
        }
        durable(&active, &json!({"id":plan.id}))?;
        let directory = epoch.join("retiring");
        fs::create_dir_all(&directory)?;
        for path in candidates(plan) {
            let file = directory.join(Path::new(&path).file_name().unwrap());
            OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(false)
                .open(file)?;
        }
        syncdir(&directory)?;
        let target = PathBuf::from("gc").join(&plan.id).join("retiring");
        let link = self.base.join("retiring");
        if present(&link) {
            ensure!(
                fs::read_link(&link)? == target,
                "different retirement barrier"
            );
        } else {
            symlink(target, &link)?;
            syncdir(&self.base)?;
        }
        failpoint("online-after-prepare");
        Ok(json!({"prepared":true}))
    }

    fn online_require(&self, id: &str) -> Result<PathBuf> {
        valid_id(id)?;
        ensure!(
            read_json(&self.base.join("online-gc.json"))?["id"] == id,
            "online epoch mismatch"
        );
        Ok(self.gc_epoch(id)?)
    }

    pub fn online_plan(&self, plan: &Plan) -> Result<Value> {
        plan.validate()?;
        let epoch = self.online_require(&plan.id)?;
        let original: Plan = serde_json::from_value(read_json(&epoch.join("candidates.json"))?)?;
        for (final_set, initial) in plan
            .workers
            .iter()
            .chain([&plan.origin])
            .zip(original.workers.iter().chain([&original.origin]))
        {
            ensure!(
                final_set.is_subset(initial),
                "deletion plan exceeds fenced candidates"
            );
        }
        let file = epoch.join("online-plan.json");
        if file.exists() {
            ensure!(
                read_json(&file)? == serde_json::to_value(plan)?,
                "immutable plan changed"
            );
        } else {
            durable(&file, plan)?;
        }
        // Keep the over-approximation fenced until completion. Removing a marker
        // before every peer stores the final plan would complicate safe retries.
        Ok(json!({"planned":true}))
    }

    pub fn online_sweep(&self, id: &str, node: Option<usize>) -> Result<Value> {
        let epoch = self.online_require(id)?;
        let plan: Plan = serde_json::from_value(read_json(&epoch.join("online-plan.json"))?)?;
        plan.validate()?;
        let role = if node.is_some() { "worker" } else { "origin" };
        let ack = epoch.join(format!("online-{role}-swept.json"));
        if ack.exists() {
            return read_json(&ack);
        }
        let _admit = Lock::acquire(&self.base.join("admit.lock"), false)?;
        let (root, dead) = if let Some(index) = node {
            ensure!(index < 3, "worker index");
            (&self.root, &plan.workers[index])
        } else {
            let master = read_json(&self.base.join("online-master.json"))?;
            ensure!(
                master["id"] == id && master["plan"] == serde_json::to_value(&plan)?,
                "origin lacks coordinator plan"
            );
            ensure!(
                master["acks"]
                    .as_array()
                    .is_some_and(|a| a.len() == 3 && a.iter().all(|v| v["id"] == id)),
                "all three worker acknowledgements required"
            );
            (&self.origin, &plan.origin)
        };
        if node.is_some() {
            self.gc_checkpoint_filter(id, |path| !dead.contains(path))?;
            let old = Admissions::open(&epoch.join("old-admissions"))?;
            let kinds = old.known(dead)?;
            for path in dead {
                let destination = physical(root, path);
                if present(&destination) && mountpoint(&destination)? {
                    ensure!(
                        matches!(kinds.get(path), Some(Kind::MountDir | Kind::MountFile)),
                        "unknown mount: {path}"
                    );
                    run(Command::new("umount").arg(destination))?;
                }
            }
            syncdir(&root.join("nix/store"))?;
            failpoint("online-after-unmount");
        } else {
            for file in journals(&self.origin.join(".distributed-nix-publications"))? {
                if Manifest::read(&file)?
                    .paths
                    .keys()
                    .any(|p| dead.contains(p))
                {
                    remove_file(
                        &self
                            .base
                            .join("pending-publications")
                            .join(file.file_name().unwrap()),
                    )?;
                    remove_file(&file)?;
                }
            }
        }
        let safety = root.join("nix/var/nix/gcroots/distributed-nix");
        if safety.exists() {
            for file in fs::read_dir(&safety)? {
                let file = file?.path();
                let target = fs::read_link(&file)?;
                if dead.contains(target.to_str().context("safety root")?) {
                    fs::remove_file(file)?;
                }
            }
            syncdir(&safety)?;
        }
        let result = self.gc_native(root, "delete", &serde_json::to_value(dead)?)?;
        run(Command::new("sync").arg("-f").arg(root))?;
        failpoint("online-after-delete");
        let row = json!({"id":id,"role":role,"result":result});
        durable(&ack, &row)?;
        Ok(row)
    }

    pub fn online_finish(&self, id: &str) -> Result<Value> {
        let _gate = Lock::acquire(&self.base.join("online-roots.lock"), false)?;
        let epoch = self.gc_epoch(id)?;
        let complete = epoch.join("online-finished.json");
        if complete.exists() && !self.base.join("online-gc.json").exists() {
            return read_json(&complete);
        }
        self.online_require(id)?;
        ensure!(
            epoch.join("online-worker-swept.json").exists(),
            "worker sweep incomplete"
        );
        if self.origin.join("nix/var/nix/db/db.sqlite").exists() {
            ensure!(
                epoch.join("online-origin-swept.json").exists(),
                "origin sweep incomplete"
            );
        }
        let result = json!({"id":id,"finished":true});
        durable(&complete, &result)?;
        remove_file(&self.base.join("retiring"))?;
        remove_file(&self.base.join("online-gc.json"))?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const DEAD: &str = "/nix/store/11111111111111111111111111111111-dead";
    const LIVE: &str = "/nix/store/22222222222222222222222222222222-live";
    fn node(base: &Path) -> Node {
        Node {
            base: base.into(),
            root: base.join("root"),
            origin: base.join("origin"),
            lower: base.join("lower"),
        }
    }
    fn plan() -> Plan {
        Plan {
            id: "00000000000000000000000000000001".into(),
            keep: BTreeSet::from([LIVE.into()]),
            workers: vec![BTreeSet::from([DEAD.into()]); 3],
            origin: BTreeSet::from([DEAD.into()]),
        }
    }

    #[test]
    fn pod_roots_outlive_daemon_connections_until_cri_teardown() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let node = node(temp.path());
        let group = group(&node.base, &node.root, Some("pod-uid"))?;
        pin(&node.base, &node.root, &group.name, &[LIVE.into()])?;
        // A held lease protects processes even when the CRI list no longer has the pod.
        node.prune_client_roots(&BTreeSet::new())?;
        ensure!(node.client_roots()?.contains(LIVE));
        drop(group);
        node.prune_client_roots(&BTreeSet::from(["pod-uid".into()]))?;
        ensure!(node.client_roots()?.contains(LIVE));
        let end = std::time::Instant::now() + Duration::from_secs(3);
        loop {
            node.prune_client_roots(&BTreeSet::new())?;
            if node.client_roots()?.is_empty() {
                break;
            }
            ensure!(std::time::Instant::now() < end, "released lease still held");
            std::thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    }

    #[test]
    fn retirement_blocks_only_selected_paths_and_survives_restart() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let node = node(temp.path());
        let group = group(&node.base, &node.root, Some("one"))?;
        let plan = plan();
        node.online_prepare(&plan)?;
        node.online_prepare(&plan)?;
        let restarted = node.clone();
        ensure!(guard(&restarted.base, &[DEAD.into()], false).is_err());
        pin(&node.base, &node.root, &group.name, &[LIVE.into()])?;
        let (send, receive) = std::sync::mpsc::channel();
        let base = node.base.clone();
        let root = node.root.clone();
        let name = group.name.clone();
        let child = std::thread::spawn(move || {
            send.send(pin(&base, &root, &name, &[DEAD.into()])).unwrap()
        });
        ensure!(receive.recv_timeout(Duration::from_millis(100)).is_err());
        ensure!(node.online_finish(&plan.id).is_err());
        node.online_plan(&plan)?;
        let mut changed = plan.clone();
        changed.origin.clear();
        ensure!(node.online_plan(&changed).is_err());
        durable(
            &node.gc_epoch(&plan.id)?.join("online-worker-swept.json"),
            &json!({"id":plan.id}),
        )?;
        restarted.online_finish(&plan.id)?;
        receive.recv_timeout(Duration::from_secs(2))??;
        child.join().unwrap();
        ensure!(node.client_roots()?.contains(DEAD));
        Ok(())
    }

    #[test]
    fn late_roots_preserve_transitive_candidates_across_nodes() -> Result<()> {
        let plan = plan();
        let mut snapshots = vec![
            Snapshot {
                live: BTreeSet::new(),
                graph: std::collections::BTreeMap::from([
                    (LIVE.into(), BTreeSet::from([DEAD.into()])),
                    (DEAD.into(), BTreeSet::new())
                ])
            };
            4
        ];
        let initial = crate::gc::plan(&plan.id, &snapshots)?;
        ensure!(initial.origin.contains(DEAD));
        snapshots[2].live.insert(LIVE.into());
        let final_plan = crate::gc::plan(&plan.id, &snapshots)?;
        ensure!(final_plan.origin.is_empty() && final_plan.workers.iter().all(BTreeSet::is_empty));
        Ok(())
    }

    #[test]
    fn online_checkpoint_retains_admissions_created_after_mark() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let node = node(temp.path());
        let info = json!({"narHash":"sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=","narSize":1,"references":[],"ca":null,"signatures":[],"ultimate":true});
        let manifest = Manifest {
            version: 1,
            roots: vec![LIVE.into(), DEAD.into()],
            paths: std::collections::BTreeMap::from([
                (LIVE.into(), info.clone()),
                (DEAD.into(), info),
            ]),
            realisations: Default::default(),
        };
        Admissions::open(&node.base.join("admissions"))?.begin(&crate::node::Journal {
            manifest,
            plan: std::collections::BTreeMap::from([
                (LIVE.into(), Kind::Local),
                (DEAD.into(), Kind::Copy),
            ]),
            status: crate::node::Status::Committed,
        })?;
        node.gc_checkpoint_filter(&plan().id, |path| path != DEAD)?;
        let db = Admissions::open(&node.base.join("admissions"))?;
        ensure!(
            db.known(&[LIVE.into(), DEAD.into()])?
                == std::collections::BTreeMap::from([(LIVE.into(), Kind::Local)])
        );
        Ok(())
    }
}
