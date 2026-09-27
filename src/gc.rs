//! Stop-the-world, resumable cluster GC. No node may expire another node's vote.
use crate::{
    admissions::Admissions,
    manifest::{Manifest, realisation_path, valid_path},
    node::{Journal, Kind, Node, Status, journals, mountpoint, physical, present, roots_named},
    util::*,
};
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub live: BTreeSet<String>,
    pub graph: BTreeMap<String, BTreeSet<String>>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    pub id: String,
    pub keep: BTreeSet<String>,
    pub workers: Vec<BTreeSet<String>>,
    pub origin: BTreeSet<String>,
}
impl Plan {
    pub fn validate(&self) -> Result<()> {
        valid_id(&self.id)?;
        ensure!(self.workers.len() == 3, "GC requires exactly three workers");
        for set in self.workers.iter().chain([&self.origin]) {
            ensure!(
                set.is_disjoint(&self.keep),
                "GC plan deletes a retained path"
            );
            ensure!(
                set.iter().all(|p| valid_path(p)),
                "invalid GC deletion path"
            );
        }
        ensure!(
            self.keep.iter().all(|p| valid_path(p)),
            "invalid GC retained path"
        );
        Ok(())
    }
}
pub fn plan(id: &str, snapshots: &[Snapshot]) -> Result<Plan> {
    ensure!(snapshots.len() == 4, "missing GC snapshot");
    let mut graph: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut keep = BTreeSet::new();
    for snap in snapshots {
        ensure!(
            snap.live.iter().all(|p| valid_path(p)),
            "invalid GC live path"
        );
        keep.extend(snap.live.iter().cloned());
        for (p, refs) in &snap.graph {
            ensure!(
                valid_path(p) && refs.iter().all(|r| valid_path(r)),
                "invalid GC graph"
            );
            graph
                .entry(p.clone())
                .or_default()
                .extend(refs.iter().cloned());
        }
    }
    let mut pending: Vec<_> = keep.iter().cloned().collect();
    while let Some(p) = pending.pop() {
        if let Some(refs) = graph.get(&p) {
            for r in refs {
                if keep.insert(r.clone()) {
                    pending.push(r.clone());
                }
            }
        }
    }
    let dead: Vec<BTreeSet<String>> = snapshots
        .iter()
        .map(|s| {
            s.graph
                .keys()
                .filter(|p| !keep.contains(*p))
                .cloned()
                .collect()
        })
        .collect();
    let p = Plan {
        id: id.into(),
        keep,
        workers: dead[..3].to_vec(),
        origin: dead[3].clone(),
    };
    p.validate()?;
    Ok(p)
}
pub(crate) fn valid_id(id: &str) -> Result<()> {
    ensure!(
        id.len() == 32 && id.bytes().all(|c| c.is_ascii_hexdigit()),
        "invalid GC epoch"
    );
    Ok(())
}
pub(crate) fn remove_file(p: &Path) -> Result<()> {
    if present(p) {
        fs::remove_file(p)?;
        syncdir(p.parent().context("parent")?)?;
    }
    Ok(())
}
pub(crate) fn rename(from: &Path, to: &Path) -> Result<()> {
    fs::rename(from, to)?;
    syncdir(from.parent().unwrap())?;
    syncdir(to.parent().unwrap())
}
impl Node {
    pub(crate) fn gc_native(&self, root: &Path, operation: &str, request: &Value) -> Result<Value> {
        // Native roots can point to /work/result inside the canonical view.
        // Resolving those links from the VM host namespace would lose live roots.
        // Keep the environment's actual mount identities too. Cloning and
        // pivoting a mount namespace changes how /proc/PID/fd symlinks resolve:
        // ordinary chroot clients can appear under /srv/... instead of /nix.
        let mut cmd = Command::new(std::env::current_exe()?);
        cmd.arg("native-gc").arg(root).arg(operation);
        Ok(serde_json::from_slice(
            &input(&mut cmd, &serde_json::to_vec(request)?)?.stdout,
        )?)
    }
    pub(crate) fn gc_active(&self) -> PathBuf {
        self.base.join("gc-active.json")
    }
    pub(crate) fn gc_epoch(&self, id: &str) -> Result<PathBuf> {
        valid_id(id)?;
        Ok(self.base.join("gc").join(id))
    }
    fn gc_state(&self, id: &str) -> Result<Value> {
        valid_id(id)?;
        let s = read_json(&self.gc_active())?;
        ensure!(s["id"] == id, "different GC epoch is active");
        Ok(s)
    }
    fn gc_require(&self, id: &str) -> Result<()> {
        self.gc_state(id)?;
        Ok(())
    }
    fn gc_store_plan(&self, id: &str, p: &Plan) -> Result<()> {
        self.gc_require(id)?;
        p.validate()?;
        ensure!(p.id == id, "GC plan epoch mismatch");
        let file = self.gc_epoch(id)?.join("plan.json");
        if file.exists() {
            ensure!(
                serde_json::from_value::<Plan>(read_json(&file)?)? == *p,
                "GC plan cannot change after preparation"
            );
        } else {
            durable(&file, p)?;
        }
        Ok(())
    }
    fn gc_load_plan(&self, id: &str) -> Result<Plan> {
        self.gc_require(id)?;
        let p: Plan = serde_json::from_value(read_json(&self.gc_epoch(id)?.join("plan.json"))?)?;
        p.validate()?;
        ensure!(p.id == id, "GC epoch mismatch");
        Ok(p)
    }
    pub(crate) fn gc_master(&self, action: &str, dry: bool) -> Result<Value> {
        let file = self.base.join("gc-master.json");
        match action {
            "status" => Ok(if file.exists() {
                read_json(&file)?
            } else {
                Value::Null
            }),
            "begin" => {
                if file.exists() {
                    let s = read_json(&file)?;
                    ensure!(
                        s["dry_run"] == dry,
                        "resume GC with its original dry-run setting"
                    );
                    return Ok(s);
                }
                let id = format!(
                    "{:032x}",
                    SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
                );
                let s = json!({"id":id,"phase":"freeze","dry_run":dry});
                durable(&file, &s)?;
                Ok(s)
            }
            "plan" => {
                let mut s = read_json(&file)?;
                let p: Plan = serde_json::from_slice(&read_stdin()?)?;
                p.validate()?;
                ensure!(s["id"] == p.id, "master epoch mismatch");
                if let Some(old) = s.get("plan") {
                    ensure!(*old == serde_json::to_value(&p)?, "master plan changed");
                }
                s["plan"] = serde_json::to_value(p)?;
                s["phase"] = json!("sweep");
                durable(&file, &s)?;
                Ok(s)
            }
            "finish" => {
                let mut s = read_json(&file)?;
                s["phase"] = json!("finish");
                durable(&file, &s)?;
                Ok(s)
            }
            "done" => {
                let s = read_json(&file)?;
                ensure!(s["phase"] == "finish", "GC not ready to finish");
                let id = s["id"].as_str().context("epoch")?;
                durable(&self.gc_epoch(id)?.join("complete.json"), &s)?;
                remove_file(&file)?;
                Ok(s)
            }
            _ => bail!("unknown GC master operation"),
        }
    }
    pub(crate) fn gc_freeze(&self, id: &str) -> Result<Value> {
        valid_id(id)?;
        let _gate = Lock::acquire(&self.base.join("maintenance.lock"), false)?;
        ensure!(
            !self.base.join("online-gc.json").exists(),
            "resume online GC first"
        );
        if self.gc_active().exists() {
            self.gc_require(id)?;
        } else {
            durable(&self.gc_active(), &json!({"id":id,"stage":"frozen"}))?;
        }
        remove_file(&self.base.join("ready"))?;
        // Native connection workers have drained through maintenance.lock.
        // Keep the listener alive: the GC caller is waiting on its own socket
        // with its lease released, and must receive the native protocol reply.
        failpoint("gc-after-freeze");
        Ok(json!({"id":id,"frozen":true}))
    }
    fn gc_root_backup(root: &Path) -> PathBuf {
        root.join("nix/var/nix/distributed-nix-gc-saved-roots")
    }
    fn gc_restore_roots(root: &Path) -> Result<()> {
        let saved = Self::gc_root_backup(root);
        let active = root.join("nix/var/nix/gcroots/distributed-nix");
        if saved.exists() {
            ensure!(!active.exists(), "both saved and active safety roots exist");
            rename(&saved, &active)?;
        }
        Ok(())
    }
    pub(crate) fn gc_snapshot(&self, id: &str, origin: bool) -> Result<Value> {
        let _gate = Lock::acquire(&self.base.join("maintenance.lock"), false)?;
        self.gc_require(id)?;
        ensure!(
            !self.gc_epoch(id)?.join("plan.json").exists(),
            "cannot resnapshot after sweep planning"
        );
        let root = if origin { &self.origin } else { &self.root };
        Self::gc_restore_roots(root)?;
        // Finish old interrupted admissions while the collection is still intact.
        if !origin {
            self.prune_client_roots(&BTreeSet::new())?;
            self.restore_admissions()?;
            self.prune_uncommitted_outbox()?;
        } else {
            ensure!(
                self.origin.join("nix/var/nix/db/db.sqlite").exists(),
                "origin filesystem is not restored"
            );
            self.prepare(true)?;
        }
        let active = root.join("nix/var/nix/gcroots/distributed-nix");
        let saved = Self::gc_root_backup(root);
        if active.exists() {
            rename(&active, &saved)?;
        }
        failpoint("gc-after-root-backup");
        let result = self.gc_native(root, "snapshot", &Value::Null);
        Self::gc_restore_roots(root)?;
        let mut snapshot: Snapshot = serde_json::from_value(result?)?;
        if !origin {
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
                for value in journal.manifest.realisations.values() {
                    let path = realisation_path(&value["outPath"])?;
                    if let Some(edges) = snapshot.graph.get_mut(&path) {
                        for dependency in value["dependentRealisations"]
                            .as_object()
                            .context("realisation dependencies")?
                            .values()
                        {
                            edges.insert(realisation_path(dependency)?);
                        }
                    }
                }
                Ok(())
            })?;
        }
        if origin {
            // Interrupted pre-commit copies remain protected until retried.
            for f in journals(&self.base.join("pending-publications"))? {
                if !self
                    .origin
                    .join(".distributed-nix-publications")
                    .join(f.file_name().unwrap())
                    .exists()
                {
                    snapshot.live.extend(Manifest::read(&f)?.paths.into_keys());
                }
            }
        }
        Ok(serde_json::to_value(snapshot)?)
    }
    fn gc_checkpoint(&self, id: &str, keep: &BTreeSet<String>) -> Result<()> {
        self.gc_checkpoint_filter(id, |path| keep.contains(path))
    }
    pub(crate) fn gc_checkpoint_filter(
        &self,
        id: &str,
        retain: impl Fn(&str) -> bool,
    ) -> Result<()> {
        let epoch = self.gc_epoch(id)?;
        let old = epoch.join("old-admissions");
        let next = epoch.join("new-admissions");
        let active = self.base.join("admissions");
        if !old.exists() {
            let mut paths = BTreeMap::new();
            let mut realisations = BTreeMap::new();
            let mut kinds = BTreeMap::new();
            Admissions::open(&active)?.for_each(|j| {
                for (id, value) in &j.manifest.realisations {
                    if retain(&realisation_path(&value["outPath"])?) {
                        realisations.insert(id.clone(), value.clone());
                    }
                }
                for (p, k) in j.plan {
                    if retain(&p) {
                        paths.insert(p.clone(), j.manifest.paths[&p].clone());
                        if let Some(previous) = kinds.insert(p, k) {
                            ensure!(previous == k, "inconsistent admission plans");
                        }
                    }
                }
                Ok(())
            })?;
            // Replace abandoned staging data only before the first rename.
            if next.exists() {
                fs::remove_dir_all(&next)?;
            }
            let mut checkpoint = Admissions::open(&next)?;
            if !paths.is_empty() {
                let m = Manifest {
                    version: 1,
                    roots: paths.keys().cloned().collect(),
                    paths,
                    realisations,
                };
                m.validate()?;
                let j = Journal {
                    manifest: m.clone(),
                    plan: kinds,
                    status: Status::Committed,
                };
                checkpoint.begin(&j)?;
            }
            drop(checkpoint);
            syncdir(&next)?;
            syncdir(&epoch)?;
            rename(&active, &old)?;
            failpoint("gc-after-journal-rename");
        }
        if !active.exists() {
            rename(&next, &active)?;
        }
        Ok(())
    }
    fn gc_reset_safety_roots(root: &Path, keep: &BTreeSet<String>) -> Result<()> {
        Self::gc_restore_roots(root)?;
        let dir = root.join("nix/var/nix/gcroots/distributed-nix");
        if dir.exists() {
            for f in fs::read_dir(&dir)? {
                let p = f?.path();
                let target = fs::read_link(&p)?;
                if !keep.contains(target.to_str().context("root target")?) {
                    fs::remove_file(p)?;
                }
            }
            syncdir(&dir)?;
        }
        // Even a path needed exclusively by another node is protected locally.
        roots_named(root, keep, "distributed-nix")
    }
    pub(crate) fn gc_restore_derivation(&self, path: &str, kind: Option<Kind>) -> Result<()> {
        // Nix's keep-derivations traversal reads .drv contents after unmount.
        // Retry even when the previous process died between unmount and copy.
        if kind == Some(Kind::MountFile) && path.ends_with(".drv") {
            let destination = physical(&self.root, path);
            ensure!(!mountpoint(&destination)?, "derivation mount still active");
            fs::copy(physical(&self.lower, path), &destination)?;
            std::fs::File::open(&destination)?.sync_all()?;
        }
        Ok(())
    }
    pub(crate) fn gc_sweep(&self, id: &str, node: Option<usize>) -> Result<Value> {
        let _gate = Lock::acquire(&self.base.join("maintenance.lock"), false)?;
        let p = self.gc_load_plan(id)?;
        let epoch = self.gc_epoch(id)?;
        let role = if node.is_some() { "worker" } else { "origin" };
        let ack = epoch.join(format!("{role}-swept.json"));
        if ack.exists() {
            return read_json(&ack);
        }
        let (root, dead) = if let Some(n) = node {
            ensure!(n < 3, "invalid worker");
            (&self.root, &p.workers[n])
        } else {
            (&self.origin, &p.origin)
        };
        if node.is_some() {
            self.gc_checkpoint(id, &p.keep)?;
            // A paused worker may have rebooted since planning. Restore only
            // the retained checkpoint, never the retired admission directory.
            self.restore_admissions()?;
            let mut kinds = BTreeMap::new();
            Admissions::open(&epoch.join("old-admissions"))?.for_each(|journal| {
                kinds.extend(journal.plan);
                Ok(())
            })?;
            for path in dead {
                let dst = physical(root, path);
                if present(&dst) && mountpoint(&dst)? {
                    ensure!(
                        matches!(kinds.get(path), Some(Kind::MountDir | Kind::MountFile)),
                        "refusing unknown mount: {path}"
                    );
                    // Never lazy-unmount: busy mounts prevent the origin deletion phase.
                    run(Command::new("umount").arg(&dst))?;
                }
                self.gc_restore_derivation(path, kinds.get(path).copied())?;
            }
            syncdir(&root.join("nix/store"))?;
            failpoint("gc-after-unmount");
        } else {
            for file in journals(&self.origin.join(".distributed-nix-publications"))? {
                let m = Manifest::read(&file)?;
                if m.paths.keys().any(|q| dead.contains(q)) {
                    remove_file(
                        &self
                            .base
                            .join("pending-publications")
                            .join(file.file_name().unwrap()),
                    )?;
                    remove_file(&file)?;
                }
            }
            failpoint("gc-after-publication-retire");
        }
        Self::gc_reset_safety_roots(root, &p.keep)?;
        let result = self.gc_native(root, "delete", &serde_json::to_value(dead)?)?;
        run(Command::new("sync").arg("-f").arg(root))?;
        failpoint(if node.is_some() {
            "gc-after-worker-delete"
        } else {
            "gc-after-origin-delete"
        });
        let row = json!({"id":id,"role":role,"result":result});
        durable(&ack, &row)?;
        Ok(row)
    }
    pub(crate) fn gc_finish(&self, id: &str) -> Result<Value> {
        let _gate = Lock::acquire(&self.base.join("maintenance.lock"), false)?;
        let marker = self.gc_epoch(id)?.join("finished.json");
        if marker.exists() && !self.gc_active().exists() {
            return read_json(&marker);
        }
        self.gc_require(id)?;
        // Covers an interrupted snapshot, including a preview-only run.
        Self::gc_restore_roots(&self.root)?;
        if self.origin.join("nix/var/nix/db/db.sqlite").exists() {
            Self::gc_restore_roots(&self.origin)?;
        }
        durable(&self.gc_active(), &json!({"id":id,"stage":"finishing"}))?;
        // Keep retained mounts in place for running shells. The lower NFS
        // mount uses fresh name lookups, so deleted names can be recreated.
        self.recover()?;
        let r = json!({"id":id,"resumed":true});
        durable(&marker, &r)?;
        remove_file(&self.gc_active())?;
        Ok(r)
    }
    pub(crate) fn gc_dispatch(&self, args: &[String]) -> Result<Value> {
        let op = arg(args, 0)?;
        match op {
            "gc-report" => {
                let epoch = self.gc_epoch(arg(args, 1)?)?;
                let mut bytes = 0u64;
                for role in ["worker", "origin"] {
                    let file = epoch.join(format!("{role}-swept.json"));
                    if file.exists() {
                        bytes += read_json(&file)?["result"]["bytes_freed"]
                            .as_u64()
                            .context("GC bytes")?;
                    }
                }
                Ok(json!({"bytes_freed":bytes}))
            }
            "gc-master" => self.gc_master(arg(args, 1)?, args.get(2).is_some_and(|s| s == "dry")),
            "gc-preflight" | "gc-preflight-maintenance" => {
                let _gate = Lock::acquire(&self.base.join("maintenance.lock"), true)?;
                ensure!(
                    args[0] == "gc-preflight-maintenance"
                        || !self.base.join("gc-maintenance-only").exists(),
                    "GC requires the administrator coordinator; run cibox-maintenance gc on the origin"
                );
                Ok(
                    json!({"active":if self.gc_active().exists(){read_json(&self.gc_active())?}else{Value::Null}}),
                )
            }
            "gc-freeze" => self.gc_freeze(arg(args, 1)?),
            "gc-snapshot" => {
                self.gc_snapshot(arg(args, 1)?, args.get(2).is_some_and(|s| s == "origin"))
            }
            "gc-plan" => {
                let _gate = Lock::acquire(&self.base.join("maintenance.lock"), false)?;
                let p: Plan = serde_json::from_slice(&read_stdin()?)?;
                self.gc_store_plan(arg(args, 1)?, &p)?;
                Ok(json!({"planned":true}))
            }
            "gc-sweep" => self.gc_sweep(
                arg(args, 1)?,
                if arg(args, 2)? == "origin" {
                    None
                } else {
                    Some(arg(args, 2)?.parse()?)
                },
            ),
            "gc-finish" => self.gc_finish(arg(args, 1)?),
            _ => bail!("unknown GC operation"),
        }
    }
}

#[cfg(test)]
mod checkpoint_tests {
    use super::*;
    use std::os::unix::process::ExitStatusExt;

    const KEEP: &str = "/nix/store/00000000000000000000000000000000-keep";
    const DEAD: &str = "/nix/store/11111111111111111111111111111111-dead";
    const EPOCH: &str = "11111111111111111111111111111111";

    fn node(base: &Path) -> Node {
        Node {
            base: base.into(),
            root: base.join("root"),
            origin: base.join("origin"),
            lower: base.join("lower"),
        }
    }

    #[test]
    fn collection_restores_unmounted_derivations_before_native_liveness_checks() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let node = node(temp.path());
        let expression = format!(
            "builtins.derivation {{ name=\"large-gc\"; system=\"x86_64-linux\"; builder=\"/bin/sh\"; padding=\"{}\"; }}",
            "x".repeat(70000)
        );
        let result = output(
            Command::new("nix-instantiate")
                .args(["--option", "build-users-group", "", "--store"])
                .arg(&node.lower)
                .args(["--expr", &expression]),
        )?;
        let path = String::from_utf8(result.stdout)?.trim().to_owned();
        let manifest = crate::native::dump(&node.lower, std::slice::from_ref(&path))?;
        let destination = physical(&node.root, &path);
        fs::create_dir_all(destination.parent().unwrap())?;
        fs::copy(physical(&node.lower, &path), &destination)?;
        crate::native::register(&node.root, &manifest)?;
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&destination, fs::Permissions::from_mode(0o644))?;
        fs::write(&destination, "")?;
        let dead = BTreeSet::from([path.clone()]);
        ensure!(crate::native::gc_delete(&node.root, &dead).is_err());
        node.gc_restore_derivation(&path, Some(Kind::MountFile))?;
        ensure!(fs::metadata(&destination)?.len() > 65536);
        crate::native::gc_delete(&node.root, &dead)?;
        ensure!(!destination.exists());
        Ok(())
    }

    #[test]
    fn checkpoint_child() -> Result<()> {
        if let Some(base) = std::env::var_os("ADMISSIONS_CHECKPOINT_TEST") {
            node(Path::new(&base)).gc_checkpoint(EPOCH, &BTreeSet::from([KEEP.into()]))?;
        }
        Ok(())
    }

    #[test]
    fn sqlite_checkpoint_survives_interrupted_directory_swap_and_forgets_dead_paths() -> Result<()>
    {
        for legacy in [true, false] {
            let temp = tempfile::tempdir()?;
            let node = node(temp.path());
            let paths = [KEEP, DEAD].into_iter().map(|p| (p.to_owned(), json!({"narHash":"sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=","narSize":120,"references":[],"ca":null,"signatures":[],"ultimate":true}))).collect();
            let mut manifest = Manifest {
                version: 1,
                roots: vec![KEEP.into(), DEAD.into()],
                paths,
                realisations: BTreeMap::new(),
            };
            let ca_id = format!("sha256:{}!out", "1".repeat(64));
            manifest.realisations.insert(ca_id.clone(), json!({"id":ca_id,"outPath":KEEP.trim_start_matches("/nix/store/"),"signatures":[],"dependentRealisations":{}}));
            let journal = Journal {
                manifest,
                plan: BTreeMap::from([(KEEP.into(), Kind::MountDir), (DEAD.into(), Kind::Copy)]),
                status: Status::Committed,
            };
            let active = node.base.join("admissions");
            if legacy {
                durable(
                    &active.join(format!("{}.json", journal.manifest.id()?)),
                    &journal,
                )?;
            } else {
                Admissions::open(&active)?.begin(&journal)?;
            }
            let status = Command::new(std::env::current_exe()?)
                .args([
                    "--exact",
                    "gc::checkpoint_tests::checkpoint_child",
                    "--nocapture",
                ])
                .env("ADMISSIONS_CHECKPOINT_TEST", temp.path())
                .env("DISTRIBUTED_NIX_FAILPOINT", "gc-after-journal-rename")
                .status()?;
            ensure!(
                status.signal() == Some(9),
                "checkpoint failpoint not reached"
            );
            ensure!(!active.exists());
            node.gc_checkpoint(EPOCH, &BTreeSet::from([KEEP.into()]))?;
            node.gc_checkpoint(EPOCH, &BTreeSet::from([KEEP.into()]))?;
            let db = Admissions::open(&active)?;
            let ids = db.ids()?;
            ensure!(ids.len() == 1);
            let retained = db.get(&ids[0])?.unwrap();
            ensure!(retained.plan == BTreeMap::from([(KEEP.into(), Kind::MountDir)]));
            ensure!(retained.manifest.realisations == journal.manifest.realisations);
            let dead = vec![DEAD.to_owned()];
            ensure!(db.known(&dead)?.is_empty());
            drop(db);
            let retired = Admissions::open(&node.gc_epoch(EPOCH)?.join("old-admissions"))?;
            ensure!(retired.known(&dead)?[DEAD] == Kind::Copy);
            drop(retired);
            node.gc_checkpoint("22222222222222222222222222222222", &BTreeSet::new())?;
            ensure!(Admissions::open(&active)?.ids()?.is_empty());
        }
        Ok(())
    }
}
