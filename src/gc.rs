//! Shared mark graph and crash-safe admission checkpoint helpers for online GC.
use crate::{
    admissions::Admissions,
    manifest::{Manifest, realisation_path, valid_path},
    node::{Journal, Kind, Node, Status, mountpoint, physical, present},
    util::*,
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet, BinaryHeap},
    fs,
    path::{Path, PathBuf},
    process::Command,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PathMetadata {
    pub nar_size: u64,
    pub registered_at: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub live: BTreeSet<String>,
    pub graph: BTreeMap<String, BTreeSet<String>>,
    #[serde(default)]
    pub metadata: BTreeMap<String, PathMetadata>,
    #[serde(default)]
    pub last_used: BTreeMap<String, u64>,
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
        ensure!(!self.workers.is_empty(), "GC requires participants");
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
    ensure!(snapshots.len() >= 2, "missing GC snapshots");
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
        workers: dead[..dead.len() - 1].to_vec(),
        origin: dead.last().unwrap().clone(),
    };
    p.validate()?;
    Ok(p)
}
/// Select least recently used garbage first, including any unrooted referrers.
/// NAR sizes estimate reclaimable bytes; native deletion still enforces liveness.
pub fn oldest_first(mut all: Plan, snapshots: &[Snapshot], wanted: &[u64]) -> Result<Plan> {
    ensure!(
        snapshots.len() == wanted.len() && wanted.len() == all.workers.len() + 1,
        "GC budget membership mismatch"
    );
    let mut reverse: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut ages = BTreeMap::new();
    let eligible: BTreeSet<_> = all
        .workers
        .iter()
        .chain([&all.origin])
        .flatten()
        .cloned()
        .collect();
    for snapshot in snapshots {
        for (path, refs) in &snapshot.graph {
            let metadata = snapshot
                .metadata
                .get(path)
                .context("GC snapshot lacks size/age metadata; upgrade every participant")?;
            let used = metadata
                .registered_at
                .max(snapshot.last_used.get(path).copied().unwrap_or(0));
            ages.entry(path.clone())
                .and_modify(|age: &mut u64| *age = (*age).max(used))
                .or_insert(used);
            for reference in refs {
                reverse
                    .entry(reference.clone())
                    .or_default()
                    .insert(path.clone());
            }
        }
    }
    // A dependency is as recent as the newest closure that still needs it.
    let mut recent: BinaryHeap<_> = ages
        .iter()
        .map(|(path, age)| (*age, path.clone()))
        .collect();
    while let Some((age, path)) = recent.pop() {
        if ages[&path] != age {
            continue;
        }
        for snapshot in snapshots {
            for reference in snapshot.graph.get(&path).into_iter().flatten() {
                if let Some(previous) = ages.get_mut(reference) {
                    if *previous < age {
                        *previous = age;
                        recent.push((age, reference.clone()));
                    }
                }
            }
        }
    }
    let mut ordered: Vec<_> = eligible.iter().collect();
    ordered.sort_by_key(|path| (ages.get(*path).copied().unwrap_or(0), *path));
    let mut selected = BTreeSet::new();
    let mut remaining = wanted.to_vec();
    for path in ordered {
        if remaining.iter().all(|size| *size == 0) {
            break;
        }
        if !snapshots
            .iter()
            .zip(&remaining)
            .any(|(snapshot, bytes)| *bytes > 0 && snapshot.graph.contains_key(path))
        {
            continue;
        }
        let mut pending = vec![path.clone()];
        while let Some(path) = pending.pop() {
            ensure!(
                eligible.contains(&path),
                "eviction reaches a protected referrer"
            );
            if !selected.insert(path.clone()) {
                continue;
            }
            for (snapshot, bytes) in snapshots.iter().zip(&mut remaining) {
                if let Some(info) = snapshot.metadata.get(&path) {
                    *bytes = bytes.saturating_sub(info.nar_size);
                }
            }
            pending.extend(reverse.get(&path).into_iter().flatten().cloned());
        }
    }
    for paths in all.workers.iter_mut().chain([&mut all.origin]) {
        all.keep.extend(paths.difference(&selected).cloned());
        paths.retain(|path| selected.contains(path));
    }
    all.validate()?;
    Ok(all)
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
    pub(crate) fn gc_epoch(&self, id: &str) -> Result<PathBuf> {
        valid_id(id)?;
        Ok(self.base.join("gc").join(id))
    }
    #[cfg(test)]
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
            let queued = Admissions::open(&active)?
                .relocations()?
                .into_iter()
                .filter(|(path, _)| retain(path))
                .collect();
            checkpoint.queue_relocations(&queued)?;
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
}

#[cfg(test)]
mod checkpoint_tests {
    use super::*;
    use serde_json::json;
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
        {
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
            Admissions::open(&active)?.begin(&journal)?;
            Admissions::open(&active)?.queue_relocations(&journal.manifest.paths)?;
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
            ensure!(db.relocations()?.keys().cloned().collect::<Vec<_>>() == vec![KEEP.to_owned()]);
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

#[derive(Clone, Debug)]
pub struct Policy {
    pub min_free_percent: u8,
    pub target_free_percent: u8,
    pub max_store_bytes: u64,
    pub target_store_bytes: u64,
}
impl Policy {
    pub fn from_env() -> Result<Self> {
        fn setting(name: &str, default: u64) -> Result<u64> {
            Ok(std::env::var(format!("DISTRIBUTED_NIX_GC_{name}"))
                .ok()
                .map(|v| v.parse())
                .transpose()?
                .unwrap_or(default))
        }
        let result = Self {
            min_free_percent: setting("MIN_FREE_PERCENT", 25)?.try_into()?,
            target_free_percent: setting("TARGET_FREE_PERCENT", 30)?.try_into()?,
            max_store_bytes: setting("MAX_STORE_BYTES", 0)?,
            target_store_bytes: setting("TARGET_STORE_BYTES", 0)?,
        };
        result.validate()?;
        Ok(result)
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.min_free_percent > 0
                && self.min_free_percent < self.target_free_percent
                && self.target_free_percent < 100,
            "require 0 < min free percent < target free percent < 100"
        );
        ensure!(
            (self.max_store_bytes == 0 && self.target_store_bytes == 0)
                || (self.target_store_bytes > 0 && self.target_store_bytes < self.max_store_bytes),
            "require 0 < target store bytes < max store bytes, or both zero"
        );
        Ok(())
    }
    /// Byte goals use filesystem usage, including metadata and build scratch.
    pub fn goals(&self, reports: &[Value]) -> Result<(bool, Vec<u64>)> {
        self.validate()?;
        ensure!(!reports.is_empty(), "missing filesystem usage");
        let mut pressure = false;
        let mut wanted = Vec::new();
        for (index, report) in reports.iter().enumerate() {
            let total = u128::from(report["blocks"].as_u64().context("total blocks")?);
            let available = u128::from(report["available"].as_u64().context("available blocks")?);
            let block_size = u128::from(
                report["block_size"]
                    .as_u64()
                    .context("block size; upgrade every participant")?,
            );
            ensure!(
                total > 0 && block_size > 0 && available <= total,
                "invalid filesystem usage"
            );
            let free = u128::from(report["free"].as_u64().context("free blocks")?);
            ensure!(
                available <= free && free <= total,
                "invalid free block count"
            );
            let used = (total - free) * block_size;
            pressure |= available * 100 < total * u128::from(self.min_free_percent);
            let mut goal = (total * u128::from(self.target_free_percent) / 100)
                .saturating_sub(available)
                * block_size;
            if index == 0 && self.max_store_bytes > 0 {
                pressure |= used > u128::from(self.max_store_bytes);
                goal = goal.max(used.saturating_sub(u128::from(self.target_store_bytes)));
            }
            wanted.push(goal.try_into()?);
        }
        // The origin and node zero's worker share a filesystem.
        wanted.push(wanted[0]);
        wanted[0] = 0;
        Ok((pressure, wanted))
    }
}

#[cfg(test)]
mod policy_tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn size_budget_and_watermarks_are_independent_and_validated() -> Result<()> {
        let mut policy = Policy {
            min_free_percent: 25,
            target_free_percent: 30,
            max_store_bytes: 100,
            target_store_bytes: 80,
        };
        let usage =
            |available| json!({"blocks":700,"available":available,"free":available,"block_size":1});
        assert_eq!(
            policy.goals(&[usage(590), usage(600)])?,
            (true, vec![0, 0, 30])
        );
        assert!(!policy.goals(&[usage(610), usage(600)])?.0);
        assert_eq!(
            policy.goals(&[usage(610), usage(170)])?,
            (true, vec![0, 40, 10])
        );
        let reserved = json!({"blocks":700,"available":570,"free":620,"block_size":1});
        assert!(
            !policy.goals(&[reserved])?.0,
            "reserved blocks are not used cache space"
        );
        policy.target_store_bytes = 100;
        assert!(policy.validate().is_err());
        policy.max_store_bytes = 0;
        assert!(policy.validate().is_err());
        policy.target_store_bytes = 0;
        assert!(policy.validate().is_ok());
        policy.target_free_percent = 25;
        assert!(policy.validate().is_err());
        Ok(())
    }
}

impl Node {
    /// Called only by the coordinator holding the exclusive publication lease.
    pub fn maintain(&self, current_epoch: &str) -> Result<Value> {
        let seconds: u64 = std::env::var("DISTRIBUTED_NIX_PUBLICATION_MAX_AGE_SECONDS")
            .unwrap_or_else(|_| "86400".into())
            .parse()?;
        ensure!(seconds > 0, "publication maximum age must be positive");
        self.maintain_at(
            current_epoch,
            std::time::SystemTime::now(),
            std::time::Duration::from_secs(seconds),
        )
    }
    fn maintain_at(
        &self,
        current_epoch: &str,
        now: std::time::SystemTime,
        max_age: std::time::Duration,
    ) -> Result<Value> {
        valid_id(current_epoch)?;
        ensure!(
            !self.base.join("online-gc.json").exists()
                && !self.base.join("online-master.json").exists(),
            "resume GC before maintenance"
        );
        let _admit = Lock::acquire(&self.base.join("admit.lock"), false)?;
        let _queue = Lock::acquire(&self.base.join("outbox.lock"), false)?;
        let mut expired = 0;
        for queue in ["outbox", "ca-outbox"] {
            for file in crate::node::journals(&self.base.join(queue))? {
                if now
                    .duration_since(fs::metadata(&file)?.modified()?)
                    .unwrap_or_default()
                    < max_age
                {
                    continue;
                }
                let root = self
                    .root
                    .join(format!("nix/var/nix/gcroots/distributed-nix-{queue}"))
                    .join(file.file_stem().context("queue filename")?);
                remove_file(&root)?;
                remove_file(&file)?;
                expired += 1;
            }
        }
        // Recover the record-first acknowledgements written by older versions.
        for queue in ["outbox", "ca-outbox"] {
            let roots = self
                .root
                .join(format!("nix/var/nix/gcroots/distributed-nix-{queue}"));
            if roots.exists() {
                for entry in fs::read_dir(&roots)? {
                    let entry = entry?;
                    if entry.file_type()?.is_symlink()
                        && !self
                            .base
                            .join(queue)
                            .join(format!(
                                "{}.json",
                                entry.file_name().to_str().context("queue filename")?
                            ))
                            .exists()
                    {
                        remove_file(&entry.path())?;
                    }
                }
            }
        }
        let incoming = crate::node::journals(&self.base.join("incoming"))?;
        for file in &incoming {
            remove_file(file)?;
        }
        let epochs = self.base.join("gc");
        fs::create_dir_all(&epochs)?;
        let mut finished = Vec::new();
        for entry in fs::read_dir(&epochs)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(id) = name.to_str() else {
                continue;
            };
            if id == current_epoch || valid_id(id).is_err() || !entry.file_type()?.is_dir() {
                continue;
            }
            let marker = entry.path().join("online-finished.json");
            if marker.exists() {
                finished.push((fs::metadata(marker)?.modified()?, entry.path()));
            }
        }
        finished.sort();
        let histories = finished.len().saturating_sub(4);
        for (_, path) in finished.into_iter().take(histories) {
            fs::remove_dir_all(path)?;
        }
        syncdir(&epochs)?;
        let mut reports = Vec::new();
        for file in crate::node::journals(&self.base.join("cluster/results/distributed-nix"))? {
            let name = file.file_name().unwrap().to_string_lossy();
            if name
                .strip_prefix("publication-")
                .or_else(|| name.strip_prefix("reconcile-"))
                .and_then(|id| id.strip_suffix(".json"))
                .is_some_and(|id| id.len() == 64 && id.bytes().all(|c| c.is_ascii_hexdigit()))
            {
                reports.push((fs::metadata(&file)?.modified()?, file));
            }
        }
        reports.sort();
        let report_count = reports.len().saturating_sub(32);
        for (_, file) in reports.into_iter().take(report_count) {
            remove_file(&file)?;
        }
        Ok(
            serde_json::json!({"expired_publications":expired,"incoming_removed":incoming.len(),"histories_removed":histories,"reports_removed":report_count}),
        )
    }
}

#[cfg(test)]
mod maintenance_tests {
    use super::*;
    use serde_json::json;
    use std::{
        os::unix::fs::symlink,
        time::{Duration, SystemTime},
    };
    #[test]
    fn bounded_metadata_preserves_new_publications_active_roots_and_incomplete_epochs() -> Result<()>
    {
        let dir = tempfile::tempdir()?;
        let node = Node {
            base: dir.path().join("state"),
            root: dir.path().join("root"),
            ..Node::default()
        };
        let now = SystemTime::now();
        let path = "/nix/store/11111111111111111111111111111111-output";
        for queue in ["outbox", "ca-outbox"] {
            let roots = node
                .root
                .join(format!("nix/var/nix/gcroots/distributed-nix-{queue}"));
            fs::create_dir_all(&roots)?;
            for (name, age) in [("old", 90000), ("new.drv", 1)] {
                let file = node.base.join(queue).join(format!("{name}.json"));
                durable(&file, &json!({"path":path}))?;
                fs::File::open(file)?.set_modified(now - Duration::from_secs(age))?;
                symlink(path, roots.join(name))?;
            }
        }
        let active = node.root.join("nix/var/nix/gcroots/active-job");
        symlink(path, &active)?;
        durable(&node.base.join("incoming/manifest.json"), &json!({}))?;
        for i in 1..=8 {
            let epoch = node.gc_epoch(&format!("{i:032x}"))?;
            durable(&epoch.join("candidates.json"), &json!({}))?;
            if i < 8 {
                durable(&epoch.join("online-finished.json"), &json!({}))?;
            }
        }
        for i in 0..40 {
            durable(
                &node.base.join(format!(
                    "cluster/results/distributed-nix/publication-{i:064x}.json"
                )),
                &json!({}),
            )?;
        }
        let current = format!("{:032x}", 9);
        durable(&node.base.join("online-master.json"), &json!({}))?;
        assert!(
            node.maintain_at(&current, now, Duration::from_secs(86400))
                .is_err()
        );
        remove_file(&node.base.join("online-master.json"))?;
        let report = node.maintain_at(&current, now, Duration::from_secs(86400))?;
        assert_eq!(
            report,
            json!({"expired_publications":2,"incoming_removed":1,"histories_removed":3,"reports_removed":8})
        );
        for queue in ["outbox", "ca-outbox"] {
            assert!(!node.base.join(queue).join("old.json").exists());
            assert!(node.base.join(queue).join("new.drv.json").exists());
            assert!(present(&node.root.join(format!(
                "nix/var/nix/gcroots/distributed-nix-{queue}/new.drv"
            ))));
            assert!(!present(&node.root.join(format!(
                "nix/var/nix/gcroots/distributed-nix-{queue}/old"
            ))));
        }
        assert!(present(&active));
        assert!(node.gc_epoch(&format!("{:032x}", 8))?.exists());
        assert_eq!(
            node.maintain_at(&current, now, Duration::from_secs(86400))?["histories_removed"],
            0
        );
        Ok(())
    }
}
