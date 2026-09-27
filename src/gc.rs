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
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    process::Command,
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
