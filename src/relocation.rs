//! Reclaim published local payloads only while native clients are fenced.
use crate::{
    admissions::Admissions,
    native,
    node::{Kind, Node, pathlocks, physical, present},
    util::*,
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    time::Duration,
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Intent {
    kinds: BTreeMap<String, Kind>,
}

impl Node {
    // Called before admitting clients after a crash, including before runtime seeding.
    pub fn resume_relocation(&self) -> Result<()> {
        let _gate = Lock::acquire(&self.base.join("maintenance.lock"), false)?;
        let _operation = Lock::acquire(&self.base.join("online-operation.lock"), false)?;
        let _admit = Lock::acquire(&self.base.join("admit.lock"), false)?;
        self.online_restore_checkpoint()?;
        if self.base.join("relocation.json").exists() {
            self.finish_relocation()?;
        }
        Ok(())
    }

    fn finish_relocation(&self) -> Result<usize> {
        let file = self.base.join("relocation.json");
        let intent: Intent = serde_json::from_value(read_json(&file)?)?;
        ensure!(!intent.kinds.is_empty(), "empty relocation intent");
        let _gc = Lock::acquire(&self.root.join("nix/var/nix/gc.lock"), true)?;
        let _paths = pathlocks(&self.root, intent.kinds.keys().cloned(), intent.kinds.len())?;
        refresh_metadata(&self.lower.join("nix/store"))?;
        for (path, kind) in &intent.kinds {
            ensure!(crate::manifest::valid_path(path), "invalid relocation path");
            ensure!(
                matches!(kind, Kind::MountDir | Kind::MountFile),
                "invalid relocation kind"
            );
            let source = physical(&self.lower, path);
            let target = physical(&self.root, path);
            refresh_metadata(&source)?;
            let metadata = source.symlink_metadata()?;
            ensure!(
                if *kind == Kind::MountDir {
                    metadata.is_dir()
                } else {
                    metadata.is_file()
                },
                "relocation source changed type"
            );
            if !crate::linux::mountpoint(&target)? {
                if present(&target) {
                    if target.symlink_metadata()?.is_dir() {
                        fs::remove_dir_all(&target)?;
                    } else {
                        fs::remove_file(&target)?;
                    }
                }
                syncdir(target.parent().unwrap())?;
                failpoint("relocation-after-delete");
                if *kind == Kind::MountDir {
                    fs::create_dir(&target)?;
                } else {
                    OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(&target)?;
                }
                crate::linux::bind(&source, &target)?;
            }
            use std::os::unix::fs::MetadataExt;
            let mounted = target.metadata()?;
            ensure!(
                (metadata.dev(), metadata.ino()) == (mounted.dev(), mounted.ino()),
                "relocation mount provenance mismatch"
            );
            crate::linux::mount(
                None,
                &target,
                None,
                libc::MS_REMOUNT | libc::MS_BIND | libc::MS_RDONLY,
                None,
            )?;
            failpoint("relocation-after-mount");
        }
        syncdir(&self.root.join("nix/store"))?;
        Admissions::open(&self.base.join("admissions"))?.relocated(&intent.kinds)?;
        failpoint("relocation-after-database-commit");
        fs::remove_file(file)?;
        syncdir(&self.base)?;
        Ok(intent.kinds.len())
    }

    pub fn relocate(&self, seed: &[String], pods: &BTreeSet<String>) -> Result<Value> {
        let Some(_gate) = Lock::try_exclusive(&self.base.join("maintenance.lock"))? else {
            return Ok(json!({"deferred":"native clients active"}));
        };
        let _operation = Lock::acquire(&self.base.join("online-operation.lock"), false)?;
        ensure!(
            self.base.join("ready").exists(),
            "store recovery incomplete"
        );
        ensure!(
            !self.base.join("relocation.json").exists(),
            "unfinished relocation requires recovery"
        );
        if self.base.join("online-gc.json").exists() {
            return Ok(json!({"deferred":"online GC active"}));
        }
        self.prune_client_roots(pods)?;
        let _roots = Lock::acquire(&self.base.join("online-roots.lock"), false)?;
        let _admit = Lock::acquire(&self.base.join("admit.lock"), false)?;
        let mut db = Admissions::open(&self.base.join("admissions"))?;
        let pending = db.relocations()?;
        if pending.is_empty() {
            return Ok(json!({"relocated":0}));
        }

        let mut protected = self.client_roots()?;
        protected.extend(seed.iter().cloned());
        if !protected.is_empty() {
            let roots: Vec<_> = protected.into_iter().collect();
            let valid: Vec<String> =
                serde_json::from_value(native::valid_paths(&self.root, &roots)?)?;
            protected = if valid.is_empty() {
                BTreeSet::new()
            } else {
                native::dump(&self.root, &valid)?
                    .paths
                    .into_keys()
                    .collect()
            };
        }
        let paths: Vec<_> = pending
            .keys()
            .filter(|p| !protected.contains(*p))
            .cloned()
            .collect();
        if paths.is_empty() {
            return Ok(json!({"relocated":0,"retained":pending.len()}));
        }
        let current = native::dump(&self.root, &paths)?;
        let mut kinds = BTreeMap::new();
        let mut retained = BTreeMap::new();
        let known = db.known(paths.iter())?;
        refresh_metadata(&self.lower.join("nix/store"))?;
        for path in &paths {
            ensure!(
                known.get(path) == Some(&Kind::Local),
                "relocation queue is not local: {path}"
            );
            let expected = &pending[path];
            let actual = &current.paths[path];
            // A nondeterministic input-addressed build can differ from the shared winner.
            if ["narHash", "narSize", "references", "ca"]
                .iter()
                .any(|k| actual[*k] != expected[*k])
            {
                retained.insert(path.clone(), Kind::Local);
                continue;
            }
            let source = physical(&self.lower, path);
            refresh_metadata(&source)?;
            let metadata = source.symlink_metadata()?;
            if metadata.is_dir() {
                kinds.insert(path.clone(), Kind::MountDir);
            } else if metadata.is_file() && metadata.len() > 65536 {
                kinds.insert(path.clone(), Kind::MountFile);
            } else {
                // Tiny standalone files and symlinks intentionally remain local.
                retained.insert(path.clone(), Kind::Local);
            }
        }
        if !retained.is_empty() {
            db.relocated(&retained)?;
        }
        if kinds.is_empty() {
            return Ok(json!({"relocated":0,"retained":pending.len()}));
        }
        let ready = read_json(&self.base.join("ready"))?;
        fs::remove_file(self.base.join("ready"))?;
        syncdir(&self.base)?;
        durable(&self.base.join("relocation.json"), &Intent { kinds })?;
        failpoint("relocation-after-intent");
        let count = self.finish_relocation()?;
        durable(&self.base.join("ready"), &ready)?;
        Ok(json!({"relocated":count}))
    }

    pub fn relocator(&self) -> Result<Value> {
        let seed: Vec<String> =
            serde_json::from_value(self.runtime()?["seed"].clone()).context("runtime seed")?;
        let pods = std::env::var("DISTRIBUTED_NIX_POD_UID")
            .ok()
            .into_iter()
            .collect();
        loop {
            let report = self.relocate(&seed, &pods)?;
            durable(&self.base.join("relocator-status.json"), &report)?;
            std::thread::sleep(Duration::from_secs(5));
        }
    }
}
