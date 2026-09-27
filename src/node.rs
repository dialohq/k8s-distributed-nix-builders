//! Privileged Linux control plane. Native Nix owns store metadata transactions.
use crate::{
    admissions::Admissions,
    manifest::{Manifest, valid_path},
    util::*,
};
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    ffi::CString,
    fs::{self, File, OpenOptions},
    os::unix::{
        fs::{MetadataExt, PermissionsExt, symlink},
        process::CommandExt,
    },
    path::{Path, PathBuf},
    process::Command,
    time::Instant,
};

pub const BASE: &str = "/var/lib/distributed-nix";
pub const ROOT: &str = "/srv/distributed-nix/worker";
pub const ORIGIN: &str = "/srv/distributed-nix/origin";
pub const BIN: &str = "/var/lib/distributed-nix/bin/distributed-nix";
#[derive(Clone, Debug)]
pub struct Node {
    pub base: PathBuf,
    pub root: PathBuf,
    pub origin: PathBuf,
    pub lower: PathBuf,
}
impl Default for Node {
    fn default() -> Self {
        Self {
            base: BASE.into(),
            root: ROOT.into(),
            origin: ORIGIN.into(),
            lower: PathBuf::from(BASE).join("lower"),
        }
    }
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum Kind {
    Local,
    Symlink,
    Copy,
    MountDir,
    MountFile,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Journal {
    pub manifest: Manifest,
    pub plan: BTreeMap<String, Kind>,
    pub status: Status,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Pending,
    Committed,
}
impl Journal {
    pub fn validate(&self, id: &str) -> Result<()> {
        self.manifest.validate()?;
        ensure!(self.manifest.id()? == id, "journal identity mismatch");
        ensure!(
            self.plan.keys().eq(self.manifest.paths.keys()),
            "journal plan differs from manifest paths"
        );
        Ok(())
    }
}
pub(crate) fn physical(root: &Path, p: &str) -> PathBuf {
    root.join(p.trim_start_matches('/'))
}
pub(crate) fn present(p: &Path) -> bool {
    p.symlink_metadata().is_ok()
}
pub(crate) fn mountpoint(p: &Path) -> Result<bool> {
    Ok(Command::new("mountpoint")
        .arg("-q")
        .arg(p)
        .status()?
        .success())
}
fn readonly_mounts(info: &str) -> std::collections::BTreeSet<PathBuf> {
    let mut mounts = BTreeMap::new();
    for line in info.lines() {
        let fields: Vec<_> = line.split_whitespace().collect();
        if fields.len() < 10 || !fields.contains(&"-") {
            continue;
        }
        let entry = mounts.entry(fields[4]).or_insert((0, false));
        entry.0 += 1;
        entry.1 = fields[5].split(',').any(|option| option == "ro");
    }
    mounts
        .into_iter()
        .filter(|(_, (count, readonly))| *count == 1 && *readonly)
        .map(|(path, _)| PathBuf::from(path))
        .collect()
}
fn mount(src: &Path, dst: &Path, opts: &str, typ: Option<&str>) -> Result<()> {
    fs::create_dir_all(dst)?;
    if !mountpoint(dst)? {
        let mut c = Command::new("mount");
        c.args(["-o", opts]);
        if let Some(t) = typ {
            c.args(["-t", t]);
        }
        run(c.arg(src).arg(dst))?;
    }
    Ok(())
}
fn bind(src: &Path, dst: &Path) -> Result<()> {
    mount(src, dst, "bind", None)?;
    run(Command::new("mount").arg("--make-private").arg(dst))
}
fn link(target: &Path, dst: &Path) -> Result<()> {
    if present(dst) {
        ensure!(
            fs::read_link(dst)? == target,
            "conflicting symlink {}",
            dst.display()
        );
    } else {
        symlink(target, dst)?;
    }
    Ok(())
}
pub fn roots(root: &Path, paths: impl IntoIterator<Item = impl AsRef<str>>) -> Result<()> {
    roots_named(root, paths, "distributed-nix")
}
pub(crate) fn roots_named(
    root: &Path,
    paths: impl IntoIterator<Item = impl AsRef<str>>,
    name: &str,
) -> Result<()> {
    let d = root.join("nix/var/nix/gcroots").join(name);
    fs::create_dir_all(&d)?;
    for p in paths {
        let p = p.as_ref();
        ensure!(valid_path(p), "invalid root path");
        link(
            Path::new(p),
            &d.join(Path::new(p).file_name().context("store basename")?),
        )?;
    }
    syncdir(&d)?;
    syncdir(d.parent().unwrap())
}

fn replace_bootstrap_roots(root: &Path, seed: &[&str]) -> Result<()> {
    roots_named(root, seed, "distributed-nix-bootstrap")?;
    let directory = root.join("nix/var/nix/gcroots/distributed-nix-bootstrap");
    let names: std::collections::BTreeSet<_> = seed
        .iter()
        .map(|path| Path::new(path).file_name().unwrap())
        .collect();
    for entry in fs::read_dir(&directory)? {
        let entry = entry?;
        if !names.contains(entry.file_name().as_os_str()) {
            fs::remove_file(entry.path())?;
        }
    }
    syncdir(&directory)
}

pub(crate) fn journals(d: &Path) -> Result<Vec<PathBuf>> {
    fs::create_dir_all(d)?;
    let mut ps = Vec::new();
    for entry in fs::read_dir(d)? {
        let p = entry?.path();
        if p.extension().is_some_and(|e| e == "json") {
            ps.push(p);
        }
    }
    ps.sort();
    Ok(ps)
}
pub(crate) fn load_journal(p: &Path) -> Result<Journal> {
    let j: Journal = serde_json::from_value(read_json(p)?)?;
    j.validate(
        p.file_stem()
            .and_then(|s| s.to_str())
            .context("journal name")?,
    )?;
    Ok(j)
}
fn pathlocks(root: &Path, paths: impl Iterator<Item = String>, count: usize) -> Result<Vec<Lock>> {
    // SAFETY: initialized rlimit pointer, platform constants supplied by libc.
    unsafe {
        let mut limit: libc::rlimit = std::mem::zeroed();
        ensure!(
            libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) == 0,
            "getrlimit failed"
        );
        limit.rlim_cur = limit
            .rlim_cur
            .max((count + 256) as libc::rlim_t)
            .min(limit.rlim_max);
        ensure!(
            libc::setrlimit(libc::RLIMIT_NOFILE, &limit) == 0,
            "setrlimit failed"
        );
    }
    let mut held = Vec::new();
    for p in paths {
        loop {
            let l = Lock::acquire(&physical(root, &format!("{p}.lock")), false)?;
            // Nix marks unlinked lock files. Reopen rather than locking an orphan inode.
            if !l.stale()? {
                held.push(l);
                break;
            }
        }
    }
    Ok(held)
}
impl Node {
    pub fn admit(&self, file: &Path, recovery: bool) -> Result<Value> {
        let m = Manifest::read(file)?;
        let id = m.id()?;
        let start = Instant::now();
        let paths = m.paths.keys().cloned().collect::<Vec<_>>();
        let _roots = if recovery {
            None
        } else {
            Some(crate::online::guard(&self.base, &paths, false)?)
        };
        let _admit = Lock::acquire(&self.base.join("admit.lock"), false)?;
        let _gc = Lock::acquire(&self.root.join("nix/var/nix/gc.lock"), true)?;
        let _paths = pathlocks(&self.root, m.paths.keys().cloned(), m.paths.len())?;
        refresh_metadata(&self.lower.join("nix/store"))?;
        // Only accept metadata durably published alongside the shared collection.
        if !recovery {
            let accepted = Manifest::read(
                &self
                    .lower
                    .join(".distributed-nix-publications")
                    .join(format!("{id}.json")),
            )?;
            ensure!(accepted == m, "manifest differs from origin publication");
        }
        self.online_restore_checkpoint()?;
        let mut admissions = Admissions::open(&self.base.join("admissions"))?;
        let state = if let Some(j) = admissions.get(&id)? {
            ensure!(j.manifest == m, "journal manifest mismatch");
            j
        } else {
            let known = admissions.known(m.paths.keys())?;
            let existing = crate::native::valid_paths(
                &self.root,
                &m.paths.keys().cloned().collect::<Vec<_>>(),
            )?;
            let existing = existing.as_array().context("native existing paths")?;
            let mut plan = BTreeMap::new();
            for p in m.paths.keys() {
                let src = physical(&self.lower, p);
                let dst = physical(&self.root, p);
                let kind = if let Some(k) = known.get(p) {
                    *k
                } else if existing.iter().any(|v| v.as_str() == Some(p)) {
                    ensure!(present(&dst), "valid local path has no files: {p}");
                    Kind::Local
                } else {
                    refresh_metadata(&src)?;
                    let md = src
                        .symlink_metadata()
                        .with_context(|| format!("origin path unavailable: {p}"))?;
                    if md.file_type().is_symlink() {
                        Kind::Symlink
                    } else if md.is_file() && md.len() <= 65536 {
                        Kind::Copy
                    } else if md.is_dir() {
                        Kind::MountDir
                    } else if md.is_file() {
                        Kind::MountFile
                    } else {
                        bail!("unsupported origin file type: {p}");
                    }
                };
                plan.insert(p.clone(), kind);
            }
            let j = Journal {
                manifest: m.clone(),
                plan,
                status: Status::Pending,
            };
            let local: Vec<_> = j
                .plan
                .iter()
                .filter(|(_, k)| **k == Kind::Local)
                .map(|(p, _)| p.clone())
                .collect();
            crate::native::admit_local(&self.root, &m, &local, false)?;
            admissions.begin(&j)?;
            j
        };
        failpoint("after-journal");
        let local: Vec<_> = state
            .plan
            .iter()
            .filter(|(_, k)| **k == Kind::Local)
            .map(|(p, _)| p.clone())
            .collect();
        crate::native::admit_local(&self.root, &m, &local, false)?;
        roots(&self.root, m.paths.keys())?;
        let readonly = readonly_mounts(&fs::read_to_string("/proc/self/mountinfo")?);
        let mut copied = 0u64;
        let mut mounted = 0;
        for (p, kind) in &state.plan {
            let src = physical(&self.lower, p);
            let dst = physical(&self.root, p);
            if *kind != Kind::Local {
                refresh_metadata(&src)?;
            }
            match kind {
                Kind::Local => ensure!(present(&dst), "local path disappeared: {p}"),
                Kind::Symlink => link(&fs::read_link(&src)?, &dst)?,
                Kind::Copy => {
                    if !present(&dst) {
                        let tmp = dst.with_extension("admit-tmp");
                        // Destination belongs to us under both controller and native path locks.
                        if present(&tmp) {
                            fs::remove_file(&tmp)?;
                        }
                        fs::copy(&src, &tmp)?;
                        File::open(&tmp)?.sync_all()?;
                        fs::rename(tmp, &dst)?;
                    }
                    ensure!(
                        dst.symlink_metadata()?.is_file(),
                        "copy destination is not a regular file"
                    );
                    ensure!(
                        fs::read(&src)? == fs::read(&dst)?,
                        "small-file content conflict: {p}"
                    );
                    ensure!(
                        src.metadata()?.mode() & 0o111 == dst.metadata()?.mode() & 0o111,
                        "small-file executable bit conflict"
                    );
                    copied += dst.metadata()?.len();
                }
                Kind::MountDir | Kind::MountFile => {
                    if !present(&dst) {
                        if *kind == Kind::MountDir {
                            fs::create_dir(&dst)?;
                        } else {
                            OpenOptions::new().write(true).create_new(true).open(&dst)?;
                        }
                    }
                    ensure!(
                        !dst.symlink_metadata()?.file_type().is_symlink(),
                        "mount destination is a symlink"
                    );
                    let already_readonly = readonly.contains(&dst);
                    if !already_readonly && !mountpoint(&dst)? {
                        run(Command::new("mount").arg("--bind").arg(&src).arg(&dst))?;
                    }
                    let (a, b) = (src.metadata()?, dst.metadata()?);
                    ensure!(
                        (a.dev(), a.ino()) == (b.dev(), b.ino()),
                        "mount provenance mismatch: {p}"
                    );
                    if !already_readonly {
                        run(Command::new("mount")
                            .args(["-o", "remount,bind,ro"])
                            .arg(&dst))?;
                    }
                    mounted += 1;
                }
            }
        }
        syncdir(&self.root.join("nix/store"))?;
        failpoint("after-mounts");
        let registered = crate::native::admit_local(&self.root, &m, &local, true)?;
        failpoint("after-register");
        admissions.commit(&id)?;
        Ok(
            json!({"batch":id,"records":registered["paths"],"local_variants":registered["local_variants"],"mounted":mounted,"small_file_bytes":copied,"seconds":start.elapsed().as_secs_f64(),"recovered":recovery}),
        )
    }
    pub fn runtime(&self) -> Result<Value> {
        read_json(Path::new("/etc/distributed-nix/runtime.json"))
    }
    pub fn prepare(&self, origin: bool) -> Result<()> {
        let root = if origin { &self.origin } else { &self.root };
        let m = self.runtime()?;
        fs::create_dir_all(&self.base)?;
        fs::create_dir_all(root)?;
        bind(root, root)?;
        for d in [
            "nix",
            "nix/var",
            "nix/var/nix",
            "nix/store",
            "nix/var/nix/daemon-socket",
            "etc/nix",
            "proc",
            "dev",
            "tmp",
            "work",
            "bin",
            "usr/bin",
            "root",
            "run",
        ] {
            fs::create_dir_all(root.join(d))?;
        }
        fs::set_permissions(root.join("tmp"), fs::Permissions::from_mode(0o1777))?;
        let mut passwd =
            String::from("root:x:0:0:root:/root:/bin/sh\nnobody:x:65534:65534:nobody:/:/bin/sh\n");
        let mut builders = Vec::new();
        let build_group = m["buildGroupId"].as_u64().unwrap_or(30000);
        for index in 1..=32 {
            let name = format!("cibox-nixbld{index}");
            passwd.push_str(&format!(
                "{name}:x:{}:{build_group}:Nix build user:/var/empty:/bin/false\n",
                62000 + index
            ));
            builders.push(name);
        }
        fs::write(root.join("etc/passwd"), passwd)?;
        fs::write(
            root.join("etc/group"),
            format!(
                "root:x:0:\nnogroup:x:65534:\ncibox-nixbld:x:{build_group}:{}\n",
                builders.join(",")
            ),
        )?;
        fs::write(root.join("etc/hosts"), "127.0.0.1 localhost\n")?;
        fs::copy("/etc/resolv.conf", root.join("etc/resolv.conf"))?;
        fs::create_dir_all(root.join("etc/ssl/certs"))?;
        fs::copy(
            "/etc/ssl/certs/ca-certificates.crt",
            root.join("etc/ssl/certs/ca-certificates.crt"),
        )?;
        let bash = m["bash"].as_str().context("runtime bash")?;
        let core = m["coreutils"].as_str().context("runtime coreutils")?;
        let path = m["runtime"]
            .as_array()
            .context("runtime")?
            .iter()
            .map(|p| format!("{}/bin", p.as_str().unwrap_or("")))
            .collect::<Vec<_>>()
            .join(":");
        fs::write(
            root.join("etc/profile"),
            format!("export PATH={}\n", sh(&path)),
        )?;
        for dest in ["bin/sh", "bin/bash"] {
            link(Path::new(&format!("{bash}/bin/bash")), &root.join(dest))?;
        }
        link(
            Path::new(&format!("{core}/bin/env")),
            &root.join("usr/bin/env"),
        )?;
        let seed = m["seed"]
            .as_array()
            .context("runtime paths")?
            .iter()
            .map(|v| v.as_str().context("runtime path"))
            .collect::<Result<Vec<_>>>()?;
        replace_bootstrap_roots(root, &seed)?;
        bind(Path::new("/dev"), &root.join("dev"))?;
        bind(Path::new("/dev/pts"), &root.join("dev/pts"))?;
        bind(Path::new("/proc"), &root.join("proc"))?;
        if !origin {
            bind(&self.base, &root.join("run/distributed-nix"))?;
        }
        fs::write(
            root.join("etc/nix/nix.conf"),
            m["nixConfig"].as_str().context("runtime nixConfig")?,
        )?;
        if !origin {
            // The NixOS module mounts the backend; admission only needs POSIX paths.
            ensure!(mountpoint(&self.lower)?, "shared collection is not mounted");
            mount(
                &root.join("nix/store"),
                &root.join("nix/store"),
                "bind",
                None,
            )?;
            run(Command::new("mount")
                .arg("--make-shared")
                .arg(root.join("nix/store")))?;
        }
        fs::create_dir_all(self.base.join("admissions"))?;
        Ok(())
    }
    pub fn command(&self, cmd: &str, origin: bool, lease: bool) -> Result<Value> {
        let root = if origin { &self.origin } else { &self.root };
        if lease {
            let l = Lock::acquire(&self.base.join("clients.lock"), true)?;
            let r =
                read_json(&self.base.join("ready")).context("node recovery has not completed")?;
            ensure!(
                r["boot_id"].as_str()
                    == Some(fs::read_to_string("/proc/sys/kernel/random/boot_id")?.trim()),
                "mount recovery required after reboot"
            );
            l.inherit()?;
        }
        let m = self.runtime()?;
        let path = m["runtime"]
            .as_array()
            .context("runtime")?
            .iter()
            .map(|p| format!("{}/bin", p.as_str().unwrap_or("")))
            .collect::<Vec<_>>()
            .join(":");
        let err = Command::new("unshare")
            .args(["--mount", "--propagation", "slave", "--uts", "--ipc"])
            .arg(std::env::current_exe()?)
            .arg("pivot")
            .arg(root)
            .arg(format!(
                "{}/bin/env",
                m["coreutils"].as_str().context("coreutils")?
            ))
            .args(["-i", "HOME=/root", "USER=root"])
            .arg(format!("PATH={path}"))
            .arg("NIX_REMOTE=local?path-info-cache-size=0")
            .arg(format!("{}/bin/bash", m["bash"].as_str().context("bash")?))
            .arg("-c")
            .arg(format!("set -e; {cmd}"))
            .exec();
        Err(err).context("launch namespace with unshare")
    }
    pub(crate) fn restore_admissions(&self) -> Result<Vec<Value>> {
        self.online_restore_checkpoint()?;
        self.prepare(false)?;
        let mut rows = Vec::new();
        let admissions = Admissions::open(&self.base.join("admissions"))?;
        for id in admissions.ids()? {
            let state = admissions.get(&id)?.context("missing recovery batch")?;
            let file = self.base.join("recovery-manifest.json");
            durable(&file, &state.manifest)?;
            rows.push(self.admit(&file, true)?);
        }
        Ok(rows)
    }
    pub fn recover(&self) -> Result<Value> {
        let _clients = Lock::acquire(&self.base.join("clients.lock"), false)?;
        if self.base.join("ready").exists() {
            fs::remove_file(self.base.join("ready"))?;
            syncdir(&self.base)?;
        }
        let rows = self.restore_admissions()?;
        durable(
            &self.base.join("ready"),
            &json!({"recovered_batches":rows.len(),"boot_id":fs::read_to_string("/proc/sys/kernel/random/boot_id")?.trim()}),
        )?;
        Ok(json!({"recovered":rows}))
    }
    pub fn publication(&self, file: &Path, commit: bool) -> Result<Value> {
        let m = Manifest::read(file)?;
        let id = m.id()?;
        let _roots = crate::online::guard(
            &self.base,
            &m.paths.keys().cloned().collect::<Vec<_>>(),
            false,
        )?;
        let _publish = Lock::acquire(&self.base.join("publish.lock"), false)?;
        let _gc = Lock::acquire(&self.origin.join("nix/var/nix/gc.lock"), true)?;
        let result = crate::native::check(&self.origin, &m)?;
        if commit {
            ensure!(
                result["existing"]
                    .as_array()
                    .context("native existing paths")?
                    .len()
                    == m.paths.len(),
                "publication has missing paths"
            );
        }
        roots(&self.origin, &m.roots)?;
        if commit {
            crate::native::register(&self.origin, &m)?;
            run(Command::new("sync").arg("-f").arg(&self.origin))?;
            // Export manifest together with data. Readers trust this administrator-owned marker.
            durable(
                &self
                    .origin
                    .join(".distributed-nix-publications")
                    .join(format!("{id}.json")),
                &m,
            )?;
            failpoint("after-origin-commit");
            let pending = self.base.join("pending-publications");
            let committed_roots: std::collections::BTreeSet<_> = m.roots.iter().collect();
            for file in journals(&pending)? {
                let reservation = Manifest::read(&file)?;
                ensure!(
                    file.file_stem().and_then(|s| s.to_str()) == Some(reservation.id()?.as_str()),
                    "reservation identity mismatch"
                );
                // A competing copy may have selected another valid local variant.
                if reservation
                    .roots
                    .iter()
                    .collect::<std::collections::BTreeSet<_>>()
                    == committed_roots
                {
                    fs::remove_file(file)?;
                }
            }
            syncdir(&pending)?;
        } else {
            durable(
                &self
                    .base
                    .join("pending-publications")
                    .join(format!("{id}.json")),
                &m,
            )?;
        }
        Ok(json!({"batch":id,"records":m.paths.len(),"committed":commit}))
    }
    pub fn pin(&self, paths: &[String]) -> Result<Value> {
        let _roots = crate::online::guard(&self.base, paths, false)?;
        ensure!(
            !paths.is_empty() && paths.iter().all(|p| valid_path(p)),
            "invalid pin paths"
        );
        let _gc = Lock::acquire(&self.root.join("nix/var/nix/gc.lock"), true)?;
        run(Command::new("nix-store")
            .arg("--store")
            .arg(&self.root)
            .arg("--check-validity")
            .args(paths))?;
        roots(&self.root, paths)?;
        Ok(json!({"pinned":paths}))
    }
    pub fn resume_origin(&self) -> Result<Value> {
        ensure!(
            self.origin.join("nix/var/nix/db/db.sqlite").exists(),
            "origin not initialized"
        );
        self.prepare(true)?;
        Ok(json!({"origin":"ready"}))
    }
    fn hold_lease(&self) -> Result<Value> {
        use std::io::Write;
        let _publication = Lock::acquire(&self.base.join("publication.lock"), true)?;
        println!("leased");
        std::io::stdout().flush()?;
        std::io::copy(&mut std::io::stdin(), &mut std::io::sink())?;
        Ok(Value::Null)
    }
    pub fn dispatch(&self, args: &[String]) -> Result<Value> {
        fs::create_dir_all(&self.base)?;
        let op = arg(args, 0)?;
        if op == "connection" {
            return self.connection(arg(args, 1)? == "trusted");
        }
        if op == "upgrade-mounts" {
            let _gate = Lock::acquire(&self.base.join("maintenance.lock"), false)?;
            let _clients = Lock::acquire(&self.base.join("clients.lock"), false)?;
            if self.base.join("ready").exists() {
                fs::remove_file(self.base.join("ready"))?;
            }
            let mut paths = std::collections::BTreeSet::new();
            Admissions::open(&self.base.join("admissions"))?.for_each(|journal| {
                for (p, kind) in journal.plan {
                    if matches!(kind, Kind::MountDir | Kind::MountFile) {
                        paths.insert(p);
                    }
                }
                Ok(())
            })?;
            for p in paths {
                let dst = physical(&self.root, &p);
                if present(&dst) && mountpoint(&dst)? {
                    run(Command::new("umount").arg(dst))?;
                }
            }
            if mountpoint(&self.lower)? {
                run(Command::new("umount").arg(&self.lower))?;
            }
            return Ok(json!({"mounts_ready_for_recovery":true}));
        }
        if op == "runner-daemon" {
            return self.serve();
        }
        if op == "recover" {
            let _gate = Lock::acquire(&self.base.join("maintenance.lock"), false)?;
            return self.recover();
        }
        // Internal native stdio copies and metadata operations also participate.
        let _gate = {
            let gate = Lock::acquire(&self.base.join("maintenance.lock"), true)?;
            if matches!(op, "stdio" | "origin-stdio") {
                gate.inherit()?;
                None
            } else {
                Some(gate)
            }
        };
        match op {
            "ca-outbox" => self.ca_outbox(),
            "ca-acknowledge" => self.ca_acknowledge(&args[1..]),
            "ca-backfill" => self.ca_backfill(),
            "realisation-conflicts" => {
                let manifest = Manifest::read(Path::new(arg(args, 1)?))?;
                let worker = crate::native::realisation_conflicts(&self.root, &manifest)?;
                let origin = if self.origin.join("nix/var/nix/db/db.sqlite").exists() {
                    crate::native::realisation_conflicts(&self.origin, &manifest)?
                } else {
                    json!([])
                };
                Ok(json!({"worker":worker,"origin":origin}))
            }
            "canonical-manifest" => Ok(serde_json::to_value(crate::native::canonical_manifest(
                &self.origin,
                &Manifest::read(Path::new(arg(args, 1)?))?,
            )?)?),
            "outbox" => self.outbox(),
            "acknowledge" => self.acknowledge(&args[1..]),
            "valid-paths" => crate::native::valid_paths(&self.root, &args[1..]),
            "lease" => self.hold_lease(),
            "dump" | "dump-origin" => {
                let root = if args[0] == "dump-origin" {
                    &self.origin
                } else {
                    &self.root
                };
                Ok(serde_json::to_value(crate::native::dump(
                    root,
                    &args[1..],
                )?)?)
            }
            "admit" => self.admit(Path::new(arg(args, 1)?), false),
            "recover" => self.recover(),
            "prepare" => {
                self.prepare(false)?;
                Ok(json!({"prepared":true}))
            }
            "prepare-origin" => {
                self.prepare(true)?;
                Ok(json!({"prepared":true}))
            }
            "resume-origin" => self.resume_origin(),
            "reserve" | "commit" => self.publication(Path::new(arg(args, 1)?), args[0] == "commit"),
            "pin-local" => self.pin(&args[1..]),
            "stdio" | "origin-stdio" => {
                let m = self.runtime()?;
                let nix = m["nix"].as_str().context("nix runtime")?;
                let origin = args[0] == "origin-stdio";
                self.command(
                    &format!("{nix}/bin/nix daemon --stdio --store local?path-info-cache-size=0"),
                    origin,
                    !origin,
                )
            }
            "receive" => {
                let m = Manifest::parse(serde_json::from_slice(&read_stdin()?)?)?;
                let p = self.base.join("incoming").join(format!("{}.json", m.id()?));
                durable(&p, &m)?;
                Ok(json!({"file":p,"batch":m.id()?}))
            }
            _ => bail!("unknown node operation"),
        }
    }
}

pub fn pivot(args: &[String]) -> Result<()> {
    enter_root(arg(args, 0)?)?;
    Err(Command::new(arg(args, 1)?).args(&args[2..]).exec().into())
}
pub fn enter_chroot(root: &str) -> Result<()> {
    let root = CString::new(root)?;
    // SAFETY: this dedicated privileged collector uses the same mount view as
    // the environment; chdir immediately discards the old working directory.
    ensure!(
        unsafe { libc::chroot(root.as_ptr()) } == 0,
        "chroot collector: {}",
        std::io::Error::last_os_error()
    );
    std::env::set_current_dir("/")?;
    Ok(())
}
pub fn enter_root(root: &str) -> Result<()> {
    std::env::set_current_dir(root)?;
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let old = format!(".oldroot-{}-{suffix}", std::process::id());
    fs::create_dir(&old)?;
    let dot = CString::new(".")?;
    let old_c = CString::new(old.as_str())?;
    // SAFETY: NUL-terminated paths remain alive for the Linux syscall.
    ensure!(
        unsafe { libc::syscall(libc::SYS_pivot_root, dot.as_ptr(), old_c.as_ptr()) } == 0,
        "pivot_root: {}",
        std::io::Error::last_os_error()
    );
    std::env::set_current_dir("/")?;
    let detach = CString::new(format!("/{old}"))?;
    ensure!(
        unsafe { libc::umount2(detach.as_ptr(), libc::MNT_DETACH) } == 0,
        "detach old root: {}",
        std::io::Error::last_os_error()
    );
    fs::remove_dir(format!("/{old}"))?;
    Ok(())
}

#[cfg(test)]
mod mount_tests {
    use super::readonly_mounts;
    use std::path::Path;

    #[test]
    fn only_unambiguous_readonly_mounts_skip_remount() {
        let mounts = readonly_mounts(
            "1 0 0:1 / /store/ro ro,relatime - ext4 /dev/a rw\n\
             2 0 0:1 / /store/rw rw,relatime - ext4 /dev/a ro\n\
             3 0 0:1 / /store/stacked ro - ext4 /dev/a rw\n\
             4 3 0:2 / /store/stacked rw - ext4 /dev/b rw\n\
             malformed line\n",
        );
        assert_eq!(mounts.len(), 1);
        assert!(mounts.contains(Path::new("/store/ro")));
        assert!(!mounts.contains(Path::new("/store/rw")));
        assert!(!mounts.contains(Path::new("/store/stacked")));
        assert!(!mounts.contains(Path::new("/store/missing")));
    }
}

#[cfg(test)]
mod bootstrap_tests {
    use super::*;
    #[test]
    fn replacing_runtime_roots_releases_old_versions_without_touching_other_roots() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let old = "/nix/store/00000000000000000000000000000000-old";
        let current = "/nix/store/11111111111111111111111111111111-current";
        roots_named(dir.path(), [old], "operator")?;
        replace_bootstrap_roots(dir.path(), &[old])?;
        replace_bootstrap_roots(dir.path(), &[current])?;
        let roots = dir.path().join("nix/var/nix/gcroots");
        ensure!(fs::read_dir(roots.join("distributed-nix-bootstrap"))?.count() == 1);
        ensure!(
            fs::read_link(
                roots
                    .join("distributed-nix-bootstrap")
                    .join(Path::new(current).file_name().unwrap())
            )? == Path::new(current)
        );
        ensure!(
            fs::read_link(
                roots
                    .join("operator")
                    .join(Path::new(old).file_name().unwrap())
            )? == Path::new(old)
        );
        Ok(())
    }
}
