use anyhow::{Result, ensure};
use distributed_nix::{
    admissions::Admissions,
    linux, native,
    node::{Kind, Node},
    online,
    util::{Lock, durable, output},
};
use serde_json::json;
use std::{
    collections::BTreeSet,
    fs,
    os::unix::{
        fs::{MetadataExt, symlink},
        process::ExitStatusExt,
    },
    path::{Path, PathBuf},
    process::Command,
};

fn node(directory: &Path) -> Node {
    Node {
        base: directory.join("state"),
        root: directory.join("worker"),
        origin: directory.join("origin"),
        lower: directory.join("origin"),
    }
}

fn fixture(directory: &Path) -> Result<(Node, Vec<String>, PathBuf)> {
    let node = node(directory);
    fs::create_dir_all(node.root.join("nix/store"))?;
    let mut paths = Vec::new();
    for name in ["directory", "large-file"] {
        let input = directory.join(name);
        if name == "directory" {
            fs::create_dir(&input)?;
            fs::write(input.join("payload"), vec![b'x'; 262144])?;
        } else {
            fs::write(&input, vec![b'y'; 262144])?;
        }
        let added = output(
            Command::new("nix-store")
                .args(["--option", "build-users-group", "", "--store"])
                .arg(&node.origin)
                .arg("--add")
                .arg(input),
        )?;
        paths.push(String::from_utf8(added.stdout)?.trim().to_owned());
    }
    native::copy(&node.origin, node.root.to_str().unwrap(), &paths)?;
    let manifest = native::dump(&node.origin, &paths)?;
    let file = directory.join("manifest.json");
    durable(&file, &manifest)?;
    durable(
        &node
            .origin
            .join(".distributed-nix-publications")
            .join(format!("{}.json", manifest.id()?)),
        &manifest,
    )?;
    node.admit(&file, false)?;
    // A second overlapping batch must be updated together with the first.
    let subset = native::dump(&node.origin, &paths[..1])?;
    durable(
        &node
            .origin
            .join(".distributed-nix-publications")
            .join(format!("{}.json", subset.id()?)),
        &subset,
    )?;
    let second = directory.join("subset.json");
    durable(&second, &subset)?;
    node.admit(&second, false)?;
    durable(&node.base.join("ready"), &json!({"ready":true}))?;
    Ok((node, paths, file))
}

fn target(node: &Node, path: &str) -> PathBuf {
    node.root.join(path.trim_start_matches('/'))
}

fn verify(node: &Node, paths: &[String]) -> Result<()> {
    for path in paths {
        let to = target(node, path);
        let from = node.lower.join(path.trim_start_matches('/'));
        ensure!(linux::mountpoint(&to)?);
        let a = to.metadata()?;
        let b = from.metadata()?;
        ensure!((a.dev(), a.ino()) == (b.dev(), b.ino()));
        let writable = if a.is_dir() {
            to.join("payload")
        } else {
            to.clone()
        };
        ensure!(fs::write(writable, "must not write").is_err());
        output(
            Command::new("nix-store")
                .args(["--option", "build-users-group", "", "--store"])
                .arg(&node.root)
                .arg("--verify-path")
                .arg(path),
        )?;
    }
    let db = Admissions::open(&node.base.join("admissions"))?;
    ensure!(db.relocations()?.is_empty());
    for id in db.ids()? {
        let journal = db.get(&id)?.unwrap();
        for (path, kind) in journal.plan {
            ensure!(
                kind == if path.ends_with("-directory") {
                    Kind::MountDir
                } else {
                    Kind::MountFile
                }
            );
        }
    }
    Ok(())
}

#[test]
fn relocation_child() -> Result<()> {
    if let Some(directory) = std::env::var_os("RELOCATION_CHILD") {
        node(Path::new(&directory)).relocate(&[], &BTreeSet::new())?;
    }
    Ok(())
}

#[test]
#[ignore = "requires mount privileges; runs in a private mount namespace"]
fn mounted_relocation_reclaims_payloads_and_recovers_crashes() -> Result<()> {
    if std::env::var_os("RELOCATION_NAMESPACE").is_none() {
        let status = Command::new("unshare")
            .args(["--mount", "--propagation", "private", "--"])
            .arg(std::env::current_exe()?)
            .args([
                "--exact",
                "mounted_relocation_reclaims_payloads_and_recovers_crashes",
                "--ignored",
                "--nocapture",
            ])
            .env("RELOCATION_NAMESPACE", "1")
            .status()?;
        ensure!(status.success());
        return Ok(());
    }
    let temp = tempfile::tempdir()?;
    let (node, paths, file) = fixture(temp.path())?;
    ensure!(node.dispatch(&["resume-relocation".to_owned()])?["resumed"] == true);
    let metadata = native::dump(&node.root, &paths)?;
    durable(&node.base.join("online-gc.json"), &json!({"id":"pending"}))?;
    ensure!(node.relocate(&[], &BTreeSet::new())?["deferred"] == "online GC active");
    fs::remove_file(node.base.join("online-gc.json"))?;
    let source = node.lower.join(paths[0].trim_start_matches('/'));
    let unavailable = source.with_extension("unavailable");
    fs::rename(&source, &unavailable)?;
    ensure!(node.relocate(&[], &BTreeSet::new()).is_err());
    ensure!(node.base.join("ready").exists());
    ensure!(target(&node, &paths[0]).join("payload").metadata()?.len() == 262144);
    fs::rename(&unavailable, &source)?;
    let gate = Lock::acquire(&node.base.join("maintenance.lock"), true)?;
    ensure!(node.relocate(&[], &BTreeSet::new())?["deferred"].is_string());
    drop(gate);
    let group = online::group(&node.base, &node.root, Some("arc-live"))?;
    let roots = node
        .root
        .join("nix/var/nix/gcroots/distributed-nix-clients/pod-arc-live");
    symlink(&paths[1], roots.join("active"))?;
    symlink(
        "/nix/store/00000000000000000000000000000000-not-built",
        roots.join("negative-lookup"),
    )?;
    ensure!(node.relocate(&paths[..1], &BTreeSet::new())?["relocated"] == 0);
    ensure!(!linux::mountpoint(&target(&node, &paths[1]))?);
    drop(group);
    ensure!(node.relocate(&paths[..1], &BTreeSet::new())?["relocated"] == 1);
    ensure!(!linux::mountpoint(&target(&node, &paths[0]))?);
    ensure!(node.relocate(&[], &BTreeSet::new())?["relocated"] == 1);
    verify(&node, &paths)?;
    ensure!(native::dump(&node.root, &paths)?.paths == metadata.paths);
    for path in &paths {
        let to = target(&node, path);
        linux::unmount(&to)?;
        if to.is_dir() {
            ensure!(fs::read_dir(&to)?.next().is_none());
        } else {
            ensure!(to.metadata()?.len() == 0);
        }
    }
    node.admit(&file, true)?;
    verify(&node, &paths)?;
    for path in &paths {
        linux::unmount(&target(&node, path))?;
    }

    for point in [
        "relocation-after-intent",
        "relocation-after-delete",
        "relocation-after-mount",
        "relocation-before-database-commit",
        "relocation-after-database-commit",
    ] {
        let temp = tempfile::tempdir()?;
        let (node, paths, _) = fixture(temp.path())?;
        let status = Command::new(std::env::current_exe()?)
            .args(["--exact", "relocation_child", "--nocapture"])
            .env("RELOCATION_CHILD", temp.path())
            .env("DISTRIBUTED_NIX_FAILPOINT", point)
            .status()?;
        ensure!(
            status.signal() == Some(9),
            "failpoint not reached: {point}: {status}"
        );
        ensure!(!node.base.join("ready").exists());
        // Mounts disappear across pod replacement, while the local PVC survives.
        for path in &paths {
            let to = target(&node, path);
            if linux::mountpoint(&to)? {
                linux::unmount(&to)?;
            }
        }
        node.resume_relocation()?;
        node.resume_relocation()?;
        ensure!(!node.base.join("relocation.json").exists());
        verify(&node, &paths)?;
        for path in &paths {
            linux::unmount(&target(&node, path))?;
        }
    }

    // Nondeterministic input-addressed outputs must never switch to different bytes.
    let temp = tempfile::tempdir()?;
    let (node, paths, _) = fixture(temp.path())?;
    let variant = "/nix/store/00000000000000000000000000000000-variant".to_owned();
    for (root, payload) in [(&node.origin, &paths[0]), (&node.root, &paths[1])] {
        let mut info =
            native::dump(&node.origin, std::slice::from_ref(payload))?.paths[payload].clone();
        info["ca"] = serde_json::Value::Null;
        let destination = root.join(variant.trim_start_matches('/'));
        let source = node.origin.join(payload.trim_start_matches('/'));
        if source.is_dir() {
            fs::create_dir(&destination)?;
            fs::copy(source.join("payload"), destination.join("payload"))?;
        } else {
            fs::copy(source, destination)?;
        }
        let manifest = distributed_nix::manifest::Manifest::parse(
            json!({"version":1,"roots":[variant],"paths":{&variant:info}}),
        )?;
        native::register(root, &manifest)?;
    }
    let shared = native::dump(&node.origin, std::slice::from_ref(&variant))?;
    let local = native::dump(&node.root, std::slice::from_ref(&variant))?;
    let file = temp.path().join("variant.json");
    durable(&file, &shared)?;
    durable(
        &node
            .origin
            .join(".distributed-nix-publications")
            .join(format!("{}.json", shared.id()?)),
        &shared,
    )?;
    ensure!(node.admit(&file, false)?["local_variants"] == json!([variant]));
    ensure!(
        !Admissions::open(&node.base.join("admissions"))?
            .relocations()?
            .contains_key(&variant)
    );
    node.relocate(&[], &BTreeSet::new())?;
    ensure!(native::dump(&node.root, std::slice::from_ref(&variant))?.paths == local.paths);
    ensure!(!linux::mountpoint(&target(&node, &variant))?);
    ensure!(target(&node, &variant).metadata()?.len() == 262144);
    for path in &paths {
        linux::unmount(&target(&node, path))?;
    }
    Ok(())
}
