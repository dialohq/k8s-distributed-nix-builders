use anyhow::{Result, ensure};
use distributed_nix::{
    admissions::Admissions,
    manifest::Manifest,
    native,
    node::{Journal, Kind, Node, Status},
    util::{durable, output},
};
use serde_json::json;
use std::{
    collections::BTreeMap, fs, os::unix::process::ExitStatusExt, path::Path, process::Command,
};

fn journal(name: &str, kind: Kind) -> Journal {
    let path = format!("/nix/store/00000000000000000000000000000000-{name}");
    Journal {
        manifest: Manifest::parse(json!({"version":1,"roots":[path],"paths":{&path:{"narHash":"sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=","narSize":120,"references":[],"ca":null,"signatures":[],"ultimate":true}}})).unwrap(),
        plan: BTreeMap::from([(path, kind)]),
        status: Status::Pending,
    }
}

fn child(directory: &Path, action: &str, failpoint: &str) -> Result<()> {
    let status = Command::new(std::env::current_exe()?)
        .args(["--exact", "subprocess", "--nocapture"])
        .env("ADMISSIONS_TEST_DIRECTORY", directory)
        .env("ADMISSIONS_TEST_ACTION", action)
        .env("DISTRIBUTED_NIX_FAILPOINT", failpoint)
        .status()?;
    ensure!(
        status.signal() == Some(9),
        "failpoint was not reached: {status}"
    );
    Ok(())
}

#[test]
fn subprocess() -> Result<()> {
    let Some(directory) = std::env::var_os("ADMISSIONS_TEST_DIRECTORY") else {
        return Ok(());
    };
    let directory = Path::new(&directory);
    match std::env::var("ADMISSIONS_TEST_ACTION")?.as_str() {
        "insert" => {
            Admissions::open(directory)?.begin(&journal("new", Kind::Copy))?;
        }
        "admit" => {
            node(directory).admit(&directory.join("manifest.json"), false)?;
        }
        _ => panic!("unknown test action"),
    }
    Ok(())
}

#[test]
fn failed_and_killed_transactions_never_leave_partial_plans() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let mut db = Admissions::open(temp.path())?;
    let old = journal("old", Kind::Local);
    db.begin(&old)?;
    drop(db);
    child(temp.path(), "insert", "admissions-before-commit")?;
    let mut db = Admissions::open(temp.path())?;
    let new = journal("new", Kind::Copy);
    ensure!(db.get(&new.manifest.id()?)?.is_none());
    ensure!(db.known(new.plan.keys())?.is_empty());
    let mut conflicting = journal("old", Kind::MountDir);
    conflicting
        .manifest
        .paths
        .extend(new.manifest.paths.clone());
    conflicting.plan.extend(new.plan.clone());
    ensure!(db.begin(&conflicting).is_err());
    ensure!(db.known(new.plan.keys())?.is_empty());
    ensure!(db.ids()? == vec![old.manifest.id()?]);
    ensure!(db.known(old.plan.keys())? == old.plan);
    db.begin(&new)?;
    db.commit(&new.manifest.id()?)?;
    ensure!(db.ids()?.len() == 2);
    Ok(())
}

#[test]
fn corrupt_index_data_fail_closed() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let j = journal("old", Kind::Copy);
    let id = j.manifest.id()?;
    let mut db = Admissions::open(temp.path())?;
    db.begin(&j)?;
    let connection = rusqlite::Connection::open(temp.path().join("admissions.sqlite"))?;
    connection.execute("UPDATE paths SET kind='local'", [])?;
    ensure!(db.get(&id).is_err());
    connection.execute("PRAGMA user_version=2", [])?;
    ensure!(Admissions::open(temp.path()).is_err());
    Ok(())
}

#[test]
fn relocation_updates_overlapping_batches_without_rewriting_them() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let mut db = Admissions::open(temp.path())?;
    let first = journal("first", Kind::Local);
    let mut second = journal("second", Kind::Local);
    second.manifest.paths.extend(first.manifest.paths.clone());
    second.plan.extend(first.plan.clone());
    db.begin(&first)?;
    db.begin(&second)?;
    let connection = rusqlite::Connection::open(temp.path().join("admissions.sqlite"))?;
    let id = first.manifest.id()?;
    let original: String =
        connection.query_row("SELECT plan FROM batches WHERE id=?1", [&id], |r| r.get(0))?;
    connection.execute("UPDATE paths SET kind='mount-dir'", [])?;
    ensure!(db.get(&id).is_err());
    connection.execute("UPDATE paths SET kind='local'", [])?;
    let path = first.plan.keys().next().unwrap();
    let changes = BTreeMap::from([(path.clone(), Kind::MountDir)]);
    db.relocated(&changes)?;
    db.relocated(&changes)?;
    for batch in [&first, &second] {
        ensure!(db.get(&batch.manifest.id()?)?.unwrap().plan[path] == Kind::MountDir);
    }
    let unchanged: String =
        connection.query_row("SELECT plan FROM batches WHERE id=?1", [&id], |r| r.get(0))?;
    ensure!(original == unchanged);
    let checkpoint = tempfile::tempdir()?;
    let mut compacted = Admissions::open(checkpoint.path())?;
    db.for_each(|journal| compacted.begin(&journal))?;
    ensure!(compacted.get(&id)?.unwrap().plan == changes);
    Ok(())
}

fn node(directory: &Path) -> Node {
    Node {
        base: directory.join("state"),
        root: directory.join("worker"),
        origin: directory.join("origin"),
        lower: directory.join("origin"),
    }
}

#[test]
fn native_admission_recovers_every_durable_boundary() -> Result<()> {
    for point in ["after-journal", "after-mounts", "after-register"] {
        let temp = tempfile::tempdir()?;
        let node = node(temp.path());
        fs::create_dir_all(node.root.join("nix/store"))?;
        let input = temp.path().join("small-file");
        fs::write(&input, "real native Nix admission")?;
        let added = output(
            Command::new("nix-store")
                .args(["--option", "build-users-group", "", "--store"])
                .arg(&node.origin)
                .arg("--add")
                .arg(&input),
        )?;
        let path = String::from_utf8(added.stdout)?.trim().to_owned();
        let manifest = native::dump(&node.origin, &[path.clone()])?;
        let id = manifest.id()?;
        durable(
            &node
                .origin
                .join(".distributed-nix-publications")
                .join(format!("{id}.json")),
            &manifest,
        )?;
        durable(&temp.path().join("manifest.json"), &manifest)?;
        child(temp.path(), "admit", point)?;
        let db = Admissions::open(&node.base.join("admissions"))?;
        ensure!(matches!(db.get(&id)?.unwrap().status, Status::Pending));
        drop(db);
        node.admit(&temp.path().join("manifest.json"), true)?;
        node.admit(&temp.path().join("manifest.json"), false)?;
        ensure!(native::dump(&node.root, &[path.clone()])?.paths == manifest.paths);
        ensure!(fs::read(node.root.join(path.trim_start_matches('/')))? == fs::read(input)?);
        let db = Admissions::open(&node.base.join("admissions"))?;
        ensure!(matches!(db.get(&id)?.unwrap().status, Status::Committed));
        ensure!(db.get(&id)?.unwrap().plan[&path] == Kind::Copy);
    }
    Ok(())
}

#[test]
fn later_batches_preserve_shared_and_local_path_classification() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let node = node(temp.path());
    fs::create_dir_all(node.root.join("nix/store"))?;
    let mut paths = Vec::new();
    for name in ["shared", "local"] {
        let input = temp.path().join(name);
        fs::write(&input, name)?;
        let added = output(
            Command::new("nix-store")
                .args(["--option", "build-users-group", "", "--store"])
                .arg(&node.origin)
                .arg("--add")
                .arg(input),
        )?;
        paths.push(String::from_utf8(added.stdout)?.trim().to_owned());
    }
    let first = native::dump(&node.origin, &paths[..1])?;
    let file = temp.path().join("manifest.json");
    durable(&file, &first)?;
    node.admit(&file, true)?;
    native::copy(&node.origin, node.root.to_str().unwrap(), &paths[1..])?;
    let next = native::dump(&node.origin, &paths)?;
    durable(&file, &next)?;
    node.admit(&file, true)?;
    let db = Admissions::open(&node.base.join("admissions"))?;
    let journal = db.get(&next.id()?)?.unwrap();
    ensure!(journal.plan[&paths[0]] == Kind::Copy);
    ensure!(journal.plan[&paths[1]] == Kind::Local);
    ensure!(db.ids()?.len() == 2);
    Ok(())
}

#[test]
#[ignore = "requires mount privileges; runs in a private mount namespace"]
fn mounted_paths_recover_and_concurrent_admissions_keep_their_kinds() -> Result<()> {
    if std::env::var_os("ADMISSIONS_MOUNT_NAMESPACE").is_none() {
        let temp = tempfile::tempdir()?;
        let status = Command::new("unshare")
            .args(["--mount", "--propagation", "private"])
            .arg(std::env::current_exe()?)
            .args([
                "--exact",
                "mounted_paths_recover_and_concurrent_admissions_keep_their_kinds",
                "--ignored",
                "--nocapture",
            ])
            .env("ADMISSIONS_MOUNT_NAMESPACE", temp.path())
            .status()?;
        ensure!(status.success());
        return Ok(());
    }
    let directory = std::env::var("ADMISSIONS_MOUNT_NAMESPACE")?;
    let directory = Path::new(&directory);
    let node = node(directory);
    fs::create_dir_all(node.root.join("nix/store"))?;
    let mut expected = BTreeMap::new();
    for (name, kind) in [
        ("directory", Kind::MountDir),
        ("large", Kind::MountFile),
        ("link", Kind::Symlink),
        ("small", Kind::Copy),
    ] {
        let input = directory.join(name);
        match kind {
            Kind::MountDir => {
                fs::create_dir(&input)?;
                fs::write(input.join("data"), "mounted directory")?;
            }
            Kind::MountFile => fs::write(&input, vec![b'x'; 131072])?,
            Kind::Symlink => std::os::unix::fs::symlink("a-relative-target", &input)?,
            Kind::Copy => fs::write(&input, "small file")?,
            _ => unreachable!(),
        }
        let added = output(
            Command::new("nix-store")
                .args(["--option", "build-users-group", "", "--store"])
                .arg(&node.origin)
                .arg("--add")
                .arg(&input),
        )?;
        expected.insert(String::from_utf8(added.stdout)?.trim().to_owned(), kind);
    }
    let paths: Vec<_> = expected.keys().cloned().collect();
    let manifest = native::dump(&node.origin, &paths)?;
    let id = manifest.id()?;
    let file = directory.join("manifest.json");
    durable(&file, &manifest)?;
    durable(
        &node
            .origin
            .join(".distributed-nix-publications")
            .join(format!("{id}.json")),
        &manifest,
    )?;
    child(directory, "admit", "after-mounts")?;
    node.admit(&file, true)?;
    ensure!(native::dump(&node.root, &paths)?.paths == manifest.paths);
    let db = Admissions::open(&node.base.join("admissions"))?;
    ensure!(db.get(&id)?.unwrap().plan == expected);
    drop(db);
    for (path, kind) in &expected {
        if matches!(kind, Kind::MountDir | Kind::MountFile) {
            let target = node.root.join(path.trim_start_matches('/'));
            let write = if *kind == Kind::MountDir {
                target.join("data")
            } else {
                target.clone()
            };
            ensure!(fs::write(write, "must fail on read-only mount").is_err());
            output(Command::new("umount").arg(target))?;
        }
    }
    node.admit(&file, true)?;
    std::thread::scope(|scope| -> Result<()> {
        let handles: Vec<_> = (0..8)
            .map(|_| scope.spawn(|| node.admit(&file, false)))
            .collect();
        for handle in handles {
            handle.join().unwrap()?;
        }
        Ok(())
    })?;
    ensure!(Admissions::open(&node.base.join("admissions"))?.known(&paths)? == expected);
    output(
        Command::new("nix")
            .args(["--extra-experimental-features", "nix-command", "--store"])
            .arg(&node.root)
            .args(["store", "verify", "--no-trust"])
            .args(&paths),
    )?;
    Ok(())
}
