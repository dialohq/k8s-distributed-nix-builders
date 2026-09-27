//! Real native Nix stores, exercised directly through the linked C ABI.
use anyhow::{Result, ensure};
use distributed_nix::{native, util::output};
use serde_json::json;
use std::{fs, process::Command};

#[test]
fn native_transaction_roundtrip_conflict_and_concurrent_calls() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let source = tmp.path().join("source-store");
    let target = tmp.path().join("target-store");
    fs::create_dir_all(&source)?;
    fs::create_dir_all(&target)?;
    let mut paths = Vec::new();
    for i in 0..2 {
        let input = tmp.path().join(format!("fixture-{i}"));
        fs::write(&input, format!("C ABI transaction fixture {i}\n"))?;
        let out = output(
            Command::new("nix-store")
                .args(["--option", "build-users-group", "", "--store"])
                .arg(&source)
                .arg("--add")
                .arg(&input),
        )?;
        paths.push(String::from_utf8(out.stdout)?.trim().to_string());
    }
    let manifest = native::dump(&source, &paths)?;
    let catalog = native::catalog(&source)?;
    ensure!(
        catalog.paths == manifest.paths,
        "catalog changed native metadata"
    );
    ensure!(catalog.roots.len() == paths.len(), "catalog omitted a path");
    ensure!(
        manifest.paths.len() == 2,
        "expected two real native records"
    );
    let preflight = native::check(&target, &manifest)?;
    ensure!(
        preflight["existing"] == json!([]),
        "unexpected existing paths"
    );
    // The controller establishes filesystem contents before metadata admission.
    for p in manifest.paths.keys() {
        let relative = p.trim_start_matches('/');
        let dest = target.join(relative);
        fs::create_dir_all(dest.parent().unwrap())?;
        fs::copy(source.join(relative), dest)?;
    }
    let registered = native::register(&target, &manifest)?;
    ensure!(
        registered["registered"] == true && registered["paths"] == 2,
        "registration failed"
    );
    let ids = [
        format!("sha256:{}!out", "1".repeat(64)),
        format!("sha256:{}!out", "0".repeat(64)),
    ];
    let mut ca = manifest.clone();
    for (index, id) in ids.iter().enumerate() {
        ca.realisations.insert(id.clone(), json!({
            "id":id, "outPath":paths[index].trim_start_matches("/nix/store/"), "signatures":[],
            "dependentRealisations": if index == 0 { json!({}) } else { json!({&ids[0]: paths[0].trim_start_matches("/nix/store/")}) },
        }));
    }
    native::register(&target, &ca)?;
    native::register(&target, &ca)?;
    let dumped = native::dump_realisations(&target, &[ca.realisations[&ids[1]].clone()])?;
    let result = distributed_nix::manifest::Manifest::parse(dumped["manifest"].clone())?;
    ensure!(
        result.realisations == ca.realisations,
        "CA dependency closure was not preserved"
    );
    let snapshot = tmp.path().join("snapshot");
    let metadata = native::snapshot(&target, &snapshot)?;
    ensure!(
        metadata["paths"].as_array().unwrap().len() == paths.len(),
        "snapshot omitted paths"
    );
    for path in &paths {
        fs::copy(
            target.join(path.trim_start_matches('/')),
            snapshot.join(path.trim_start_matches('/')),
        )?;
    }
    let restored = native::dump_realisations(&snapshot, &[ca.realisations[&ids[1]].clone()])?;
    ensure!(
        restored["manifest"]["realisations"] == serde_json::to_value(&ca.realisations)?,
        "snapshot lost CA records"
    );
    ensure!(
        native::snapshot(&target, &snapshot).is_err(),
        "snapshot overwrote an existing store"
    );
    let private = tmp.path().join("private-snapshot");
    fs::create_dir_all(private.join("nix/var/nix/db"))?;
    fs::create_dir_all(private.join("nix/store"))?;
    for name in ["db.sqlite", "schema"] {
        fs::copy(
            snapshot.join("nix/var/nix/db").join(name),
            private.join("nix/var/nix/db").join(name),
        )?;
    }
    for path in &paths {
        fs::copy(
            target.join(path.trim_start_matches('/')),
            private.join(path.trim_start_matches('/')),
        )?;
    }
    let private_ca = native::dump_realisations(&private, &[ca.realisations[&ids[1]].clone()])?;
    ensure!(
        private_ca["manifest"]["realisations"] == serde_json::to_value(&ca.realisations)?,
        "standalone database copy lost CA records"
    );
    let new_input = tmp.path().join("private-input");
    fs::write(&new_input, "only this builder owns this input")?;
    let added = output(
        Command::new("nix-store")
            .args(["--option", "build-users-group", "", "--store"])
            .arg(&private)
            .arg("--add")
            .arg(&new_input),
    )?;
    let added = String::from_utf8(added.stdout)?.trim().to_string();
    ensure!(
        native::valid_paths(&target, &[added.clone()])? == json!([]),
        "private write modified origin metadata"
    );
    ensure!(
        native::valid_paths(&snapshot, &[added])? == json!([]),
        "private write modified snapshot metadata"
    );
    let mut conflict = ca.clone();
    conflict.realisations.get_mut(&ids[0]).unwrap()["outPath"] =
        json!(paths[1].trim_start_matches("/nix/store/"));
    conflict.realisations.get_mut(&ids[1]).unwrap()["dependentRealisations"] =
        json!({&ids[0]: paths[1].trim_start_matches("/nix/store/")});
    ensure!(
        native::register(&target, &conflict)
            .unwrap_err()
            .to_string()
            .contains("conflicting realisation"),
        "CA conflict was accepted"
    );
    let independent_id = format!("sha256:{}!out", "2".repeat(64));
    conflict.realisations.insert(
        independent_id.clone(),
        json!({
            "id": independent_id, "outPath": paths[1].trim_start_matches("/nix/store/"),
            "signatures": [], "dependentRealisations": {},
        }),
    );
    let detected: Vec<String> =
        serde_json::from_value(native::realisation_conflicts(&target, &conflict)?)?;
    ensure!(detected.contains(&ids[0]) && !detected.contains(&independent_id));
    let blocked = conflict.blocked_realisations([ids[0].clone()].into_iter().collect())?;
    ensure!(
        blocked.contains(&ids[1]) && !blocked.contains(&independent_id),
        "conflict dependency propagation failed"
    );
    ensure!(
        native::realisation_conflicts(&target, &ca)? == json!([]),
        "compatible mappings rejected"
    );
    let independent = conflict.realisation_closure(&[independent_id.clone()])?;
    ensure!(independent.paths.len() == 1 && independent.realisations.len() == 1);
    native::register(&target, &independent)?;
    let blocked = conflict.realisation_closure(&[ids[1].clone()])?;
    ensure!(blocked.realisations.len() == 2, "dependency was dropped");
    ensure!(
        native::register(&target, &blocked).is_err(),
        "dependent conflict was hidden"
    );
    let kept = native::dump_realisations(&target, &[ca.realisations[&ids[1]].clone()])?;
    ensure!(
        kept["manifest"]["realisations"] == serde_json::to_value(&ca.realisations)?,
        "existing mappings changed"
    );
    ensure!(conflict.realisation_closure(&["missing".into()]).is_err());
    let mut missing = ca.clone();
    missing.realisations.remove(&ids[0]);
    ensure!(
        native::register(&target, &missing).is_err(),
        "incomplete CA closure was accepted"
    );
    let roundtrip = native::dump(&target, &paths)?;
    for (p, info) in &manifest.paths {
        for key in [
            "narHash",
            "narSize",
            "references",
            "ca",
            "deriver",
            "signatures",
        ] {
            ensure!(
                info.get(key) == roundtrip.paths[p].get(key),
                "metadata changed: {key}"
            );
        }
    }
    let mut conflict = manifest.clone();
    conflict.paths.get_mut(&paths[0]).unwrap()["narSize"] = json!(1);
    let err = native::register(&target, &conflict)
        .unwrap_err()
        .to_string();
    ensure!(
        err.contains("conflicting already-registered"),
        "native exception lost: {err}"
    );
    ensure!(
        native::admit_local(&target, &conflict, &paths, true).is_err(),
        "CA conflict bypassed by local ownership"
    );
    ensure!(
        native::canonical_manifest(&target, &conflict).is_err(),
        "CA conflict canonicalized"
    );

    let ia_origin = tmp.path().join("ia-origin");
    let ia_worker = tmp.path().join("ia-worker");
    let path = &paths[0];
    let mut canonical = manifest.clone();
    canonical.roots = vec![path.clone()];
    canonical.paths.get_mut(path).unwrap()["ca"] = serde_json::Value::Null;
    canonical.paths.get_mut(path).unwrap()["references"] = json!([&paths[1]]);
    let mut variant = canonical.clone();
    variant.paths.retain(|p, _| p == path);
    let mut alternate = manifest.paths[&paths[1]].clone();
    alternate["ca"] = serde_json::Value::Null;
    variant.paths.insert(path.clone(), alternate);
    for (root, contents, metadata) in [
        (&ia_origin, path, &canonical),
        (&ia_worker, &paths[1], &variant),
    ] {
        let destination = root.join(path.trim_start_matches('/'));
        fs::create_dir_all(destination.parent().unwrap())?;
        fs::copy(source.join(contents.trim_start_matches('/')), destination)?;
        fs::copy(
            source.join(paths[1].trim_start_matches('/')),
            root.join(paths[1].trim_start_matches('/')),
        )?;
        native::register(root, metadata)?;
    }
    ensure!(
        native::check(&ia_worker, &canonical).is_err(),
        "unowned conflict accepted"
    );
    let accepted = native::admit_local(&ia_worker, &canonical, &[path.clone()], true)?;
    ensure!(
        accepted["local_variants"] == json!([path]),
        "local variant not reported"
    );
    let kept = native::dump(&ia_worker, &[path.clone()])?;
    ensure!(
        kept.paths[path]["narHash"] == variant.paths[path]["narHash"],
        "local metadata overwritten"
    );
    let normalized = native::canonical_manifest(&ia_origin, &variant)?;
    ensure!(
        normalized.paths[path]["narHash"] == canonical.paths[path]["narHash"],
        "origin canonical output replaced"
    );
    ensure!(
        normalized.paths.contains_key(&paths[1]),
        "canonical dependency omitted from closure"
    );
    for root in [&ia_origin, &ia_worker] {
        output(
            Command::new("nix-store")
                .arg("--store")
                .arg(root)
                .args(["--verify", "--check-contents"]),
        )?;
    }
    // Concurrent Rust threads share the ABI's serialized Nix entrypoint. Every
    // call reopens the store, so stale negative lookup caches cannot survive.
    std::thread::scope(|s| -> Result<()> {
        let threads: Vec<_> = (0..8)
            .map(|_| {
                s.spawn(|| -> Result<()> {
                    for _ in 0..8 {
                        let checked = native::check(&target, &manifest)?;
                        ensure!(
                            checked["existing"].as_array().unwrap().len() == 2,
                            "stale metadata"
                        );
                        ensure!(
                            native::dump(&target, &paths)?.paths == roundtrip.paths,
                            "transaction corrupted"
                        );
                    }
                    Ok(())
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap()?;
        }
        Ok(())
    })?;
    let integrity = output(
        Command::new("nix-store")
            .args(["--store"])
            .arg(&target)
            .args(["--verify", "--check-contents"]),
    )?;
    ensure!(
        integrity.status.success(),
        "native contents verification failed"
    );

    let root = target.join("nix/var/nix/gcroots/test-keep");
    std::os::unix::fs::symlink(&paths[0], &root)?;
    let snapshot = native::gc_snapshot(&target)?;
    ensure!(
        snapshot["live"]
            .as_array()
            .unwrap()
            .contains(&json!(paths[0])),
        "native GC lost root"
    );
    let error = native::gc_delete(&target, &[paths[0].clone()].into_iter().collect()).unwrap_err();
    ensure!(
        error.to_string().contains("still alive"),
        "GC disabled native liveness checks"
    );
    let safety = target.join("nix/var/nix/gcroots/distributed-nix");
    fs::create_dir_all(&safety)?;
    std::os::unix::fs::symlink(&paths[1], safety.join("shared"))?;
    let online = native::online_snapshot(&target)?;
    ensure!(
        online["live"]
            .as_array()
            .unwrap()
            .contains(&json!(paths[0])),
        "online mark lost an ordinary root"
    );
    ensure!(
        !online["live"]
            .as_array()
            .unwrap()
            .contains(&json!(paths[1])),
        "synthetic sharing pin made dead paths immortal"
    );
    let ordinary = native::gc_snapshot(&target)?;
    ensure!(
        ordinary["live"]
            .as_array()
            .unwrap()
            .contains(&json!(paths[1]))
    );
    fs::remove_file(safety.join("shared"))?;
    fs::remove_file(root)?;
    let dead = paths.iter().cloned().collect();
    native::gc_delete(&target, &dead)?;
    native::gc_delete(&target, &dead)?; // Idempotent deletion after a lost acknowledgement.
    ensure!(
        native::check(&target, &manifest)?["existing"] == json!([]),
        "GC left metadata behind"
    );
    Ok(())
}
