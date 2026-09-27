use distributed_nix::{
    manifest::{Manifest, valid_path},
    node::{Journal, Kind, Status},
    util::{Lock, durable, read_json, sh},
};
use serde_json::json;
use std::{collections::BTreeMap, fs, process::Command};
const P: &str = "/nix/store/00000000000000000000000000000000-example";
fn manifest() -> Manifest {
    Manifest::parse(json!({"version":1,"roots":[P],"paths":{P:{"narHash":"sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=","narSize":120,"references":[],"ca":null,"signatures":["key:signature"],"ultimate":true}}})).unwrap()
}
#[test]
fn rejects_path_traversal_and_noncanonical_paths() {
    for p in [
        "/nix/store/../etc/passwd",
        "/nix/store/00000000000000000000000000000000-a/b",
        "/nix/store/eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee-a",
        "/nix/store/00000000000000000000000000000000-",
        "/nix/store/00000000000000000000000000000000-a\nb",
    ] {
        assert!(!valid_path(p), "{p}");
    }
    assert!(valid_path(P));
}
#[test]
fn validates_complete_closure_before_side_effects() {
    let mut m = manifest();
    m.paths.get_mut(P).unwrap()["references"] =
        json!(["/nix/store/11111111111111111111111111111111-missing"]);
    assert!(m.validate().is_err());
    let mut m = manifest();
    m.roots.push("absent".into());
    assert!(m.validate().is_err());
    let mut m = manifest();
    m.version = 2;
    assert!(m.validate().is_err());
    let mut m = manifest();
    m.paths.get_mut(P).unwrap()["narSize"] = json!(-1);
    assert!(m.validate().is_err());
    let mut m = manifest();
    m.roots.push(P.into());
    assert!(m.validate().is_err());
}
#[test]
fn canonical_identity_ignores_json_key_order_and_preserves_metadata() {
    let m = manifest();
    let mut v = serde_json::to_value(&m).unwrap();
    let id = m.id().unwrap();
    assert_eq!(id, Manifest::parse(v.clone()).unwrap().id().unwrap());
    v["paths"][P]["signatures"] = json!(["new-key:signature"]);
    assert_ne!(id, Manifest::parse(v).unwrap().id().unwrap());
    assert_eq!(
        m,
        serde_json::from_slice::<Manifest>(&serde_json::to_vec(&m).unwrap()).unwrap()
    );
}
#[test]
fn tampered_journal_fails_closed() {
    let m = manifest();
    let id = m.id().unwrap();
    let mut j = Journal {
        manifest: m,
        plan: BTreeMap::from([(P.into(), Kind::MountDir)]),
        status: Status::Pending,
    };
    j.validate(&id).unwrap();
    assert!(j.validate(&"0".repeat(64)).is_err());
    j.plan.insert("/etc/passwd".into(), Kind::Local);
    assert!(j.validate(&id).is_err());
    assert!(
        serde_json::from_value::<Journal>(
            json!({"manifest":manifest(),"plan":{P:"unknown"},"status":"pending"})
        )
        .is_err()
    );
}
#[test]
fn durable_replace_survives_readers_and_leaves_no_temporary_files() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("journal.json");
    durable(&p, &json!({"generation":0})).unwrap();
    std::thread::scope(|s| {
        s.spawn(|| {
            for i in 1..100 {
                durable(&p, &json!({"generation":i})).unwrap();
            }
        });
        for _ in 0..1000 {
            assert!(read_json(&p).unwrap()["generation"].is_number());
        }
    });
    assert_eq!(read_json(&p).unwrap()["generation"], 99);
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(
        fs::metadata(&p).unwrap().permissions().mode() & 0o777,
        0o644
    );
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
}
#[test]
fn native_flock_blocks_competitor_and_releases_on_drop() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("lock");
    let l = Lock::acquire(&p, false).unwrap();
    let mut c = Command::new("flock");
    let o = c.arg("-n").arg(&p).arg("true").status().unwrap();
    assert!(!o.success());
    drop(l);
    // A concurrent test may fork while the FD is open. Its inherited CLOEXEC
    // copy is released at exec; verify bounded release, not zero-latency release.
    assert!(
        Command::new("flock")
            .args(["-w", "5"])
            .arg(&p)
            .arg("true")
            .status()
            .unwrap()
            .success()
    );
    let a = Lock::acquire(&p, true).unwrap();
    let b = Lock::acquire(&p, true).unwrap();
    drop((a, b));
}
#[test]
fn shell_quoting_preserves_literal_metacharacters() {
    let s = "a ' b\n$(touch /never-run) `false` $HOME";
    let o = Command::new("sh")
        .args(["-c", &format!("printf %s {}", sh(s))])
        .output()
        .unwrap();
    assert!(o.status.success());
    assert_eq!(o.stdout, s.as_bytes());
}

#[test]
fn durable_never_reuses_or_removes_another_process_temporary_file() {
    let dir = tempfile::tempdir().unwrap();
    for sequence in 0..200 {
        fs::write(
            dir.path()
                .join(format!(".tmp-{}-{sequence}", std::process::id())),
            b"another writer",
        )
        .unwrap();
    }
    durable(&dir.path().join("record.json"), &json!({"committed": true})).unwrap();
    for sequence in 0..200 {
        assert_eq!(
            fs::read(
                dir.path()
                    .join(format!(".tmp-{}-{sequence}", std::process::id()))
            )
            .unwrap(),
            b"another writer"
        );
    }
}
