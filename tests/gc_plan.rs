use distributed_nix::gc::{Snapshot, plan};
use std::collections::{BTreeMap, BTreeSet};
fn path(n: u8) -> String {
    format!("/nix/store/{n:032}-package")
}
fn snapshot(live: &[u8], edges: &[(u8, &[u8])]) -> Snapshot {
    Snapshot {
        live: live.iter().map(|n| path(*n)).collect(),
        graph: edges
            .iter()
            .map(|(n, rs)| (path(*n), rs.iter().map(|r| path(*r)).collect()))
            .collect(),
    }
}
#[test]
fn union_roots_keep_cross_node_dependencies_and_dead_cycles_collect() {
    let a = snapshot(&[1], &[(1, &[2]), (2, &[]), (4, &[5]), (5, &[4])]);
    let b = snapshot(&[], &[(1, &[3]), (2, &[]), (3, &[])]);
    let empty = Snapshot {
        live: BTreeSet::new(),
        graph: BTreeMap::new(),
    };
    let p = plan(&"a".repeat(32), &[a.clone(), b, empty, a]).unwrap();
    assert_eq!(p.keep, [path(1), path(2), path(3)].into_iter().collect());
    assert_eq!(p.origin, [path(4), path(5)].into_iter().collect());
}
#[test]
fn missing_node_and_malformed_or_self_conflicting_plan_fail_closed() {
    let s = snapshot(&[], &[(1, &[])]);
    assert!(plan(&"a".repeat(32), std::slice::from_ref(&s)).is_err());
    let mut p = plan(&"a".repeat(32), &[s.clone(), s.clone(), s.clone(), s]).unwrap();
    p.keep.insert(path(1));
    assert!(p.validate().is_err());
    p.keep.clear();
    p.origin.insert("/etc/passwd".into());
    assert!(p.validate().is_err());
}

#[test]
fn maintenance_policy_rejects_normal_gc_before_freezing() {
    let dir = tempfile::tempdir().unwrap();
    let node = distributed_nix::node::Node {
        base: dir.path().into(),
        ..Default::default()
    };
    std::fs::write(dir.path().join("gc-maintenance-only"), "ARC\n").unwrap();
    let error = node.dispatch(&["gc-preflight".into()]).unwrap_err();
    assert!(error.to_string().contains("administrator coordinator"));
    assert!(!dir.path().join("gc-active.json").exists());
    assert!(node.dispatch(&["gc-preflight-maintenance".into()]).is_ok());
}
