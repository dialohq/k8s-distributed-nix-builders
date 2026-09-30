use distributed_nix::gc::{Snapshot, plan};
use std::collections::{BTreeMap, BTreeSet};
fn path(n: u8) -> String {
    format!("/nix/store/{n:032}-package")
}
fn snapshot(live: &[u8], edges: &[(u8, &[u8])]) -> Snapshot {
    Snapshot {
        metadata: BTreeMap::new(),
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
        metadata: BTreeMap::new(),
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
fn oldest_first_selection_preserves_newer_closures_and_live_paths() {
    use distributed_nix::gc::{PathMetadata, oldest_first};
    let mut s = snapshot(&[1], &[(1, &[2]), (2, &[]), (3, &[4]), (4, &[]), (5, &[])]);
    s.metadata = (1..=5)
        .map(|n| {
            (
                path(n),
                PathMetadata {
                    nar_size: 10,
                    registered_at: n as u64,
                },
            )
        })
        .collect();
    let snapshots = vec![s.clone(), s];
    let all = plan(&"a".repeat(32), &snapshots).unwrap();
    let selected = oldest_first(all, &snapshots, &[0, 10]).unwrap();
    assert_eq!(selected.origin, [path(3)].into_iter().collect());
    assert!(selected.keep.contains(&path(4)) && selected.keep.contains(&path(5)));
    let all = plan(&"a".repeat(32), &snapshots).unwrap();
    let selected = oldest_first(all, &snapshots, &[100, 100]).unwrap();
    assert_eq!(
        selected.origin,
        [path(3), path(4), path(5)].into_iter().collect()
    );
    assert!(selected.keep.contains(&path(1)) && selected.keep.contains(&path(2)));
}

#[test]
fn eviction_includes_referrers_cycles_and_fails_without_metadata() {
    use distributed_nix::gc::{PathMetadata, oldest_first};
    let mut s = snapshot(&[], &[(1, &[2]), (2, &[1]), (3, &[1]), (4, &[])]);
    s.metadata = (1..=4)
        .map(|n| {
            (
                path(n),
                PathMetadata {
                    nar_size: 10,
                    registered_at: n as u64,
                },
            )
        })
        .collect();
    let snapshots = vec![s.clone(), s.clone()];
    let all = plan(&"a".repeat(32), &snapshots).unwrap();
    let selected = oldest_first(all.clone(), &snapshots, &[0, 1]).unwrap();
    assert_eq!(
        selected.origin,
        [path(1), path(2), path(3)].into_iter().collect()
    );
    s.metadata.remove(&path(4));
    assert!(oldest_first(all, &[s.clone(), s], &[0, 1]).is_err());
}

#[test]
fn old_dependencies_do_not_evict_recent_builds_before_older_garbage() {
    use distributed_nix::gc::{PathMetadata, oldest_first};
    let mut s = snapshot(&[], &[(1, &[]), (2, &[]), (3, &[1]), (4, &[3])]);
    s.metadata = [(1, 1), (2, 20), (3, 10), (4, 100)]
        .into_iter()
        .map(|(n, age)| {
            (
                path(n),
                PathMetadata {
                    nar_size: 10,
                    registered_at: age,
                },
            )
        })
        .collect();
    let snapshots = [s.clone(), s];
    let all = plan(&"a".repeat(32), &snapshots).unwrap();
    let selected = oldest_first(all, &snapshots, &[0, 10]).unwrap();
    assert_eq!(selected.origin, BTreeSet::from([path(2)]));
    assert_eq!(selected.keep, BTreeSet::from([path(1), path(3), path(4)]));
}
