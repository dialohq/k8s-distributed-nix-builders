# k8s-distributed-nix-builders

Share a Nix package collection between Kubernetes build nodes while keeping Nix metadata on local storage. Designed to complement GitHub Actions Runner Controller (ARC): ordinary Nix commands run inside a builder sidecar, runners are disposable, and CI store state survives runner replacement.

## Architecture

- Each node has a persistent CI store view and native Nix database, separate from its host Nix store.
- Builder sidecars speak the native Nix daemon protocol over a pod-local Unix socket.
- A coordinator publishes completed paths into a shared POSIX collection, currently deployed over read-only NFS. Native Nix transfers and registers metadata.
- A separate local SQLite database tracks admission plans and crash recovery. No live SQLite database is shared over NFS.
- Coordinated GC stops new runner admission, collects the union of all node roots, removes worker mounts before deleting shared files, and resumes interrupted epochs.

This is an early implementation, not a drop-in Kubernetes operator. The deployed coordinator currently uses fixed three-node membership and SSH for control-plane calls. Build clients use the ordinary Nix daemon protocol. The optional gRPC/private-builder prototype is not the deployed lifecycle; see [RPC.md](RPC.md) and [EPHEMERAL.md](EPHEMERAL.md).

The Rust process owns coordination, admission, mount management, and recovery. A C ABI wrapper calls the native Nix C++ implementation for store metadata and daemon operations. Nix is pinned to **2.33.6** because that integration is version-sensitive. The current deployment targets Linux x86-64.

## Build and test

```sh
nix build
nix flake check
nix develop -c cargo test
```

The package build runs unit tests and tests against real native Nix stores, including RPC streaming and database/admission failure recovery. Privileged mount tests run separately:

```sh
sudo nix develop -c cargo test --test admissions mounted_paths -- --ignored --nocapture
```

Historical three-machine lab tests are opt-in and need their lab setup. An ignored test is not counted as a passing integration test.

## Deployment integration

Consume `packages.x86_64-linux.default` from a pinned flake input. Use the same package for host coordination/recovery/GC services and pod builder sidecars. Its closure must be available in the CI store before pods start. `contrib/kubernetes/maintenance.py` supplies the ARC admission fence used by the current deployment; it expects the `arc-runners` namespace, cluster configuration, and node-local controller state.

Cluster-specific NixOS modules, inventory, encrypted credentials, ARC Helm values, and GitOps manifests belong in the deploying infrastructure repository. None are included here. Configuration paths and commands are listed by `distributed-nix --help`.

Conflicting content-addressed realisations remain errors and retain their pending roots. The publisher handles ordinary paths separately and checks native mappings on every participant before publishing realisations. Conflicting mappings and their dependents stay queued while unrelated outputs converge. This does not make nondeterministic derivations reproducible or resolve their conflicting mappings.

The current model requires trusted builders and privileged mount operations. The shared filesystem must retain the published data while any participant still references it; a missing participant prevents GC. A single NFS origin is not highly available.

[ADMISSIONS.md](ADMISSIONS.md) describes the SQLite schema transition and its recovery contract. Binary-only rollback to a JSON-only coordinator after migration is unsafe.
