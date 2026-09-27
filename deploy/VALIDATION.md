# Portable deployment validation

The Helm chart was exercised on a disposable namespace in a three-node Kubernetes
cluster on 2026-09-27, using four independent OpenEBS LVM PVCs. The pods had no
hostPath volumes, host PID/network namespace, host Nix store, or Kubernetes API
token. The store and builder RPC endpoints used Kubernetes Service DNS.

`deploy/e2e.py` passed these checks in **79.72 seconds** after installation:

- Native sandboxed build, followed by reuse on both peers with builds and
  binary-cache substitution disabled. `findmnt` confirmed NFS-backed outputs.
- Content-addressed output/realisations reused on both peers.
- Builder pod replacement recovered private metadata and shared mounts.
- Store pod replacement retained published data and restored NFS access.
- A missing participant prevented shared GC deletion.
- Killing the GC coordinator after durable retirement barriers, then resuming.
- A native build completed while online GC ran.
- After retiring every builder pod that had touched the output, GC deleted the
  now-unrooted shared path.

This elapsed time is the integration suite duration, not a build benchmark.
The suite uses small synthetic derivations. It does not measure `dialo/main.yml`
performance or prove compatibility with every CSI driver/CNI.

The native suite passed 33 tests; the separately invoked privileged mount recovery
test also passed. The transport test sends a 1 MiB NAR through source and destination
gRPC streams, retries publication, rejects unauthenticated requests and direct GC,
and verifies publication lease release. Helm lint and the authenticated Helm test
hook passed. Chart rendering was checked with one and five builder members.

`.github/workflows/helm.yaml` runs the same deployment test in kind on Ubuntu,
including multiple independent builder PVCs on one Kubernetes node. It passed on Ubuntu in [run 36338175220](https://github.com/dialohq/k8s-distributed-nix-builders/actions/runs/36338175220):
all eight checks passed in 117.48 seconds after installation. The Helm test hook
also passed. This provides an independent non-NixOS deployment check.

The subsequent [concurrency audit](../RACES.md) replaces timing-based internal
tests with process acknowledgements and explicit state transitions. Its native
suite passes 37 tests. The build/GC deployment test now uses FIFO barriers;
it no longer assumes a build is running after an arbitrary sleep.
