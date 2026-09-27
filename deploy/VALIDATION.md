# Portable deployment validation

The kernel-NFS image with the Rust pod supervisor was exercised on a disposable
namespace in a three-node Kubernetes cluster on 2026-09-27. Four independent
OpenEBS LVM PVCs hold the shared collection and private stores. Pods have no
hostPath volumes, host PID/network namespace, host Nix store, or Kubernetes API
token. RPC and NFS use Kubernetes Services.

The final `deploy/e2e.py` run passed all nine checks in **58.38 seconds** after
pod recreation, using the unmodified Helm configuration:

- Native sandboxed build and reuse on both peers with builds and binary-cache
  substitution disabled. `findmnt` confirmed NFS-backed output paths.
- Content-addressed output/realisations reused on both peers.
- Builder replacement recovered private metadata and shared mounts.
- Store replacement retained data and recovered existing client mounts. Reading
  a newly generated file forces a network request instead of accepting cached data.
- SIGKILL of the Rust supervisor restarted the container inside the same pod.
  NFSD recovered in the surviving network namespace; every builder could see a
  newly created file afterward.
- A missing participant prevented shared GC deletion.
- Killing the GC coordinator after durable retirement barriers, then resuming.
- A native build explicitly blocked on a FIFO while online GC ran, then completed
  after the test released it. No guessed build duration or startup sleep.
- Once all builders that touched an output were replaced, GC reclaimed that output.

The native suite passed **41 tests**. The separately invoked privileged mount
recovery/concurrent admission test passed after replacing mount utilities with
syscalls. Supervisor tests use socket acknowledgements to exercise child failure
and termination/reaping after startup failure. Helm lint passed.

The suite duration is not a build benchmark. These are synthetic derivations;
they do not measure `dialo/main.yml` performance or prove compatibility with every
CSI driver/CNI. Existing ARC workloads remain on their current deployment during
this isolated validation.

The earlier userspace-NFS version passed the deployment suite in kind on Ubuntu
in [run 36338175220](https://github.com/dialohq/k8s-distributed-nix-builders/actions/runs/36338175220).
That result is historical, not evidence for the current kernel-NFS image.
`.github/workflows/helm.yaml` now loads `nfs` and `nfsd` on the Ubuntu host and runs
the nine checks with multiple independent builders on one kind node before
optional image/chart publication.

See [RACES.md](../RACES.md) for the concurrency audit, deterministic regression
tests, and remaining assumptions.
