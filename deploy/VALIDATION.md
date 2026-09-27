# Portable deployment validation

The kernel-NFS image with the Rust pod supervisor was exercised on a disposable
namespace in a three-node Kubernetes cluster on 2026-09-27. Four independent
OpenEBS LVM PVCs hold the shared collection and private stores. Pods have no
hostPath volumes, host PID/network namespace, host Nix store, or Kubernetes API
token. RPC and NFS use Kubernetes Services.

The final `deploy/e2e.py` run passed all nine checks in **60.01 seconds** after
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

The native suite passed **42 tests**. The separately invoked privileged mount
recovery/concurrent admission test passed after replacing mount utilities with
syscalls. Supervisor tests use socket acknowledgements to exercise child failure
and termination/reaping after startup failure. Helm lint passed.

The suite duration is not a build benchmark. These are synthetic derivations;
they do not measure `dialo/main.yml` performance or prove compatibility with every
CSI driver/CNI. Existing ARC workloads remain on their current deployment during
this isolated validation.

The final image also passed all nine checks in **49.31 seconds** after installation
in kind on Ubuntu, with multiple independent builder PVCs on one Kubernetes node:
[run 36345784492](https://github.com/dialohq/k8s-distributed-nix-builders/actions/runs/36345784492).
The independent Ubuntu native/privileged test workflow passed in
[run 36345091060](https://github.com/dialohq/k8s-distributed-nix-builders/actions/runs/36345091060).

The successful kind workflow published version `0.3.1` of both artifacts.
Anonymous registry manifest requests returned HTTP 200 for both:

- Image: `ghcr.io/dialohq/k8s-distributed-nix-builders:0.3.1`,
  digest `sha256:8359f8e3f543324b0aa9c49439ce78faa5a30e081e8d3a482999be42c2f9e79f`.
- Chart: `oci://ghcr.io/dialohq/charts/k8s-distributed-nix-builders`,
  version `0.3.1`, digest `sha256:c7410e90167fc525fcc316dfbcc5ca689ecdf479fa4dd14887b894cf9c7f9106`.

Version `0.3.1` also fixes publisher queue acknowledgement: after a closure is
committed and admitted by every participant, queued dependencies covered by that
receipt are acknowledged along with the selected roots. A deterministic regression
test covers dependencies beyond the 128-root batch, unrelated/new queue entries,
and failed admissions. Large initial imports otherwise repeatedly published
already-admitted dependencies. A one-time CI cache import was explicitly bulk
published to finish migration; its duration is not a steady-state benchmark.

See [RACES.md](../RACES.md) for the concurrency audit, deterministic regression
tests, and remaining assumptions.
