# Portable deployment validation

Version `0.3.2` passed **42 native tests** and all **10 Kubernetes checks in
75.36 seconds** on the three-node OpenEBS LVM cluster. The added check replaces a
cached NFS directory ten times and immediately admits each replacement through
native Nix, verifying its new NAR without waiting for attribute-cache expiry.
The deployment uses 600-second immutable-path attribute caching; native admission
explicitly refreshes metadata. This suite still includes pod/container recovery,
missing-peer rejection and a build held at a FIFO barrier during online GC.

The kernel-NFS image with the Rust pod supervisor was exercised on a disposable
namespace in a three-node Kubernetes cluster on 2026-09-27. Four independent
OpenEBS LVM PVCs hold the shared collection and private stores. Pods have no
hostPath volumes, host PID/network namespace, host Nix store, or Kubernetes API
token. RPC and NFS use Kubernetes Services.

The final `deploy/e2e.py` run passed all ten checks in **75.36 seconds** after
pod recreation, using the unmodified Helm configuration:

- Native sandboxed build and reuse on both peers with builds and binary-cache
  substitution disabled. `findmnt` confirmed NFS-backed output paths.
- Content-addressed output/realisations reused on both peers.
- Immediate native admission after ten server-side replacements of a cached NFS directory.
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

The final image also passed all ten checks in **59.41 seconds** after installation
in kind on Ubuntu, with multiple independent builder PVCs on one Kubernetes node:
[run 36347656258](https://github.com/dialohq/k8s-distributed-nix-builders/actions/runs/36347656258).
The independent Ubuntu native/privileged test workflow passed in
[run 36347591911](https://github.com/dialohq/k8s-distributed-nix-builders/actions/runs/36347591911).

The successful kind workflow published version `0.3.2` of both artifacts.
Anonymous registry manifest requests returned HTTP 200 for both:

- Image: `ghcr.io/dialohq/k8s-distributed-nix-builders:0.3.2`,
  digest `sha256:c0301a7ced580499123501c398ef49407dc3d959778acefb4c4eca3710a35573`.
- Chart: `oci://ghcr.io/dialohq/charts/k8s-distributed-nix-builders`,
  version `0.3.2`, digest `sha256:e26baae6dcd5f7469799f6fb81919d2f165e96db175772adaa7675d377efadc4`.

The queue acknowledgement fix introduced in `0.3.1` is retained: after a closure is
committed and admitted by every participant, queued dependencies covered by that
receipt are acknowledged along with the selected roots. A deterministic regression
test covers dependencies beyond the 128-root batch, unrelated/new queue entries,
and failed admissions. Large initial imports otherwise repeatedly published
already-admitted dependencies. A one-time CI cache import was explicitly bulk
published to finish migration; its duration is not a steady-state benchmark.

See [RACES.md](../RACES.md) for the concurrency audit, deterministic regression
tests, and remaining assumptions.
