# Portable deployment validation

## ARC release 0.4.0

The [native and privileged tests](https://github.com/dialohq/k8s-distributed-nix-builders/actions/runs/36351582469)
and [Helm release workflow](https://github.com/dialohq/k8s-distributed-nix-builders/actions/runs/36351586577)
passed for commit `ce3aa682589c10406407c1524dafb70c390d02f4`. The latter adds a real
unprivileged ARC client and GitHub runner failure/cleanup test to the ten existing
Kubernetes checks. The privileged process test verifies cleanup of a detached
child using kernel cgroup events, without elapsed-time assertions.

On Cibox, actual default-ARC [main.yml](https://github.com/dialohq/dialo/actions/runs/36352536278)
and [C++ build/reuse/shell validation](https://github.com/dialohq/dialo/actions/runs/36352537773)
passed on OpenEBS LVM PVCs. Deleting an idle ARC pod released its cgroup, workspace
and native root lease; online GC then pruned its root group. These live tests are
additional to the vanilla kind tests. CI application pods have no hostPath mounts.

Published artifacts:

- Generic image `:0.4.0`: `sha256:e7d952c4d8081f03b3e073855f5378d435a36ec24646f4f674fc4efea91a2a1d`.
- ARC builder image `:0.4.0-arc`: `sha256:0bf2db5d4f01495359c51662b557c3cb1820af5a0e17631511d6693cc1188ff7`.
- Helm chart `0.4.0`: `sha256:e072783222a8ddd29b3252e72baab2dfb6d252bc411a06a4db15e439998cff3b`.

## Earlier pool validation

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
