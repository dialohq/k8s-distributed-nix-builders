# Concurrency and timing audit

Reviewed publication, admission recovery, pod-root lifetime, online GC, native
Nix callbacks, RPC cancellation, and deployment tests on 2026-09-27.

## Findings and changes

| Finding | Resolution |
| --- | --- |
| Outbox acknowledgement could run between entry creation and GC-root creation, leaving an orphan root. Queue readers could also race entry deletion. | Enqueue, acknowledgement, and queue reads share `outbox.lock`. Mutation holds it across both files; readers hold it while constructing the native metadata snapshot. Applies to ordinary and content-addressed queues. |
| Two first-time service constructors could observe absent membership and persist different configurations. | `membership.lock` serializes the read/validate/create operation. A barrier-controlled test verifies exactly one configuration wins. |
| Wall-clock GC IDs could collide after a clock adjustment and reuse old acknowledgement files. | IDs use OS randomness. Atomic directory creation reserves an epoch; an existing directory is an error, never a resumable fresh epoch. Only the durable master record selects an epoch to resume. |
| Pivot-root temporary names depended on time and PID. | `tempfile` atomically creates a unique directory. |
| Lock tests assumed dropping one descriptor immediately released a lock, even while parallel tests could have forked copies. | Dedicated owner processes acknowledge acquisition and explicit drop through Unix sockets. Assertions use nonblocking lock probes after those acknowledgements. Production flock lifetime semantics are preserved. |
| A retirement test treated 100 ms without a reply as evidence of blocking. | It directly tests rejection while the durable fence exists and acceptance after the finish transition. |
| The build/GC test slept to guess whether a build was active. | The sandboxed build acknowledges startup through a FIFO, waits on a second FIFO, and is released only after GC completes. |
| Plan subset validation used `zip`, which alone permits truncated membership. | The node also checks equal membership lengths before comparing sets, independent of RPC validation. |

`tests/transport.rs` waits for the actual exclusive publication gate after
cancelling streams. It does not use a grace-period sleep as evidence that leases
were released. The outbox test pauses a producer between its two filesystem
updates and probes the lock at that exact point.

## Safety ordering checked

- The coordinator excludes publication while marking and retiring paths.
  Unfinished epochs keep durable markers; there is no expiry-based unlock.
- Admission takes the root gate before admission, native GC, and path locks.
  Retirement installs its marker under the exclusive root gate. It does not
  retain that gate while waiting for native builds or sweeping.
- Root pruning holds the exclusive root gate and only *tries* the group lease;
  it never waits for a live process while holding that gate.
- The final mark follows retirement installation on every participant. Paths
  pinned before the barrier survive; new requests for fenced paths retry only
  after observing the marker removed.
- Worker mount removal and native deletion precede durable acknowledgements.
  Shared origin deletion requires every configured worker acknowledgement.
  Missing participants fail closed.
- Queue mutation completes before the native registration transaction begins.
  Admission and GC snapshots can read queues under the shared queue lock without
  an inverse queue-to-admission lock acquisition.
- RPC native-transfer tasks retain their file gate while killing and reaping
  the child. Pod replacement additionally requires exclusive PVC ownership;
  this does not replace Kubernetes/storage fencing on node failure.

## Remaining timers

Network connection deadlines, Kubernetes readiness polling, and subprocess
watchdogs bound interactions with external services/processes. Publisher and
automatic-GC intervals schedule work. Native retirement polling yields between
checks and keeps Nix cancellation responsive; the locked durable-state check,
not elapsed time, grants access. SQLite's busy timeout bounds contention errors;
transactions and fences provide atomicity. `Instant` measurements are reporting
only. No timer authorizes shared deletion or expires a retirement fence.

Kernel NFS lease/grace periods govern the external NFS protocol, not Nix GC
eligibility. Client recovery records persist on the store PVC through `nfsdcld`.
The Rust pod supervisor owns and reaps each child, handles SIGTERM/SIGINT, and
stops its network namespace's NFSD threads before stopping NFS helper daemons.
Container restart also resets NFSD threads left in the surviving pod network
namespace. Native daemon readiness is an explicit pipe acknowledgement. Tests
exercise unexpected child exit and cleanup after startup failure using socket
acknowledgements, without sleeps. The earlier shell supervisor was removed;
its shutdown trap could spin over an already-reaped Bash child.

Linux flock ownership follows open file descriptions, including duplicates
inherited by fork; it is not a process-local mutex. See the
[Linux flock documentation](https://man7.org/linux/man-pages/man2/flock.2.html).

This is a targeted audit with deterministic regression tests, not a proof that
all possible interleavings have been explored. The guarantees still assume
trusted builders, immutable store paths, local metadata filesystems, fixed pool
membership, and one fenced owner per PVC.
