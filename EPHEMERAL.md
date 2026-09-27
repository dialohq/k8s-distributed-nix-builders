# Private builder lifetime experiments

Status: cluster-tested prototype. Deployment-specific scripts and benchmark evidence referenced below live in the consuming infrastructure repository, not in this package. The production ARC template still uses the existing
builder setup. A separate gRPC collection service now publishes private outputs and
serves snapshot generations; see [RPC.md](RPC.md). Automatic final publication during
builder shutdown, refreshing running builders, and GC drain/fencing integration
remain requirements before production rollout.

The performance-first comparison now includes retained builders and actual
`main.yml` jobs. See the deploying infrastructure repository’s builder-comparison report.
Ephemeral lifetime is an option to measure, not a production requirement.

## Storage boundary

`arc-runners/package-collection` is a retained, read-only NFS persistent volume backed
by `/srv/distributed-nix/origin` on cibox-0. It contains the shared CI package collection;
it is separate from each NixOS host's `/nix/store` and SQLite database.

Each test builder receives its own `emptyDir` volumes for SQLite, mutable output
files, temporary files, and controller state. A retained builder keeps that pod alive
between measurements. An ephemeral builder is recreated between measurements. Both
start from the same snapshot and use the same package collection.

The prototype still mounts the host's software store read-only to bootstrap its
executable and libraries. CI metadata and output files are never written there.
A production image should contain that bootstrap runtime itself.

## SQLite snapshots

The C ABI calls `sqlite3_backup_init`, `sqlite3_backup_step`, and
`sqlite3_backup_finish` to obtain a consistent standalone database, including committed
WAL data. It never copies the live origin database file directly. Native Nix then
reads the snapshot's valid-path list. The Nix schema marker travels with the database.

The builder copies `db.sqlite` and `schema` into its own local volume before opening
its daemon. Subsequent reads and writes use private SQLite; no live database or SQLite
lock is shared over NFS. This preserves content-addressed realisations as well as
ordinary path metadata. The native tests verify both those records and that changes
to a private copy do not change the origin or snapshot.

A 26 MiB database with approximately 25,000 paths took 10–50 ms to copy in initial
cluster measurements. Reconstructing equivalent metadata using native path
registration took 16.4 seconds. These are metadata initialization timings, not total
pod startup timings.

Snapshots must match the pinned Nix version. A production snapshot service also needs
atomic generation publication and coordination with GC. Old snapshot generations
must not remain usable after their package files have been collected.

## Mount lifetime

The first experiment propagated one read-only mount per package into the host so
separate runner and builder containers could see the same view. At roughly 25,000
mounts per builder, it incurred substantial startup and teardown overhead and hit
120-second deletion timeouts. It is retained only as an explicitly selected regression
experiment (`--propagate-mounts`), not as the default candidate.

The default benchmark keeps the mounts private to a single builder container. The
client runs inside that view as UID 1001 through ordinary Nix tooling and the native
Nix Unix socket. Its permitted/effective capabilities are empty and no-new-privileges
is enabled. The privileged daemon still creates Nix build sandboxes. No per-package
mounts propagate to the host. The original `builder-lifetime.py` exercises commands
directly; `main-comparison.py` additionally runs an actual GitHub Actions agent
inside this container with a fresh workspace for every job.

Kubernetes applies setgid permissions to some volume directories. Initialization
clears those bits on the private Nix directory hierarchy: otherwise `cp` attempts to
preserve inherited setgid permissions and Nix's sandbox rejects that operation.

## Reproduction

Build `.#distributed-nix` from the repository's existing flake and make its runtime
available on the test nodes. Export a fresh generation on the collection server:

```
distributed-nix snapshot /srv/distributed-nix/origin \
  /srv/distributed-nix/origin/.distributed-nix-catalogs/GENERATION
```

Then run:

```
python3 cibox/tests/builder-lifetime.py \
  --package /nix/store/HASH-distributed-nix-0.2.0 \
  --catalog GENERATION --nodes 0 1 2 --rounds 3 \
  --evidence /tmp/builder-lifetime.json
```

The harness uses SSH only to administer Kubernetes test pods. It records pod startup,
metadata/mount initialization, all five repository flake checks, a fresh C++ build
with a distinct nonce, output execution and NAR verification, and pod teardown.
Workload order alternates between rounds. The persistent baseline is a retained
private builder, not a host database. `--legacy-baseline --propagate-mounts` explicitly
selects the original node database for historical comparison.

Test pods have deadlines and are deleted on completion/failure unless
`--keep-on-failure` is requested. Their outputs are deliberately disposable. Tests run
in the namespace that the existing GC maintenance procedure drains; bypassing that
procedure or reusing a generation after GC is unsupported.

The final production publication and coordination path must use ordinary network RPC
and Kubernetes discovery. SSH command execution is not the proposed RPC transport.

## Tuned comparison

Three rounds per node, with all three nodes tested concurrently, passed the five
flake checks, fresh C++ compilation, execution, and content verification. The final
NFS options use `sharecache,actimeo=600`. Native admission explicitly refreshes
metadata; the separate rapid replacement regression passed 20/20 iterations with
the long cache. The collection server also allows private TCP 111: `umount.nfs`
uses rpcbind even with an explicit mountd port. This removed a measured ten-second
unmount timeout.

Median seconds from `tests/evidence/builder-tuned-node-{0,1,2}.json`:

| Node | Builder | Startup | Five checks | Fresh C++ | Teardown |
| --- | --- | ---: | ---: | ---: | ---: |
| 0 | Retained | 3.80 | 16.54 | 12.69 | 1.30 |
| 0 | Ephemeral | 3.00 | 17.54 | 12.66 | 1.11 |
| 1 | Retained | 5.81 | 17.13 | 21.99 | 1.40 |
| 1 | Ephemeral | 3.82 | 18.14 | 21.61 | 0.90 |
| 2 | Retained | 5.81 | 17.14 | 21.89 | 1.32 |
| 2 | Ephemeral | 3.80 | 18.12 | 21.30 | 1.31 |

Retained startup/teardown have one sample each. Retained builders start first;
ephemeral builders benefit from the node's shared NFS cache remaining warm. These
numbers do not show that ephemeral startup is intrinsically faster. Workload order
alternates, but three samples are insufficient to interpret small differences as
speedups. Fresh builders add lifecycle time and around one second to warm checks;
they are competitive on compilation. Node 0 is the NFS server, so its filesystem
latency is different. This is a builder benchmark, not a complete ARC workflow run.
