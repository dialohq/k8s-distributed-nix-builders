# Collection RPC prototype

The collection service owns the persistent CI store. Builders own private SQLite
databases and mutable outputs. Bootstrap returns a named SQLite snapshot to copy
from the read-only shared volume. Ordinary Nix clients keep using a local Unix
daemon socket. The controller publishes new closures to the collection over gRPC.

This service is tested separately from the production ARC coordinator. It is not
yet connected to the ARC runner lifecycle.

## Protocol

`proto/collection.proto` defines bootstrap, prepare, commit, release, and a streaming
store connection. Each request carries a controller token and member ID. Kubernetes
Service DNS supplies discovery; there are no SSH commands in this transport.

Prepare canonicalizes a manifest through native Nix, pins its roots, and persists
the manifest. The client transfers missing paths using native `copyPaths` through
a private Unix socket proxy. gRPC carries bounded frames of Nix's worker protocol.
The server runs the same compiled executable as a connection worker, calling the
C ABI directly. Nix owns NAR parsing, hashing, path locking, and database writes.

The collection worker rejects builds, GC, optimisation, repair, and uploads without
path metadata. It enforces Nix's `fsync-store-paths` for imports. Commit validates
that all paths exist and match the canonical manifest, registers content-addressed
realisations, and persists a receipt before acknowledging. The client re-prepares
after transfer so concurrent input-addressed outputs use the collection's winner.

`Client::flush` batches the existing path outbox and publishes ready CA records.
It removes outbox records only after a successful commit. A failed transfer or lost
response can be retried. The shared files are never overwritten to accommodate a
different local input-addressed result.

## Lifetime and limits

Only one coordinator can hold a collection's local filesystem lock. Store streams
are capped at 32 and use bounded 64 KiB frames. Cancelling a stream closes its native
worker and releases its session. Release rejects members with active store sessions.

Member records survive server restarts. A member keeps the generation it started
with. A commit invalidates the cached generation for subsequent builders. Snapshot
cleanup preserves every active member's generation and the current cached generation.
It removes unused generations after bootstrap/release.

These member records are **not a complete GC fencing protocol**. They do not expire
by time, and an abandoned member is not automatically considered dead. The service
currently retains publication roots and offers no collection GC endpoint. Production
integration must drain builders, flush completed outputs, prove their processes are
gone, then invalidate generations before native GC. The existing production GC
coordinator must not collect a store served by this prototype.

The token grants trusted controller access and belongs outside the build user's
filesystem view. The prototype transport uses HTTP/2 on the private cluster network.
The `rpc-publication.py` harness uses an isolated store under
`.distributed-nix-rpc-tests/`; it does not open the production collection database.
The separate `main-comparison.py` performance harness uses the populated CI
collection and must not run concurrently with collection GC or another prototype
coordinator. Runtime software is mounted read-only from the host; packaging that
runtime into the runner image is still pending.

## Tests

The package's normal tests cover native transfer, content and CA metadata, failed
publication preserving outboxes, retries, server and builder replacement, generation
retention, coordinator exclusion, invalid credentials, member mismatch, interrupted
streams, oversized frames, and blocking uncoordinated native GC.

`cibox/tests/rpc-publication.py --package /nix/store/HASH-distributed-nix-0.2.0`
runs a collection pod on cibox-0, publishes a random 1 MiB file from cibox-1, deletes
the writer and replaces the collection pod, then verifies the output from fresh pods
on cibox-2 and cibox-1. Readers use copied local databases and the read-only NFS files.
Read-only local-store mode is used for these verification-only readers. The builder
performance harness separately exercises writable private stores and actual builds.

The [workflow comparison](../ci/builder-comparison.md) additionally runs actual
GitHub `main.yml` jobs, including a concurrent publishing workload, against retained
and freshly initialized private builders. Publication is explicitly flushed after
each job; this does not claim automatic ARC shutdown integration.

The Kubernetes harness uses SSH for administration only. The measured publication
time includes Kubernetes exec overhead; it is not a pure network throughput result.
