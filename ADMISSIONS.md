# SQLite admission bookkeeping

Admission uses `rusqlite` and the same system SQLite library already linked by
the native Nix bridge. Its database is
`/var/lib/distributed-nix/admissions/admissions.sqlite`, on local storage. It is
separate from both the worker's native Nix database and the host's Nix database.
There is no new server, distributed database, or database on NFS.

Two tables replace the historical JSON scan:

- `paths` indexes each known store path and its local/copy/symlink/mount kind.
- `batches` holds the original manifest, immutable plan, and pending/committed
  state. Manifest and plan payloads remain JSON to preserve their existing
  validation and content-addressed identity.

The existing admission lock serializes admission. A short SQLite transaction
persists the complete pending plan and its path index before filesystem changes.
Read-only mounts/copies and native Nix registration happen afterward; another
transaction marks the batch committed. No SQLite transaction remains open while
mounting or calling native Nix. Recovery replays batches idempotently, including
committed batches whose mounts disappeared after reboot. Nix's C ABI continues
to own all store metadata writes.

SQLite uses DELETE journal mode and `synchronous=EXTRA`. A single admission
writer needs no WAL; closing all connections before GC's directory rename avoids
moving an open database. GC builds a retained checkpoint database, closes it,
and performs the existing resumable directory swap. Retired path kinds remain
available in the old directory for safe unmounting. The active index contains
only retained paths after the swap.

A new database creates its schema atomically. JSON admission journals and their
migration code are no longer supported. All admission, recovery, and GC entry
points use the same controller revision.

## Measured bookkeeping cost

On 2026-09-27, a private copy of cibox-0's 477 journals contained 2,098,131,022
bytes and 31,601 distinct paths. An optimized Rust benchmark measured:

| Operation | Time |
| --- | ---: |
| Read, parse, validate, and rebuild the historical path map | 29.71 s |
| One-time SQLite import | 84.69 s |
| Open SQLite and look up 1,500 paths, median of ten runs | 1.62 ms |
| Open and lookup range | 1.55–1.99 ms |

The baseline directly deserializes journals and validates their identity; the
former production reader additionally constructed an intermediate JSON value.
Lookup assertions compare every returned kind with the reconstructed legacy map.
The lookup workload approximates the 730–1,522-record admission batches observed
in the builder-pool experiment. These are local bookkeeping measurements, with
warm lookup caches, on the operator machine. The scan/import ran while other
validation builds were active. They are not three-node publication timings or
workflow speedups. No claim is made yet about the previous 94-second median
convergence time. Raw measurements: `benchmarks/admission-sqlite.json`.

## Validation and reproduction

The package build runs the ordinary Rust suite, including real native Nix
registration tests. Cases kill processes before a SQLite
commit, after a pending plan, after filesystem changes, and after native
registration. They also check corrupt input, conflicting plans, mixed local and
shared paths, and GC interruption between directory renames, including an empty
checkpoint and preserved content-addressed realisations.

The separate privileged test creates an isolated mount namespace, exercises
directories, large files, symlinks, and copies, removes and restores mounts,
retries admission concurrently from eight clients, and verifies contents with
native `nix store verify`.

```sh
nix build
nix develop -c cargo test --test admissions mounted_paths -- --ignored --nocapture
```

The historical scan/import benchmark was run before removing JSON migration
support. Its harness remains in commit `5748d11`; the measurements above are
archived evidence, not a benchmark provided by the current implementation.

The cluster deployment suite covers native NFS reuse, ARC workflow convergence,
online collection failures and retries, and node reboot recovery.
