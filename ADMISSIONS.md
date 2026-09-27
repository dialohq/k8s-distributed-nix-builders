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

## Migration and deployment

Opening an uninitialized database imports existing JSON journals and sets the
schema version in one transaction. A malformed journal, conflicting path kind,
or interrupted import leaves no committed partial migration. The next attempt
retries. Imported JSON files remain as archival files and are never scanned by
normal admission again. Newly admitted batches exist only in SQLite.

The first import temporarily requires space for both the old JSON and the new
database: approximately another 2 GB for the measured node. Old files remain in
the retired directory after GC, following the existing epoch-retention behavior.
This change improves admission CPU/I/O cost; it does not normalize or deduplicate
the historical manifest payloads or optimize full reboot recovery.

Deploy all admission, recovery, and GC entry points together during the existing
drained maintenance procedure, with the publisher and automatic GC stopped.
Do not run an older JSON-only coordinator against migrated state: archival JSON
does not contain later admissions. A binary-only downgrade is not a valid
rollback. Backups must include the entire quiescent admission directory and the
matching worker state; do not delete the database to force reimport.

Deploy this schema transition with all coordinator entry points on the same revision.

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
registration tests. Added cases kill processes during migration, before a SQLite
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

The benchmark must use a disposable copy of the JSON admission directory: it
creates a SQLite database in that directory. Do not point it at live state.

```sh
ADMISSIONS_BENCH_DIRECTORY=/absolute/path/to/private-copy \
  nix develop -c cargo test --release --test admissions benchmark_legacy_history -- --ignored --nocapture
```

The older three-VM cluster tests have been updated for SQLite inspection, but
were not rerun in this change. Production NFS, ARC workflow convergence, and
disruptive three-node GC/reboot measurements remain rollout checks.
