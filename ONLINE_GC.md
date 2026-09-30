# Online shared-store collection

Run `distributed-nix gc` inside the store pod. `--dry-run` reports a plan;
`--if-needed` collects under disk pressure or resumes an interrupted epoch.
The store container runs this check periodically. Builds continue during GC.

The chart defaults to collection below 25% free space, aiming for 30% free.
`gc.maxStoreBytes` and `gc.targetStoreBytes` optionally bound the origin's
filesystem usage (both zero disables the absolute budget). For a 100 GiB
trigger and 80 GiB target, set them to `107374182400` and `85899345920`.
Collection starts at either the size limit or free-space threshold. It selects
least recently used, unrooted closures first. Completed clients' root timestamps
record use in `cache-usage.sqlite` before their roots are removed. This adds no
writes to the native lookup path and no permanent pins. Registration time is the
fallback for paths without recorded use. Dependencies inherit the newest use of
their referrers, including use on another builder. Entries for missing paths are
removed during snapshots. These are client-use hints, not filesystem access times.
Roots always win over a budget.
NAR sizes estimate space reclaimed; sparse files, hard links, metadata and
concurrent builds can make actual usage differ. Subsequent checks measure the
filesystem again. A manual `gc` still collects all eligible garbage.

Graph edges honor Nix's `keep-outputs` and `keep-derivations` independently,
including content-addressed outputs with no static derivation output entry.

Conflicting CA publications and their dependents are terminal: compare-and-delete
releases their publication roots without altering local realisations or active
job roots. The latest rejection report remains in `last-rejected-publication.json`.
Other publication attempts expire after `gc.publicationMaxAgeSeconds` (default
24 hours); builds may continue using their independent job/native roots. Expiry
abandons cross-builder sharing of that attempt, so a later job may rebuild it.

Maintenance runs under the exclusive publication lease on every non-dry check,
even below the size threshold. It expires publication roots, removes abandoned
incoming manifests and keeps four completed GC histories and 32 publication reports. Successful transfers
release incoming manifests immediately. Unfinished epochs and admission recovery
records never expire. Upgrade all participants before using the new policy;
mixed versions fail closed until the rollout completes.

Each native daemon connection pins paths before reading their metadata or
registering them. Connections share Nix roots keyed by `DISTRIBUTED_NIX_POD_UID`.
Ordinary clients use the pod UID: their pins survive until pod replacement and
lease closure, covering shells that outlive their Nix connection. ARC jobs use
their own group; the runner service holds its lease until the entire job cgroup
is empty. A cleanup failure retains the lease until the poisoned builder exits.

Published local directories and large files can be reclaimed independently of
shared GC. Relocation fences native connections, excludes active GC epochs and
all live client/runtime closures, then replaces matching local payloads with
read-only shared mounts. It persists an intent before deleting anything and
updates every overlapping admission plan in one SQLite transaction. Recovery
finishes that intent before runtime seeding or admitting new clients. Small
standalone files, symlinks and differing local variants remain local.

The coordinator pauses publication, reads the union of native roots and
reference graphs, and installs durable retirement markers for candidate paths
on every participant. It reads roots again: paths acquired between the first
mark and the barrier remain live. Requests for retiring paths wait until
collection finishes, then recheck native metadata; unrelated requests continue.

Before a new collection, the exclusive publication lease also allows the
coordinator to discard reservations left by failed or interrupted publications.
No live publication or transfer holds that lease at this point. Retries reserve
again; worker outboxes and committed publication records remain intact. Dry runs
leave reservations untouched and can therefore report fewer reclaimable paths.

Workers checkpoint admission metadata, retaining new admissions and old live
paths, remove dead mounts, and ask native Nix to delete the selected paths with
liveness enforcement enabled. Only after every configured participant has
acknowledged can the origin delete shared files. No independent collector may
delete origin files. Synthetic sharing pins are excluded from marking; ordinary
roots, client pins, pending publication, and CA dependencies count.

Publication and GC use authenticated gRPC. The runtime configuration contains
`nodes` (ordered DNS host:port endpoints), the local `index`, `token_file`, and
optional `pod_uid`. Index zero is the store. Membership is persisted on each
PVC; a changed list is rejected. Select the builder count at installation;
resizing or retiring a participant is not implemented. No Kubernetes API or CRI
socket is needed. Transport encryption is not implemented; use trusted networks.

Epochs and barriers survive crashes. Rerun GC to resume; never remove a marker
manually. Missing participants prevent shared deletion. After interruption,
affected path requests can remain blocked until recovery while unrelated builds
proceed. There is no timed expiry of safety state. GC has one mode: online.

The design assumes immutable store paths, trusted builders, and at most one
active pod owning each metadata PVC. Never bypass Kubernetes/storage fencing by
force-deleting an unreachable pod and starting a second writer.
