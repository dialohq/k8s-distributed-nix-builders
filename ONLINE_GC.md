# Online shared-store collection

Run `distributed-nix gc` inside the store pod. `--dry-run` reports a plan;
`--if-needed` collects under disk pressure or resumes an interrupted epoch.
The store container runs this check periodically. Builds continue during GC.

Each native daemon connection pins paths before reading their metadata or
registering them. Connections share permanent Nix roots keyed by
`DISTRIBUTED_NIX_POD_UID`, supplied through the downward API. A participant owns
one builder pod and its private PVC. Pins survive until that pod is replaced
and all old connection/listener leases close. This covers shells that outlive
their Nix connection and conservatively retains everything acquired during a
pod's lifetime; it does not reclaim those roots between jobs in the same pod.

The coordinator pauses publication, reads the union of native roots and
reference graphs, and installs durable retirement markers for candidate paths
on every participant. It reads roots again: paths acquired between the first
mark and the barrier remain live. Requests for retiring paths wait until
collection finishes, then recheck native metadata; unrelated requests continue.

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
