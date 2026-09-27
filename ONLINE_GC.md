# Online shared-store collection

`distributed-nix online-gc` runs on the origin node. `--dry-run` reports a plan;
`--if-needed` collects only under disk pressure, or resumes an interrupted epoch.
The ARC maintenance wrapper's `gc` and `auto-gc` commands use this protocol.
Runners remain scheduled and native Nix builds continue.

Each native daemon connection pins paths before reading their metadata or
registering them. Pod connections share permanent Nix roots keyed by the pod
UID; the builder must receive `CIBOX_POD_UID` through the downward API. Pins last
until the CRI sandbox disappears **and** all connection/listener leases close.
This covers shells that outlive their Nix connection and private PID namespaces.
Collection conservatively retains everything acquired during a pod's lifetime.

The coordinator pauses publication, reads the union of native roots and
reference graphs, and installs durable retirement markers for candidate paths
on every node. It then reads roots again. Paths acquired between the first mark
and the barrier remain live. A request for a retiring path waits until collection
finishes, then checks native metadata again; unrelated requests continue.

Workers checkpoint admission metadata, retaining new admissions as well as old
live paths, remove dead mounts, and ask **native Nix** to delete the selected
paths with liveness enforcement enabled. Only after all three durable worker
acknowledgements can the origin delete shared files. No independent collector
may delete origin files. Synthetic sharing pins are excluded from marking;
ordinary roots, client pins, pending publication, and CA dependencies count.

The coordinator and node services use authenticated gRPC on a private network.
`/etc/distributed-nix/online-gc.json` contains three `IP:port` endpoints, the local
`index`, `token_file`, an absolute `cri_command` array, and the builder `namespace`.
The token must contain 32–256 bytes. Bind/firewall the service to the trusted
private network; transport encryption is not implemented. The endpoint exposes
only typed collection operations. Existing publication transport is unchanged.

Epochs and barriers survive crashes. Rerun the same command to resume; never
remove a retirement marker manually. Missing nodes prevent origin deletion.
After interruption, affected path requests can remain blocked until recovery,
but unrelated builds can proceed. There is no timed expiry of safety state.
Upgrades and the explicit `gc-offline` repair path still use drained maintenance.

Local tests cover root/lease lifetime, late remote roots, retirement waits,
immutable plans, admission checkpoint recovery, authentication, and native
liveness. Deployment tests additionally exercise active builds, new requests,
pod deletion, coordinator interruption, and an unavailable peer. The design
assumes trusted builders, fixed three-node membership, and immutable store paths.
