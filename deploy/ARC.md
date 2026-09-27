# ARC attachment to warm builders

The optional `0.4.0-arc` builder image includes the official GitHub runner. The
ordinary image runs a thin `distributed-nix arc-client` in each ARC runner pod.
ARC supplies its normal JIT configuration; the client reserves one builder over
an authenticated bidirectional gRPC stream before sending that configuration.
There is one runner slot per builder. Additional ARC pods wait for capacity.

All job execution happens in the builder pod. The shared package collection and
each builder's SQLite database, workspace and Git mirrors live on their PVCs.
Neither component mounts a host store. ARC clients need only a ConfigMap,
Secret and small emptyDir; they run without privileges as UID 1001.

Set `builders.image` to the ARC image, provide builder-specific Nix daemon
settings in `builders.extraNixConfig`, and allow the client namespace and pod
selector in `arcClients`. The ConfigMap file `/etc/distributed-nix/arc-client.json`
has `builders` (individual builder DNS names with port 9840) and `token_file`.
Mount the pool's authentication token at that path. Set
`DISTRIBUTED_NIX_POD_UID` from the client pod's metadata.uid. Configure ARC's
runner container command as `/bin/distributed-nix arc-client` and provide a
writable emptyDir at `/run/distributed-nix-arc-client`.

The backend executes only the image's fixed GitHub runner program, as UID 1001.
It creates a native Nix daemon for that job. Both enter a child cgroup of the
builder container; builds and detached descendants inherit it. Completion or
RPC disconnection kills this cgroup and waits for the kernel's `populated 0`
event before deleting the workspace and releasing the slot. Failed cleanup
poisons the slot and fails the pod's health probe. This requires Linux cgroup v2
with `cgroup.kill` support; no CPU or memory limit is introduced by these groups.

Job-specific Nix root leases remain held by the daemon even between Nix client
connections. Online GC can retire those roots only after the job's processes
release their flock leases. The warm pod's administrative daemon has its own
lease, separate from job roots.

Attempt IDs are recorded durably before execution. The client never retries on
another builder after sending JIT credentials: ambiguous network failure ends
that attachment, and ARC can replace it through its normal runner lifecycle.
This is not a cross-node exactly-once execution claim. Keepalive timeouts detect
external network failure; they never authorize reuse of a dirty local slot.

The RPC token grants runner submission and pool administration. Keep it out of
job environments. NetworkPolicy restricts access, but gRPC itself uses plaintext
on the private pod network. A builder is a privileged trusted CI execution
boundary, not an isolation boundary for hostile tenants.
