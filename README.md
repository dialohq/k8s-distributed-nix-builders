# k8s-distributed-nix-builders

A shared Nix package collection for Kubernetes, with private native Nix databases for each warm builder. Rust coordinates publication and online GC; a C ABI wrapper calls native Nix C++ store operations. Ordinary `nix build` commands run inside builder pods.

## Install with Helm

The image and chart are public; no registry credentials are required. Ensure the
nodes meet the [kernel and storage requirements](#requirements-and-current-boundaries).

```sh
helm upgrade --install nix-builders \
  oci://ghcr.io/dialohq/charts/k8s-distributed-nix-builders --version 0.4.5 \
  --namespace nix-builders --create-namespace \
  --set store.storageClass=YOUR_BLOCK_STORAGE_CLASS \
  --set builders.storageClass=YOUR_BLOCK_STORAGE_CLASS
helm test nix-builders --namespace nix-builders
kubectl exec -n nix-builders -it nix-builders-nix-builder-0 -- nix --version
```

To build from source, use `nix build .#image`, load/publish the image to your cluster, and install `./deploy/chart` with the corresponding `image.repository` and `image.tag`. Package the chart with `helm package deploy/chart`. For GitOps, create a Secret containing a 32–256 byte `token` key and set `auth.existingSecret` to its name; otherwise Helm generates and preserves the token on upgrade. Never commit a real token in values files.

The chart creates one store StatefulSet, a three-member warm builder StatefulSet, Services, a NetworkPolicy, and PVCs. CPU and memory requests guide scheduling; there are no resource limits by default. See [values.yaml](deploy/chart/values.yaml).

## Architecture

```mermaid
flowchart LR
  A[Native Nix clients] --> B[Builder pods: native daemon + private SQLite PVC]
  B -->|Authenticated gRPC: publication and GC| S[Store pod: coordinator + kernel NFS]
  B -->|Read-only NFS| P[Shared package collection PVC]
  S --> P
```

Each builder retains its own writable store, SQLite database, admission journal, and recovery state. Completed outputs are copied once to the shared collection using native Nix transfer streams over gRPC. Peers register their metadata and bind-mount shared paths into their local store view. After publication, builders reclaim matching local package directories and large files and replace them with read-only mounts of the shared copy. Live job/client roots and the runtime closure protect files still in use. Tiny standalone files and symlinks remain local, as do nondeterministic local variants that differ from the shared package. A durable relocation record finishes interrupted moves before clients can reconnect. Startup restores each distinct store path from the admission index without replaying completed publication batches or rewriting valid Nix metadata. Only interrupted admissions or missing shared metadata require journal replay. SQLite files never live on NFS. Builds execute inside the builder pod; no host Nix installation, host store mount, SSH transport, host daemon, or Kubernetes API access is required.

The `distributed-nix pod` Rust supervisor owns startup, readiness and child shutdown.
Mounts use Linux syscalls; kernel NFSD uses its control filesystem; runtime seeding
calls native Nix through the C ABI. Native operations run in isolated child processes
so blocking Nix work can be cancelled safely. Standard `nfs-utils` authorization and
recovery daemons remain; `exportfs` prepares their export table. Tini only reaps orphan
processes as PID 1. There is no shell entrypoint.

The store pod serves NFSv4 using Linux kernel NFSD in its own network namespace and coordinates online GC. Standard `nfs-utils` helpers handle export authorization and persistent client recovery; file I/O runs in the kernel. Client recovery state lives on the store PVC. No userspace NFS server or alternative backend is included. It never deletes shared files before every configured participant acknowledges safe retirement. Kubernetes Service DNS supplies stable addresses. PVCs survive pod replacement and Helm uninstall. See [ONLINE_GC.md](ONLINE_GC.md), [ADMISSIONS.md](ADMISSIONS.md), and the [concurrency audit](RACES.md).

## Requirements and current boundaries

- Linux x86-64 nodes with kernel NFS client and server support (`nfs` and `nfsd` modules loaded; Linux 5.8 or newer), privileged pods, and mount namespace support. This is ordinary Kubernetes, but not compatible with a restricted Pod Security policy.
- RWO PVCs backed by local/block POSIX filesystems such as ext4 or XFS. Kernel NFS requires an exportable filesystem; container overlay filesystems and NFS-backed metadata PVCs are unsuitable. Choose storage that fences old writers when moving a PVC.
- Trusted builders and a trusted cluster network. RPC has bearer authentication; TLS is not implemented. The chart restricts incoming RPC/NFS to its pods when the CNI enforces NetworkPolicy. NFS uses AUTH_SYS.
- Pool membership is chosen at installation and recorded on each PVC. Resizing an existing pool is deliberately rejected until a retirement protocol exists. Missing participants prevent GC. Do not force-delete a pod whose old process may still be running.
- Roots acquired through a builder remain pinned for that pod's lifetime. Replacing the pod retires those pins after surviving leases close, while keeping its database and cache. This is conservative, not per-job reclamation.
- A single store pod is a storage availability dependency. Pod replacement recovers its PVC; this is not a highly available NFS service.
- Nix is pinned to **2.33.6** because the C++ integration is version-sensitive. The optional ARC image and authenticated runner attachment are described in [deploy/ARC.md](deploy/ARC.md). ARC clients reserve one slot per warm builder; increasing ARC runner counts does not resize pool membership.

Conflicting content-addressed realisations remain errors and retain their pending roots. Unrelated outputs continue publishing. The system does not resolve nondeterministic build outputs.

## Build and test

```sh
nix build
nix flake check
nix develop -c cargo test
sudo nix develop -c cargo test --test admissions mounted_paths -- --ignored --nocapture
helm lint deploy/chart
python3 deploy/e2e.py --namespace nix-builders --release nix-builders
```

Native tests cover publication retry, authentication, bounded transfer streams, admission recovery, GC barriers, and real Nix stores. The deployment test uses a disposable Helm installation and exercises native builds, shared reuse, pod replacement, and online collection. Do not run its failure injection against a busy production pool.
