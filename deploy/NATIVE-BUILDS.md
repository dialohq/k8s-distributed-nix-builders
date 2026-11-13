# Native remote builds (opt-in)

RoamVM guests and other ordinary Nix clients can use warm builders through stock
`ssh-ng`. Evaluation stays on the client; derivations, inputs, builds and output
copies use Nix's worker protocol. This does not execute the fixed ARC runner.
The builder serves an **untrusted Nix connection**, even though sshd must enter
its private mount namespace as root. Client keys grant native store/build access,
not a shell or pool administration. The pool remains a trusted CI boundary, not
an isolation boundary for hostile tenants.

## Builder deployment

Use an image built from this source, then set:

```yaml
nativeBuilds:
  enabled: true
  existingSecret: orb-native-builds
  clients:
    - ipBlock:
        cidr: 192.0.2.0/24 # Replace with the actual client source CIDR.
signing:
  existingSecret: pool-output-signing
  publicKeys:
    - "pool-output-1:BASE64_PUBLIC_KEY" # Replace with the actual pool public key.
```

The referenced Kubernetes Secret must contain `authorized_keys` (client public
keys, one per line) and `ssh_host_ed25519_key` (an OpenSSH server private host
key). Provision these outside Helm through your secret manager. The chart never
generates or embeds client credentials. Keep this Secret separate from
`auth.existingSecret`; **never deliver the gRPC admin token to a guest**.
Use a distinct client key per credential scope, and remove its public key to
revoke future connections. Existing authenticated connections must also be closed
when immediate revocation is required. Host-key rotation requires updating guest
trust; key removal does not cancel already-running work.

The opt-in adds TCP **2222**, named `nix-ssh`, only to builder pods and the existing
headless `<release>-nix-builders` Service. The selector is
`app.kubernetes.io/name=distributed-nix`, `app.kubernetes.io/instance=<release>`,
`app.kubernetes.io/component=builder`. Its existing `publishNotReadyAddresses`
supports pool bootstrap; sshd starts only after recovery and bootstrap. Configure
clients with **individual** endpoints:

```text
<release>-nix-builder-N.<release>-nix-builders.<namespace>.svc.cluster.local:2222
```

Do not load-balance successive native connections over different builders. Each
builder has its own writable database. Provide routing and DNS reachable from
guests; headless cluster DNS alone is not external ingress. `nativeBuilds.clients`
permits only SSH, not ports 9840/2049, and only selects builder pods. An empty list
does not open SSH ingress. CNI policy enforcement and the actual source address
(including any NAT) remain deployment responsibilities. The default deployment
and ARC authentication/lifetime are unchanged.

The server forces `/bin/distributed-nix node ssh-connection`; requested commands
are ignored. Passwords, PTYs, forwarding, tunnels, user rc files and user
environment files are disabled. SSH credentials are runtime-mounted, root-readable
files outside the worker chroot. Native Nix rejects privileged client operations;
shared GC is available only through the authenticated administrator coordinator.

## Output signatures preserve incremental builds after GC

Signing is independently opt-in. `signing.existingSecret` references a Secret with
one `key` containing a Nix binary-cache private key. Only builders mount it, with
mode 0400, at `/run/secrets/nix-signing/key`. Preparation bind-mounts that directory
into the private worker root and configures standard Nix `secret-key-files`.
Sandboxed builds cannot read the key. It is never delivered to guests, stored in
the Nix store, or mounted on the origin/store pod. `signing.publicKeys` adds public
trust to **all** pool participants, including the origin. The chart rejects a
signing Secret without public trust. Keep SSH, signing and administrator Secrets
separate; no per-orb signing key is trusted by the pool.

This is necessary for transparent incremental builds after remote collection.
Stock Nix 2.33.6's hook copies input outputs **before** uploading the untrusted
derivation graph. Without a trusted signature, B cannot import a locally cached A
after the pool collects A, even when B has ordinary derivation context. Server
signing lets the client copy A and its signature back, then safely reimport A for
B after GC. Explicit unsigned input-addressed imports remain rejected, including
with client `--no-check-sigs`. Signing does not retroactively repair old unsigned
outputs or signatures discarded by an image registration step.

Distribute new public trust to every participant before rotating a private key.
Retain old public keys while saved guests may still hold outputs signed by them.
Do not sign arbitrary client imports or disable signature checking to bypass this
contract. Without signing, the SSH transport still works, but unsigned
incremental inputs missing from the pool are not transparently recoverable.

## Guest integration contract

For each builder, the standard Nix machines entry is:

```text
ssh-ng://root@HOST:2222?ssh-key=/run/roamvm-builder/id_ed25519 x86_64-linux - 2 1
```

The last two numbers are the client's maximum concurrent builds and speed factor;
size concurrency against builder capacity and concurrent ARC work. Add supported
features only when the builder actually provides them. No custom remote program
or Nix client patch is needed. Pin the builder host key with a known_hosts line:

```text
[HOST]:2222 ssh-ed25519 BASE64_PUBLIC_HOST_KEY
```

`HOST` must exactly match the URI, without `root@`; nonstandard SSH ports require
the brackets. Provision the public host key through a trusted channel, not an
unauthenticated `ssh-keyscan` during bootstrap. A shared server key may cover
multiple endpoints; list each hostname. Guest files should be root-owned in a
0700 runtime directory, with the private key mode 0600. Keep them out of images,
Nix store paths, logs and flake sources.

Configure `builders = @/run/roamvm-builder/machines`, `max-jobs = 0` and optionally
`builders-use-substitutes = true` in the **local daemon's** configuration, then
use ordinary `nix build`. This copies outputs back into the guest's local store.
Do not replace its local store with `--store ssh-ng://...`, which leaves outputs
remote. Put these options in the daemon's environment (not only a user's shell):

```text
NIX_SSHOPTS=-oUserKnownHostsFile=/run/roamvm-builder/known_hosts -oStrictHostKeyChecking=yes -oIdentitiesOnly=yes -oBatchMode=yes
```

Pinned Nix **2.33.6** reads/splits `NIX_SSHOPTS` in `src/libstore/ssh.cc` and adds it
to SSH arguments. `src/libstore/ssh-store.cc` uses that transport for `ssh-ng`;
`src/libstore/unix/build/hook-instance.cc` inherits the daemon environment.
`src/nix/build-remote/build-remote.cc` handles untrusted builders by uploading the
derivation closure and calling `buildPathsWithResults`. Do not request trusted
mode or disable guest signature checking globally to make remote builds work.

Use `builders-use-substitutes = true` when image registration has dropped cache
signatures. Stock `QueryValidPaths` invokes server-side substitution before
requesting uploads, even on an untrusted connection (`src/libstore/daemon.cc`).
The worker must have a reachable substituter and trust its public key, and that
cache must contain the **exact** requested store paths. Do not disable
substituters for an invocation that needs this fallback. A stripped image can
therefore use signed cache.nixos.org inputs without trusting an orb signing key;
uncached unsigned inputs still require restoring their authentic signatures or
rebuilding them. No custom protocol adapter is used.

## Roots, disconnection and suspension

Each native SSH protocol connection owns a unique `client-*` root group and an
exclusive-lifetime shared flock lease. Native callbacks pin paths before metadata
reads, imports and registrations, using the existing online-GC retirement gate.
GC cannot prune a live lease. After the worker exits, the next online GC records
usage and prunes the group **without replacing the builder pod**. Publication
outboxes independently retain outputs until publication/acknowledgement or the
existing abandonment policy; connection exit alone does not bypass those roots.

The lease has no timed expiry that can race an active build/copy. SSH keepalives
(`ClientAliveInterval 15`, `ClientAliveCountMax 3`) close an unresponsive transport;
Nix's connection monitor interrupts work, and lease release follows worker exit.
The client must remain connected through output copying. A completed command has
its output locally; remote paths are not a durable guest cache or a lease that
survives suspension. An in-flight build/copy may fail when the VM suspends and
must be retried after reconnecting. Refresh credentials before daemon use on
resume. Do not extend server leases just because a VM's disk or memory is saved.

## Isolated validation

`native-build-vm.nix` declares a disposable NixOS guest and libvirt domain. It uses
a read-only host-store share and tmpfs guest stores, with real worker/origin
metadata, publication and online GC. The origin is locally bind-mounted rather
than NFS: this exercises the SSH/root contract, not Kubernetes/NFS deployment.
`native_builds.py` runs inside it via QGA and uses stock clients with independent
stores. It exercises authentication, remote build/copy-back, a FIFO-held build
during GC, a backpressured copy during GC, reclamation, suspended SSH clients,
signed A/GC/B incremental builds, unsigned-import rejection, and substitution of
an exact cache.nixos.org input whose client registration has no signatures. The
fixture generates its own server-only signing key and needs access to that cache.
The optional `listenAddress` binds a synthetic-key test endpoint on host port
22222; the default is loopback. No production credentials or pool are used.

Build with `nix build --impure --file deploy/native-build-vm.nix --argstr exchange
ABSOLUTE_SCRATCH_DIRECTORY --out-link RESULT`. The scratch directory must already
exist. Define/start `RESULT/domain.xml` using `virsh -c qemu:///session`, then use
QGA `guest-exec` to run `python3 /etc/native-build-cases.py`, capturing stdout and
stderr. The guest writes JSON results under the exchange directory. Its root
filesystem is disposable; reboot clears all stores and keys. Destroy/undefine
only this named test domain after use. This requires existing working libvirt
and KVM; configure host services declaratively if they are not already available.
