# Disposable NixOS/native protocol test guest, booted by libvirt (not run-vm).
{ exchange, nixosPkgs ? null, listenAddress ? "127.0.0.1" }:
let
  # Do not snapshot the checkout, which may contain synthetic test credentials.
  locked = (builtins.fromJSON (builtins.readFile ../flake.lock)).nodes.nixpkgs.locked;
  pkgs = import (builtins.fetchTree locked).outPath { system = "x86_64-linux"; };
  hostPkgs = if nixosPkgs == null then pkgs else import nixosPkgs { system = "x86_64-linux"; };
  package = import ../default.nix { inherit pkgs; };
  nix = pkgs.nixVersions.latest;
  runtime = [nix pkgs.bash pkgs.coreutils package];
  closure = pkgs.closureInfo { rootPaths = runtime; };
  runtimeFile = pkgs.writeText "native-proof-runtime.json" (builtins.toJSON {
    inherit runtime nix;
    seed = runtime;
    inherit (pkgs) bash coreutils;
    nixpkgs = pkgs.path;
    cacheProbe = builtins.unsafeDiscardStringContext pkgs.hello.outPath;
    nixConfig = ''
      experimental-features = nix-command flakes ca-derivations
      build-users-group = nixbld
      sandbox = true
      sandbox-fallback = false
      sandbox-paths = /proof-hold=/work/proof-hold
      max-jobs = 2
      cores = 1
      substituters = https://cache.nixos.org
      min-free = 0
      keep-outputs = false
      keep-derivations = false
    '';
  });
  guest = import (hostPkgs.path + "/nixos/lib/eval-config.nix") {
    system = "x86_64-linux";
    modules = [ ({ ... }: {
      system.stateVersion = "26.05";
      networking.hostName = "native-proof";
      networking.firewall.enable = false;
      networking.useDHCP = true;
      boot.loader.grub.enable = false;
      boot.initrd.availableKernelModules = ["virtio_pci" "virtio_blk" "virtio_net" "9p" "9pnet_virtio"];
      boot.initrd.kernelModules = ["virtio_pci" "9pnet_virtio"];
      boot.kernelParams = ["console=ttyS0" "panic=1"];
      fileSystems."/" = { device = "tmpfs"; fsType = "tmpfs"; options = ["size=3G" "mode=755"]; };
      fileSystems."/nix/store" = {
        device = "store";
        fsType = "9p";
        options = ["trans=virtio" "version=9p2000.L" "ro" "msize=1048576" "cache=loose"];
        neededForBoot = true;
      };
      fileSystems."/exchange" = {
        device = "exchange";
        fsType = "9p";
        options = ["trans=virtio" "version=9p2000.L" "msize=1048576"];
      };
      services.qemuGuest.enable = true;
      nix.package = nix;
      nix.settings.experimental-features = ["nix-command" "flakes" "ca-derivations"];
      systemd.services.nix-daemon.enable = false;
      systemd.sockets.nix-daemon.enable = false;
      documentation.enable = false;
      environment.systemPackages = runtime ++ [pkgs.openssh pkgs.python3 pkgs.util-linux];
      environment.etc."distributed-nix/runtime.json".source = runtimeFile;
      environment.etc."distributed-nix/online-gc.json".text = builtins.toJSON {
        nodes = ["127.0.0.1:9840"];
        index = 0;
        token_file = "/run/proof-token";
        pod_uid = null;
      };
      environment.etc."distributed-nix/sshd_config".source = ./chart/files/sshd_config;
      environment.etc."native-build-cases.py".source = ./native_builds.py;
      system.activationScripts.nativeProof = ''
        mkdir -p /bin
        ln -sfn ${package}/bin/distributed-nix /bin/distributed-nix
      '';
      systemd.services.native-proof-prepare = {
        wantedBy = ["multi-user.target"];
        before = ["native-proof-rpc.service" "native-proof-ssh.service"];
        path = runtime ++ [pkgs.openssh pkgs.util-linux];
        serviceConfig.Type = "oneshot";
        serviceConfig.RemainAfterExit = true;
        script = ''
          nix-store --load-db < ${closure}/registration
          mkdir -p /var/lib/distributed-nix/lower /srv/distributed-nix/{origin,worker}
          mkdir -p /srv/distributed-nix/worker/work/proof-hold
          chmod 777 /srv/distributed-nix/worker/work/proof-hold
          nix copy --no-check-sigs --to local?root=/srv/distributed-nix/origin ${builtins.toString runtime}
          nix copy --no-check-sigs --to local?root=/srv/distributed-nix/worker ${builtins.toString runtime}
          mount --bind /srv/distributed-nix/origin /var/lib/distributed-nix/lower
          mkdir -p /run/secrets/nix-signing
          chmod 700 /run/secrets/nix-signing
          nix-store --generate-binary-cache-key native-proof-1 /run/secrets/nix-signing/key /run/proof-signing.pub
          chmod 400 /run/secrets/nix-signing/key
          export DISTRIBUTED_NIX_EXTRA_NIX_CONFIG="extra-trusted-public-keys = $(cat /run/proof-signing.pub)"
          distributed-nix node prepare-origin
          distributed-nix node recover
          mkdir -p /run/secrets/native-build /run/sshd /var/empty /run/proof
          chmod 700 /run/proof /run/secrets/native-build
          ssh-keygen -q -t ed25519 -N "" -f /run/secrets/native-build/ssh_host_ed25519_key
          ssh-keygen -q -t ed25519 -N "" -f /run/proof/id_ed25519
          ssh-keygen -q -t ed25519 -N "" -f /run/proof/wrong_key
          cp /run/proof/id_ed25519.pub /run/secrets/native-build/authorized_keys
          cp /run/proof/id_ed25519 /exchange/client-key
          cp /run/secrets/native-build/ssh_host_ed25519_key.pub /exchange/host-key.pub
          printf '[127.0.0.1]:2222 ' > /run/proof/known_hosts
          cat /run/secrets/native-build/ssh_host_ed25519_key.pub >> /run/proof/known_hosts
          head -c 48 /dev/urandom | base64 > /run/proof-token
          chmod 600 /run/proof-token
        '';
      };
      systemd.services.native-proof-rpc = {
        wantedBy = ["multi-user.target"];
        requires = ["native-proof-prepare.service"];
        after = ["native-proof-prepare.service"];
        serviceConfig.ExecStart = "${package}/bin/distributed-nix serve";
      };
      systemd.services.native-proof-ssh = {
        wantedBy = ["multi-user.target"];
        requires = ["native-proof-prepare.service"];
        after = ["native-proof-prepare.service"];
        serviceConfig.ExecStart = "${pkgs.openssh}/bin/sshd -D -e -f /etc/distributed-nix/sshd_config";
      };
      users.users.root.initialHashedPassword = "";
      users.users.sshd = {
        isSystemUser = true;
        group = "nogroup";
        home = "/var/empty";
      };
    }) ];
  };
  config = guest.config;
  xml = hostPkgs.writeText "native-proof.xml" ''
    <domain type='kvm' xmlns:qemu='http://libvirt.org/schemas/domain/qemu/1.0'>
      <name>distributed-nix-native-proof</name>
      <uuid>07639e8e-1abf-4fd8-9083-fd05ce000001</uuid>
      <memory unit='MiB'>4096</memory><vcpu>4</vcpu>
      <os>
        <type arch='x86_64' machine='q35'>hvm</type>
        <kernel>${config.system.build.kernel}/${config.system.boot.loader.kernelFile}</kernel>
        <initrd>${config.system.build.initialRamdisk}/${config.system.boot.loader.initrdFile}</initrd>
        <cmdline>init=${config.system.build.toplevel}/init ${builtins.toString config.boot.kernelParams}</cmdline>
      </os>
      <cpu mode='host-passthrough'/>
      <features><acpi/><apic/></features>
      <devices>
        <emulator>${hostPkgs.qemu_kvm}/bin/qemu-system-x86_64</emulator>
        <filesystem type='mount' accessmode='passthrough'>
          <source dir='/nix/store'/><target dir='store'/><readonly/>
        </filesystem>
        <filesystem type='mount' accessmode='squash'>
          <source dir='${exchange}'/><target dir='exchange'/>
        </filesystem>
        <channel type='unix'><target type='virtio' name='org.qemu.guest_agent.0'/></channel>
        <serial type='file'><source path='${exchange}/console.log'/><target port='0'/></serial>
        <console type='file'><source path='${exchange}/console.log'/><target type='serial' port='0'/></console>
        <memballoon model='none'/>
        <rng model='virtio'><backend model='random'>/dev/urandom</backend></rng>
      </devices>
      <qemu:commandline>
        <qemu:arg value='-netdev'/>
        <qemu:arg value='user,id=proofnet,hostfwd=tcp:${listenAddress}:22222-:2222'/>
        <qemu:arg value='-device'/>
        <qemu:arg value='virtio-net-pci,netdev=proofnet,bus=pcie.0,addr=0x10'/>
      </qemu:commandline>
    </domain>
  '';
in hostPkgs.runCommand "distributed-nix-native-proof" {} ''
  mkdir -p $out
  ln -s ${xml} $out/domain.xml
  ln -s ${config.system.build.toplevel} $out/system
  ln -s ${package} $out/package
''
