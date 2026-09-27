{pkgs, package}: let
  nix = pkgs.nixVersions.latest;
  ganesha = pkgs.nfs-ganesha.override {useDbus = false;};
  runtime = with pkgs; [nix bash coreutils tini util-linux findutils gnugrep gnused gnutar gzip git gcc gnumake pkg-config cacert python3 nfs-utils iana-etc ganesha package];
  runtimeJSON = pkgs.writeText "runtime.json" (builtins.toJSON {
    inherit runtime nix;
    seed = runtime ++ [pkgs.path];
    inherit (pkgs) bash coreutils;
    buildGroupId = 30000;
    nixpkgs = pkgs.path;
    nixConfig = ''
      store = daemon
      experimental-features = nix-command flakes ca-derivations
      flake-registry =
      build-users-group = nixbld
      sandbox = true
      sandbox-fallback = false
      max-jobs = 2
      cores = 0
      min-free = 0
      auto-optimise-store = false
    '';
  });
  files = pkgs.runCommand "distributed-nix-image-files" {} ''
    mkdir -p $out/{bin,etc/distributed-nix,etc/ganesha,etc/nix,etc/ssl/certs,tmp,root,work,run}
    cp ${./entrypoint.sh} $out/bin/distributed-nix-entrypoint
    chmod +x $out/bin/distributed-nix-entrypoint
    cp ${runtimeJSON} $out/etc/distributed-nix/runtime.json
    ln -s ${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt $out/etc/ssl/certs/ca-certificates.crt
    ln -s ${pkgs.bash}/bin/bash $out/bin/bash
    ln -s ${pkgs.bash}/bin/sh $out/bin/sh
    echo 'root:x:0:0:root:/root:/bin/bash' > $out/etc/passwd
    echo 'root:x:0:' > $out/etc/group
    cat > $out/etc/ganesha/ganesha.conf <<'CONFIG'
    NFS_CORE_PARAM { Protocols = 4; Enable_NLM = false; Enable_RQUOTA = false; }
    NFSv4 { Grace_Period = 10; Lease_Lifetime = 10; }
    EXPORT {
      Export_Id = 1;
      Path = /srv/distributed-nix/origin;
      Pseudo = /collection;
      Protocols = 4;
      Transports = TCP;
      Access_Type = RO;
      Squash = Root_Squash;
      SecType = sys;
      FSAL { Name = VFS; }
    }
    CONFIG
  '';
in pkgs.dockerTools.buildLayeredImage {
  name = "ghcr.io/dialohq/k8s-distributed-nix-builders";
  tag = "0.3.0";
  contents = runtime ++ [files pkgs.path];
  includeNixDB = true;
  maxLayers = 100;
  extraCommands = ''
    rm -rf etc/distributed-nix etc/ganesha
    mkdir -p etc/distributed-nix etc/ganesha usr/bin
    ln -sf /proc/mounts etc/mtab
    cp ${files}/etc/distributed-nix/runtime.json etc/distributed-nix/runtime.json
    cp ${files}/etc/ganesha/ganesha.conf etc/ganesha/ganesha.conf
    rm -f bin/distributed-nix-entrypoint etc/passwd etc/group
    cp ${files}/bin/distributed-nix-entrypoint bin/distributed-nix-entrypoint
    cp ${files}/etc/passwd etc/passwd
    cp ${files}/etc/group etc/group
    ln -sf ${pkgs.coreutils}/bin/env usr/bin/env
    rm -rf tmp
    mkdir tmp
    chmod 1777 tmp
  '';
  config = {
    Entrypoint = ["${pkgs.tini}/bin/tini" "-g" "--" "/bin/distributed-nix-entrypoint"];
    Env = ["PATH=${pkgs.lib.makeBinPath runtime}" "SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt" "NIX_SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt" "NIX_CONFIG=experimental-features = nix-command flakes ca-derivations" "DISTRIBUTED_NIX_ONLINE_CONFIG=/run/distributed-nix-config.json"];
    WorkingDir = "/work";
  };
}
