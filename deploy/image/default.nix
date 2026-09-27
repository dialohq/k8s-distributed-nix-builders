{pkgs, package, githubRunner ? null}: let
  nix = pkgs.nixVersions.latest;
  runtime = with pkgs; [nix bash coreutils tini util-linux findutils gnugrep gnused gnutar gzip git gcc gnumake pkg-config cacert python3 nfs-utils iana-etc package] ++ pkgs.lib.optionals (githubRunner != null) [githubRunner pkgs.openssh pkgs.jq];
  runtimeJSON = pkgs.writeText "runtime.json" (builtins.toJSON ({
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
  } // pkgs.lib.optionalAttrs (githubRunner != null) {githubRunner = "${githubRunner}/bin/Runner.Listener";}));
  files = pkgs.runCommand "distributed-nix-image-files" {} ''
    mkdir -p $out/{bin,etc/distributed-nix,etc/nix,etc/ssl/certs,tmp,root,work,run}
    cp ${runtimeJSON} $out/etc/distributed-nix/runtime.json
    ln -s ${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt $out/etc/ssl/certs/ca-certificates.crt
    ln -s ${pkgs.bash}/bin/bash $out/bin/bash
    ln -s ${pkgs.bash}/bin/sh $out/bin/sh
    echo 'root:x:0:0:root:/root:/bin/bash' > $out/etc/passwd
    echo 'root:x:0:' > $out/etc/group
  '';
in pkgs.dockerTools.buildLayeredImage {
  name = "ghcr.io/dialohq/k8s-distributed-nix-builders";
  tag = if githubRunner == null then "0.4.0" else "0.4.0-arc";
  contents = runtime ++ [files pkgs.path];
  includeNixDB = true;
  maxLayers = 100;
  extraCommands = ''
    rm -rf etc/distributed-nix
    mkdir -p etc/distributed-nix usr/bin
    ln -sf ${package}/bin/distributed-nix bin/distributed-nix
    ln -sf /proc/mounts etc/mtab
    cp ${files}/etc/distributed-nix/runtime.json etc/distributed-nix/runtime.json
    rm -f etc/passwd etc/group
    cp ${files}/etc/passwd etc/passwd
    cp ${files}/etc/group etc/group
    ln -sf ${pkgs.coreutils}/bin/env usr/bin/env
    rm -rf tmp
    mkdir tmp
    chmod 1777 tmp
  '';
  config = {
    Labels = {
      "org.opencontainers.image.source" = "https://github.com/dialohq/k8s-distributed-nix-builders";
      "org.opencontainers.image.version" = "0.4.0";
    };
    Entrypoint = ["${pkgs.tini}/bin/tini" "--" "${package}/bin/distributed-nix" "pod"];
    Env = ["PATH=${pkgs.lib.makeBinPath runtime}" "SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt" "NIX_SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt" "NIX_CONFIG=experimental-features = nix-command flakes ca-derivations" "DISTRIBUTED_NIX_ONLINE_CONFIG=/run/distributed-nix-config.json"];
    WorkingDir = "/work";
  };
}
