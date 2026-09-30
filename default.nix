{pkgs}:
assert pkgs.nixVersions.latest.version == "2.33.6";
  pkgs.rustPlatform.buildRustPackage {
    pname = "distributed-nix";
    version = (builtins.fromTOML (builtins.readFile ./Cargo.toml)).package.version;
    src = pkgs.lib.fileset.toSource {
      root = ./.;
      fileset = pkgs.lib.fileset.unions [./src ./native ./proto ./tests ./Cargo.toml ./Cargo.lock ./build.rs];
    };
    nativeBuildInputs = [pkgs.pkg-config pkgs.protobuf];
    buildInputs = [pkgs.nixVersions.latest.libs.nix-main pkgs.nixVersions.latest.libs.nix-store pkgs.nlohmann_json pkgs.sqlite];
    nativeCheckInputs = [pkgs.util-linux pkgs.nixVersions.latest];
    preCheck = ''
      export NIX_CONFIG="experimental-features = nix-command flakes ca-derivations"
    '';
    cargoLock.lockFile = ./Cargo.lock;
  }
