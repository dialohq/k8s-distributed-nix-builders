{
  description = "Shared Nix packages with local metadata for ARC and Kubernetes builders";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/b6018f87da91d19d0ab4cf979885689b469cdd41";
  outputs = {
    self,
    nixpkgs,
  }: let
    system = "x86_64-linux";
    pkgs = import nixpkgs {inherit system;};
    package = import ./default.nix {inherit pkgs;};
  in {
    packages.${system} = {
      default = package;
      distributed-nix = package;
    };
    checks.${system}.distributed-nix = package;
    devShells.${system}.default = pkgs.mkShell {
      inputsFrom = [package];
      nativeBuildInputs = [pkgs.cargo pkgs.rustc pkgs.rustfmt pkgs.util-linux pkgs.nixVersions.latest];
      NIX_CONFIG = "experimental-features = nix-command flakes ca-derivations";
    };
  };
}
