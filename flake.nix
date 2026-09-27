{
  description = "Shared Nix packages with local metadata for ARC and Kubernetes builders";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/b6018f87da91d19d0ab4cf979885689b469cdd41";
  inputs.runnerPkgs.url = "github:NixOS/nixpkgs/e158d9ed9b51c98974c5e66e1ba1c9e0255fecaa";
  outputs = {
    self,
    nixpkgs,
    runnerPkgs,
  }: let
    system = "x86_64-linux";
    pkgs = import nixpkgs {inherit system;};
    package = import ./default.nix {inherit pkgs;};
  in {
    packages.${system} = {
      default = package;
      distributed-nix = package;
      deploymentTools = pkgs.buildEnv {
        name = "distributed-nix-deployment-tools";
        paths = [pkgs.kubernetes-helm pkgs.kubectl pkgs.kind];
      };
      arcImage = import ./deploy/image { inherit pkgs package; githubRunner = (import runnerPkgs {inherit system;}).github-runner; };
      image = import ./deploy/image {inherit pkgs package;};
    };
    checks.${system}.distributed-nix = package;
    devShells.${system}.default = pkgs.mkShell {
      inputsFrom = [package];
      nativeBuildInputs = [pkgs.cargo pkgs.rustc pkgs.rustfmt pkgs.util-linux pkgs.nixVersions.latest];
      NIX_CONFIG = "experimental-features = nix-command flakes ca-derivations";
    };
  };
}
