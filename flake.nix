{
  description = "BioGRID BioBrick";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-23.05";
    flake-utils.url = "github:numtide/flake-utils";
    dev-shell.url = "github:biobricks-ai/dev-shell";
  };

  outputs = { self, nixpkgs, flake-utils, dev-shell }:
    flake-utils.lib.eachDefaultSystem (system:
      with import nixpkgs { inherit system; }; {
        devShells.default = dev-shell.devShells.${system}.default.overrideAttrs
          (oldAttrs: {
            buildInputs = oldAttrs.buildInputs ++ [
              (python3.withPackages (ps: with ps; [ bs4 numpy pandas pyarrow requests ]))
            ];
          });
      });
}