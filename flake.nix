{
  description = "Serial MCP Server for Embedded Linux Testing";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        
        # Define dependencies
        pythonPackages = ps: with ps; [
          fastmcp
          pyserial
        ];
        
        pythonEnv = pkgs.python3.withPackages pythonPackages;
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = [ pythonEnv ];
        };

        packages.default = pkgs.stdenv.mkDerivation {
          name = "serial-mcp";
          src = ./.;
          buildInputs = [ pythonEnv pkgs.makeWrapper ];
          installPhase = ''
            mkdir -p $out/bin
            cp server.py $out/bin/serial-mcp
            chmod +x $out/bin/serial-mcp
            
            # Wrap the script to use the correct python environment
            wrapProgram $out/bin/serial-mcp \
              --prefix PATH : ${pythonEnv}/bin
          '';
        };

        apps.default = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/serial-mcp";
        };
      }
    );
}
