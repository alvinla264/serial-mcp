{
  description = "Generic Serial Transport MCP Server";

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
          pytest
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
            mkdir -p $out/bin $out/share/serial-mcp
            # Modules live beside the launcher copy so both the legacy MCP
            # server and the daemon (which imports server.py) resolve them.
            cp server.py serial_mcp_ipc.py serial_mcp_daemon.py serial_mcp_terminal.py $out/share/serial-mcp/
            cp server.py $out/bin/serial-mcp
            chmod +x $out/bin/serial-mcp

            # Wrap the script to use the correct python environment
            wrapProgram $out/bin/serial-mcp \
              --prefix PATH : ${pythonEnv}/bin \
              --prefix PYTHONPATH : $out/share/serial-mcp

            makeWrapper ${pythonEnv}/bin/python3 $out/bin/serial-mcp-daemon \
              --add-flags "-m serial_mcp_daemon" \
              --prefix PYTHONPATH : $out/share/serial-mcp

            makeWrapper ${pythonEnv}/bin/python3 $out/bin/serial-mcp-terminal \
              --add-flags "-m serial_mcp_terminal" \
              --prefix PYTHONPATH : $out/share/serial-mcp
          '';
        };

        apps.default = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/serial-mcp";
        };

        apps.daemon = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/serial-mcp-daemon";
        };

        apps.terminal = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/serial-mcp-terminal";
        };
      }
    );
}
