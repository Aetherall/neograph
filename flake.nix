{
  description = "neographzig - Reactive graph database in Zig";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    zig-overlay.url = "github:mitchellh/zig-overlay";
  };

  outputs = { self, nixpkgs, flake-utils, zig-overlay }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ zig-overlay.overlays.default ];
        };

        # Test timeout (5 seconds per test)
        testTimeout = "5s";
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            zigpkgs.master  # Latest Zig from master
            zls             # Zig Language Server
          ];

          shellHook = ''
            echo "neographzig development environment"
            echo "Zig version: $(zig version)"
            echo "Tip: Run tests with timeout using: zig build test -- --test-timeout 5s"
          '';
        };

        # Convenience apps for running tests with timeout
        apps = {
          test = {
            type = "app";
            program = "${pkgs.writeShellScript "test" ''
              ${pkgs.zigpkgs.master}/bin/zig build test -- --test-timeout ${testTimeout}
            ''}";
          };
          test-unit = {
            type = "app";
            program = "${pkgs.writeShellScript "test-unit" ''
              ${pkgs.zigpkgs.master}/bin/zig build test-unit -- --test-timeout ${testTimeout}
            ''}";
          };
          test-integration = {
            type = "app";
            program = "${pkgs.writeShellScript "test-integration" ''
              ${pkgs.zigpkgs.master}/bin/zig build test-integration -- --test-timeout ${testTimeout}
            ''}";
          };
        };
      }
    );
}
