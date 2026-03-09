{
  description = "Simple Rust environment";
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };
  outputs = { self, nixpkgs, rust-overlay }:
    let
      system = "x86_64-darwin";
      overlays = [ (import rust-overlay) ];
      pkgs = import nixpkgs {
        inherit system overlays;
      };
    in {
      devShells.${system}.default = pkgs.mkShell {
        buildInputs = with pkgs; [
          # Switched from nightly to stable. This guarantees rustfmt works!
          (rust-bin.stable.latest.default.override {
            extensions = [ "rust-src" "rust-analyzer" "rustfmt" ];
          })
          pkg-config
          cargo-insta
          openssl
          git
          python3
        ];
      };
    };
}
