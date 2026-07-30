{
  description = "Development pgrust in FHS izolation";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, utils }:
    utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
      in
      {
        devShells.default = (pkgs.buildFHSEnv {
          name = "pgrust-fhs-shell";

          targetPkgs = pkgs: with pkgs; [
            gcc
            gnumake
            pkg-config

            rustc
            cargo

            glibc.dev

            re2
            abseil-cpp
            icu
          ];

          profile = ''
            unset NIX_CFLAGS_COMPILE
            unset NIX_LDFLAGS
            export CC=gcc
            export CXX=g++

            mkdir -p .nix-fhs-home

            export HOME="$PWD/.nix-fhs-home"

            cat << 'EOF' > "$HOME/.bashrc"
              [ -f /etc/bash.bashrc ] && source /etc/bash.bashrc

              export PROJECT_ROOT="$PWD"

              get_relative_project_path() {
                  if [ "$PWD" = "$PROJECT_ROOT" ]; then
                    echo "pgrust"
                  elif [[ "$PWD" == "$PROJECT_ROOT/"* ]]; then
                    echo "pgrust''${PWD#$PROJECT_ROOT}"
                  else
                    echo "$PWD"
                  fi
              }

              set_my_prompt() {
                  local rel_path=$(get_relative_project_path)
                  export PS1="(pgrust-fhs) $rel_path \$ "
              }

              export PROMPT_COMMAND=set_my_prompt
EOF
          '';
        }).env;
      }
    );
}
