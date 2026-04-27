#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT_DIR="$ROOT/web/wasm-demo/pkg"

if ! command -v wasm-bindgen >/dev/null 2>&1; then
  echo "wasm-bindgen CLI is required."
  echo "Install it with:"
  echo "  cargo install wasm-bindgen-cli"
  exit 1
fi

cd "$ROOT"
# Keep the default crate output lean for normal builds; the wasm demo is the
# only place that needs a cdylib artifact.
cargo rustc --target wasm32-unknown-unknown --release --lib --crate-type cdylib
TARGET_DIR="$("$ROOT/scripts/cargo_target_dir.sh")"
WASM_ARTIFACT="$TARGET_DIR/wasm32-unknown-unknown/release/pgrust.wasm"
if [[ ! -f "$WASM_ARTIFACT" ]]; then
  WASM_ARTIFACT="$TARGET_DIR/wasm32-unknown-unknown/release/deps/pgrust.wasm"
fi
if [[ ! -f "$WASM_ARTIFACT" ]]; then
  echo "could not find pgrust.wasm under $TARGET_DIR/wasm32-unknown-unknown/release" >&2
  exit 1
fi
mkdir -p "$OUT_DIR"
wasm-bindgen \
  --target web \
  --out-dir "$OUT_DIR" \
  "$WASM_ARTIFACT"

# Newer wasm-bindgen output can emit a bare `env` module import that plain
# browser ESM won't resolve. Rewrite it to a local stub for static serving.
if grep -q 'from "env"' "$OUT_DIR/pgrust.js"; then
  perl -0pi -e 's/from "env"/from "..\/env.js"/g' "$OUT_DIR/pgrust.js"
fi

echo "Built browser package into $OUT_DIR"
