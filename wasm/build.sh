#!/usr/bin/env bash
# build.sh — assemble the pgrust wasm webapp assets (postgres.wasm + VFS image).
#
# Produces under wasm/assets/ (or PGRUST_ASSETS):
#   postgres.wasm   — the wasm32-wasip1 single-user module
#   vfs.img         — packed VFS file bytes (datadir at /pgdata, share at /share)
#   vfs.json        — VFS manifest (paths/offsets/sizes/modes)
#   *.gz / *.br     — precompressed variants for serve.mjs
#
# These are gitignored build artifacts — regenerate, don't commit.
#
# Inputs (override via env):
#   PGRUST_ASSETS  output directory for browser assets (default: ./assets)
#   PGRUST_WASM    path to a built postgres.wasm
#                  (default: target/wasm32-wasip1/wasm-release/postgres.wasm,
#                   falling back to the debug build)
#   PGRUST_DATADIR an initdb'd datadir to bundle at /pgdata; when unset, one is
#                  minted here with the C PostgreSQL 18 initdb (set PGINSTALL
#                  to its bin dir; /opt/homebrew/bin is probed)
#   PGRUST_SHARE   a share dir carrying timezone/timezonesets, bundled at
#                  /share; when unset it is derived from the initdb install.
#                  MUST be symlink-free (this script copies with cp -RL —
#                  WASI-style preopens refuse symlink escapes).
#
# To build the wasm module itself (pinned nightly + build-std + wasm-EH):
#   PGRUST_WASM_PROFILE=wasm-release wasm/wasm-build.sh
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
ASSETS="${PGRUST_ASSETS:-$HERE/assets}"
mkdir -p "$ASSETS"

WASM="${PGRUST_WASM:-}"
if [[ -z "$WASM" ]]; then
  for cand in "$ROOT/target/wasm32-wasip1/wasm-release/postgres.wasm" \
              "$ROOT/target/wasm32-wasip1/debug/postgres.wasm"; do
    [[ -f "$cand" ]] && WASM="$cand" && break
  done
fi
[[ -n "$WASM" && -f "$WASM" ]] || { echo "wasm not found (build it first — see header)"; exit 1; }

VFSROOT="$(mktemp -d)"
trap 'rm -rf "$VFSROOT" "$VFSROOT-datadir"' EXIT

DATADIR="${PGRUST_DATADIR:-}"
SHARE="${PGRUST_SHARE:-}"
if [[ -z "$DATADIR" ]]; then
  PGBIN=""
  for cand in "${PGINSTALL:-}" /tmp/pgrust_pginstall/bin /opt/homebrew/bin; do
    [[ -n "$cand" && -x "$cand/initdb" ]] && PGBIN="$cand" && break
  done
  [[ -n "$PGBIN" ]] || { echo "no PostgreSQL 18 initdb found (set PGINSTALL or PGRUST_DATADIR)"; exit 1; }
  echo "[build] minting datadir with $PGBIN/initdb..."
  DATADIR="$VFSROOT-datadir"
  "$PGBIN/initdb" -D "$DATADIR" --no-locale --encoding=UTF8 -U postgres -A trust > "$VFSROOT/initdb.log" 2>&1 \
    || { tail -20 "$VFSROOT/initdb.log"; exit 1; }
  rm -f "$VFSROOT/initdb.log"
  if [[ -z "$SHARE" ]]; then
    for tz in "$PGBIN/../share/postgresql/timezone" "$PGBIN/../share/postgresql@18/timezone"; do
      [[ -d "$tz" ]] && SHARE="$(dirname "$tz")" && break
    done
  fi
fi
[[ -d "$DATADIR" ]] || { echo "datadir not found: $DATADIR"; exit 1; }
[[ -n "$SHARE" && -d "$SHARE/timezone" ]] || { echo "share dir with timezone/ not found (set PGRUST_SHARE)"; exit 1; }

echo "[build] copying wasm module ($WASM)..."
cp "$WASM" "$ASSETS/postgres.wasm"

echo "[build] assembling VFS tree (datadir at /pgdata, share at /share)..."
mkdir -p "$VFSROOT/pgdata" "$VFSROOT/share"
cp -R "$DATADIR/." "$VFSROOT/pgdata/"
# cp -RL: timezone entries can be absolute symlinks (homebrew Cellar) and the
# in-memory VFS, like a WASI preopen, has no symlinks.
cp -RL "$SHARE/timezone" "$VFSROOT/share/timezone"
[[ -d "$SHARE/timezonesets" ]] && cp -RL "$SHARE/timezonesets" "$VFSROOT/share/timezonesets"

echo "[build] packing VFS image..."
node "$HERE/pack-vfs.mjs" "$VFSROOT" "$ASSETS/vfs"

if [[ "${PGRUST_COMPRESS:-1}" != "0" ]]; then
  echo "[build] compressing large browser assets..."
  gzip -kf9 "$ASSETS/postgres.wasm" "$ASSETS/vfs.img"
  if command -v brotli >/dev/null 2>&1; then
    brotli -f -q 9 "$ASSETS/postgres.wasm" "$ASSETS/vfs.img"
  fi
else
  echo "[build] compression SKIPPED (PGRUST_COMPRESS=0)"
fi

echo "[build] done:"
ls -la "$ASSETS"
