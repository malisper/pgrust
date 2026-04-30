#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PGRUST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

repo_name="$(printf '%s' "$(basename "$PGRUST_DIR")" | tr -c '[:alnum:]_.-' '-')"

pool_size="${PGRUST_TARGET_POOL_SIZE:-8}"
if ! [[ "$pool_size" =~ ^[1-9][0-9]*$ ]]; then
  echo "PGRUST_TARGET_POOL_SIZE must be a positive integer, got: $pool_size" >&2
  exit 2
fi

pool_root="${PGRUST_TARGET_POOL_DIR:-/tmp/pgrust-target-pool/${repo_name}}"
if [[ -n "${PGRUST_TARGET_SLOT:-}" ]]; then
  if ! [[ "$PGRUST_TARGET_SLOT" =~ ^[0-9]+$ ]]; then
    echo "PGRUST_TARGET_SLOT must be a non-negative integer, got: $PGRUST_TARGET_SLOT" >&2
    exit 2
  fi
  if (( PGRUST_TARGET_SLOT >= pool_size )); then
    echo "PGRUST_TARGET_SLOT must be less than PGRUST_TARGET_POOL_SIZE ($pool_size), got: $PGRUST_TARGET_SLOT" >&2
    exit 2
  fi
  target_slot="$PGRUST_TARGET_SLOT"
else
  slot_key="${PGRUST_TARGET_POOL_KEY:-$PGRUST_DIR}"
  slot_hash="$(printf '%s' "$slot_key" | shasum -a 1 | awk '{print substr($1, 1, 8)}')"
  target_slot=$((16#$slot_hash % pool_size))
fi

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${pool_root}/${target_slot}}"

if [[ "${1:-}" == "--print-target-dir" ]]; then
  printf '%s\n' "$CARGO_TARGET_DIR"
  exit 0
fi

if [[ -z "${RUSTC_WRAPPER:-}" ]] && command -v sccache >/dev/null 2>&1; then
  export RUSTC_WRAPPER="$PGRUST_DIR/scripts/rustc_sccache_wrapper.sh"
fi

exec cargo "$@"
