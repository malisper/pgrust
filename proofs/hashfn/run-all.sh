#!/bin/sh
# Run every equivalence harness individually, print per-harness verdict +
# wall time. kissat for expected-green single-claim runs (solver law);
# the must-fail control runs separately with DEFAULT. From the crate dir:
#   sh run-all.sh [timeout-secs]
set -u
cd "$(dirname "$0")"
CAP="${1:-600}"
HARNESSES="
eq_hash_bytes_len16
eq_hash_bytes_extended_len16
eq_hash_bytes_uint32
eq_hash_bytes_uint32_extended
eq_string_hash_len8
eq_string_hash_len8_huge_keysize
eq_tag_hash_len12
eq_uint32_hash
eq_hash_combine
eq_hash_combine64
eq_murmurhash32
eq_murmurhash64
eq_rotate_high_and_low_32bits
murmur32_inverse_roundtrip
"
for h in $HARNESSES; do
    t0=$(date +%s)
    out=$(timeout "$CAP" "$HOME/.cargo/bin/cargo-kani" kani -Z c-ffi --c-lib c_hashfn.c --harness "$h" --solver kissat 2>&1)
    rc=$?
    t1=$(date +%s)
    verdict=$(printf '%s\n' "$out" | grep 'VERIFICATION:' | tail -1)
    echo "$h rc=$rc wall=$((t1 - t0))s ${verdict:-NO-VERDICT (timeout/crash)}"
done
