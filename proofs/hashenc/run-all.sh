#!/bin/sh
# Run every standing hashenc equivalence harness individually (120s hard cap
# each), print per-harness verdict + wall time, then run the negative
# control, which MUST FAIL (non-vacuity check). Greens run under kissat;
# the control runs under the DEFAULT solver — suite rule: controls validate
# by counterexample, and kissat does not terminate usefully on failing
# harnesses.
#   sh run-all.sh
set -u
cd "$(dirname "$0")"
KANI="${HOME}/.cargo/bin/cargo-kani"
run() {
    h=$1
    shift
    for _try in 1 2 3 4 5; do
        t0=$(date +%s)
        out=$(timeout 240 "$KANI" kani -Z c-ffi --c-lib c/pg_hashenc_kani.c "$@" --harness "$h" 2>&1)
        rc=$?
        t1=$(date +%s)
        if printf '%s\n' "$out" | grep -q 'CBMC failed with status 15'; then
            echo "$h sniped (external SIGTERM), retrying"
            sleep 2
            continue
        fi
        verdict=$(printf '%s\n' "$out" | grep 'VERIFICATION:' | tail -1)
        echo "$h rc=$rc wall=$((t1 - t0))s ${verdict:-NO-VERDICT (timeout/crash)}"
        return
    done
    echo "$h NO-VERDICT (sniped 5x)"
}
HARNESSES="
eq_b64_enc_len eq_b64_dec_len
eq_b64_encode_len0 eq_b64_encode_len1 eq_b64_encode_len2 eq_b64_encode_len3
eq_b64_encode_len4 eq_b64_encode_len5 eq_b64_encode_len6
eq_b64_encode_len4_short eq_b64_encode_len6_short
eq_b64_decode_len0 eq_b64_decode_len1 eq_b64_decode_len2 eq_b64_decode_len3
eq_b64_decode_len4 eq_b64_decode_len5 eq_b64_decode_len6 eq_b64_decode_len7
eq_b64_decode_len8
eq_bytes_to_hex
eq_ascii_safe_strlcpy
"
for h in $HARNESSES; do
    run "$h" --solver kissat
done
echo "--- negative control (MUST FAIL) ---"
run control_bytes_to_hex_uppercase_must_fail
