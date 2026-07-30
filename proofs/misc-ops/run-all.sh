#!/bin/sh
# Run every STANDING misc-ops equivalence harness individually (30s hard
# cap each, prove-target budget), print per-harness verdict + wall time.
# From the crate directory:  sh run-all.sh
#
# Deliberately NOT run here (expected-FAIL, DEFAULT solver — run manually):
#   control_booland_vs_c_boolor   -- negative control, MUST fail
#   control_date_pli_vs_c_mii     -- negative control, MUST fail
#   census_tidin_empty_field      -- proved-divergence witness, MUST fail
set -u
cd "$(dirname "$0")"
KANI="${HOME}/.cargo/bin/cargo-kani"
run() {
    # Retry on external SIGTERM contamination ("CBMC failed with status 15"):
    # other agents' scoped kills can snipe an in-flight run. Not a verdict.
    for _try in 1 2 3; do
        t0=$(date +%s)
        # DEFAULT (incremental) solver: measured on this crate it beats
        # kissat, which wedges in per-batch propositional reduction on the
        # many-property mixed-arm harnesses here (tidout r1: default 20s
        # GREEN vs kissat >30s wall; u32in_len4: default 16s GREEN vs
        # kissat wall). Skill's kissat-first advice inverted by data.
        out=$(timeout 30 "$KANI" kani -Z c-ffi -Z stubbing --exact --harness "proofs::$1" 2>&1)
        rc=$?
        t1=$(date +%s)
        # timeout-killed cargo-kani orphans its cbmc children: kill scoped
        # to THIS crate's target path only (never global pkill).
        [ $rc -eq 124 ] && pkill -f 'misc-ops/target' 2>/dev/null
        if printf '%s\n' "$out" | grep -q 'CBMC failed with status 15'; then
            echo "$1 sniped (external SIGTERM), retrying"
            sleep 2
            continue
        fi
        verdict=$(printf '%s\n' "$out" | grep 'VERIFICATION:' | tail -1)
        echo "$1 rc=$rc wall=$((t1 - t0))s ${verdict:-NO-VERDICT (timeout/crash)}"
        return
    done
    echo "$1 NO-VERDICT (sniped 3x)"
}
# MEASURED WALLS (not standing; kept in src/lib.rs for re-measurement):
#   eq_tidin_len11..len20            -- symbolic-length cost is total-length
#     bound (len9 15s, len10 22s, len11+ >30s); tidin symbolic domain is
#     len <= 10, wider inputs covered by eq_tidin_spots only.
#   eq_u32in_len6..len12, eq_u64in_len6..len12, eq_u64in_digits13..21
#     -- general-grammar symbolic bytes wall past len 5 (len5 23-26s
#     unloaded, len6+ >30s); wider inputs covered by the split concrete
#     spot batches. NOTE the spot sets were originally ONE harness each and
#     WALLED at 30s: CBMC re-solves per property batch, so few-assertion
#     harnesses are the ladder step (each batch now 4-15s).
#   artifact_probe_tidout_d5..d7b, artifact_probe_ultoa_dest_offset1
#     -- expected FAIL, CBMC destination-offset artifact (see src/lib.rs).
#   eq_tidin_accept_* (all 14)       -- same total-length wall: even with
#     concrete field widths and digits-only bytes, 13+ byte inputs exceed
#     30s. Full-syntax accepts covered by eq_tidin_spots concretely.
HARNESSES="
eq_booland_statefunc eq_boolor_statefunc
eq_date_pli eq_date_mii
eq_tidin_len0 eq_tidin_len1 eq_tidin_len2 eq_tidin_len3 eq_tidin_len4
eq_tidin_len5 eq_tidin_len6 eq_tidin_len7 eq_tidin_len8 eq_tidin_len9
eq_tidin_len10 cover_tidin_len_split
eq_tidin_spots
eq_tidout_r1_lt1e4 cover_tidout_block_split eq_tidout_spots
artifact_control_ultoa_dest_offset0
eq_u32in_len0 eq_u32in_len1 eq_u32in_len2 eq_u32in_len3 eq_u32in_len4
eq_u32in_len5 cover_u32in_len_split
eq_u32in_spots_dec eq_u32in_spots_base eq_u32in_spots_base_edge
eq_u32in_spots_neg eq_u32in_spots_space eq_u32in_spots_reject
eq_u32in_spots_range eq_u32in_spots_hexmax
eq_u64in_len0 eq_u64in_len1 eq_u64in_len2 eq_u64in_len3 eq_u64in_len4
eq_u64in_len5 cover_u64in_len_split
eq_u64in_spots_dec eq_u64in_spots_range eq_u64in_spots_hex
eq_u64in_spots_octal eq_u64in_spots_misc
eq_oidout_r1_lt1e4 eq_oidout_d5 eq_oidout_d6 eq_oidout_d7a eq_oidout_d7b
cover_oidout_split eq_oidout_spots
eq_xid8out_r1_lt1e4 eq_xid8out_d5 eq_xid8out_d6 eq_xid8out_d7a eq_xid8out_d7b
cover_xid8out_split eq_xid8out_spots
"
for h in $HARNESSES; do run "$h"; done
