#!/bin/zsh
# run-cmp.sh <harness> <cmp_iters> [nested]
#
# Round-2 cmp-cell runner: run-one.sh plus the EXACT per-loop unwind bounds
# the cmp formulas need. Dead loop copies of the token walk multiply the
# jsonb state-machine cross-product ~10x per copy (measured), so the two
# compare loops (Rust compare_containers_fixed + C compareJsonbContainers)
# get exact-fit bounds per cell, and the iterator-internal loops get their
# structural bounds. The Rust labels embed the harness-crate hash; if
# src/lib.rs changes, re-harvest via:
#   grep -ohE "Unwinding loop [^ ]+" /tmp/jp2-*.log | sort -u
CRATE_HASH=slPHXV9xPVYO
h=$1
iters=$2
nested=${3:-flat}
offb=${OFFB:-5}
rustcmp1="_RINvNtCs6piKExmlbnT_9adt_jsonb3ops24compare_containers_fixedKj1_EC${CRATE_HASH}_17proof_jsonb_probe.0"
rustcmp2="_RINvNtCs6piKExmlbnT_9adt_jsonb3ops24compare_containers_fixedKj2_EC${CRATE_HASH}_17proof_jsonb_probe.0"
rustoff="_RNvNtCs6piKExmlbnT_9adt_jsonb9container16get_jsonb_offset.0"
rustvstr="_RNvCs5hWUnuVq0gd_7varlena10varstr_cmp.0"
itnext3=2
[ "$nested" = nested ] && itnext3=3
exec "$(dirname "$0")/run-one.sh" "$h" --exact ${=EXTRA} -Z unstable-options --cbmc-args ${=CBMCX} \
  --unwindset "${rustcmp1}:${iters}" \
  --unwindset "${rustcmp2}:${iters}" \
  --unwindset "pg_compareJsonbContainers.0:${iters}" \
  --unwindset "pg_compareJsonbContainers.1:3" \
  --unwindset "pg_compareJsonbContainers.2:3" \
  --unwindset "pg_JsonbIteratorNext.3:${itnext3}" \
  --unwindset "pg_getJsonbOffset.0:${offb}" \
  --unwindset "${rustoff}:${offb}" \
  --unwindset "${rustvstr}:4" \
  --unwindset "memcmp.0:4" \
  --unwindset "pgp_cmp_staged.0:25" \
  --unwindset "pgp_cmp_staged.1:25"
