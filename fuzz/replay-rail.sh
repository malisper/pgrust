#!/bin/bash
# fuzz/replay-rail.sh — CI regression rail for the differential fuzz targets
# (fuzzuproof-crate done-gate item 4). Replays every COMMITTED corpus input
# through its target once (-runs=0: load corpus, execute, no fuzzing) and
# fails on any divergence/crash. The committed corpora are the durable bank
# (CI cluster S3 has a 7-day lifecycle); this replay is also the oracle-drift
# detector (LIT-REVIEW section 6.4).
#
#   cd fuzz && ./replay-rail.sh [target...]     # default: all with corpora
#
# Needs nightly-2026-07-17 (libFuzzer). PGRUST_FUZZ_CSANCOV optional here —
# replay compares planes either way.
set -eu
cd "$(dirname "$0")"
NIGHTLY=nightly-2026-07-17
TARGETS="${*:-}"
if [ -z "$TARGETS" ]; then
  TARGETS=$(for d in corpus/*/; do basename "$d"; done)
fi
# NON-LIVE corpus dirs (explicit, documented — extend only with a reason):
#   encode_diff     — scaffold target, todo!() body; adopt per
#                     README-TODO-encode_diff.md before removing this skip.
#   formatting_diff — banked seeds only, no fuzz target yet; several seeds
#                     are EXPECTED-DIVERGENCE cells (Y,YYY carve, ledger oids
#                     1778/1780) — see corpus/formatting_diff/README.md.
NOT_LIVE="encode_diff formatting_diff"
rc=0
for t in $TARGETS; do
  case " $NOT_LIVE " in *" $t "*) echo "SKIP $t (not live: see replay-rail.sh header)"; continue;; esac
  [ -d "corpus/$t" ] || { echo "SKIP $t (no corpus)"; continue; }
  n=$(find "corpus/$t" -type f | wc -l | tr -d ' ')
  echo "== replay $t over $n inputs"
  cargo +$NIGHTLY fuzz run "$t" -- -runs=0 "corpus/$t" >/dev/null 2>&1 \
    || { echo "FAIL: $t replay diverged/crashed"; rc=1; }
done
[ $rc -eq 0 ] && echo "REPLAY RAIL GREEN"
exit $rc
