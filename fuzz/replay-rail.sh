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
# STRAY-ARTIFACT NAME GUARD (task #95, 2026-08-01): the CI cluster runner sweeps
# the WHOLE fuzz tree for libFuzzer artifact names (crash-*/oom-*/timeout-*/
# leak-*, pruning only artifacts/ corpus/ target/ coverage/) and classifies
# every hit as the CURRENT job's divergence.  Committed evidence banks named
# crash-<sha1> (CI-evidence/, artifacts-triage/) therefore polluted EVERY
# CI cluster job's verdict with identical cross-target failures.  Banked evidence
# must use the banked-crash-* prefix; this guard fails the rail loudly if a
# raw libFuzzer artifact name ever gets committed outside corpus/ again.
strays=$(git ls-files . 2>/dev/null \
  | grep -vE '^corpus/' \
  | grep -E '(^|/)(crash|oom|timeout|leak)-[0-9a-f]' || true)
if [ -n "$strays" ]; then
  echo "FAIL: committed libFuzzer-artifact-named file(s) outside corpus/ —"
  echo "the CI cluster runner's stray sweep will misattribute these to every job."
  echo "Rename with the banked- prefix:"
  echo "$strays"
  rc=1
fi
for t in $TARGETS; do
  case " $NOT_LIVE " in *" $t "*) echo "SKIP $t (not live: see replay-rail.sh header)"; continue;; esac
  [ -d "corpus/$t" ] || { echo "SKIP $t (no corpus)"; continue; }
  n=$(find "corpus/$t" -type f | wc -l | tr -d ' ')
  echo "== replay $t over $n inputs"
  # -rss_limit_mb=8192: the replay rail's verdict is the comparator planes,
  # not libFuzzer's memory heuristic.  cargo-fuzz builds with ASan, whose
  # shadow/redzones inflate RSS ~10x past the 2048MB default on legitimately
  # memory-hungry banked units (witnessed 2026-08-01: regexp_diff units
  # `.(\y|){21,}...` and oom-617cb6e8 `(l*|\y){11,}?...` —
  # REG_MAX_COMPILE_SPACE-bounded, C-parity, pass all planes, 480MB native
  # RSS, but >2GiB / >4GiB respectively under ASan).  OOM DISCOVERY stays
  # owned by the fuzz-mode CI cluster legs at the default limit.
  cargo +$NIGHTLY fuzz run "$t" -- -runs=0 -rss_limit_mb=8192 "corpus/$t" >/dev/null 2>&1 \
    || { echo "FAIL: $t replay diverged/crashed"; rc=1; }
done
[ $rc -eq 0 ] && echo "REPLAY RAIL GREEN"
exit $rc
