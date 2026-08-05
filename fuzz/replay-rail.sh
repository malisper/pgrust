#!/bin/bash
# fuzz/replay-rail.sh — CI regression rail for the differential fuzz targets
# (fuzzuproof-crate done-gate item 4). Replays every COMMITTED corpus input
# through its target once (-runs=0: load corpus, execute, no fuzzing) and
# fails on any divergence/crash. The committed corpora are the durable bank
# (fleet S3 has a 7-day lifecycle); this replay is also the oracle-drift
# detector (LIT-REVIEW section 6.4).
#
#   cd fuzz && ./replay-rail.sh [target...]     # default: all with corpora
#
# Needs nightly-2026-07-17 (libFuzzer). PGRUST_FUZZ_CSANCOV optional here —
# replay compares planes either way.
#
# Failure classification (task #151): per-target replay output is persisted
# to target/replay-rail/<target>.log (gitignored). A nonzero replay whose
# log carries an "ERROR: AddressSanitizer"/"ERROR: LeakSanitizer" report is
# printed as "ASAN-FINDING: <target>" — a sanitizer side channel per the
# standing ruling, distinguishable from a genuine "FAIL: <target> replay
# diverged/crashed" — but it still fails the rail (rc=1): a sanitizer hit
# during replay must never silently pass.
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
# STRAY-ARTIFACT NAME GUARD (task #95, 2026-08-01): the fleet runner sweeps
# the WHOLE fuzz tree for libFuzzer artifact names (crash-*/oom-*/timeout-*/
# leak-*, pruning only artifacts/ corpus/ target/ coverage/) and classifies
# every hit as the CURRENT job's divergence.  Committed evidence banks named
# crash-<sha1> (fleet-evidence/, artifacts-triage/) therefore polluted EVERY
# fleet job's verdict with identical cross-target failures.  Banked evidence
# must use the banked-crash-* prefix; this guard fails the rail loudly if a
# raw libFuzzer artifact name ever gets committed outside corpus/ again.
strays=$(git ls-files . 2>/dev/null \
  | grep -vE '^corpus/' \
  | grep -E '(^|/)(crash|oom|timeout|leak)-[0-9a-f]' || true)
if [ -n "$strays" ]; then
  echo "FAIL: committed libFuzzer-artifact-named file(s) outside corpus/ —"
  echo "the fleet runner's stray sweep will misattribute these to every job."
  echo "Rename with the banked- prefix:"
  echo "$strays"
  rc=1
fi
# Replay logs: NOT discarded (task #151 — a FAIL with the output thrown away
# is untriageable). target/ is gitignored and pruned by the fleet stray
# sweep; the .log names never match the libFuzzer artifact-name guard above.
LOGDIR="target/replay-rail"
mkdir -p "$LOGDIR"
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
  # owned by the fuzz-mode fleet legs at the default limit.
  log="$LOGDIR/$t.log"
  if ! cargo +$NIGHTLY fuzz run "$t" -- -runs=0 -rss_limit_mb=8192 "corpus/$t" >"$log" 2>&1; then
    if grep -qE 'ERROR: (AddressSanitizer|LeakSanitizer)' "$log"; then
      # Sanitizer report during replay: side channel (never a differential
      # verdict), reported distinctly — but still fails the rail.
      echo "ASAN-FINDING: $t replay hit a sanitizer report (side channel, still red) — log: $(pwd)/$log"
    else
      echo "FAIL: $t replay diverged/crashed — log: $(pwd)/$log"
    fi
    tail -n 15 "$log" | sed 's/^/  | /'
    rc=1
  fi
done
[ $rc -eq 0 ] && echo "REPLAY RAIL GREEN"
exit $rc
