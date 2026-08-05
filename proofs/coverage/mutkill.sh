#!/bin/bash
# mutkill.sh <file> <line> <old> <new> <target>
# Apply a single-token mutation at file:line, rebuild the fuzz target, replay
# the committed corpus (-runs=0), report KILLED/SURVIVED, revert.
#
# The one-line answer to "does the differential plane actually kill this
# cargo-mutants survivor?" A SURVIVED verdict means the corpus lacks a
# witnessing input, not that the mutant is equivalent — seed the plane and
# re-run (Lane-0B mac/mac8 pilot, 2026-07-30).
#
# An unmatched <old> token is a HARD ERROR (exit 2), never a verdict: the
# pre-fix script ran the UNMUTATED build after a typo'd token and printed
# SURVIVED — a fail-open (gate-blindness law, fixed 2026-07-31, task #55).
set -u
cd "$(git rev-parse --show-toplevel)" || exit 2
if [ $# -ne 5 ]; then
  echo "usage: mutkill.sh <file> <line> <old> <new> <target>   (exactly 5 args; there is NO col argument)" >&2
  exit 2
fi
F=$1; L=$2; OLD=$3; NEW=$4; T=$5
if ! python3 - "$F" "$L" "$OLD" "$NEW" <<'PY'
import sys
f,l,old,new=sys.argv[1],int(sys.argv[2]),sys.argv[3],sys.argv[4]
lines=open(f).read().splitlines(keepends=True)
if l < 1 or l > len(lines):
    sys.stderr.write("mutkill: line %d out of range (%s has %d lines)\n" % (l, f, len(lines)))
    sys.exit(1)
if old not in lines[l-1]:
    sys.stderr.write("mutkill: token %r NOT FOUND at %s:%d: %r\n" % (old, f, l, lines[l-1]))
    sys.exit(1)
lines[l-1]=lines[l-1].replace(old,new,1)
open(f,'w').write(''.join(lines))
PY
then
  echo "ERROR    $F:$L $OLD->$NEW — mutation NOT APPLIED (token/line mismatch); refusing to conclude" >&2
  exit 2
fi
(cd fuzz && cargo +nightly fuzz build "$T" >/dev/null 2>&1 && cargo +nightly fuzz run "$T" "corpus/$T" -- -runs=0 >/dev/null 2>&1)
rc=$?
git checkout "$F" >/dev/null 2>&1
if [ $rc -ne 0 ]; then echo "KILLED   $F:$L $OLD->$NEW (via $T corpus)"; else echo "SURVIVED $F:$L $OLD->$NEW (via $T corpus)"; fi
