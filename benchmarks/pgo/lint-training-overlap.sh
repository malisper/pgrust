#!/usr/bin/env sh
# Fail if any PGO training statement is a published measurement statement.
# POSIX sh + awk only: runs on a laptop, in CI, and inside the build pod
# immediately before the instrumented profile pass.
#
#   usage: pgo/lint-training-overlap.sh [<training-file> ...]
#          no args = lint the checked-in corpus
#          PGO_DENYLIST_DIR=<dir>  override the denylist location
#
# Canonicalization and the argument for why it is sound: see
# pgo/lint-training-overlap.awk and pgo/README.md.
#
# Exit 0 = disjoint (prints a PROOF line). Exit 1 = overlap found.
# Exit 2 = usage/IO error — never treat as a pass.
set -u
export LC_ALL=C   # bytes-as-bytes: no locale-dependent matching, no multibyte warnings

HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
DENYDIR="${PGO_DENYLIST_DIR:-$HERE/denylist}"
EXEMPT="$DENYDIR/EXEMPTIONS.txt"
PROG="$HERE/lint-training-overlap.awk"

[ -r "$PROG" ] || { echo "pgo-lint: FATAL missing $PROG" >&2; exit 2; }
[ -d "$DENYDIR" ] || { echo "pgo-lint: FATAL denylist dir missing: $DENYDIR" >&2; exit 2; }
[ -r "$EXEMPT" ] || EXEMPT=""

if [ "$#" -eq 0 ]; then
  set -- "$HERE/corpus/analytics-hits.sql" \
         "$HERE/corpus/oltp-generic.sql" \
         "$HERE/corpus/oltp-schema.sql"
fi
for f in "$@"; do
  [ -r "$f" ] || { echo "pgo-lint: FATAL unreadable training file: $f" >&2; exit 2; }
done

deny_files=$(find "$DENYDIR" -maxdepth 1 -name '*.sql' | sort)
[ -n "$deny_files" ] || { echo "pgo-lint: FATAL no *.sql in $DENYDIR" >&2; exit 2; }

PGO_DENY_FILES="$deny_files" PGO_EXEMPT_FILE="$EXEMPT" awk -f "$PROG" "$@"
