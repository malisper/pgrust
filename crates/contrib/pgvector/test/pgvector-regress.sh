#!/usr/bin/env bash
# Run pgvector's own regression suite (test/sql + test/expected, v0.8.5)
# against a pgrust server and diff the output.
#
# pg_regress hides psql's "psql:file:line: " prefix on error lines and runs
# from the test directory (copy.sql writes results/vector.bin); we do the same.
set -euo pipefail

HOST=/tmp; PORT=5440; USER=postgres; KEEP=0
while getopts "h:p:U:k" o; do
  case "$o" in h) HOST=$OPTARG;; p) PORT=$OPTARG;; U) USER=$OPTARG;; k) KEEP=1;; *) exit 2;; esac
done
shift $((OPTIND-1))

PGVECTOR_REF=${PGVECTOR_REF:-v0.8.5}
SRC=${PGVECTOR_SRC:-${TMPDIR:-/tmp}/pgvector-src-$PGVECTOR_REF}
if [ ! -d "$SRC/test/sql" ]; then
  git clone -q --depth 1 --branch "$PGVECTOR_REF" https://github.com/pgvector/pgvector.git "$SRC"
fi

# Tests whose features pgrust does not ship (bit opclass, ivfflat).
SKIP_RE='^(bit|hnsw_bit|ivfflat_.*)$'
if [ $# -eq 0 ]; then
  set -- $(cd "$SRC/test/sql" && ls *.sql | sed 's/\.sql$//' | grep -Ev "$SKIP_RE")
fi

PSQL=${PSQL:-/usr/lib/postgresql/18/bin/psql}
DB=pgvector_regress_$$
"$PSQL" -h "$HOST" -p "$PORT" -U "$USER" -X -q -d postgres -c "CREATE DATABASE $DB" >/dev/null
trap '[ "$KEEP" = 1 ] || "$PSQL" -h "$HOST" -p "$PORT" -U "$USER" -X -q -d postgres -c "DROP DATABASE $DB" >/dev/null' EXIT
"$PSQL" -h "$HOST" -p "$PORT" -U "$USER" -X -q -d "$DB" -c "CREATE EXTENSION vector" >/dev/null

OUT=$(mktemp -d); mkdir -p "$SRC/test/results"
fail=0
for t in "$@"; do
  ( cd "$SRC/test" && "$PSQL" -h "$HOST" -p "$PORT" -U "$USER" -X -a -q -d "$DB" -f "sql/$t.sql" ) \
    > "$OUT/$t.raw" 2>&1 || true
  sed -E 's/^psql:[^:]+:[0-9]+: //' "$OUT/$t.raw" > "$OUT/$t.out"
  if diff -u "$SRC/test/expected/$t.out" "$OUT/$t.out" > "$OUT/$t.diff"; then
    echo "ok    $t"
  else
    echo "FAIL  $t  ($(grep -c '^[-+][^-+]' "$OUT/$t.diff") changed lines, see $OUT/$t.diff)"
    fail=1
  fi
done
[ "$fail" = 0 ] && echo "all selected tests passed"
exit $fail
