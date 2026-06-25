#!/bin/bash
# Data-correctness gate for the short-varlena-packing campaign.
# Boots the lane's freshly-built postgres on a fresh C-initdb cluster and
# round-trips every major varlena type, asserting exact values, lengths, and
# index/TOAST round-trips. Returns 0 only if every check passes.
set -u
WT="$(cd "$(dirname "$0")/../../.." && pwd)"
BIN=$WT/target/debug/postgres
INITDB=/tmp/pgrust_pginstall/bin/initdb
PSQL=/tmp/pgrust_pginstall/bin/psql
REGRESS=/Users/malisper/workspace/work/pgrust/postgres-18.3/src/test/regress
PORT=${PORT:-49555}
DD=/private/tmp/varlena_dd_$$
SOCK=/private/tmp/varlena_sock_$$
mkdir -p "$SOCK"
export PG_ABS_SRCDIR="$REGRESS" PG_LIBDIR=/tmp/pgrust_pginstall/lib/postgresql PG_DLSUFFIX=.dylib
export PGTZ="America/Los_Angeles" PGDATESTYLE="Postgres, MDY" LANG=C LC_MESSAGES=C
export PGOPTIONS="-c intervalstyle=postgres_verbose"
unset LC_ALL LC_COLLATE LC_CTYPE LC_MONETARY LC_NUMERIC LC_TIME LANGUAGE

cleanup() { kill -9 $PM 2>/dev/null; pkill -9 -f "$DD " 2>/dev/null; rm -rf "$DD" "$SOCK"; }
trap cleanup EXIT

rm -rf "$DD"
"$INITDB" -D "$DD" --no-locale --encoding=UTF8 -U postgres >/tmp/varlena_initdb.log 2>&1 || { echo "INITDB FAIL"; cat /tmp/varlena_initdb.log; exit 1; }
"$BIN" -D "$DD" -F -c listen_addresses= -k "$SOCK" -p $PORT -c io_method=sync \
  -c max_stack_depth=7000 -c checkpoint_timeout=3600 -c autovacuum=on \
  -c max_logical_replication_workers=0 >/tmp/varlena_pm.log 2>&1 &
PM=$!
booted=0
for k in $(seq 1 120); do "$PSQL" -h "$SOCK" -p $PORT -U postgres -c 'SELECT 1' >/dev/null 2>&1 && { booted=1; break; }; sleep 0.5; done
[ "$booted" = 1 ] || { echo "BOOT FAIL"; tail -30 /tmp/varlena_pm.log; exit 1; }

Q() { "$PSQL" -h "$SOCK" -p $PORT -U postgres -X -q -t -A "$@"; }

fail=0
check() { # desc expected actual
  if [ "$2" != "$3" ]; then echo "FAIL: $1 -- expected [$2] got [$3]"; fail=1
  else echo "ok: $1"; fi
}

Q < "$(dirname "$0")/varlena_gate.sql" 2>/tmp/varlena_setup.log
if grep -qi error /tmp/varlena_setup.log; then echo "SETUP ERRORS:"; cat /tmp/varlena_setup.log; fi

# text exact value, short
check "text short value" "short text" "$(Q -c "SELECT t FROM vt WHERE id=1")"
check "text short length" "10" "$(Q -c "SELECT length(t) FROM vt WHERE id=1")"
check "varchar short value" "short varchar" "$(Q -c "SELECT vc FROM vt WHERE id=1")"
# text long (>127) value+length
check "text long length" "200" "$(Q -c "SELECT length(t) FROM vt WHERE id=2")"
check "text long firstchar" "A" "$(Q -c "SELECT substr(t,1,1) FROM vt WHERE id=2")"
# bytea exact + octet_length
check "bytea short value" "\\x00ff10" "$(Q -c "SELECT b FROM vt WHERE id=1")"
check "bytea short octet_length" "3" "$(Q -c "SELECT octet_length(b) FROM vt WHERE id=1")"
check "bytea long octet_length" "200" "$(Q -c "SELECT octet_length(b) FROM vt WHERE id=2")"
# numeric exact
check "numeric short" "123.456" "$(Q -c "SELECT n FROM vt WHERE id=1")"
check "numeric long" "9876543210.0123456789" "$(Q -c "SELECT n FROM vt WHERE id=2")"
# int[] / text[]
check "int[] value" "{1,2,3}" "$(Q -c "SELECT ia FROM vt WHERE id=1")"
check "int[] elem" "2" "$(Q -c "SELECT ia[2] FROM vt WHERE id=1")"
check "text[] value" "{a,bb,ccc}" "$(Q -c "SELECT ta FROM vt WHERE id=1")"
check "text[] elem" "ccc" "$(Q -c "SELECT ta[3] FROM vt WHERE id=1")"
# jsonb
check "jsonb field" "v" "$(Q -c "SELECT jb->>'k' FROM vt WHERE id=1")"
check "jsonb arr elem" "2" "$(Q -c "SELECT jb->'arr'->>1 FROM vt WHERE id=1")"
# composite (point is a fixed-len type but test rowtype path)
check "point value" "(1.5,2.5)" "$(Q -c "SELECT comp FROM vt WHERE id=1")"
# TOAST round-trip
check "toast length" "5000" "$(Q -c "SELECT length(t) FROM vt WHERE id=3")"
check "toast firstlast" "QQ" "$(Q -c "SELECT substr(t,1,1)||substr(t,5000,1) FROM vt WHERE id=3")"

# indexed text probe (short) — force index scan
check "indexed text probe" "1" "$(Q -c "SET enable_seqscan=off; SELECT id FROM vt WHERE t='short text'")"
check "indexed text probe long" "2" "$(Q -c "SET enable_seqscan=off; SELECT id FROM vt WHERE t=repeat('A',200)")"

# composite rowtype round-trip via a real composite type
check "composite text field" "hello" "$(Q -c "SELECT (c).a FROM ct")"
check "composite int field" "7" "$(Q -c "SELECT (c).b FROM ct")"

if [ "$fail" = 0 ]; then echo "GATE: ALL PASS"; else echo "GATE: FAILURES PRESENT"; fi
exit $fail
