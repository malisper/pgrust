#!/bin/bash
# Differential sampler driver: adt_cash::cash_numeric / numeric_cash vs REAL
# PostgreSQL 18.3 (docker pg183-lane0b). See core/examples/cash_numeric_sample.rs.
#
#   ./cash_numeric_sample.sh [N1] [N2]     (defaults 10M plane-1, 1M plane-2)
set -euo pipefail
cd "$(dirname "$0")"

N1=${1:-10000000}
N2=${2:-1000000}
CN=pg183-lane0b
WORK=${WORK:-artifacts/cash_numeric_sample}
mkdir -p "$WORK"

# --- container: reuse if running, else start the same one -------------------
if [ "$(docker inspect -f '{{.State.Running}}' $CN 2>/dev/null || true)" != "true" ]; then
  docker rm -f $CN >/dev/null 2>&1 || true
  docker run -d --name $CN -e POSTGRES_HOST_AUTH_METHOD=trust postgres:18.3 >/dev/null
fi
until docker exec $CN pg_isready -U postgres >/dev/null 2>&1; do sleep 1; done

PSQL="docker exec -i $CN psql -U postgres -v ON_ERROR_STOP=1 -q -X -A -t"

# --- generate inputs (deterministic) ----------------------------------------
cargo run --release -p decoder_fuzz --example cash_numeric_sample -- gen "$WORK" "$N1" "$N2"

# --- plane 1: money built from int8 arithmetic, observed ::numeric::text ----
$PSQL <<'SQL'
SET lc_monetary = 'C';
DROP TABLE IF EXISTS p1;
CREATE UNLOGGED TABLE p1(t int8);
SQL
$PSQL -c "COPY p1 FROM STDIN" < "$WORK/plane1.txt"

# Sanity: literal '$d.cc' construction must agree with the int8-arithmetic
# construction, including negatives (checked on all |t|<=200000 plus a
# random sample; excludes i64::MIN where abs() overflows).
LIT_DISAGREE=$($PSQL <<'SQL'
SET lc_monetary = 'C';
SELECT count(*)
FROM (SELECT t FROM p1
      WHERE t <> -9223372036854775808
        AND (abs(t) <= 200000 OR random() < 0.001)) s
WHERE ((CASE WHEN t < 0 THEN '-' ELSE '' END) || '$' ||
       (abs(t)/100)::text || '.' || lpad((abs(t)%100)::text, 2, '0'))::money
      <> ((t/100)::int8::money + (t%100)::int8::money/100::int8);
SQL
)
echo "plane1 literal-vs-arithmetic money construction disagreements: $LIT_DISAGREE"
[ "$LIT_DISAGREE" = "0" ] || { echo "FATAL: money construction not equivalent"; exit 1; }

$PSQL -c "SET lc_monetary='C';
          COPY (SELECT t, ((t/100)::int8::money + (t%100)::int8::money/100::int8)::numeric::text
                FROM p1) TO STDOUT" > "$WORK/plane1_out.tsv"

# --- plane 2: s::numeric::money::numeric::text, errors as ERR:<sqlstate> ----
$PSQL <<'SQL'
SET lc_monetary = 'C';
DROP TABLE IF EXISTS p2;
CREATE UNLOGGED TABLE p2(s text);
CREATE OR REPLACE FUNCTION p2f(s text) RETURNS text LANGUAGE plpgsql AS $f$
BEGIN
  RETURN (s::numeric::money)::numeric::text;
EXCEPTION WHEN OTHERS THEN
  RETURN 'ERR:' || SQLSTATE;
END
$f$;
SQL
$PSQL -c "COPY p2 FROM STDIN" < "$WORK/plane2.txt"
$PSQL -c "SET lc_monetary='C'; COPY (SELECT s, p2f(s) FROM p2) TO STDOUT" > "$WORK/plane2_out.tsv"

# --- compare on the pgrust side ---------------------------------------------
cargo run --release -p decoder_fuzz --example cash_numeric_sample -- check1 "$WORK/plane1_out.tsv"
cargo run --release -p decoder_fuzz --example cash_numeric_sample -- check2 "$WORK/plane2_out.tsv"
echo "cash_numeric_sample: ALL CLEAN"
