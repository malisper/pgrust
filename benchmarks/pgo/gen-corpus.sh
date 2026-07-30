#!/usr/bin/env bash
# Expand the checked-in clean PGO training corpus into the files a profile run
# consumes, then prove the expansion is disjoint from every published
# measurement statement.
#
#   usage: pgo/gen-corpus.sh <out-dir>
#
# Deterministic: same inputs -> byte-identical outputs (fixed LCG seeds, no
# clock, no randomness). The out-dir contents are what gets trained on, so the
# lint runs over the EXPANSION, not just the templates.
#
# Emitted:
#   analytics-serial.sql    the analytics corpus, plan-default vector
#   analytics-runtime.sql   the analytics corpus, engine-forced vector
#   oltp-main.sql           transactional corpus, simple protocol
#   oltp-txn.sql            transactional corpus, explicit multi-statement txns
#   oltp-mc.sql             concurrent-client phase
#   oltp-ext.sql            extended protocol via \parse + \bind_named
#   oltp-prep-point.pgbench prepared-protocol point lookup
#   oltp-prep-const.pgbench prepared-protocol constant projection
#   oltp-prep-write.pgbench prepared-protocol write transaction
#   oltp-schema.sql         fixture DDL (copied)
#   copy.dat                bulk-load input for the COPY path
set -euo pipefail
# Byte-wise locale for awk/sort: the corpus + denylist are treated as bytes,
# so UTF-8 locales neither warn on non-UTF-8 literals nor change matching.
export LC_ALL=C

HERE="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
OUT="${1:?usage: gen-corpus.sh <out-dir>}"
mkdir -p "$OUT"

ROWS="${PGO_CLEAN_ROWS:-100000}"          # ledger_acct row count
POINT_N="${PGO_CLEAN_POINT_N:-8000}"
RANGE_N="${PGO_CLEAN_RANGE_N:-1000}"
SCAN_N="${PGO_CLEAN_SCAN_N:-100}"
CONST_N="${PGO_CLEAN_CONST_N:-8000}"
TXN_N="${PGO_CLEAN_TXN_N:-1500}"          # read transactions (10 lookups each)
WTXN_N="${PGO_CLEAN_WTXN_N:-1500}"        # write transactions
UPD_N="${PGO_CLEAN_UPD_N:-2000}"
INS_N="${PGO_CLEAN_INS_N:-2000}"
BATCH_N="${PGO_CLEAN_BATCH_N:-200}"
COPY_N="${PGO_CLEAN_COPY_N:-4}"
EXT_N="${PGO_CLEAN_EXT_N:-2000}"
ANALYTICS_REPS="${PGO_CLEAN_ANALYTICS_REPS:-1}"

cp "$HERE/corpus/oltp-schema.sql" "$OUT/oltp-schema.sql"

# ---- analytics vectors -------------------------------------------------------
# The bare corpus carries no SET prefixes; the two vectors are produced by
# joining it with corpus/analytics-hits-engines.tsv. Engine classes are assigned
# BY SHAPE CLASS (see pgo/README.md) so the profile covers serial kernels, the
# morsel-runtime scan/agg/sort pools, and the planner-default path in roughly
# the proportion a production mix hits them.
gen_analytics() {  # <vector: serial|runtime> <out>
  awk -v vec="$1" -v reps="$ANALYTICS_REPS" -v tsv="$HERE/corpus/analytics-hits-engines.tsv" \
      -v qtsv="$HERE/corpus/QUARANTINE.tsv" '
    BEGIN {
      while ((getline l < tsv) > 0) {
        if (l ~ /^#/ || l ~ /^[ \t]*$/) continue
        split(l, f, "\t"); SER[f[1]] = f[2]; ENGV[f[1]] = f[3]
      }
      close(tsv)
      while ((getline l < qtsv) > 0) {
        if (l ~ /^#/ || l ~ /^[ \t]*$/) continue
        split(l, q, "\t"); QUAR[q[1]] = q[2]
      }
      close(qtsv)
      ser  = "SET pgrust.parallel_engine = legacy; SET max_parallel_workers_per_gather = 0; "
      rt   = ser "SET pgrust.runtime_scan_pool = 16; SET pgrust.runtime_agg_pool = 16; SET pgrust.runtime_sort_pool = 16; "
      rta  = ser "SET pgrust.runtime_agg_pool = 16; "
      mpw0 = "SET max_parallel_workers_per_gather = 0; "
      nosort = "SET enable_sort = off; "
      n = 0
    }
    /^--[ \t]*A[0-9]+ / { id = $2; next }
    /^--/ || /^[ \t]*$/ { next }
    {
      if (id in QUAR) {
        if (vec == "serial")
          printf "gen-corpus: QUARANTINED %s (held out of the training vectors) — %s\n", id, QUAR[id] > "/dev/stderr"
        next
      }
      n++
      pre = ""
      if (SER[id] == "nosort") pre = nosort
      if (vec == "runtime") {
        c = ENGV[id]
        if (c == "ser")   pre = pre ser
        else if (c == "rt")   pre = pre rt
        else if (c == "rta")  pre = pre rta
        else if (c == "mpwpg0") pre = pre mpw0
        # "default" adds nothing: the planner-default path must train too
      }
      LINE[n] = "RESET ALL; " pre $0
    }
    END {
      if (n == 0) { print "gen-corpus: FATAL empty analytics corpus" > "/dev/stderr"; exit 1 }
      for (r = 1; r <= reps; r++) for (i = 1; i <= n; i++) print LINE[i]
    }
  ' "$HERE/corpus/analytics-hits.sql" > "$2"
}
gen_analytics serial  "$OUT/analytics-serial.sql"
gen_analytics runtime "$OUT/analytics-runtime.sql"

# ---- transactional corpus ----------------------------------------------------
# Literals come from a fixed multiplicative LCG (seeded per family) so the key
# distribution is spread but the file is reproducible byte-for-byte.
{
  echo '\o /dev/null'
  awk -v n="$POINT_N" -v rows="$ROWS" 'BEGIN{
    k=7; for(i=1;i<=n;i++){ k=(k*48271)%2147483647
      printf "SELECT note FROM ledger_acct WHERE acct_id = %d;\n", (k%rows)+1 } }'
  awk -v n="$RANGE_N" -v rows="$ROWS" -v span=100 'BEGIN{
    k=17; for(i=1;i<=n;i++){ k=(k*48271)%2147483647; b=(k%(rows-span))+1
      printf "SELECT note FROM ledger_acct WHERE acct_id BETWEEN %d AND %d;\n", b, b+span-1
      printf "SELECT SUM(owner_id) FROM ledger_acct WHERE acct_id BETWEEN %d AND %d;\n", b, b+span-1
      printf "SELECT note FROM ledger_acct WHERE acct_id BETWEEN %d AND %d ORDER BY note;\n", b, b+span-1
      printf "SELECT DISTINCT note FROM ledger_acct WHERE acct_id BETWEEN %d AND %d ORDER BY note;\n", b, b+span-1
      printf "SELECT count(*) FROM ledger_acct WHERE owner_id BETWEEN %d AND %d;\n", b, b+span-1 } }'
  for _ in $(seq 1 "$SCAN_N"); do echo "SELECT count(*) FROM ledger_acct;"; done
  for _ in $(seq 1 "$SCAN_N"); do echo "SELECT bal FROM ledger_acct ORDER BY bal LIMIT 25;"; done
  for _ in $(seq 1 "$CONST_N"); do echo "SELECT 7;"; done
  awk -v n="$UPD_N" -v rows="$ROWS" 'BEGIN{
    k=97; for(i=1;i<=n;i++){ k=(k*48271)%2147483647; a=(k%rows)+1
      printf "UPDATE ledger_acct SET owner_id = owner_id + 1 WHERE acct_id = %d;\n", a
      printf "UPDATE ledger_acct SET note = md5(%d::text) WHERE acct_id = %d;\n", a, a } }'
  awk -v n="$INS_N" 'BEGIN{
    k=13; for(i=1;i<=n;i++){ k=(k*48271)%2147483647
      printf "INSERT INTO ledger_batch VALUES (%d, %d);\n", k%1000000, (k%1000000)*7 } }'
  awk -v n="$BATCH_N" -v rows=100 'BEGIN{
    k=29; for(i=1;i<=n;i++){ printf "INSERT INTO ledger_batch VALUES "
      for(j=1;j<=rows;j++){ k=(k*48271)%2147483647
        printf "%s(%d, %d)", (j>1?",":""), k%1000000, (k%1000000)*7 }
      printf ";\n" } }'
  for _ in $(seq 1 "$COPY_N"); do echo "COPY ledger_bulk FROM '$OUT/copy.dat';"; done
  echo '\o'
} > "$OUT/oltp-main.sql"

awk 'BEGIN{ for(i=1;i<=50000;i++) printf "%d\t%d\n", i, i*7 }' > "$OUT/copy.dat"
chmod 644 "$OUT/copy.dat"

# Explicit multi-statement transactions: the transactional benchmarks of record
# wrap their statements in BEGIN/COMMIT, and a corpus of autocommit singletons
# leaves the transaction-block, snapshot-reuse and multi-statement bind paths
# profile-cold.
{
  echo '\o /dev/null'
  awk -v n="$TXN_N" -v rows="$ROWS" -v span=100 'BEGIN{
    k=131
    for(i=1;i<=n;i++){
      print "BEGIN;"
      for(j=1;j<=10;j++){ k=(k*48271)%2147483647
        printf "SELECT note FROM ledger_acct WHERE acct_id = %d;\n", (k%rows)+1 }
      k=(k*48271)%2147483647; b=(k%(rows-span))+1
      printf "SELECT note FROM ledger_acct WHERE acct_id BETWEEN %d AND %d;\n", b, b+span-1
      printf "SELECT SUM(owner_id) FROM ledger_acct WHERE acct_id BETWEEN %d AND %d;\n", b, b+span-1
      printf "SELECT note FROM ledger_acct WHERE acct_id BETWEEN %d AND %d ORDER BY note;\n", b, b+span-1
      printf "SELECT DISTINCT note FROM ledger_acct WHERE acct_id BETWEEN %d AND %d ORDER BY note;\n", b, b+span-1
      print "COMMIT;" } }'
  awk -v n="$WTXN_N" -v rows="$ROWS" 'BEGIN{
    k=211
    for(i=1;i<=n;i++){
      k=(k*48271)%2147483647; a=(k%rows)+1; g=(k%1000)+1; h=(k%10)+1; d=(k%9001)-4500
      print "BEGIN;"
      printf "UPDATE ledger_acct SET bal = bal + %d WHERE acct_id = %d;\n", d, a
      printf "SELECT bal FROM ledger_acct WHERE acct_id = %d;\n", a
      printf "UPDATE ledger_agent SET agent_bal = agent_bal + %d WHERE agent_id = %d;\n", d, g
      printf "UPDATE ledger_hub SET hub_bal = hub_bal + %d WHERE hub_id = %d;\n", d, h
      printf "INSERT INTO ledger_event (agent_id, hub_id, acct_id, amt, at) VALUES (%d, %d, %d, %d, CURRENT_TIMESTAMP);\n", g, h, a, d
      print "COMMIT;" } }'
  awk -v n=400 -v rows="$ROWS" 'BEGIN{
    k=307
    for(i=1;i<=n;i++){ k=(k*48271)%2147483647; a=(k%rows)+1
      print "BEGIN;"
      printf "DELETE FROM ledger_acct WHERE acct_id = %d;\n", a
      printf "INSERT INTO ledger_acct (acct_id, owner_id, note, tag, bal) VALUES (%d, %d, '\''a'\'', '\''b'\'', 0);\n", a, a
      print "COMMIT;" } }'
  echo '\o'
} > "$OUT/oltp-txn.sql"

# Concurrent-client phase: same shapes, run by several sessions at once so the
# scheduler/park/unpark ceremony trains under contention.
{
  echo '\o /dev/null'
  awk -v n=500 -v rows="$ROWS" 'BEGIN{
    k=401; for(i=1;i<=n;i++){ k=(k*48271)%2147483647
      printf "SELECT note FROM ledger_acct WHERE acct_id = %d;\n", (k%rows)+1 } }'
  for _ in $(seq 1 500); do echo "SELECT 7;"; done
  awk -v n=200 -v rows="$ROWS" 'BEGIN{
    k=503; for(i=1;i<=n;i++){ k=(k*48271)%2147483647; a=(k%rows)+1; d=(k%9001)-4500
      print "BEGIN;"
      printf "UPDATE ledger_acct SET bal = bal + %d WHERE acct_id = %d;\n", d, a
      printf "UPDATE ledger_hub SET hub_bal = hub_bal + %d WHERE hub_id = %d;\n", d, (k%10)+1
      print "COMMIT;" } }'
  echo '\o'
} > "$OUT/oltp-mc.sql"

# ---- protocol legs -----------------------------------------------------------
printf '\\set k random(1, %d)\nSELECT note FROM ledger_acct WHERE acct_id = :k;\n' "$ROWS" \
  > "$OUT/oltp-prep-point.pgbench"
printf 'SELECT 7;\n' > "$OUT/oltp-prep-const.pgbench"
printf '\\set k random(1, %d)\n\\set d random(-5000, 5000)\nBEGIN;\nUPDATE ledger_acct SET bal = bal + :d WHERE acct_id = :k;\nSELECT bal FROM ledger_acct WHERE acct_id = :k;\nUPDATE ledger_hub SET hub_bal = hub_bal + :d WHERE hub_id = 1;\nINSERT INTO ledger_event (agent_id, hub_id, acct_id, amt, at) VALUES (1, 1, :k, :d, CURRENT_TIMESTAMP);\nCOMMIT;\n' "$ROWS" \
  > "$OUT/oltp-prep-write.pgbench"

awk -v n="$EXT_N" -v rows="$ROWS" 'BEGIN{
  print "SELECT note FROM ledger_acct WHERE acct_id = $1 \\parse clean_pt"
  print "SELECT count(*) FROM ledger_acct WHERE owner_id BETWEEN $1 AND $2 \\parse clean_rg"
  print "\\o /dev/null"
  k=53; for(i=1;i<=n;i++){ k=(k*48271)%2147483647
    printf "\\bind_named clean_pt %d \\g\n", (k%rows)+1
    printf "\\bind_named clean_rg %d %d \\g\n", (k%rows)+1, (k%rows)+101 }
  print "\\o" }' > "$OUT/oltp-ext.sql"

# ---- the proof ---------------------------------------------------------------
"$HERE/lint-training-overlap.sh" \
  "$OUT/analytics-serial.sql" "$OUT/analytics-runtime.sql" \
  "$OUT/oltp-main.sql" "$OUT/oltp-txn.sql" "$OUT/oltp-mc.sql" \
  "$OUT/oltp-ext.sql" "$OUT/oltp-schema.sql" \
  "$OUT/oltp-prep-point.pgbench" "$OUT/oltp-prep-const.pgbench" "$OUT/oltp-prep-write.pgbench" \
  || { echo "gen-corpus: FATAL the expanded corpus overlaps a published measurement vector" >&2; exit 1; }

echo "gen-corpus: wrote $(ls -1 "$OUT" | wc -l | tr -d ' ') files to $OUT"
