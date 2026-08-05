#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# run-clickbench.sh — the official ClickBench protocol against the pgrust RC.
#
# RUN THIS ON THE CLICKBENCH BOX (see hosts.env: $CB_HOST).
#
# METHODOLOGY PROVENANCE
#   Protocol : https://github.com/ClickHouse/ClickBench  (the per-query
#              cold-cycle + 3-tries loop in its benchmark harness)
#   Leaderboard / baselines : https://benchmark.clickhouse.com/
#   Queries  : queries.sql in this directory, byte-identical to ClickBench's
#              postgresql/queries.sql (43 queries).
#
# WHAT IT DOES, exactly
#   For each of the 43 queries, in order:
#     1. stop the server
#     2. wait until it is genuinely down (not merely told to stop)
#     3. drop the OS page cache  (sync; echo 3 > /proc/sys/vm/drop_caches)
#     4. start the server, wait for it to answer SELECT 1
#     5. run the query 3 times, recording each wall time
#   Try 1 is therefore the COLD number (cold process + cold page cache).
#   Tries 2 and 3 are HOT; the scorer takes min(try2, try3).
#
#   This is a full-restart-per-query sweep. It takes a while — budget roughly
#   40-70 minutes for all 43 queries. That is inherent to the protocol, not a
#   slow script.
#
# OUTPUT
#   $RESULTS_DIR/clickbench-<timestamp>/
#       result.json   ClickBench-format {load_time, data_size, result[43][3]}
#       result.csv    one row per (query, try, seconds)
#       run.log       full transcript
#   then prints the scoring (official metric AND pgrust's combined formula).
#
# WHICH BINARY
#   Default: the preinstalled artifact /opt/pgrust/bin/postgres, whose sha256
#   is verified against hosts.env and warned about loudly on mismatch.
#
#   --binary PATH runs YOUR build instead — e.g. the one build-pgo.sh produces
#   from source with a training corpus provably disjoint from these queries.
#   The measured binary's sha256 then goes into the results header and into
#   the emitted result.json, so a result can never be attributed to a binary
#   that did not produce it.
#
# USAGE
#   ./run-clickbench.sh                              # shipped binary, full sweep
#   ./run-clickbench.sh --smoke                      # 2 queries x 3, path proof
#   ./run-clickbench.sh --binary ~/audit/build/postgres
#   ./run-clickbench.sh --binary ~/audit/build/postgres --smoke
#   ./run-clickbench.sh --queries 1,5,29
# ---------------------------------------------------------------------------
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$HERE/hosts.env"

# The datadir of record. hosts.env sets it; the rig derives its CB_DD from it.
# Do not introduce a second name for this -- see clickbench-rig/env.sh.
PGDATA_CB="${PGDATA_CB:-/data/clickbench/pgdata}"
# Refuse a stale alias rather than measure one datadir while reporting another.
if [ -n "${CB_DD:-}" ] && [ "${CB_DD}" != "$PGDATA_CB" ]; then
  echo "FATAL: CB_DD=$CB_DD disagrees with PGDATA_CB=$PGDATA_CB." >&2
  echo "       PGDATA_CB is authoritative; unset CB_DD." >&2
  exit 3
fi
PGSOCK="${PGSOCK:-/data/clickbench/sock}"
RIG="${RIG:-/data/clickbench/rig}"
PGSVC="${PGSVC:-pgrust}"     # the datadir's owning service account
PGDB="${PGDB:-test}"
TRIES="${BENCH_TRIES:-3}"
QUERY_FILE="$HERE/queries.sql"

SMOKE=0
QSEL=""
BIN_OVERRIDE=""
while [ $# -gt 0 ]; do
  case "$1" in
    --binary)  shift; BIN_OVERRIDE="${1:-}" ;;
    --smoke)   SMOKE=1; QSEL="1,20" ;;
    --queries) shift; QSEL="$1" ;;
    --tries)   shift; TRIES="$1" ;;
    -h|--help) sed -n '2,44p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
done

PGBIN="${BIN_OVERRIDE:-${PGRUST_BIN:-/opt/pgrust/bin/postgres}}"
[ -x "$PGBIN" ] || { echo "no such binary: $PGBIN" >&2; exit 2; }
ACTUAL_SHA=$(sha256sum "$PGBIN" | cut -d' ' -f1)

TS="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="$RESULTS_DIR/clickbench-$TS"
mkdir -p "$OUT"
exec > >(tee -a "$OUT/run.log") 2>&1

echo "=============================================================================="
echo "ClickBench official protocol — pgrust release candidate"
echo "=============================================================================="
echo "  methodology     : https://github.com/ClickHouse/ClickBench"
echo "  baselines       : https://benchmark.clickhouse.com/ (vendored in baselines/)"
if [ -n "$BIN_OVERRIDE" ]; then
echo "  binary          : $PGBIN   [YOUR BUILD — --binary]"
echo "  binary sha256   : $ACTUAL_SHA"
echo "  provenance      : not the preinstalled artifact. If build-pgo.sh made"
echo "                    it, see $(dirname "$PGBIN")/build-manifest.txt for the"
echo "                    source sha, corpus sha and toolchain."
else
echo "  binary          : $PGBIN   [preinstalled artifact]"
echo "  binary sha256   : $ACTUAL_SHA"
echo "  expected sha256 : $PGRUST_SHA256"
echo "  source sha      : $PGRUST_GATED_SHA"
fi
echo "  datadir         : $PGDATA_CB"
echo "  tries per query : $TRIES   (try 1 = cold, min(2,3) = hot)"
echo "  started         : $(date -u +%FT%TZ)"
echo "  output          : $OUT"
echo "=============================================================================="
echo

if [ -z "$BIN_OVERRIDE" ] && [ "$ACTUAL_SHA" != "$PGRUST_SHA256" ]; then
  echo "!! WARNING: binary sha256 does NOT match the recorded RC artifact."
  echo "!! You are not measuring the binary this audit kit documents."
  echo
fi

# --- server lifecycle ------------------------------------------------------
command -v psql >/dev/null 2>&1 || { echo "FATAL: psql not found on PATH." >&2; exit 3; }

pg_is_up() { psql -h "$PGSOCK" -p 5432 -U postgres -d "$PGDB" -tAc 'SELECT 1' >/dev/null 2>&1; }

pg_stop() {
  # The datadir is owned by the service account, and PostgreSQL refuses to run
  # against a datadir it does not own, so lifecycle goes through the rig.
  sudo -u "$PGSVC" env PGDATA_CB="$PGDATA_CB" "$RIG/stop" >/dev/null 2>&1 || true
  # Wait until it is genuinely gone. A server that has been *told* to stop but
  # still holds its data files open will keep those pages pinned, and the
  # cache drop below would be a no-op — the "cold" run would then read from a
  # warm page cache and be silently wrong.
  for _ in $(seq 1 120); do pg_is_up || return 0; sleep 1; done
  echo "  !! server did not stop within 120s; proceeding anyway" >&2
}

drop_caches() { sync; echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null; }

pg_start() {
  sudo -u "$PGSVC" env PGDATA_CB="$PGDATA_CB" ${BIN_OVERRIDE:+CB_BIN_OVERRIDE="$BIN_OVERRIDE"} "$RIG/start" >/dev/null 2>&1 || true
  for _ in $(seq 1 300); do pg_is_up && return 0; sleep 1; done
  echo "  !! server failed to come up within 300s" >&2
  return 1
}

cold_cycle() { pg_stop; drop_caches; pg_start; }

# --- load time and data size ----------------------------------------------
# These are scored axes. The load is done ONCE, at provisioning time, by the
# RC's stock load path; its measured wall time is recorded in
# /opt/pgrust/LOADINFO.txt so this script can report it without re-loading a
# 100M-row table on every sweep.
LOADINFO=/opt/pgrust/LOADINFO.txt
if [ -r "$LOADINFO" ]; then
  LOAD_TIME=$(awk -F= '/^load_time_seconds=/{print $2}' "$LOADINFO")
else
  LOAD_TIME=""
fi
if [ -z "${LOAD_TIME:-}" ]; then
  echo "!! No recorded load time found in $LOADINFO."
  echo "!! Load time is a SCORED ClickBench axis. Re-run the documented load"
  echo "!! (see README 'Reproducing the data load') to obtain it honestly,"
  echo "!! or the combined score below will be missing a term."
  LOAD_TIME=0
fi

pg_start >/dev/null 2>&1 || true
# Assert we are measuring the datadir this run claims to measure. A rig that
# booted a different PGDATA would otherwise produce a correct-looking result
# attributed to the wrong storage.
LIVE_DD=$(psql -h "$PGSOCK" -p 5432 -U postgres -d "$PGDB" -tAc 'SHOW data_directory' 2>/dev/null | tr -d ' ')
if [ -n "$LIVE_DD" ] && [ "$LIVE_DD" != "$PGDATA_CB" ]; then
  echo "!! FATAL: the running server's data_directory is"
  echo "!!   $LIVE_DD"
  echo "!! but this run is configured for"
  echo "!!   $PGDATA_CB"
  echo "!! Refusing to produce a result attributed to the wrong storage."
  exit 4
fi
echo "  live data_directory : ${LIVE_DD:-<could not read>}"
DATA_SIZE=$(sudo -u "$PGSVC" "$RIG/data-size" 2>/dev/null || sudo du -bs "$PGDATA_CB" | awk '{print $1}')
ROWS=$(psql -h "$PGSOCK" -p 5432 -U postgres -d "$PGDB" -tAc 'SELECT count(*) FROM hits' 2>/dev/null || echo "?")
echo "  load time (recorded at load) : $LOAD_TIME s"
echo "  data size (measured now)     : $DATA_SIZE B"
echo "  hits row count               : $ROWS"
echo

# --- which queries ---------------------------------------------------------
mapfile -t ALLQ < "$QUERY_FILE"
NQ=${#ALLQ[@]}
if [ "$NQ" -ne 43 ]; then
  echo "!! queries.sql has $NQ queries, expected 43. Refusing to produce a"
  echo "!! result that would be scored against a 43-query baseline."
  exit 3
fi
if [ -n "$QSEL" ]; then
  IFS=',' read -ra IDX <<< "$QSEL"
else
  IDX=($(seq 1 43))
fi
[ "$SMOKE" = 1 ] && echo "  *** SMOKE MODE: ${#IDX[@]} of 43 queries. NOT a scorable result. ***" && echo

# --- the sweep -------------------------------------------------------------
echo "query,try,seconds" > "$OUT/result.csv"
declare -A TIMES
for n in "${IDX[@]}"; do
  q="${ALLQ[$((n-1))]}"
  printf '  q%-3s ' "$n"
  cold_cycle
  row=()
  for t in $(seq 1 "$TRIES"); do
    s=$( { /usr/bin/time -f '%e' psql -h "$PGSOCK" -p 5432 -U postgres -d "$PGDB" \
             -tA -c "$q" >/dev/null; } 2>&1 | tail -n1 )
    if [[ "$s" =~ ^[0-9]+\.?[0-9]*$ ]]; then
      row+=("$s"); printf '%8s' "$s"
    else
      row+=("null"); printf '%8s' "ERR"
      echo "$n,$t,ERROR" >> "$OUT/result.csv"
      continue
    fi
    echo "$n,$t,$s" >> "$OUT/result.csv"
  done
  TIMES[$n]="$(IFS=,; echo "${row[*]}")"
  echo
done
echo

# --- emit ClickBench-format JSON ------------------------------------------
CBNOTE=""
[ -n "$BIN_OVERRIDE" ] && CBNOTE=" (locally rebuilt, clean PGO corpus)"
{
  echo '{'
  echo '  "system": "pgrust",'
  echo '  "date": "'"$(date -u +%F)"'",'
  echo '  "machine": "c8g.4xlarge",'
  echo '  "cluster_size": 1,'
  echo '  "proprietary": "no",'
  # "tuned": "yes" is the honest declaration. This bank is NOT a stock heap
  # PostgreSQL table: it uses pgrust's columnar access method (cbstore) with an
  # lz4 codec and a five-column presorted ingest. In ClickBench's taxonomy that
  # is a *tuned* entry, and our own load driver labels it exactly that way.
  # Declaring "no" here would misrepresent the row.
  echo '  "tuned": "yes",'
  echo '  "comment_schema": "cbstore columnar AM, codec=lz4, presort (CounterID,EventDate,UserID,EventTime,WatchID) - ClickBench tuned-row equivalent, not stock heap PostgreSQL",'
  echo '  "comment": "pgrust binary sha256 '"$ACTUAL_SHA$CBNOTE"'",'
  echo '  "load_time": '"$LOAD_TIME"','
  echo '  "data_size": '"$DATA_SIZE"','
  echo '  "result": ['
  last=$(( ${#IDX[@]} - 1 )); i=0
  for n in "${IDX[@]}"; do
    IFS=',' read -ra r <<< "${TIMES[$n]}"
    printf '    [%s]' "$(IFS=', '; echo "${r[*]}")"
    [ $i -lt $last ] && echo ',' || echo
    i=$((i+1))
  done
  echo '  ]'
  echo '}'
} > "$OUT/result.json"

echo "  raw results: $OUT/result.json"
echo

# --- score -----------------------------------------------------------------
if [ "$SMOKE" = 1 ] || [ -n "$QSEL" ]; then
  echo "Partial sweep — not scored. The scorer refuses partial results on"
  echo "purpose: a subset of queries is not comparable to a published full"
  echo "sweep, and silently dropping queries is how benchmark results get"
  echo "flattered. Run without --smoke/--queries for a scorable number."
else
  python3 "$HERE/scorers/score-clickbench.py" "$OUT/result.json" --all-baselines --per-query \
    | tee "$OUT/score.txt"
fi

pg_stop
echo "  server stopped. finished $(date -u +%FT%TZ)"
