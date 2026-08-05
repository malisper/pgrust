#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# run-oltp-rw.sh — pgbench read-write, both arms, ratio table.
#
# RUN THIS ON THE OLTP CLIENT BOX (see hosts.env: $OLTP_CLIENT_HOST).
#
# METHODOLOGY PROVENANCE — READ THIS, IT MATTERS
#   The read-write shape here is *pgrust's own standing protocol*, NOT
#   PlanetScale's. PlanetScale's published write test is TPCC (Percona
#   sysbench-tpcc, 20 tables x scale 250, ~500 GB); their exact TPCC commands
#   are reproduced in README.md as an optional extra, and in
#   planetscale-methodology.md section 1.
#
#   What this script runs is the pgbench protocol pgrust reports internally:
#       pgbench -M prepared -c 256 -j 16, 60 s warmup + 600 s timed
#   pgbench's default TPC-B-like transaction mix. This is a legitimate
#   read-write benchmark, but it is OUR choice of shape, and the numbers it
#   produces are not comparable to PlanetScale's TPCC figures.
#
#   Scale on this rig: the two ~300 GB read-only datadirs occupy most of the
#   872 GB NVMe, so the resident read-write datasets are scale 6849 (~100 GB)
#   per arm. See README.md "What fits on the disk, and what does not" — the
#   500 GB read-write shape does NOT fit alongside them and requires the
#   documented re-init procedure (reinit-rw.sh).
#
# USAGE
#   ./run-oltp-rw.sh                # 60s warmup + 3 x 600s timed, both arms
#   ./run-oltp-rw.sh --smoke        # 60s legs, 1 rep — proves the path only
#   ./run-oltp-rw.sh --reps 2 --time 300
#   ./run-oltp-rw.sh --arm pgrust
# ---------------------------------------------------------------------------
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$HERE/hosts.env"

CLIENTS=256
JOBS=16
TIME_S=600
WARMUP_S=60
REPS=3
ARMS="pgrust cpg"
BINNAME=""
SCALE=6849
SMOKE=0

while [ $# -gt 0 ]; do
  case "$1" in
    --smoke)   SMOKE=1; TIME_S=60; WARMUP_S=30; REPS=1; CLIENTS=32; JOBS=8 ;;
    --reps)    shift; REPS="$1" ;;
    --time)    shift; TIME_S="$1" ;;
    --clients) shift; CLIENTS="$1" ;;
    --arm)     shift; ARMS="$1" ;;
    --binary)  shift; BINNAME="${1:-}" ;;
    -h|--help) sed -n '2,34p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
done

# Resolve pgbench EXPLICITLY. PGDG installs it outside the default PATH, and a
# non-login shell does not source /etc/profile.d. An unqualified `pgbench` here
# once produced a run in which BOTH arms failed with "command not found" while
# the script still printed "failed 0" and exited 0 -- a totally failed run that
# looked clean. Never again: resolve it, or refuse to start.
PGBENCH="${PGBENCH:-}"
if [ -z "$PGBENCH" ]; then
  for c in "$(command -v pgbench 2>/dev/null)" /usr/pgsql-18/bin/pgbench \
           /usr/local/bin/pgbench /usr/bin/pgbench; do
    [ -n "$c" ] && [ -x "$c" ] && { PGBENCH="$c"; break; }
  done
fi
[ -n "$PGBENCH" ] && [ -x "$PGBENCH" ] || {
  echo "FATAL: pgbench not found. Looked on PATH and in /usr/pgsql-18/bin," >&2
  echo "       /usr/local/bin, /usr/bin. Set PGBENCH=/path/to/pgbench." >&2
  exit 3; }

TS="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="$RESULTS_DIR/oltp-rw-$TS"
mkdir -p "$OUT"
exec > >(tee -a "$OUT/run.log") 2>&1

SSH="ssh -o StrictHostKeyChecking=no ${AUDIT_USER}@${OLTP_SERVER_PRIVATE}"

echo "=============================================================================="
echo "pgbench read-write — pgrust RC vs C PostgreSQL 18.3"
echo "=============================================================================="
echo "  shape        : pgbench -M prepared -c $CLIENTS -j $JOBS, scale $SCALE (~100 GB)"
echo "  protocol     : ${WARMUP_S}s warmup (discarded) + $REPS x ${TIME_S}s timed"
echo "  PROVENANCE   : this read-write shape is pgrust's OWN standing protocol,"
echo "                 not PlanetScale's. PlanetScale's write test is TPCC —"
echo "                 see README.md for their verbatim TPCC commands."
echo "  server       : $OLTP_SERVER_PRIVATE (i8g.xlarge, 4 vCPU / 32 GB / NVMe)"
echo "  client       : this box ($OLTP_CLIENT_PRIVATE, c8g.2xlarge), separate machine"
echo "  pgbench      : $("$PGBENCH" --version)  [$PGBENCH]"
echo "  started      : $(date -u +%FT%TZ)"
echo "  output       : $OUT"
echo "=============================================================================="
[ "$SMOKE" = 1 ] && { echo; echo "  *** SMOKE MODE: ${TIME_S}s legs, c=$CLIENTS. NOT an official reading. ***"; }
echo

pgb() {
  local secs="$1"
  "$PGBENCH" -h "$OLTP_SERVER_PRIVATE" -p 5432 -U "$PGUSER_BENCH" -d "$PGDATABASE_BENCH" \
    -M prepared -c "$CLIENTS" -j "$JOBS" -T "$secs" -P 10 --progress-timestamp
}

declare -A RES
RUN_BROKEN=0
for arm in $ARMS; do
  echo "------------------------------------------------------------------------------"
  echo "ARM: $arm"
  echo "------------------------------------------------------------------------------"
  PGBIN_ARG=""
  [ -n "$BINNAME" ] && [ "$arm" = pgrust ] && PGBIN_ARG="/opt/pgrust/bin/postgres.$BINNAME"
  echo "  switching server to rw-$arm ${PGBIN_ARG:+(binary: $PGBIN_ARG)} ..."
  $SSH "sudo /opt/audit/switch-arm.sh rw-$arm $PGBIN_ARG" || { echo "  !! arm switch FAILED"; continue; }
  echo "  arm witness (executable behind the running pid, not a version string):"
  $SSH "sudo /opt/audit/switch-arm.sh status" | sed 's/^/    /'

  echo "  warmup ${WARMUP_S}s (discarded) ..."
  pgb "$WARMUP_S" > "$OUT/$arm-warmup.txt" 2>&1

  for r in $(seq 1 "$REPS"); do
    f="$OUT/$arm-rep$r.txt"
    printf '  %-6s rep %s/%s ... ' "$arm" "$r" "$REPS"
    pgb "$TIME_S" > "$f" 2>&1
    tps=$(awk '/^tps =/ {print $3; exit}' "$f")
    lat=$(awk '/^latency average/ {print $4; exit}' "$f")
    failed=$(awk '/^number of failed transactions:/ {print $5; exit}' "$f")
    # No tps line means pgbench did not run to completion. Defaulting a missing
    # value to 0 here is how a broken run reports itself as healthy; treat an
    # unparseable rep as the failure it is.
    if [ -z "$tps" ]; then
      echo "FAILED — no tps line in output"
      echo "    !! pgbench produced no result. First lines of $f:"
      sed -n '1,5p' "$f" | sed 's/^/       /'
      RUN_BROKEN=1
      continue
    fi
    RES["$arm,$r"]="$tps|$lat|${failed:-0}"
    printf 'tps %-12s lat_avg %-9s ms  failed %s\n' "$tps" "$lat" "${failed:-0}"
    if [ -n "${failed:-}" ] && [ "${failed}" != "0" ]; then
      echo "    !! non-zero failed transactions — this rep is suspect, do not bank it"
      RUN_BROKEN=1
    fi
  done
  echo "  stopping server arm"
  $SSH "sudo /opt/audit/switch-arm.sh stop" >/dev/null 2>&1
  echo
done

med() { printf '%s\n' "$@" | sort -n | awk '{a[NR]=$1} END{print (NR%2)?a[(NR+1)/2]:(a[NR/2]+a[NR/2+1])/2}'; }

echo "=============================================================================="
echo "RESULT — median of $REPS reps"
echo "=============================================================================="
pv=(); cv=(); pl=(); cl=()
for r in $(seq 1 "$REPS"); do
  IFS='|' read -ra a <<< "${RES[pgrust,$r]:-||}"; IFS='|' read -ra b <<< "${RES[cpg,$r]:-||}"
  [ -n "${a[0]:-}" ] && { pv+=("${a[0]}"); pl+=("${a[1]}"); }
  [ -n "${b[0]:-}" ] && { cv+=("${b[0]}"); cl+=("${b[1]}"); }
done
if [ ${#pv[@]} -gt 0 ] && [ ${#cv[@]} -gt 0 ]; then
  pm=$(med "${pv[@]}"); cm=$(med "${cv[@]}")
  plm=$(med "${pl[@]}"); clm=$(med "${cl[@]}")
  ratio=$(awk -v p="$pm" -v c="$cm" 'BEGIN{printf "%.3f", p/c}')
  printf '  %-14s %-14s %-14s %s\n' "metric" "pgrust" "C 18.3" "ratio"
  printf '  %-14s %-14s %-14s %sx\n' "tps (median)" "$pm" "$cm" "$ratio"
  printf '  %-14s %-14s %-14s %s\n'  "latency avg" "$plm" "$clm" "(ms, lower better)"
else
  echo "  Only one arm produced results; no ratio computed."
fi
echo
echo "  Per-rep values:"
for arm in $ARMS; do
  for r in $(seq 1 "$REPS"); do
    printf '    %-6s rep %s: %s\n' "$arm" "$r" "${RES[$arm,$r]:-<none>}"
  done
done
echo
echo "  Node-draw caveat: read-write ratios on this rig class have historically"
echo "  moved by 4-8 percent per arm from run to run on identical binaries. A"
echo "  single pair of runs cannot resolve a difference smaller than that. If"
echo "  you care about a small delta, alternate the arms repeatedly within one"
echo "  session rather than running all of one arm and then all of the other."
echo "=============================================================================="
echo "  finished $(date -u +%FT%TZ)"
if [ "$RUN_BROKEN" = 1 ]; then
  echo
  echo "  !! THIS RUN HAD FAILURES. Do not quote anything above as a result."
  exit 1
fi
