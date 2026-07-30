#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# run-oltp-ro.sh — sysbench oltp_read_only, PlanetScale-exact flags, both arms.
#
# RUN THIS ON THE OLTP CLIENT BOX (see hosts.env: $OLTP_CLIENT_HOST).
# It drives the OLTP SERVER over the private network and switches the server
# between the pgrust arm and the C PostgreSQL 18.3 arm for you.
#
# METHODOLOGY PROVENANCE
#   PlanetScale benchmark methodology (pinned copy: planetscale-methodology.md
#   in this directory; sources listed at the top of that file):
#     https://planetscale.com/blog/benchmarking-postgres
#     https://planetscale.com/benchmarks
#     https://planetscale.xyz/benchmarks/instructions/oltp300g   (verbatim commands)
#   Workload: sysbench oltp_read_only — https://github.com/akopytov/sysbench
#
# THE FLAGS ARE PLANETSCALE'S, VERBATIM
#     --tables=10 --table-size=130000000 --time=300 --threads={32,64}
#     --report-interval=1 --histogram=off --percentile=99 --db-driver=pgsql
#   10 tables x 130M rows is the ~300 GB shape. 300 s per timed run.
#   The pgsql driver uses server-side prepared statements (extended protocol).
#
# WARMUP
#   600 s at 32 threads per engine, discarded, before any timed rep. The
#   dataset is ~300 GB against 16 GB shared_buffers on a 32 GB box, so this is
#   an I/O-bound workload and the warmup is doing real work: it is filling
#   both the buffer pool and the OS cache to a steady state. Timed reps taken
#   without it read low and noisy.
#
# WHAT IT PRINTS
#   Per arm, per thread count: qps, tps, p99 latency, error count. Then the
#   pgrust/C ratio table.
#
# USAGE
#   ./run-oltp-ro.sh                       # full: 600s warmup + 3x300s, {32,64} thr, both arms
#   ./run-oltp-ro.sh --smoke               # 60s legs, 1 rep, 32 thr — proves the path only
#   ./run-oltp-ro.sh --threads 32 --reps 2
#   ./run-oltp-ro.sh --arm pgrust          # one arm only
# ---------------------------------------------------------------------------
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$HERE/hosts.env"

TABLES=10
TABLE_SIZE=130000000
TIME_S=300
WARMUP_S=600
THREADS="32 64"
REPS=3
ARMS="pgrust cpg"
BINNAME=""
SMOKE=0

while [ $# -gt 0 ]; do
  case "$1" in
    --smoke)   SMOKE=1; TIME_S=60; WARMUP_S=60; THREADS="32"; REPS=1 ;;
    --threads) shift; THREADS="${1//,/ }" ;;
    --reps)    shift; REPS="$1" ;;
    --time)    shift; TIME_S="$1" ;;
    --warmup)  shift; WARMUP_S="$1" ;;
    --arm)     shift; ARMS="$1" ;;
    --binary)  shift; BINNAME="${1:-}" ;;
    -h|--help) sed -n '2,37p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
done

# Resolve sysbench EXPLICITLY. This runner only escaped the failure that hit
# the read-write one because sysbench happens to land in /usr/local/bin. Do
# not rely on that.
SYSBENCH="${SYSBENCH:-}"
if [ -z "$SYSBENCH" ]; then
  for c in "$(command -v sysbench 2>/dev/null)" /usr/local/bin/sysbench /usr/bin/sysbench; do
    [ -n "$c" ] && [ -x "$c" ] && { SYSBENCH="$c"; break; }
  done
fi
[ -n "$SYSBENCH" ] && [ -x "$SYSBENCH" ] || {
  echo "FATAL: sysbench not found. Set SYSBENCH=/path/to/sysbench." >&2; exit 3; }
"$SYSBENCH" --help 2>&1 | grep -q pgsql || \
  echo "!! WARNING: this sysbench may lack the pgsql driver (--with-pgsql)."

TS="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="$RESULTS_DIR/oltp-ro-$TS"
mkdir -p "$OUT"
exec > >(tee -a "$OUT/run.log") 2>&1

SSH="ssh -o StrictHostKeyChecking=no ${AUDIT_USER}@${OLTP_SERVER_PRIVATE}"

echo "=============================================================================="
echo "sysbench oltp_read_only — PlanetScale methodology — pgrust RC vs C PostgreSQL"
echo "=============================================================================="
echo "  methodology  : planetscale-methodology.md (this directory)"
echo "                 https://planetscale.xyz/benchmarks/instructions/oltp300g"
echo "  workload     : sysbench oltp_read_only, $TABLES tables x $TABLE_SIZE rows (~300 GB)"
echo "  flags        : --tables=$TABLES --table-size=$TABLE_SIZE --time=$TIME_S"
echo "                 --report-interval=1 --histogram=off --percentile=99 --db-driver=pgsql"
echo "  warmup       : ${WARMUP_S}s at 32 threads per engine, discarded"
echo "  thread counts: $THREADS"
echo "  reps         : $REPS per (arm, threads) cell"
echo "  server       : $OLTP_SERVER_PRIVATE (i8g.xlarge, 4 vCPU / 32 GB / NVMe)"
echo "  client       : this box ($OLTP_CLIENT_PRIVATE, c8g.2xlarge) — separate machine,"
echo "                 per the methodology, so client CPU never steals from the engine"
echo "  sysbench     : $("$SYSBENCH" --version)  [$SYSBENCH]"
echo "  started      : $(date -u +%FT%TZ)"
echo "  output       : $OUT"
echo "=============================================================================="
[ "$SMOKE" = 1 ] && { echo; echo "  *** SMOKE MODE: ${TIME_S}s legs. NOT an official reading. ***"; }
echo

sb() {
  local threads="$1" secs="$2"
  "$SYSBENCH" oltp_read_only \
    --pgsql-host="$OLTP_SERVER_PRIVATE" --pgsql-port=5432 \
    --pgsql-user="$PGUSER_BENCH" --pgsql-db="$PGDATABASE_BENCH" \
    --tables="$TABLES" --table-size="$TABLE_SIZE" \
    --time="$secs" --threads="$threads" \
    --report-interval=1 --histogram=off --percentile=99 \
    --db-driver=pgsql run
}

# Parse sysbench's summary block.
parse() {
  local f="$1" key="$2"
  case "$key" in
    qps)    awk '/queries:/ {gsub(/[()]/,"",$3); print $3; exit}' "$f" ;;
    tps)    awk '/transactions:/ {gsub(/[()]/,"",$3); print $3; exit}' "$f" ;;
    p99)    awk '/99th percentile:/ {print $3; exit}' "$f" ;;
    errors) awk '/ignored errors:/ {print $3; exit}' "$f" ;;
  esac
}

declare -A RES
RUN_BROKEN=0
for arm in $ARMS; do
  echo "------------------------------------------------------------------------------"
  echo "ARM: $arm"
  echo "------------------------------------------------------------------------------"
  # The override applies to the pgrust arm only; the C arm is always the same
  # stock PostgreSQL 18.3, so a shipped-vs-your-build comparison moves exactly
  # one side.
  PGBIN_ARG=""
  [ -n "$BINNAME" ] && [ "$arm" = pgrust ] && PGBIN_ARG="/opt/pgrust/bin/postgres.$BINNAME"
  echo "  switching server to ro-$arm ${PGBIN_ARG:+(binary: $PGBIN_ARG)} ..."
  $SSH "sudo /opt/audit/switch-arm.sh ro-$arm $PGBIN_ARG" || { echo "  !! arm switch FAILED"; continue; }

  # Identity witness: assert we are really talking to the arm we think we are.
  # A version string is not enough — both engines answer as "PostgreSQL". We
  # check the running executable behind the datadir on the server itself.
  echo "  arm witness (executable behind the running pid, not a version string):"
  $SSH "sudo /opt/audit/switch-arm.sh status" | sed 's/^/    /'

  echo "  warmup ${WARMUP_S}s at 32 threads (discarded) ..."
  sb 32 "$WARMUP_S" > "$OUT/$arm-warmup.txt" 2>&1
  echo "    warmup qps $(parse "$OUT/$arm-warmup.txt" qps)"

  for th in $THREADS; do
    for r in $(seq 1 "$REPS"); do
      f="$OUT/$arm-t$th-rep$r.txt"
      printf '  %-6s threads=%-3s rep %s/%s ... ' "$arm" "$th" "$r" "$REPS"
      sb "$th" "$TIME_S" > "$f" 2>&1
      q=$(parse "$f" qps); t=$(parse "$f" tps); p=$(parse "$f" p99); e=$(parse "$f" errors)
      # An unparseable rep means sysbench did not complete. Defaulting to 0
      # would report a broken run as a healthy one.
      if [ -z "$q" ]; then
        echo "FAILED — no summary in output"
        echo "    !! sysbench produced no result. First lines of $f:"
        sed -n '1,5p' "$f" | sed 's/^/       /'
        RUN_BROKEN=1
        continue
      fi
      RES["$arm,$th,$r"]="$q|$t|$p|$e"
      printf 'qps %-10s tps %-9s p99 %-8s errors %s\n' "$q" "$t" "$p" "$e"
      if [ -n "${e:-}" ] && [ "${e}" != "0" ]; then
        echo "    !! non-zero ignored errors — this rep is suspect, do not bank it"
        RUN_BROKEN=1
      fi
    done
  done
  echo "  stopping server arm"
  $SSH "sudo /opt/audit/switch-arm.sh stop" >/dev/null 2>&1
  echo
done

# --- comparison table ------------------------------------------------------
med() { printf '%s\n' "$@" | sort -n | awk '{a[NR]=$1} END{print (NR%2)?a[(NR+1)/2]:(a[NR/2]+a[NR/2+1])/2}'; }

echo "=============================================================================="
echo "RESULT — median of $REPS reps per cell"
echo "=============================================================================="
printf '  %-8s %-10s %-12s %-12s %-10s %-10s %s\n' \
       "threads" "metric" "pgrust" "C 18.3" "ratio" "" ""
for th in $THREADS; do
  for metric in qps tps p99; do
    pv=(); cv=()
    for r in $(seq 1 "$REPS"); do
      IFS='|' read -ra a <<< "${RES[pgrust,$th,$r]:-||}"
      IFS='|' read -ra b <<< "${RES[cpg,$th,$r]:-||}"
      case $metric in qps) i=0;; tps) i=1;; p99) i=2;; esac
      [ -n "${a[$i]:-}" ] && pv+=("${a[$i]}")
      [ -n "${b[$i]:-}" ] && cv+=("${b[$i]}")
    done
    [ ${#pv[@]} -eq 0 ] || [ ${#cv[@]} -eq 0 ] && continue
    pm=$(med "${pv[@]}"); cm=$(med "${cv[@]}")
    ratio=$(awk -v p="$pm" -v c="$cm" 'BEGIN{ if(c>0) printf "%.3f", p/c; else print "n/a" }')
    note=""
    [ "$metric" = p99 ] && note="(lower is better)"
    printf '  %-8s %-10s %-12s %-12s %-10s %s\n' "$th" "$metric" "$pm" "$cm" "${ratio}x" "$note"
  done
done
echo
echo "  Raw sysbench output for every rep is in $OUT/."
echo
echo "  How to read this: the ratio is pgrust divided by C PostgreSQL 18.3 on"
echo "  the same box, same dataset, same config, arms run back to back. For qps"
echo "  and tps, above 1.0 means pgrust is faster. For p99, BELOW 1.0 means"
echo "  pgrust has the lower (better) tail."
echo
echo "  Per-rep spread matters. If the reps within a cell differ by more than a"
echo "  few percent, the cell is noisy and its ratio should not be quoted to"
echo "  three digits. Check the per-rep numbers above before believing a small"
echo "  difference."
echo "=============================================================================="
echo "  finished $(date -u +%FT%TZ)"
if [ "$RUN_BROKEN" = 1 ]; then
  echo
  echo "  !! THIS RUN HAD FAILURES. Do not quote anything above as a result."
  exit 1
fi
