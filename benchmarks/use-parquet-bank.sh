#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# use-parquet-bank.sh — point the rig + runner at the PARQUET-loaded bank
# (admin-provided 2026-07-28; same self-service pattern as
# use-reference-storage.sh: --check previews, --revert restores, everything
# it changes is backed up first and printed).
#
# The parquet bank is the OFFICIAL parquet cut: hits.parquet loaded via
# COPY (FREEZE, FORMAT parquet) with the parallel decoder, schema
# create-parquet.sql (time columns BIGINT/INTEGER — so it REQUIRES the
# parquet query set, which this script swaps in), datadir on the gp2
# reference volume, binary rc1-sinkfix.
#
# WHAT IT CHANGES (all reversible with --revert):
#   1. hosts.env PGDATA_CB -> /data2/cb-parquet/pgdata   (backup .pre-parquet)
#   2. queries.sql        -> queries-parquet.sql          (original kept as queries-tsv-original.sql)
#   3. /opt/pgrust/LOADINFO.txt -> the parquet load record (original kept as LOADINFO.tsv.txt)
#   4. restarts the serving postgres on the parquet datadir
# ---------------------------------------------------------------------------
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PQ_DD=/data2/cb-parquet/pgdata
AS_DD=/data/clickbench/pgdata
RIG=/data/clickbench/rig
MODE="${1:-adopt}"
case "$MODE" in
  --check)
    echo "current hosts.env PGDATA_CB: $(grep "^export PGDATA_CB=" "$HERE/hosts.env")"
    echo "live server datadir:         $(psql -h /data/clickbench/sock -p 5432 -U postgres -tAc "SHOW data_directory" 2>/dev/null || echo "<down>")"
    echo "queries.sql is:              $(cmp -s "$HERE/queries.sql" "$HERE/queries-parquet.sql" && echo PARQUET || echo TSV/original)"
    [ -f /data2/parquet-stage/LOADINFO.parquet.txt ] && grep -E "^(load_time_seconds|rows|pg_total_relation_size)=" /data2/parquet-stage/LOADINFO.parquet.txt || echo "parquet LOADINFO: not present (load not complete?)"
    exit 0 ;;
  --revert)
    [ -f "$HERE/hosts.env.pre-parquet" ] && cp "$HERE/hosts.env.pre-parquet" "$HERE/hosts.env"
    [ -f "$HERE/queries-tsv-original.sql" ] && cp "$HERE/queries-tsv-original.sql" "$HERE/queries.sql"
    [ -f /opt/pgrust/LOADINFO.tsv.txt ] && sudo cp /opt/pgrust/LOADINFO.tsv.txt /opt/pgrust/LOADINFO.txt
    cd "$RIG"; sudo -u pgrust env PGDATA_CB=$PQ_DD ./stop >/dev/null 2>&1 || true
    sudo -u pgrust env PGDATA_CB=$AS_DD ./start
    for i in $(seq 1 60); do sudo -u pgrust env PGDATA_CB=$AS_DD ./check >/dev/null 2>&1 && break; sleep 2; done
    echo "REVERTED: as-delivered TSV bank serving; hosts.env, queries.sql, LOADINFO restored."
    exit 0 ;;
  adopt|"") ;;
  *) echo "usage: $0 [--check|--revert]"; exit 2 ;;
esac
# adopt
grep -q "^rows=99997497$" /data2/parquet-stage/LOADINFO.parquet.txt 2>/dev/null || {
  echo "FATAL: parquet bank identity not verified (rows!=99997497 or load incomplete). Refusing."; exit 1; }
cp -n "$HERE/hosts.env" "$HERE/hosts.env.pre-parquet"
cp -n "$HERE/queries.sql" "$HERE/queries-tsv-original.sql"
sudo cp -n /opt/pgrust/LOADINFO.txt /opt/pgrust/LOADINFO.tsv.txt
sed -i "s|^export PGDATA_CB=.*|export PGDATA_CB=$PQ_DD|" "$HERE/hosts.env"
# OPTION A (2026-07-29): the parquet bank now loads the STANDARD TIMESTAMP/DATE
# schema via COPY ... COERCE_EPOCH, so the STANDARD queries.sql applies. Swapping
# in queries-parquet.sql (the bigint-epoch rewrites) would defeat the ts-extract
# fast kernel and make q19 ~65x slower. Left disabled deliberately.
# cp "$HERE/queries-parquet.sql" "$HERE/queries.sql"
sudo cp /data2/parquet-stage/LOADINFO.parquet.txt /opt/pgrust/LOADINFO.txt
cd "$RIG"
sudo -u pgrust env PGDATA_CB=$AS_DD ./stop >/dev/null 2>&1 || true
sudo -u pgrust env PGDATA_CB=$PQ_DD ./start
for i in $(seq 1 60); do sudo -u pgrust env PGDATA_CB=$PQ_DD ./check >/dev/null 2>&1 && break; sleep 2; done
echo "ADOPTED: parquet bank serving at $PQ_DD (gp2), parquet queries active."
echo "Run: ./run-clickbench.sh --smoke   then   ./run-clickbench.sh"
