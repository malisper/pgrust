#!/bin/bash
# Generate the pg_combinebackup interop fixtures from STOCK PostgreSQL 18.3
# (docker image postgres:18.3). Produces, under $OUT:
#   full.tar.gz, incr1.tar.gz, incr2.tar.gz   - a real backup chain
#       (full + two incrementals) taken with stock pg_basebackup from a
#       server with summarize_wal=on, including an in-place tablespace,
#       a dropped+recreated table, a truncated table, and enough churn for
#       real block lists in the incremental files.
#   golden-combined.sha256      - sha256 of every file (except
#       backup_manifest) in STOCK pg_combinebackup's reconstruction of the
#       chain, paths relative to the output dir.
#   golden-backup_manifest      - STOCK pg_combinebackup's output manifest.
#   golden-dryrun-debug.stderr  - stock `pg_combinebackup -d -n` stderr
#       (input paths appear under the container path /tmp/fx; tests
#       normalize their extraction root to that prefix).
#   golden-err-*.stderr         - stock chain-validation error outputs.
#   golden-query.out            - query output proving the stock-combined
#       directory starts and serves queries (validity witness).
#
# The WAL segment size is 1MB (initdb --wal-segsize=1) to keep fixtures small.
set -euo pipefail

OUT=${1:?usage: generate-fixtures.sh OUTDIR}
mkdir -p "$OUT"
OUT=$(cd "$OUT" && pwd)

docker run --rm -v "$OUT":/out postgres:18.3 bash -ec '
chown postgres /out
exec su postgres -s /bin/bash <<'\''SCRIPT'\''
set -euo pipefail
export PATH=/usr/lib/postgresql/18/bin:$PATH
export PGDATA=/tmp/pgdata PGUSER=postgres PGHOST=/tmp
FX=/tmp/fx && mkdir -p $FX

initdb -D $PGDATA -U postgres --no-sync --wal-segsize=1 >/dev/null
cat >> $PGDATA/postgresql.conf <<EOF
summarize_wal = on
listen_addresses = '\'''\''
unix_socket_directories = '\''/tmp'\''
autovacuum = off
EOF
pg_ctl -D $PGDATA -w -l /tmp/log start >/dev/null

PGOPTIONS="-c allow_in_place_tablespaces=on" psql -qc "create tablespace ts_inplace location '\'''\'';"
psql -qc "create table t_big as select g, md5(g::text) t from generate_series(1,20000) g;"
psql -qc "create table t_ts (g int, t text) tablespace ts_inplace;"
psql -qc "insert into t_ts select g, md5(g::text) from generate_series(1,5000) g;"
psql -qc "create table t_drop as select g from generate_series(1,1000) g;"
psql -qc "create table t_trunc as select g, repeat('\''x'\'',200) r from generate_series(1,5000) g;"

pg_basebackup -D $FX/full --no-sync -c fast

# churn for incr1: scattered updates, drop/recreate, truncate, growth
psql -qc "update t_big set t = md5(t) where g % 7 = 0;"
psql -qc "update t_ts set t = md5(t) where g % 5 = 0;"
psql -qc "drop table t_drop; create table t_drop as select g from generate_series(1,500) g;"
psql -qc "truncate t_trunc;"
psql -qc "insert into t_trunc select g, repeat('\''y'\'',100) from generate_series(1,200) g;"
psql -qc "insert into t_big select g, md5(g::text) from generate_series(20001,25000) g;"
psql -qc "checkpoint;"

pg_basebackup -D $FX/incr1 --incremental=$FX/full/backup_manifest --no-sync -c fast

# churn for incr2
psql -qc "update t_big set t = md5(t) where g % 11 = 0;"
psql -qc "delete from t_ts where g % 3 = 0;"
psql -qc "insert into t_big select g, md5(g::text) from generate_series(25001,26000) g;"
psql -qc "checkpoint;"

pg_basebackup -D $FX/incr2 --incremental=$FX/incr1/backup_manifest --no-sync -c fast

pg_ctl -D $PGDATA -w stop >/dev/null

# Golden reconstruction with the stock tool.
pg_combinebackup $FX/full $FX/incr1 $FX/incr2 -o $FX/combined --no-sync
(cd $FX/combined && find . -type f ! -name backup_manifest -print0 | sort -z | xargs -0 sha256sum) > /out/golden-combined.sha256
cp $FX/combined/backup_manifest /out/golden-backup_manifest

# Dry-run debug output (paths under /fx).
pg_combinebackup -d -n $FX/full $FX/incr1 $FX/incr2 -o /tmp/nonexistent-out --no-sync 2> /out/golden-dryrun-debug.stderr || { echo "dry run failed"; exit 1; }

# Chain-validation error outputs.
set +e
pg_combinebackup $FX/incr1 $FX/full $FX/incr2 -o /tmp/e1 2> /out/golden-err-order.stderr
echo "exit=$?" >> /out/golden-err-order.stderr
pg_combinebackup $FX/full $FX/incr2 -o /tmp/e2 2> /out/golden-err-skip.stderr
echo "exit=$?" >> /out/golden-err-skip.stderr
pg_combinebackup $FX/incr1 $FX/incr2 -o /tmp/e3 2> /out/golden-err-nofull.stderr
echo "exit=$?" >> /out/golden-err-nofull.stderr
pg_combinebackup $FX/full $FX/incr1 $FX/incr2 -o /tmp/e4 --manifest-checksums=BOGUS 2> /out/golden-err-badalg.stderr
echo "exit=$?" >> /out/golden-err-badalg.stderr
set -e

# Validity witness: start a server on the stock-combined directory.
chmod 700 $FX/combined
cat >> $FX/combined/postgresql.conf <<EOF
summarize_wal = off
allow_in_place_tablespaces = on
EOF
pg_ctl -D $FX/combined -w -l /tmp/log2 start >/dev/null || { cat /tmp/log2; exit 1; }
psql -h /tmp -c "select count(*), min(g), max(g) from t_big" \
     -c "select count(*) from t_ts" \
     -c "select count(*) from t_trunc" \
     -c "select count(*) from t_drop" > /out/golden-query.out
pg_ctl -D $FX/combined -w stop >/dev/null

# Pack the input backups.
tar -C $FX -czf /out/full.tar.gz full
tar -C $FX -czf /out/incr1.tar.gz incr1
tar -C $FX -czf /out/incr2.tar.gz incr2
chmod -R a+r /out
SCRIPT
'
echo "fixtures written to $OUT"
