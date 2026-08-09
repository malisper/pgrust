#!/bin/bash
# Reverse-interop verification of a pgrust pg_combinebackup output directory
# using STOCK PostgreSQL 18.3 (docker image postgres:18.3):
#   1. stock pg_verifybackup validates the directory against the
#      backup_manifest pgrust wrote (file set + checksums + manifest trailer);
#   2. a stock server is started on a copy of the directory and queried.
#
# usage: verify-output-with-stock.sh COMBINED_DIR
set -euo pipefail

DIR=${1:?usage: verify-output-with-stock.sh COMBINED_DIR}
DIR=$(cd "$DIR" && pwd)

docker run --rm -v "$DIR":/combined:ro postgres:18.3 bash -ec '
export PATH=/usr/lib/postgresql/18/bin:$PATH
cp -a /combined /tmp/dd
chown -R postgres /tmp/dd
chmod 700 /tmp/dd
su postgres -s /bin/bash -c "
set -e
export PATH=/usr/lib/postgresql/18/bin:\$PATH
pg_verifybackup -n /tmp/dd && echo PG_VERIFYBACKUP_OK
printf \"summarize_wal = off\nallow_in_place_tablespaces = on\nlisten_addresses = \047\047\nunix_socket_directories = \047/tmp\047\n\" >> /tmp/dd/postgresql.conf
pg_ctl -D /tmp/dd -w -l /tmp/log start >/dev/null || { cat /tmp/log; exit 1; }
psql -h /tmp -U postgres -c \"select count(*), min(g), max(g) from t_big\" \
     -c \"select count(*) from t_ts\" \
     -c \"select count(*) from t_trunc\" \
     -c \"select count(*) from t_drop\"
pg_ctl -D /tmp/dd -w stop >/dev/null
echo SERVER_START_OK
"
'
