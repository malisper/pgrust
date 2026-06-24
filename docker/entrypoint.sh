#!/usr/bin/env bash
# Entrypoint for the pgrust runtime image.
#
# Subcommands:
#   query   (default) boot pgrust, run `select version()` + a sanity query, exit
#   server            boot pgrust in the foreground and keep it running
#   psql              boot pgrust in the background, then drop into psql
#   <anything else>   exec it verbatim (escape hatch)
#
# pgrust's own initdb is unported, so the datadir is created by the C `initdb`
# from the bundled PostgreSQL 18.3 install.
set -euo pipefail

PGDATA="${PGDATA:-/var/lib/pgrust/data}"
PGSOCKDIR="${PGSOCKDIR:-/tmp}"
PGPORT="${PGPORT:-5432}"
PGUSER_BOOT="postgres"

# The pgrust server lives at an absolute path. The C `postgres` backend shipped
# alongside initdb (for initdb's bootstrap step) is also on PATH, so we never
# resolve the server by bare name — always use this absolute path.
PGRUST_BIN="${PGRUST_BIN:-/usr/local/bin/postgres}"

log() { echo "[entrypoint] $*" >&2; }

# pgrust's per-statement frames are large; the C stack default refuses to boot.
ulimit -s 65520 || true
export RUST_MIN_STACK="${RUST_MIN_STACK:-33554432}"

init_datadir() {
    if [ ! -s "${PGDATA}/PG_VERSION" ]; then
        log "initializing datadir at ${PGDATA} (C initdb)"
        initdb -D "${PGDATA}" --no-locale --encoding=UTF8 -U "${PGUSER_BOOT}" >/dev/null
    fi
}

PGRUST_PID=""
boot_pgrust() {
    log "booting pgrust on port ${PGPORT} (socket dir ${PGSOCKDIR})"
    "${PGRUST_BIN}" -D "${PGDATA}" \
        -k "${PGSOCKDIR}" -p "${PGPORT}" \
        -c io_method=sync -c max_stack_depth=60000 \
        -c listen_addresses='*' &
    PGRUST_PID=$!
    # Wait for the server to accept connections.
    for _ in $(seq 1 60); do
        if psql -h "${PGSOCKDIR}" -p "${PGPORT}" -U "${PGUSER_BOOT}" \
                -d postgres -tAc 'select 1' >/dev/null 2>&1; then
            log "pgrust is accepting connections"
            return 0
        fi
        if ! kill -0 "${PGRUST_PID}" 2>/dev/null; then
            log "pgrust exited during startup"
            wait "${PGRUST_PID}" || true
            exit 1
        fi
        sleep 0.5
    done
    log "timed out waiting for pgrust to accept connections"
    exit 1
}

stop_pgrust() {
    if [ -n "${PGRUST_PID}" ] && kill -0 "${PGRUST_PID}" 2>/dev/null; then
        kill "${PGRUST_PID}" 2>/dev/null || true
        wait "${PGRUST_PID}" 2>/dev/null || true
    fi
}

cmd="${1:-query}"
case "${cmd}" in
    query)
        init_datadir
        boot_pgrust
        trap stop_pgrust EXIT
        echo "=== select version() ==="
        psql -h "${PGSOCKDIR}" -p "${PGPORT}" -U "${PGUSER_BOOT}" -d postgres \
            -c 'select version();'
        echo "=== select count(*), sum(g) from generate_series(1,1000) g ==="
        psql -h "${PGSOCKDIR}" -p "${PGPORT}" -U "${PGUSER_BOOT}" -d postgres \
            -c 'select count(*), sum(g) from generate_series(1,1000) g;'
        ;;
    server)
        init_datadir
        log "booting pgrust in the foreground (port ${PGPORT})"
        exec "${PGRUST_BIN}" -D "${PGDATA}" \
            -k "${PGSOCKDIR}" -p "${PGPORT}" \
            -c io_method=sync -c max_stack_depth=60000 \
            -c listen_addresses='*'
        ;;
    psql)
        init_datadir
        boot_pgrust
        trap stop_pgrust EXIT
        shift || true
        psql -h "${PGSOCKDIR}" -p "${PGPORT}" -U "${PGUSER_BOOT}" -d postgres "$@"
        ;;
    *)
        exec "$@"
        ;;
esac
