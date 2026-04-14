#!/usr/bin/env bash
set -Eeuo pipefail

docker_create_db_directories() {
    mkdir -p "$PGDATA" /var/run/postgresql
    chmod 700 "$PGDATA" || :
    chmod 3777 /var/run/postgresql || :

    if [ "$(id -u)" = '0' ]; then
        chown -R postgres:postgres "$PGDATA" /var/run/postgresql
    fi
}

if [ "${1:0:1}" = '-' ]; then
    set -- pgrust_server "$@"
fi

if [ "${1:-}" = 'pgrust_server' ]; then
    docker_create_db_directories

    if [ "$(id -u)" = '0' ]; then
        exec gosu postgres "$BASH_SOURCE" "$@"
    fi

    shift
    if [ "$#" -eq 0 ]; then
        set -- "$PGDATA" "$PGRUST_PORT" "$PGRUST_BUFFER_POOL_SIZE"
    fi

    exec pgrust_server "$@"
fi

exec "$@"
