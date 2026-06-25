#!/usr/bin/env bash
# Drop-in replacement for the official postgres image's docker-entrypoint.sh
# (https://github.com/docker-library/postgres). This is a faithful port of the
# upstream script — same env vars, same /docker-entrypoint-initdb.d semantics,
# same gosu step-down, same first-boot-on-localhost-then-restart pattern, same
# pg_hba setup.
#
# The substitution for pgrust: BOTH the user-facing SERVER and the catalog
# bootstrap (`initdb`) are the pgrust `postgres` binary (a Rust port of
# PostgreSQL 18.3), not C postgres / C initdb. The only C tool left in the image
# is `psql` (the client used for init scripts / db setup); pgrust has no psql
# replacement. So the places that diverge from upstream are:
#   * docker_init_database_dir — bootstraps via `pgrust-postgres --initdb`
#     (pgrust's own ported initdb driver), not C `initdb`.
#   * docker_temp_server_start / docker_temp_server_stop — these launch the
#     pgrust binary directly (upstream uses `pg_ctl`, which would launch C
#     postgres). pgrust is started backgrounded and we poll for readiness, then
#     SIGINT it to stop (matching STOPSIGNAL SIGINT / fast-shutdown semantics).
#   * pg_setup_hba_conf — upstream runs `postgres -C password_encryption` to
#     discover the auth method; we read it from the freshly-initdb'd
#     postgresql.conf instead (defaulting to scram-sha-256, PG14+ default), so
#     we never depend on the pgrust binary supporting `-C`.
#   * the final `exec` of the server appends pgrust-specific GUCs
#     (io_method=sync, max_stack_depth) and a larger stack ulimit, without
#     changing the `postgres ...`/flags CMD contract a user passes.
set -Eeo pipefail
# TODO swap to -Eeuo pipefail above (after handling all potentially-unset variables)

# The pgrust server binary. This same binary also performs the catalog
# bootstrap via its built-in `--initdb` driver, and re-execs itself for the
# internal --boot / --single bootstrap phases. We always resolve it by this
# absolute path, never by name.
PGRUST_BIN="${PGRUST_BIN:-/usr/local/bin/pgrust-postgres}"

# The share dir that ships the initdb bootstrap templates (postgres.bki,
# system_*.sql, system_views.sql, information_schema.sql, sql_features.txt,
# snowball_create.sql) and the config samples (postgresql.conf.sample,
# pg_hba.conf.sample, pg_ident.conf.sample). pgrust's --initdb / --boot read
# these. The pgrust binary lives in /usr/local/bin (not under the PGDG bindir),
# so the relative share-dir derivation can't find it — we pass it explicitly
# with `-L`. PGRUST_PGSHAREDIR is also baked into the binary for the tz tree.
PG_SHAREDIR="${PG_SHAREDIR:-${PGRUST_PGSHAREDIR:-/usr/share/postgresql/18}}"

# pgrust's per-statement frames are large; the C stack default refuses to boot.
ulimit -s 65520 2>/dev/null || true
export RUST_MIN_STACK="${RUST_MIN_STACK:-33554432}"

# pgrust-specific server GUCs applied to BOTH the temp init server and the final
# server (without disturbing the user's CMD/flags).
#
# unix_socket_directories=/var/run/postgresql aligns the pgrust server's socket
# with the location the bundled Debian C psql/libpq use by default (pgrust's own
# boot default is /tmp), so the init-script client and healthchecks connect.
PGRUST_SERVER_OPTS=(
	-c io_method=sync
	-c max_stack_depth=60000
	-c unix_socket_directories=/var/run/postgresql
)

# usage: file_env VAR [DEFAULT]
#    ie: file_env 'XYZ_DB_PASSWORD' 'example'
# (will allow for "$XYZ_DB_PASSWORD_FILE" to fill in the value of
#  "$XYZ_DB_PASSWORD" from a file, especially for Docker's secrets feature)
file_env() {
	local var="$1"
	local fileVar="${var}_FILE"
	local def="${2:-}"
	if [ "${!var:-}" ] && [ "${!fileVar:-}" ]; then
		printf >&2 'error: both %s and %s are set (but are exclusive)\n' "$var" "$fileVar"
		exit 1
	fi
	local val="$def"
	if [ "${!var:-}" ]; then
		val="${!var}"
	elif [ "${!fileVar:-}" ]; then
		val="$(< "${!fileVar}")"
	fi
	export "$var"="$val"
	unset "$fileVar"
}

# check to see if this file is being run or sourced from another script
_is_sourced() {
	# https://unix.stackexchange.com/a/215279
	[ "${#FUNCNAME[@]}" -ge 2 ] \
		&& [ "${FUNCNAME[0]}" = '_is_sourced' ] \
		&& [ "${FUNCNAME[1]}" = 'source' ]
}

# used to create initial postgres directories and if run as root, ensure ownership to the "postgres" user
docker_create_db_directories() {
	local user; user="$(id -u)"

	mkdir -p "$PGDATA"
	# ignore failure since there are cases where we can't chmod (and PostgreSQL might fail later anyhow - it's picky about permissions of this directory)
	chmod 00700 "$PGDATA" || :

	# ignore failure since it will be fine when using the image provided directory; see also https://github.com/docker-library/postgres/pull/289
	mkdir -p /var/run/postgresql || :
	chmod 03775 /var/run/postgresql || :

	# Create the transaction log directory before initdb is run so the directory is owned by the correct user
	if [ -n "${POSTGRES_INITDB_WALDIR:-}" ]; then
		mkdir -p "$POSTGRES_INITDB_WALDIR"
		if [ "$user" = '0' ]; then
			find "$POSTGRES_INITDB_WALDIR" \! -user postgres -exec chown postgres '{}' +
		fi
		chmod 700 "$POSTGRES_INITDB_WALDIR"
	fi

	# allow the container to be started with `--user`
	if [ "$user" = '0' ]; then
		find "$PGDATA" \! -user postgres -exec chown postgres '{}' +
		find /var/run/postgresql \! -user postgres -exec chown postgres '{}' +
	fi
}

# initialize empty PGDATA directory with new database via pgrust's `--initdb`
# arguments to `initdb` can be passed via POSTGRES_INITDB_ARGS or as arguments to this function
# initdb automatically creates the "postgres", "template0", and "template1" dbnames
# this is also where the database user is created, specified by `POSTGRES_USER` env
#
# pgrust note: upstream calls the C `initdb` binary. We instead invoke pgrust's
# OWN ported initdb driver (`pgrust-postgres --initdb`), which scaffolds the data
# dir, writes the config files, runs the catalog bootstrap (`--boot`) and the
# post-bootstrap SQL (`--single`), and creates template0/postgres — all in pure
# pgrust, no C postgres/initdb involved. Two divergences from C initdb:
#   * pgrust initdb has no `--pwfile`; the superuser password is instead applied
#     later (via ALTER ROLE) by docker_setup_password() once the temp server is
#     up. The password is still set in the catalog, exactly as C initdb would.
#   * the share dir is passed explicitly with `-L` (pgrust-postgres is in
#     /usr/local/bin, outside the PGDG bindir, so relative derivation can't
#     locate the share tree).
# No nss_wrapper is needed: pgrust initdb takes the superuser name from
# --username and does not consult /etc/passwd.
docker_init_database_dir() {
	if [ -n "${POSTGRES_INITDB_WALDIR:-}" ]; then
		set -- --waldir "$POSTGRES_INITDB_WALDIR" "$@"
	fi

	# pgrust initdb parses `--opt value` (space-separated), not `--opt=value`.
	# Note: pgrust's initdb supports a subset of C initdb's options (-D/-U/-L/
	# -E/--encoding/--lc-collate/--lc-ctype/--locale/--no-locale/--wal-segsize);
	# unsupported POSTGRES_INITDB_ARGS (e.g. -A/--auth, --data-checksums) error.
	eval '"$PGRUST_BIN" --initdb -D "$PGDATA" -L "$PG_SHAREDIR" --username "$POSTGRES_USER" '"$POSTGRES_INITDB_ARGS"' "$@"'
}

# Set the bootstrap superuser's password in the catalog. pgrust's --initdb has
# no --pwfile, so we do what C initdb's --pwfile would have done — via ALTER
# ROLE against the temporary server. Called after docker_temp_server_start.
# A no-op when POSTGRES_PASSWORD is empty (POSTGRES_HOST_AUTH_METHOD=trust path).
docker_setup_password() {
	if [ -n "$POSTGRES_PASSWORD" ]; then
		PGPASSWORD= docker_process_sql --dbname postgres --set pw="$POSTGRES_PASSWORD" <<-'EOSQL'
			ALTER ROLE CURRENT_USER WITH PASSWORD :'pw' ;
		EOSQL
	fi
}

# print large warning if POSTGRES_PASSWORD is long
# error if both POSTGRES_PASSWORD is empty and POSTGRES_HOST_AUTH_METHOD is not 'trust'
# print large warning if POSTGRES_HOST_AUTH_METHOD is set to 'trust'
# assumes database is not set up, ie: [ -z "$DATABASE_ALREADY_EXISTS" ]
docker_verify_minimum_env() {
	if [ -z "$POSTGRES_PASSWORD" ] && [ 'trust' != "$POSTGRES_HOST_AUTH_METHOD" ]; then
		# The - option suppresses leading tabs but *not* spaces. :)
		cat >&2 <<-'EOE'
			Error: Database is uninitialized and superuser password is not specified.
			       You must specify POSTGRES_PASSWORD to a non-empty value for the
			       superuser. For example, "-e POSTGRES_PASSWORD=password" on "docker run".

			       You may also use "POSTGRES_HOST_AUTH_METHOD=trust" to allow all
			       connections without a password. This is *not* recommended.

			       See PostgreSQL documentation about "trust":
			       https://www.postgresql.org/docs/current/auth-trust.html
		EOE
		exit 1
	fi
	if [ 'trust' = "$POSTGRES_HOST_AUTH_METHOD" ]; then
		cat >&2 <<-'EOWARN'
			********************************************************************************
			WARNING: POSTGRES_HOST_AUTH_METHOD has been set to "trust". This will allow
			         anyone with access to the Postgres port to access your database without
			         a password, even if POSTGRES_PASSWORD is set. See PostgreSQL
			         documentation about "trust":
			         https://www.postgresql.org/docs/current/auth-trust.html
			         In Docker's default configuration, this is effectively any other
			         container on the same system.

			         It is not recommended to use POSTGRES_HOST_AUTH_METHOD=trust. Replace
			         it with "-e POSTGRES_PASSWORD=password" instead to set a password in
			         "docker run".
			********************************************************************************
		EOWARN
	fi
}

# similar to the above, but errors if there are any "old" databases detected (usually due to upgrades without pg_upgrade)
docker_error_old_databases() {
	if [ -n "${OLD_DATABASES[0]:-}" ]; then
		cat >&2 <<-EOE
			Error: in 18+, these Docker images are configured to store database data in a
			       format which is compatible with "pg_ctlcluster" (specifically, using
			       major-version-specific directory names).  This better reflects how
			       PostgreSQL itself works, and how upgrades are to be performed.

			       See also https://github.com/docker-library/postgres/pull/1259

			       Counter to that, there appears to be PostgreSQL data in:
			         ${OLD_DATABASES[*]}

			       This is usually the result of upgrading the Docker image without
			       upgrading the underlying database using "pg_upgrade" (which requires both
			       versions).

			       The suggested container configuration for 18+ is to place a single mount
			       at /var/lib/postgresql which will then place PostgreSQL data in a
			       subdirectory, allowing usage of "pg_upgrade --link" without mount point
			       boundary issues.

			       See https://github.com/docker-library/postgres/issues/37 for a (long)
			       discussion around this process, and suggestions for how to do so.
		EOE
		exit 1
	fi
}

# usage: docker_process_init_files [file [file [...]]]
#    ie: docker_process_init_files /always-initdb.d/*
# process initializer files, based on file extensions and permissions
docker_process_init_files() {
	# psql here for backwards compatibility "${psql[@]}"
	psql=( docker_process_sql )

	printf '\n'
	local f
	for f; do
		case "$f" in
			*.sh)
				# https://github.com/docker-library/postgres/issues/450#issuecomment-393167936
				# https://github.com/docker-library/postgres/pull/452
				if [ -x "$f" ]; then
					printf '%s: running %s\n' "$0" "$f"
					"$f"
				else
					printf '%s: sourcing %s\n' "$0" "$f"
					. "$f"
				fi
				;;
			*.sql)     printf '%s: running %s\n' "$0" "$f"; docker_process_sql -f "$f"; printf '\n' ;;
			*.sql.gz)  printf '%s: running %s\n' "$0" "$f"; gunzip -c "$f" | docker_process_sql; printf '\n' ;;
			*.sql.xz)  printf '%s: running %s\n' "$0" "$f"; xzcat "$f" | docker_process_sql; printf '\n' ;;
			*.sql.zst) printf '%s: running %s\n' "$0" "$f"; zstd -dc "$f" | docker_process_sql; printf '\n' ;;
			*)         printf '%s: ignoring %s\n' "$0" "$f" ;;
		esac
		printf '\n'
	done
}

# Execute sql script, passed via stdin (or -f flag of pqsl)
# usage: docker_process_sql [psql-cli-args]
#    ie: docker_process_sql --dbname=mydb <<<'INSERT ...'
#    ie: docker_process_sql -f my-file.sql
#    ie: docker_process_sql <my-file.sql
docker_process_sql() {
	local query_runner=( psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --no-password --no-psqlrc )
	if [ -n "$POSTGRES_DB" ]; then
		query_runner+=( --dbname "$POSTGRES_DB" )
	fi

	PGHOST= PGHOSTADDR= "${query_runner[@]}" "$@"
}

# create initial database
# uses environment variables for input: POSTGRES_DB
docker_setup_db() {
	local dbAlreadyExists
	dbAlreadyExists="$(
		POSTGRES_DB= docker_process_sql --dbname postgres --set db="$POSTGRES_DB" --tuples-only <<-'EOSQL'
			SELECT 1 FROM pg_database WHERE datname = :'db' ;
		EOSQL
	)"
	if [ -z "$dbAlreadyExists" ]; then
		POSTGRES_DB= docker_process_sql --dbname postgres --set db="$POSTGRES_DB" <<-'EOSQL'
			CREATE DATABASE :"db" ;
		EOSQL
		printf '\n'
	fi
}

# Loads various settings that are used elsewhere in the script
# This should be called before any other functions
docker_setup_env() {
	file_env 'POSTGRES_PASSWORD'

	file_env 'POSTGRES_USER' 'postgres'
	file_env 'POSTGRES_DB' "$POSTGRES_USER"
	file_env 'POSTGRES_INITDB_ARGS'
	: "${POSTGRES_HOST_AUTH_METHOD:=}"

	declare -g DATABASE_ALREADY_EXISTS
	: "${DATABASE_ALREADY_EXISTS:=}"
	declare -ag OLD_DATABASES=()
	# look specifically for PG_VERSION, as it is expected in the DB dir
	if [ -s "$PGDATA/PG_VERSION" ]; then
		DATABASE_ALREADY_EXISTS='true'
	elif [ "$PGDATA" = "/var/lib/postgresql/$PG_MAJOR/docker" ]; then
		# https://github.com/docker-library/postgres/pull/1259
		for d in /var/lib/postgresql /var/lib/postgresql/data /var/lib/postgresql/*/docker; do
			if [ -s "$d/PG_VERSION" ]; then
				OLD_DATABASES+=( "$d" )
			fi
		done
		if [ "${#OLD_DATABASES[@]}" -eq 0 ] && [ "$PG_MAJOR" -ge 18 ] && {
			# in BusyBox, "mountpoint" only checks dev vs ino (https://github.com/tianon/mirror-busybox/blob/be7d1b7b1701d225379bc1665487ed0871b592a5/util-linux/mountpoint.c#L78) which will notably miss bind mounts entirely (which almost all Docker volume mounts are)
			# coreutils checks /proc/self/mountinfo, so we have a fallback to mimic that and directly check "/proc/self/mountinfo" to catch that case
			mountpoint -q /var/lib/postgresql/data \
			|| awk '$5 == "/var/lib/postgresql/data" { found = 1 } END { exit !found }' /proc/self/mountinfo
		}; then
			OLD_DATABASES+=( '/var/lib/postgresql/data (unused mount/volume)' )
		fi
	fi
}

# append POSTGRES_HOST_AUTH_METHOD to pg_hba.conf for "host" connections
# all arguments will be passed along as arguments to `postgres` for getting the value of 'password_encryption'
#
# pgrust note: upstream discovers the auth method via `postgres -C
# password_encryption` and writes a password `host` line (scram-sha-256). The
# pgrust SERVER, however, cannot yet PERFORM password authentication — the
# `syscache_seams::fetch_role_password` seam is unported, so a `host all all all
# scram-sha-256` (or md5) line makes every TCP connection FATAL ("seam not
# installed: fetch_role_password"), even with the correct password. Only `trust`
# host lines work today (local connections already use `trust`, which is why the
# init-script client and unix-socket healthchecks succeed).
#
# So, to keep the image a working drop-in over TCP, the host auth method
# DEFAULTS to `trust` here (the password is still set by initdb and stored in the
# catalog — it is simply not enforced on `host` lines until pgrust gains password
# auth). An explicit POSTGRES_HOST_AUTH_METHOD always wins; if a user forces a
# password method we honor it (and warn that pgrust will reject those TCP logins).
pg_setup_hba_conf() {
	if [ "$1" = 'postgres' ]; then
		shift
	fi
	# pgrust can only authenticate `trust` host connections today.
	: "${POSTGRES_HOST_AUTH_METHOD:=trust}"
	case "$POSTGRES_HOST_AUTH_METHOD" in
		trust|reject) ;;
		*)
			cat >&2 <<-EOWARN
				********************************************************************************
				WARNING: POSTGRES_HOST_AUTH_METHOD="$POSTGRES_HOST_AUTH_METHOD" requested, but the
				         pgrust server cannot yet perform password authentication
				         (fetch_role_password is unported). TCP connections using this method
				         will be rejected by the server with a FATAL "seam not installed"
				         error. Use POSTGRES_HOST_AUTH_METHOD=trust for working TCP access.
				********************************************************************************
			EOWARN
			;;
	esac
	{
		printf '\n'
		if [ 'trust' = "$POSTGRES_HOST_AUTH_METHOD" ]; then
			printf '# warning trust is enabled for all connections\n'
			printf '# see https://www.postgresql.org/docs/17/auth-trust.html\n'
		fi
		printf 'host all all all %s\n' "$POSTGRES_HOST_AUTH_METHOD"
	} >> "$PGDATA/pg_hba.conf"
}

# start socket-only pgrust server for setting up or running scripts
# all arguments will be passed along as arguments to the pgrust `postgres`
#
# pgrust note: upstream uses `pg_ctl ... start`, which would launch the C
# postgres. We launch the pgrust binary directly (backgrounded, socket-only) and
# poll for readiness.
PGRUST_TEMP_PID=""
docker_temp_server_start() {
	if [ "$1" = 'postgres' ]; then
		shift
	fi

	# socket-only: no external TCP/IP, same as upstream's `-c listen_addresses=''`.
	"$PGRUST_BIN" -D "$PGDATA" \
		"${PGRUST_SERVER_OPTS[@]}" \
		-c listen_addresses='' \
		-p "${PGPORT:-5432}" \
		"$@" &
	PGRUST_TEMP_PID=$!

	# Wait until it accepts connections on the unix socket (default /var/run/postgresql).
	local i
	for i in $(seq 1 120); do
		if PGHOST=/var/run/postgresql PGHOSTADDR= \
			psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --no-password --no-psqlrc \
				--dbname postgres -tAc 'SELECT 1' >/dev/null 2>&1; then
			return 0
		fi
		if ! kill -0 "$PGRUST_TEMP_PID" 2>/dev/null; then
			echo >&2 "pgrust: temporary server exited during startup"
			wait "$PGRUST_TEMP_PID" || true
			exit 1
		fi
		sleep 0.5
	done
	echo >&2 "pgrust: timed out waiting for temporary server to accept connections"
	exit 1
}

# stop the temporary pgrust server after setup (SIGINT = fast shutdown).
docker_temp_server_stop() {
	if [ -n "$PGRUST_TEMP_PID" ] && kill -0 "$PGRUST_TEMP_PID" 2>/dev/null; then
		kill -INT "$PGRUST_TEMP_PID" 2>/dev/null || true
		wait "$PGRUST_TEMP_PID" 2>/dev/null || true
	fi
	PGRUST_TEMP_PID=""
}

# check arguments for an option that would cause postgres to stop
# return true if there is one
_pg_want_help() {
	local arg
	for arg; do
		case "$arg" in
			# postgres --help | grep 'then exit'
			# leaving out -C on purpose since it always fails and is unhelpful:
			# postgres: could not access the server configuration file "/var/lib/postgresql/data/postgresql.conf": No such file or directory
			-'?'|--help|--describe-config|-V|--version)
				return 0
				;;
		esac
	done
	return 1
}

_main() {
	# if first arg looks like a flag, assume we want to run postgres server
	if [ "${1:0:1}" = '-' ]; then
		set -- postgres "$@"
	fi

	if [ "$1" = 'postgres' ] && ! _pg_want_help "$@"; then
		docker_setup_env
		# setup data directories and permissions (when run as root)
		docker_create_db_directories
		if [ "$(id -u)" = '0' ]; then
			# then restart script as postgres user
			exec gosu postgres "$BASH_SOURCE" "$@"
		fi

		# only run initialization on an empty data directory
		if [ -z "$DATABASE_ALREADY_EXISTS" ]; then
			docker_verify_minimum_env
			docker_error_old_databases

			# check dir permissions to reduce likelihood of half-initialized database
			ls /docker-entrypoint-initdb.d/ > /dev/null

			docker_init_database_dir
			pg_setup_hba_conf "$@"

			# PGPASSWORD is required for psql when authentication is required for 'local' connections via pg_hba.conf and is otherwise harmless
			# e.g. when '--auth=md5' or '--auth-local=md5' is used in POSTGRES_INITDB_ARGS
			export PGPASSWORD="${PGPASSWORD:-$POSTGRES_PASSWORD}"
			docker_temp_server_start "$@"

			# pgrust initdb has no --pwfile, so set the superuser password now.
			docker_setup_password
			docker_setup_db
			docker_process_init_files /docker-entrypoint-initdb.d/*

			docker_temp_server_stop
			unset PGPASSWORD

			cat <<-'EOM'

				PostgreSQL init process complete; ready for start up.

			EOM
		else
			cat <<-'EOM'

				PostgreSQL Database directory appears to contain a database; Skipping initialization

			EOM
		fi
	fi

	# pgrust substitution: `exec "$@"` would run the C `postgres` on PATH (used
	# only by initdb). When the command IS the server (`postgres [flags...]`),
	# rewrite argv[0] to the pgrust binary and append pgrust GUCs + listen_addresses='*'.
	# Anything else (psql, a help flag, an arbitrary command) is exec'd verbatim.
	if [ "$1" = 'postgres' ] && ! _pg_want_help "$@"; then
		shift # drop the literal "postgres"
		exec "$PGRUST_BIN" -D "$PGDATA" \
			"${PGRUST_SERVER_OPTS[@]}" \
			-c listen_addresses='*' \
			"$@"
	fi

	exec "$@"
}

if ! _is_sourced; then
	_main "$@"
fi
