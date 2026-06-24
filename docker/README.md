# Docker image for pgrust — a drop-in `postgres` replacement

A self-contained, reproducible image that **builds** and **runs** the Linux
build of pgrust (the Rust port of PostgreSQL 18.3), and is a **drop-in
replacement for the official `postgres` Docker image**
([docker-library/postgres](https://github.com/docker-library/postgres)).

Swap `postgres` → `pgrust` in your `docker run` / `docker compose` and it
behaves the same: same env vars, same first-init scripts, same data dir, same
unix user, same signals. The one substitution is that the actual SERVER is the
pgrust binary instead of C postgres.

## Build

From the repository root:

```sh
docker build -t pgrust .
```

The build runs three stages:

1. **`rustbuild`** — builds the pgrust `postgres` binary
   (`cargo build --release --locked --bin postgres`) on `rust:1-bookworm` with
   `build-essential pkg-config libicu-dev`. `nodetags.h` is **vendored** in the
   repo (`crates/_support/types/nodes/vendor/nodetags.h`), so this stage needs
   no PostgreSQL source — the build is fully self-contained. `PGRUST_PGSHAREDIR`
   is baked to the share tree's location in the final image. This is the long
   stage (~1400 crates).
2. **`pgtools`** — installs PostgreSQL 18 from the PGDG apt repo and copies out
   just the minimal pieces the runtime needs: `initdb`, `psql`, the C `postgres`
   backend (used *only* by initdb's bootstrap — see below), the shared libs
   those pull in (libpq, libicu, ...), and the PostgreSQL `share/` tree
   (timezone data + initdb's catalog templates).
3. **`final`** — an official-image-compatible `debian:bookworm-slim` runtime
   with the pgrust binary, those minimal C tools, `libicu72`, `gosu`, and
   `nss_wrapper`. Declares `VOLUME /var/lib/postgresql/data`, `EXPOSE 5432`,
   `STOPSIGNAL SIGINT`, `ENTRYPOINT ["docker-entrypoint.sh"]`,
   `CMD ["postgres"]`, and the `postgres` user (uid/gid 999).

### Why the C `postgres` backend is shipped

pgrust's own `initdb`/catalog bootstrap is unported, so the data dir is created
by the **C** `initdb`. C `initdb` runs `postgres --boot` internally to build the
catalog, so the C backend must be present *for initdb*. It is **not** the
user-facing server: it lives at its PGDG path as `postgres`, while the pgrust
server is installed as `/usr/local/bin/pgrust-postgres` and the entrypoint always
launches the server by that path.

## Run — same contract as the official image

```sh
# Boot a server (you must give a password, or opt into trust — same as official):
docker run --rm -e POSTGRES_PASSWORD=secret -p 5432:5432 pgrust

# then, from the host:
psql postgresql://postgres:secret@localhost:5432/postgres -c 'select version()'
#  -> pgrust 18.3 ...
```

### Environment variables (identical to the official image)

| Var                         | Default            | Meaning                                                             |
|-----------------------------|--------------------|---------------------------------------------------------------------|
| `POSTGRES_PASSWORD`         | *(required)*       | superuser password. Omit only with `POSTGRES_HOST_AUTH_METHOD=trust`. Also `_FILE`. |
| `POSTGRES_USER`             | `postgres`         | superuser role created by initdb. Also `_FILE`.                      |
| `POSTGRES_DB`               | `$POSTGRES_USER`   | default database created on first init. Also `_FILE`.                |
| `POSTGRES_INITDB_ARGS`      | *(empty)*          | extra args passed to `initdb`.                                       |
| `POSTGRES_INITDB_WALDIR`    | *(empty)*          | separate WAL dir for initdb.                                         |
| `POSTGRES_HOST_AUTH_METHOD` | *(empty)*          | auth method for `host` lines in `pg_hba.conf`; `trust` disables passwords. |
| `PGDATA`                    | `/var/lib/postgresql/data` | data directory (a declared `VOLUME`).                        |

### First-init scripts: `/docker-entrypoint-initdb.d/`

On the **first** boot of an empty data dir (after `initdb` + db setup, before the
server is opened to the network), files mounted into
`/docker-entrypoint-initdb.d/` are processed in sorted order, same semantics as
the official image:

- `*.sql` — run with `psql`
- `*.sql.gz` / `*.sql.xz` / `*.sql.zst` — decompressed, then run with `psql`
- `*.sh` — sourced (or executed if the executable bit is set)
- anything else — ignored

```sh
docker run --rm \
  -e POSTGRES_PASSWORD=secret \
  -v "$PWD/init.sql:/docker-entrypoint-initdb.d/init.sql" \
  -p 5432:5432 pgrust
```

### Persistence

Data persists across runs via a volume on `/var/lib/postgresql/data`:

```sh
docker run --rm -e POSTGRES_PASSWORD=secret -v pgdata:/var/lib/postgresql/data pgrust
# ... later, a fresh container with the same volume skips init and keeps your data.
```

### Other forms (all official-image-compatible)

```sh
# pass server flags (argv[0] '-...' is treated as `postgres ...`):
docker run --rm -e POSTGRES_PASSWORD=secret pgrust -c max_connections=200

# run an arbitrary command in the image (escape hatch):
docker run --rm -it pgrust bash

# version/help short-circuit (no init), like the official image:
docker run --rm pgrust postgres --version
```

## How it differs from the official entrypoint

`docker/entrypoint.sh` is a faithful port of upstream `docker-entrypoint.sh`
(same `file_env`, `docker_create_db_directories`, `docker_init_database_dir`,
`docker_verify_minimum_env`, `docker_process_init_files`, `docker_setup_db`,
`pg_setup_hba_conf`, `_main`, gosu step-down, first-boot-on-localhost pattern).
Only three places diverge, all because the server is pgrust:

1. **`docker_temp_server_start` / `docker_temp_server_stop`** launch the pgrust
   binary directly (upstream uses `pg_ctl`, which would launch C postgres),
   backgrounded + polled, and `SIGINT` to stop (fast shutdown).
2. **`pg_setup_hba_conf`** reads `password_encryption` from the freshly-written
   `postgresql.conf` (defaulting to `scram-sha-256`) instead of running
   `postgres -C password_encryption`, so it never depends on pgrust supporting
   `-C`.
3. The **final `exec`** rewrites `postgres ...` to the pgrust binary and appends
   pgrust GUCs (`io_method=sync`, `max_stack_depth=60000`,
   `unix_socket_directories=/var/run/postgresql`) and a larger stack ulimit /
   `RUST_MIN_STACK`, without changing the flags a user passed.

## Notes

- The PostgreSQL 18 catalog version is frozen across the 18.x minor series, so
  the PGDG `postgresql-18` package produces a data dir whose control-file
  `catalog_version_no` matches what pgrust (18.3) expects.
- The image targets the host architecture (verified on `aarch64`; `x86_64`
  works the same way).
- A trailing `ShutdownXLOG xlog-driver` message on shutdown is pre-existing and
  harmless.
