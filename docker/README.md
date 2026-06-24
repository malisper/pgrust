# Docker image for pgrust

A self-contained, reproducible image that **builds** and **runs** the Linux
build of pgrust (the Rust port of PostgreSQL 18.3). No host mounts are required.

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
   backend (used *only* by initdb's bootstrap step — see below), the shared libs
   those three pull in (libpq, libicu, ...), and the PostgreSQL `share/` tree
   (timezone data + initdb's catalog templates).
3. **`final`** — a slim `debian:bookworm-slim` runtime with the pgrust binary,
   those minimal C tools, and `libicu72`. Runs as a non-root user.

### Why the C `postgres` backend is shipped

pgrust's own `initdb`/catalog bootstrap is unported, so the datadir is created
by the **C** `initdb`. C `initdb` runs `postgres --boot` internally to build the
catalog, so the C backend must be present *for initdb*. It is **not** the
user-facing server: the entrypoint always boots pgrust by its absolute path
(`/usr/local/bin/postgres`). initdb finds its sibling C backend by exec
directory, so the two never collide.

## Run

The default command boots pgrust and runs a couple of sanity queries:

```sh
docker run --rm pgrust
```

Expected output includes:

```
=== select version() ===
                              version
-------------------------------------------------------------------
 pgrust 18.3 ...
=== select count(*), sum(g) from generate_series(1,1000) g ===
 count |  sum
-------+--------
  1000 | 500500
```

Other entrypoint subcommands:

```sh
# Drop into an interactive psql session against a freshly-booted pgrust:
docker run --rm -it pgrust psql

# Run pgrust as a long-lived server (foreground); expose the port:
docker run --rm -p 5432:5432 pgrust server

# Escape hatch — run any command in the image:
docker run --rm -it pgrust bash
```

## Boot details (baked into the entrypoint)

pgrust's per-statement frames are large, so the entrypoint sets `ulimit -s
65520` and `RUST_MIN_STACK`, and boots with the mandatory GUCs:

```
/usr/local/bin/postgres -D "$PGDATA" -k "$PGSOCKDIR" -p "$PGPORT" \
    -c io_method=sync -c max_stack_depth=60000 -c listen_addresses='*'
```

The datadir (default `/var/lib/pgrust/data`) is created on first boot by the
bundled C `initdb` with `--no-locale --encoding=UTF8 -U postgres`.

## Configuration

Override via `-e`:

| Var              | Default                | Meaning                  |
|------------------|------------------------|--------------------------|
| `PGDATA`         | `/var/lib/pgrust/data` | datadir location         |
| `PGPORT`         | `5432`                 | listen port              |
| `PGSOCKDIR`      | `/tmp`                 | unix socket directory    |
| `RUST_MIN_STACK` | `33554432`             | worker thread stack size |

## Notes

- The PostgreSQL 18 catalog version is frozen across the 18.x minor series, so
  the PGDG `postgresql-18` package produces a datadir whose control-file
  `catalog_version_no` matches what pgrust (18.3) expects.
- The image targets the host architecture (verified on `aarch64`; `x86_64`
  works the same way).
- A trailing `ShutdownXLOG xlog-driver` message on shutdown is pre-existing and
  harmless.
