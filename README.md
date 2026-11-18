<h1 align="center">pgrust</h1>

<p align="center">
  <strong>A Postgres-compatible database, rewritten in Rust, built for fast analytics on your Postgres data.</strong>
</p>

<p align="center">
  <img alt="Postgres 18.6" src="https://img.shields.io/badge/Postgres-18.6-336791">
  <img alt="Version: v0.4-beta" src="https://img.shields.io/badge/version-v0.4--beta-blue">
  <a href="LICENSE"><img alt="License: AGPL-3.0" src="https://img.shields.io/badge/license-AGPL--3.0-blue"></a>
</p>

<div align="center">
  <a href="https://pgrust.com"><strong>Try pgrust in your browser →</strong></a>
  <span>&nbsp;&nbsp;|&nbsp;&nbsp;</span>
  <a href="https://discord.gg/FZZ4dbdvwU">Discord</a>
  <span>&nbsp;&nbsp;|&nbsp;&nbsp;</span>
  <a href="https://pgrust.com/#updates">Updates</a>
  <span>&nbsp;&nbsp;|&nbsp;&nbsp;</span>
  <a href="https://github.com/malisper/pgrust/issues">Issues</a>
</div>

<br />

> **New:** [Video of Michael's talk at PlanetScale on pgrust](https://www.youtube.com/watch?v=7L_nG3EBjck) — and the [slides](https://drive.google.com/file/d/1_wDk4HPE9-MuiPBo_X-QdJUmFtVV0p8p/view).
>
> [How AI Changes the Economics of JIT Compilers](https://malisper.me/how-ai-changes-the-economics-of-jit-compilers/)
>
> **v0.4-beta is out:** [the release post](https://pgrust.com/blog/pgrust-v0-4-beta/) and [what's new](#whats-new-in-v04-beta).

pgrust is a drop-in replacement for Postgres built for fast analytics. It is
wire compatible and SQL dialect compatible with Postgres 18, so your existing
drivers, ORMs, `psql` and tools work unchanged. It is Postgres rebuilt the way
it would be built in 2026.

Every line is Rust, written to match the behavior of the C implementation.
Rust makes it easy to re-architect several core Postgres pieces. pgrust has:

- Gravity, a new vectorized push-based, JIT compiled query engine
- A thread based concurrency model
- A query scheduler designed to keep any individual query from taking down
  your database
- An automatic memory manager: one memory budget, sized from the machine,
  covers the buffer pool, the columnar caches and running queries, and gives
  pgrust control of what happens when you're running low on memory, greatly
  reducing the chance of your whole DB being taken down by the OS OOM killer

and many other really awesome pieces. The scheduler and the memory manager go
after two of
[the four horsemen behind thousands of Postgres outages](https://malisper.me/the-four-horsemen-behind-thousands-of-postgres-outages/).

## Quickstart

**In your browser:** <https://pgrust.com>, a full pgrust server compiled to
WebAssembly, no install.

**On your machine:** download a release tarball from
<https://pgrust.com/downloads/v0.4-beta/>. Each one is self-contained: it
carries pgrust's own `bin/postgres`, `bin/initdb` and `bin/psql` plus the
share files they read, so you do not need a PostgreSQL install. The Linux
binaries need glibc 2.34 or newer (Debian 12, Ubuntu 22.04, RHEL/Rocky 9 and
later); the macOS binaries need macOS 11 or newer.

| Tarball | Runs on |
|---|---|
| [`pgrust-0.4-beta-linux-x86_64.tar.gz`](https://pgrust.com/downloads/v0.4-beta/pgrust-0.4-beta-linux-x86_64.tar.gz) | any x86-64 CPU |
| [`pgrust-0.4-beta-linux-aarch64.tar.gz`](https://pgrust.com/downloads/v0.4-beta/pgrust-0.4-beta-linux-aarch64.tar.gz) | any 64-bit Arm CPU |
| [`pgrust-0.4-beta-linux-x86_64-znver3.tar.gz`](https://pgrust.com/downloads/v0.4-beta/pgrust-0.4-beta-linux-x86_64-znver3.tar.gz) | tuned for AMD Zen 3 and newer |
| [`pgrust-0.4-beta-linux-aarch64-neoverse-v2.tar.gz`](https://pgrust.com/downloads/v0.4-beta/pgrust-0.4-beta-linux-aarch64-neoverse-v2.tar.gz) | tuned for Neoverse-V2 (AWS Graviton4, NVIDIA Grace) |
| [`pgrust-0.4-beta-macos-arm64.tar.gz`](https://pgrust.com/downloads/v0.4-beta/pgrust-0.4-beta-macos-arm64.tar.gz) | macOS on Apple Silicon |
| [`pgrust-0.4-beta-macos-x86_64.tar.gz`](https://pgrust.com/downloads/v0.4-beta/pgrust-0.4-beta-macos-x86_64.tar.gz) | macOS on Intel |
| [`pgrust-0.4-beta-macos-universal.tar.gz`](https://pgrust.com/downloads/v0.4-beta/pgrust-0.4-beta-macos-universal.tar.gz) | macOS, either (both architectures in one binary) |

The Linux builds are profile-guided optimized. The tuned builds use
instructions only those CPUs (and newer ones) have; when in doubt, take the
generic build. Every tarball has a `.sha256` file next to it, and
[`MANIFEST.txt`](https://pgrust.com/downloads/v0.4-beta/MANIFEST.txt) lists
them all.

### Linux

```bash
# Pick your build: linux-x86_64, linux-aarch64, linux-x86_64-znver3 or
# linux-aarch64-neoverse-v2 (see the table above).
PLATFORM=linux-x86_64

# Download pgrust, verify the checksum and unpack it.
curl -LO "https://pgrust.com/downloads/v0.4-beta/pgrust-0.4-beta-$PLATFORM.tar.gz"
curl -LO "https://pgrust.com/downloads/v0.4-beta/pgrust-0.4-beta-$PLATFORM.tar.gz.sha256"
sha256sum -c "pgrust-0.4-beta-$PLATFORM.tar.gz.sha256"
tar xzf "pgrust-0.4-beta-$PLATFORM.tar.gz"
cd pgrust-0.4-beta-linux-*/

# Create a data directory with pgrust's initdb.
bin/initdb -D /tmp/pgrust-data --no-locale --encoding UTF8 -U postgres

# Start the server. It runs in the foreground and logs to this terminal.
ulimit -s 65520
RUST_MIN_STACK=33554432 bin/postgres \
  -D /tmp/pgrust-data \
  -k /tmp -p 5432 \
  -c listen_addresses= \
  -c io_method=sync \
  -c max_stack_depth=60000
```

Leave the server running and connect from a second terminal, in the same
directory:

```bash
bin/psql -h /tmp -p 5432 -U postgres -c "select version()"
```

You should see `PostgreSQL 18.6 (pgrust 0.4-beta, build <commit>) on ...`,
where `<commit>` is the short commit the binary was built from.
`bin/postgres --version` prints `pgrust 0.4-beta (PostgreSQL 18.6 compatible)`.

### Docker

The prebuilt image on Docker Hub,
[`malisper/pgrust`](https://hub.docker.com/r/malisper/pgrust), is a drop-in
replacement for the official `postgres` image: same env vars
(`POSTGRES_PASSWORD`, `POSTGRES_USER`, `POSTGRES_DB`, `PGDATA`, ...), same
`/docker-entrypoint-initdb.d` init scripts, same data volume at
`/var/lib/postgresql/data`. It carries the same `postgres` and `initdb`
binaries as the Linux release tarballs above, and initializes the cluster with
pgrust's own `initdb`; the only C PostgreSQL component in the image is `psql`
for the init scripts. `malisper/pgrust:v0.4-beta` is multi-arch (amd64 +
arm64); `v0.4-beta-amd64` and `v0.4-beta-arm64` pin one architecture.

```bash
docker run -d --name pgrust -e POSTGRES_PASSWORD=secret -p 5432:5432 malisper/pgrust:v0.4-beta
```

Then connect, with your own `psql` or the one in the image:

```bash
psql -h localhost -p 5432 -U postgres
docker exec -it pgrust psql -U postgres
```

`malisper/pgrust:latest` still points at v0.3; use the `v0.4-beta` tag to get
this release. A data volume written by v0.3 holding columnar tables needs the
dump and restore described under
[Stopping, restarting, cleaning up](#stopping-restarting-cleaning-up).

### macOS

```bash
# Pick your build: macos-arm64 (Apple Silicon), macos-x86_64 (Intel) or
# macos-universal (both).
PLATFORM=macos-arm64

# Download pgrust, verify the checksum and unpack it. Download with curl: curl
# does not set the quarantine flag a browser download gets, so Gatekeeper stays
# out of the way (see the note below).
curl -LO "https://pgrust.com/downloads/v0.4-beta/pgrust-0.4-beta-$PLATFORM.tar.gz"
curl -LO "https://pgrust.com/downloads/v0.4-beta/pgrust-0.4-beta-$PLATFORM.tar.gz.sha256"
shasum -a 256 -c "pgrust-0.4-beta-$PLATFORM.tar.gz.sha256"
tar xzf "pgrust-0.4-beta-$PLATFORM.tar.gz"
cd "pgrust-0.4-beta-$PLATFORM"

# Create a data directory with pgrust's initdb.
bin/initdb -D /tmp/pgrust-data --no-locale --encoding UTF8 -U postgres

# Start the server. It runs in the foreground and logs to this terminal.
ulimit -s 65520
RUST_MIN_STACK=33554432 bin/postgres \
  -D /tmp/pgrust-data \
  -k /tmp -p 5432 \
  -c listen_addresses= \
  -c io_method=sync \
  -c max_stack_depth=60000
```

Leave the server running and connect from a second terminal, in the same
directory:

```bash
bin/psql -h /tmp -p 5432 -U postgres -c "select version()"
```

**A note on Gatekeeper:** the macOS binaries are signed ad hoc, not notarized
by Apple. Downloaded with curl as above, they are never quarantined and none
of this comes up. If you download with a browser instead, macOS refuses to run
them with "Apple could not verify ... is free of malware". Clear the flag with
`xattr -dr com.apple.quarantine pgrust-0.4-beta-$PLATFORM`, or approve each
binary under System Settings > Privacy & Security > "Open Anyway".

### Columnar tables

Columnar storage (`pgrcolumnar`) is a table access method. Register it once in
each database, then create tables with `USING pgrcolumnar`:

```sql
CREATE ACCESS METHOD pgrcolumnar TYPE TABLE HANDLER heap_tableam_handler;

CREATE TABLE events (id bigint, kind text, val double precision, at timestamp)
  USING pgrcolumnar;
```

### Stopping, restarting, cleaning up

- **Stop the server** with Ctrl-C in its terminal, or `kill -INT <pid>`. That
  is Postgres' fast shutdown and it is safe.
- **Start it again** by rerunning the same server command with the same data
  directory. Your data is preserved across restarts.
- **Start over** by deleting the data directory: `rm -rf /tmp/pgrust-data`.
  This deletes all data in it.
- **If startup fails with** `FATAL: lock file "/tmp/.s.PGSQL.5432.lock"
  already exists`, another server, most likely an existing Postgres install,
  is already using port 5432. Start pgrust on a different port with `-p 5433`
  and connect with `psql -h /tmp -p 5433 -U postgres`. Whenever a regular
  Postgres is running on the same machine, check `select version()` after
  connecting; it is easy to silently connect to the wrong server and wonder
  why nothing behaves like pgrust.
- **If startup fails with** `could not open directory
  "/usr/local/pgsql/share/timezone"`, the server cannot find its share files.
  It reads them from the `share` directory beside the binary, so run
  `bin/postgres` from the unpacked tarball as is, without moving the binary
  away from `bin/share`. `PGRUST_PGSHAREDIR` and `PGRUST_TZDIR` point it
  elsewhere.
- **Columnar tables from v0.3** (`USING pgrcolumnar`) are refused with an
  error saying the table is in the v0.3.x columnar format. Dump them with the
  v0.3 binary (`pg_dump`) and restore them into v0.4-beta.

## When to use pgrust

- **Analytics on your Postgres data without a separate warehouse.** Put
  large tables in `pgrcolumnar` and query them with the same SQL, in the same
  database as your application data.
- **Speeding up slow Postgres queries** with large scans, joins and
  aggregations, such as dashboards and reporting.
- **One database for transactions and analytics**, instead of Postgres plus
  ClickHouse or a warehouse kept in sync by a pipeline.

## Status

We're working with a few early design partners and are targeting
production readiness in Q1 2027. Testing and reliability are our top
priority; see [Testing](#testing) for how we verify pgrust against
Postgres. We run pgrust ourselves internally today.

## Extensions

pgrust ships the most-used Postgres extensions built in: pgvector, pg_cron,
pg_stat_statements, pg_trgm, hstore, pgcrypto, postgres_fdw and most of
contrib. Install them with `CREATE EXTENSION` as usual.

pgrust doesn't load C extension binaries compiled for Postgres, but porting an
existing extension to pgrust is straightforward, and we add new ones
regularly. If you need an extension that isn't listed,
[open an issue](https://github.com/malisper/pgrust/issues) and we'll
prioritize it. PL/Python, PL/Perl and PL/Tcl are not supported yet.

## Performance

Measured on `c8g.4xlarge` (AWS Graviton4), pgrust with pgrcolumnar, its
builtin columnar layout, runs ClickBench's analytical queries up to 1,600x
faster than PostgreSQL (per query, cold runs, against PostgreSQL's published
results on the same instance type).

For a code-level walkthrough of one part of that speedup, read
[how batching, operator fusion, and SIMD speed up the query engine](https://malisper.me/how-we-made-postgres-hundreds-of-times-faster-the-query-engine/).

## Writing

- [How AI Changes the Economics of JIT Compilers](https://malisper.me/how-ai-changes-the-economics-of-jit-compilers/):
  building a copy-and-patch ARM64 JIT in Rust.
- [pgrust: Rebuilding Postgres in Rust with AI](https://malisper.me/pgrust-rebuilding-postgres-in-rust-with-ai/):
  why we started the project.

## Testing

The most common question we get about pgrust is: how can anyone trust it?
Passing a test suite doesn't mean our code is correct.
Getting to the point where people can trust pgrust is our number one priority
today.

To achieve that goal, we're taking several different approaches:

1. Of the 3,400 user facing Postgres functions, we've formally verified
   that 1,397 have identical behavior in pgrust with
   [Kani](https://github.com/model-checking/kani); 466 more are tested
   against Postgres. The ledger is
   [`proofs/USER_FACING_FUNCTIONS.tsv`](proofs/USER_FACING_FUNCTIONS.tsv).
2. We're engaging with [Antithesis](https://antithesis.com) to do simulation
   testing of pgrust to battle test pgrust.
3. We're doing aggressive differential fuzz testing to ensure pgrust's
   behavior is identical to Postgres's.

## Unsafe Code

pgrust uses unsafe code, but only for the specific things that need it.
Postgres represents every internal value as a Datum, an untyped 8 byte
value, and Rust has no safe equivalent, so the code that packs and unpacks
internal values is unsafe. The same goes for the places where pgrust must
match Postgres' memory layouts byte for byte.

If there are particular unsafes that are not necessary and removing them
would not have an impact on performance, let us know, as we would love to
remove more unsafes.

## Building from source

You need the Rust toolchain (any [rustup](https://rustup.rs) install works;
`rust-toolchain.toml` pins the build to Rust 1.96.0 and rustup fetches it
automatically) and the RE2 regex library:

- Debian/Ubuntu: `sudo apt-get install -y build-essential pkg-config libre2-dev`
- macOS: `brew install re2 pkg-config`

```bash
cargo build --release --locked -p main_main -p initdb
```

`select version()` names the build: a build in a git checkout prints its short
commit (`PostgreSQL 18.6 (pgrust 0.4-beta, build 1a2b3c4) on ...`); set
`PGRUST_BUILD_ID` at build time to choose the id, or build outside git for none.
`postgres --version` prints `pgrust 0.4-beta (PostgreSQL 18.6 compatible)`;
`postgres -V` prints `postgres (PostgreSQL) 18.6`, the line PostgreSQL's own
`pg_ctl`, `initdb` and `pg_upgrade` check.

The server binary lands at `target/release/postgres`, pgrust's `initdb` beside
it, and the share tree it needs (`postgres.bki`, the setup SQL, `timezone`,
`timezonesets`, tsearch data, extension files) is staged at `target/release/share/`. No C
PostgreSQL is involved in creating or running a cluster:

```bash
target/release/initdb -D /tmp/pgrust-data --no-locale --encoding UTF8 -U postgres
ulimit -s 65520
RUST_MIN_STACK=33554432 target/release/postgres -D /tmp/pgrust-data \
  -k /tmp -p 5432 -c listen_addresses= -c io_method=sync -c max_stack_depth=60000
```

The time zone database is PostgreSQL 18.6's own compiled tzdata, read from
`share/timezone` beside the binary (never the host `zoneinfo`, like C built
without `--with-system-tzdata`). To use the system zoneinfo instead, build with
`PGRUST_SYSTEM_TZDATA=/usr/share/zoneinfo` (C's `--with-system-tzdata`);
`PGRUST_TZDIR` and `PGRUST_PGSHAREDIR` remain runtime overrides, not
requirements. A data directory made
by stock PostgreSQL 18 `initdb` still works too (same on-disk format).
Release builds refuse to compile without RE2 on purpose: a
build with only the fallback regex engine is drastically slower on regex
heavy workloads. The binaries we publish additionally use `--profile dist`,
which turns on fat LTO; it produces a faster binary but takes much longer to
compile.

## What's new in v0.4-beta

- **Gravity, a new query engine.** Gravity runs every query and every kind
  of statement (DML with ON CONFLICT and RETURNING, triggers, row locks,
  cursors, PL/pgSQL and SPI, COPY) with PostgreSQL's semantics, on columnar
  tables and on the ordinary row store alike. It replaces our previous
  engine and runs analytical queries as much as 3x faster than ClickHouse. Its JIT
  was rewritten around a new low-level IR: it compiles each plan to AArch64
  or x86-64 machine code, and to WebAssembly in the browser build, or
  interprets it when compiling would not pay off.
- **pgrcolumnar grows up.** `pgrcolumnar` stores compressed, immutable parts
  with zone maps, delete vectors and background compaction. Reads see
  PostgreSQL MVCC snapshots, and writes are transactional: they roll back
  with the transaction. Small inserts land in a row-store front that is
  folded into columnar parts in the background, and UPDATE and DELETE mark
  rows in delete vectors instead of rewriting parts (ON CONFLICT and MERGE
  are not supported on columnar tables yet). Columnar tables take
  non-unique B-tree, hash, GiST, SP-GiST and GIN indexes, are written to
  WAL, so they survive crashes and replicate to standbys, and support row
  locks and a `sorted_by` table option.
- **One memory budget** covers the buffer pool, the columnar caches and
  running queries. Near the memory limit pgrust cancels the statement holding
  the most memory instead of letting the OS kill the server.
- **jsonb statistics.** `ANALYZE` collects statistics on the keys and values
  inside `jsonb` columns, and the planner uses them for `->`, `->>`, `#>`,
  `#>>` and `?`, `?|`, `?&`, so it estimates jsonb filters instead of
  guessing. On by default (`pgrust.jsonb_path_stats`).
- **The condition cache.** Filter results on columnar tables are reused
  across statements over the same immutable data. `full` is the default
  (also spelled `on`) and stores match counts too. `rows` keeps row
  selections and per-dictionary-code filter tables without counts;
  `granule` keeps only empty-granule verdicts; `off` disables predicate
  reuse. `pgrust.dict_filter_cache` and `pgrust.zone_store` turn off the
  per-dictionary-code filter tables and the zone-map result store on their
  own. Sizing feedback, data caches, and startup prewarm have separate
  controls.
- **pgrust's own `initdb`**, and `pg_cron` 1.6 is included.
- **Upgrading:** columnar tables from v0.3 must be dumped and restored. UTF8 is
  the only server encoding.

v0.4-beta is a beta: it shows what is coming in v0.4 while we finish
stabilizing the rest.

## Developing in the open

Up to v0.3 we developed pgrust privately and published it in large monthly
releases. Starting with v0.4-beta, development happens in this repository:
features, optimizations and bug fixes are pushed here as soon as they are
ready, instead of being held back for the next release.

## Implementation

**Executor**

- Gravity, a vectorized, push-based executor that compiles plans to machine
  code (AArch64 and x86-64) with a JIT that emits code directly.

**Parallelism**

- Threads instead of processes, for both connections and parallel query.
- A query scheduler, new to Postgres. Queries get a priority, and
  long-running queries have theirs lowered, so heavy queries interfere less
  with fast ones.
- Parallel query rebuilt around work stealing: idle threads are assigned
  dynamically to speed up queries already in flight.

**Storage**

- `pgrcolumnar`, a column-oriented format with dictionary encoding and
  several other compression schemes, WAL-logged.
- Pipelined `fsync`. A worker releases its locks after calling `fsync` but
  before it completes. This is safe because the query is not acknowledged,
  and none of its effects are observable, until the `fsync` finishes.

**Other**

- The automatic memory manager. Near the machine's memory limit, pgrust
  cancels the statement holding the most memory, so that one query dies and
  the server itself can keep running.

## Roadmap

Next we're working on testing and battle testing pgrust and making it so
people can trust pgrust with their data.

Beyond that we want to add:

- Instant forking
- Autoscaling including scale to zero
- Planner support for JSON
- Undo log support for a no-vacuum design
- Adaptive planner to prevent bad plans
- A mini version of pgrust designed for testing and being embedded

## Contributing

We are not actively accepting pull requests right now. If you want to help
out with the project, reach out on [Discord](https://discord.gg/FZZ4dbdvwU).

Please do still open an issue if something breaks, if setup is confusing, or
if there is a Postgres behavior you want matched.

## License

pgrust is licensed under [AGPL-3.0](LICENSE). Portions derived from
PostgreSQL remain under the PostgreSQL License. The JIT's code generator
(`crates/tpde/`) is ported from TPDE (Apache-2.0 WITH LLVM-exception) and
Fadec and Disarm (BSD-3-Clause). See [`NOTICE`](NOTICE).

Acknowledgements: the research and other systems whose ideas pgrust uses are
credited in [`ACKNOWLEDGEMENTS.md`](ACKNOWLEDGEMENTS.md).

## Contact

- Discord: <https://discord.gg/FZZ4dbdvwU>
- Email: maintainers@pgrust.com
- Twitter/X: [@pgrustdb](https://x.com/pgrustdb)
- Updates: <https://pgrust.com/#updates>
