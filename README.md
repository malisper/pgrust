<h1 align="center">pgrust</h1>

<p align="center">
  <strong>A Rust rewrite of Postgres that's faster than Postgres and ClickHouse.</strong>
</p>

<p align="center">
  <img alt="Postgres 18.3" src="https://img.shields.io/badge/Postgres-18.3-336791">
  <img alt="Regression suite: 100%" src="https://img.shields.io/badge/regression_suite-46%2C066%2F46%2C066-brightgreen">
  <img alt="Version: v0.2" src="https://img.shields.io/badge/version-v0.2-blue">
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

pgrust is a re-implementation of Postgres meant to show what Postgres would
look like if it was built in 2026. It is wire compatible and even SQL dialect
compatible with Postgres. It passes
[all 46,066 tests in Postgres' regression suite](https://malisper.me/pgrust-passes-100-of-postgresqls-regression-tests/).
For the story of why we're building it, see
[pgrust: rebuilding Postgres in Rust with AI](https://malisper.me/pgrust-rebuilding-postgres-in-rust-with-ai/).

Every line is Rust, written to match the behavior of the C implementation.
Rust makes it easy to re-architect several core Postgres pieces. pgrust has:

- A new vectorized push-based, JIT compiled executor
- A thread based concurrency model
- A query scheduler designed to keep any individual query from taking down
  your database
- A built-in OOM killer that gives pgrust control of what happens when you're
  running low on memory, greatly reducing the chance of your whole DB being
  taken down by the OS OOM killer

and many other really awesome pieces. The scheduler and the OOM killer go
after two of
[the four horsemen behind thousands of Postgres outages](https://malisper.me/the-four-horsemen-behind-thousands-of-postgres-outages/).
See [What's new in v0.2](#whats-new-in-v02) for more.

## Status

pgrust is not production ready. Do not put data you care about in it.

pgrust currently passes the Postgres regression suite. It's faster than
Postgres and ClickHouse, but it still has a lot of bugs. Our #1 priority right
now is testing and reliability.

If you need production ready Postgres today, use Postgres.

Existing PostgreSQL extensions do not work. There is no stable extension ABI
in pgrust yet. Some bundled contrib modules are ported, but PL/Python,
PL/Perl, and PL/Tcl are not.

pgrust is specifically tuned for Graviton4 and the JIT compiler only targets
Graviton4. pgrust will still work on other platforms, but will not have
similar performance.

## Performance

Measured on `c8g.4xlarge` (AWS Graviton4) against PostgreSQL 18.3.

On the ClickBench combined score, pgrust scored 18.5% faster than ClickHouse
and hundreds of times faster than PostgreSQL. This was using pgrcolumnar,
pgrust's builtin columnar layout.

On sysbench-oltp, pgrust achieved 30% higher throughput than Postgres 18.3 on
read-only workloads at 300GB scale.

These runs were reviewed independently by **Greg Smith**, author of
*PostgreSQL 9.0 High Performance*.

Two honest caveats. We had previously reported that pgrust was over 50%
faster than Postgres. On Kubernetes we measure a larger OLTP gap, 50-60%
rather than 30%, and we have not isolated why the same binaries behave
differently there than on bare EC2, so we quote the lower number. Second, the
binaries we publish are generic for their architecture. The benchmark numbers
come from builds tuned for Graviton4 (`-Ctarget-cpu=neoverse-v2`), so you
will not reproduce them exactly from a download.

Benchmarks and durability settings are unchanged from a default install:
`fsync` is on. The harnesses are in [`benchmarks/`](benchmarks/) so you can
run them yourself.

## Testing

The most common question we get about pgrust is: how can anyone trust it?
Just because we passed the test suite doesn't mean our code is correct.
Getting to the point where people can trust pgrust is our number one priority
today. (Getting the regression suite to pass at all took three failed
attempts; we wrote up the dead ends in
[Postgres in Rust: getting the regression suite to pass](https://malisper.me/postgres-in-rust-regression-suite/).)

To achieve that goal, we're taking several different approaches:

1. Of the 3000 user facing Postgres functions, we've been able to formally
   verify 1000 of them have identical behavior in pgrust with
   [Kani](https://github.com/model-checking/kani). In the process of doing so
   we found 12 divergences between pgrust and Postgres. Four of those
   were bugs in Postgres itself. See [`proofs/`](proofs/).
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

## Quickstart

**In your browser:** <https://pgrust.com>, a full pgrust server compiled to
WebAssembly, no install.

**On your machine:** pgrust does not ship its own `initdb` or `psql` yet, so
each flow below installs the PostgreSQL 18 client tools first, then downloads
pgrust, then initializes and starts a database. Paste the blocks top to
bottom.

### macOS (Apple Silicon)

```bash
# PostgreSQL 18 client tools (initdb, psql). The formula is keg-only, so
# Homebrew does not put it on your PATH; the export is required.
brew install postgresql@18
export PATH="$(brew --prefix postgresql@18)/bin:$PATH"

# Download pgrust and verify the checksum. Download with curl: curl does not
# set the quarantine flag a browser download gets, so Gatekeeper stays out of
# the way (see the note below).
curl -LO https://pgrust.com/downloads/v0.2/pgrust-0.2-macos-arm64
curl -LO https://pgrust.com/downloads/v0.2/pgrust-0.2-macos-arm64.sha256
shasum -a 256 -c pgrust-0.2-macos-arm64.sha256
chmod +x pgrust-0.2-macos-arm64

# Create a data directory using Postgres' initdb.
initdb -D /tmp/pgrust-data --no-locale --encoding UTF8 -U postgres

# pgrust reads the timezone database and other data files from a Postgres
# share directory at runtime. Without these two variables it looks in
# /usr/local/pgsql/share and refuses to start.
export PGRUST_PGSHAREDIR="$(brew --prefix postgresql@18)/share/postgresql"
export PGRUST_TZDIR="$PGRUST_PGSHAREDIR/timezone"

# Start the server. It runs in the foreground and logs to this terminal.
ulimit -s 65520
RUST_MIN_STACK=33554432 ./pgrust-0.2-macos-arm64 \
  -D /tmp/pgrust-data \
  -k /tmp -p 5432 \
  -c listen_addresses= \
  -c io_method=sync \
  -c max_stack_depth=60000
```

Leave the server running and connect from a second terminal:

```bash
export PATH="$(brew --prefix postgresql@18)/bin:$PATH"
psql -h /tmp -p 5432 -U postgres -c "select version()"
```

You should see `pgrust 0.2 (PostgreSQL 18.3 compatible)`.

**macOS Intel:** the same flow works with `pgrust-0.2-macos-x86_64` in place
of `pgrust-0.2-macos-arm64` (`brew --prefix` handles the different Homebrew
prefix). There is also `pgrust-0.2-macos-universal`, a universal binary
covering both.

**A note on Gatekeeper:** the binaries are not yet notarized by Apple. If you
download with curl as above, the file is never quarantined and none of this
comes up. If you download with a browser instead, macOS will refuse to run
the binary with "Apple could not verify ... is free of malware". Clear the
quarantine flag with `xattr -d com.apple.quarantine pgrust-0.2-macos-arm64`,
or approve the binary under System Settings > Privacy & Security > "Open
Anyway".

### Linux (Debian and Ubuntu)

```bash
# Pick your architecture.
PLATFORM=linux-x86_64    # or: linux-aarch64

# PostgreSQL 18 client tools (initdb, psql) from the PGDG apt repo. On
# Debian and Ubuntu, initdb ships in the server package, postgresql-18, and
# is not placed on PATH, hence the export.
sudo apt-get update
sudo apt-get install -y curl ca-certificates gnupg
sudo install -d /usr/share/postgresql-common/pgdg
sudo curl -fsSL -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
  https://www.postgresql.org/media/keys/ACCC4CF8.asc
echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] http://apt.postgresql.org/pub/repos/apt $(. /etc/os-release && echo $VERSION_CODENAME)-pgdg main" \
  | sudo tee /etc/apt/sources.list.d/pgdg.list
sudo apt-get update
sudo apt-get install -y postgresql-18 postgresql-client-18
export PATH="/usr/lib/postgresql/18/bin:$PATH"

# Download pgrust and verify the checksum.
curl -LO "https://pgrust.com/downloads/v0.2/pgrust-0.2-$PLATFORM"
curl -LO "https://pgrust.com/downloads/v0.2/pgrust-0.2-$PLATFORM.sha256"
sha256sum -c "pgrust-0.2-$PLATFORM.sha256"
chmod +x "pgrust-0.2-$PLATFORM"

# Create a data directory using Postgres' initdb.
initdb -D /tmp/pgrust-data --no-locale --encoding UTF8 -U postgres

# pgrust reads the timezone database and other data files from a Postgres
# share directory at runtime. Without these two variables it looks in
# /usr/local/pgsql/share and refuses to start. Debian builds Postgres
# against the system tzdata, so PGRUST_TZDIR points at zoneinfo.
export PGRUST_PGSHAREDIR=/usr/share/postgresql/18
export PGRUST_TZDIR=/usr/share/zoneinfo

# Start the server. It runs in the foreground and logs to this terminal.
ulimit -s 65520
RUST_MIN_STACK=33554432 "./pgrust-0.2-$PLATFORM" \
  -D /tmp/pgrust-data \
  -k /tmp -p 5432 \
  -c listen_addresses= \
  -c io_method=sync \
  -c max_stack_depth=60000
```

Leave the server running and connect from a second terminal:

```bash
psql -h /tmp -p 5432 -U postgres -c "select version()"
```

You should see `pgrust 0.2 (PostgreSQL 18.3 compatible)`.

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
  "/usr/local/pgsql/share/timezone"`, the `PGRUST_PGSHAREDIR` and
  `PGRUST_TZDIR` variables from the quickstart are not set in the shell
  running the server.

## Building from source

You need the Rust toolchain (any [rustup](https://rustup.rs) install works;
`rust-toolchain.toml` pins the build to Rust 1.96.0 and rustup fetches it
automatically) and the RE2 regex library:

- Debian/Ubuntu: `sudo apt-get install -y build-essential pkg-config libre2-dev`
- macOS: `brew install re2 pkg-config`

```bash
cargo build --release --locked --bin postgres
```

The server binary lands at `target/release/postgres`. Run it exactly like the
downloaded binaries in the quickstart, including `PGRUST_PGSHAREDIR` and
`PGRUST_TZDIR`. Release builds refuse to compile without RE2 on purpose: a
build with only the fallback regex engine is drastically slower on regex
heavy workloads. The binaries we publish additionally use `--profile dist`,
which turns on fat LTO; it produces a faster binary but takes much longer to
compile.

## Docker

The prebuilt image on Docker Hub is a drop-in replacement for the official
`postgres` image: same env vars (`POSTGRES_PASSWORD`, `POSTGRES_USER`,
`POSTGRES_DB`, `PGDATA`, ...), same `/docker-entrypoint-initdb.d` init
scripts, same data volume at `/var/lib/postgresql/data`. Multi-arch
(amd64 + arm64):

```bash
docker run -d --name pgrust -e POSTGRES_PASSWORD=secret -p 5432:5432 malisper/pgrust:v0.2
```

Then connect:

```bash
psql -h localhost -p 5432 -U postgres
```

To build the image from source instead, the repo's
[`Dockerfile`](Dockerfile) uses BuildKit cache mounts, so build it with buildx
(the default on current Docker; on older installs run
`DOCKER_BUILDKIT=1 docker build` instead):

```bash
docker buildx build --load -t pgrust .
```

## What's new in v0.2

**Executor**

- A new vectorized, push-based executor with JIT compilation.
- A JIT that emits machine code directly, cutting compile time from ~50ms to
  ~5µs. It currently targets neoverse-v2 (Graviton4) only.
- A cache-optimized hash table that adapts its strategy to maximize L1/L2
  hits. Used for aggregations today.

**Parallelism**

- Threads instead of processes, for both connections and parallel query.
- A query scheduler, new to Postgres. Queries get a priority, and
  long-running queries have theirs lowered, so heavy queries interfere less
  with fast ones.
- Parallel query rebuilt around work stealing: idle threads are assigned
  dynamically to speed up queries already in flight.

**Storage**

- `pgrcolumnar`, a column-oriented format with dictionary encoding and
  several other compression schemes.
- Pipelined `fsync`. A worker releases its locks after calling `fsync` but
  before it completes. This is safe because the query is not acknowledged,
  and none of its effects are observable, until the `fsync` finishes. It
  speeds up highly contended updates by 30-50x.

**Other**

- A built-in OOM killer. Near the machine's memory limit, pgrust picks a
  worker to kill, so that one query dies and the server itself can keep
  running.
- Executable layout optimized with PGO

## Roadmap

v0.2 proves we can achieve better performance than Postgres and ClickHouse
while maintaining identical behavior to Postgres. Next we're working on
testing and battle testing pgrust and making it so people can trust pgrust
with their data.

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
PostgreSQL remain under the PostgreSQL License; see [`NOTICE`](NOTICE).

## Contact

- Discord: <https://discord.gg/FZZ4dbdvwU>
- Email: maintainers@pgrust.com
- Updates: <https://pgrust.com/#updates>
