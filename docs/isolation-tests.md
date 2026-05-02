# PostgreSQL Isolation Tests

This repo ships a runner for upstream PostgreSQL's concurrency test suite
(`src/test/isolation/specs/*.spec`) against pgrust. The suite tests
concurrent-transaction behaviour — row locking, SSI write skew, deadlock
detection, FK contention, etc. — things the single-connection `.sql`
regression suite can't exercise.

The runner is **manual-only**. It is not part of CI. Run it when you're
working on lock manager, MVCC, or transaction code.

## Status

Available for manual runs. The upstream `isolationtester` binary requires two
pgrust features that are now implemented:

1. `pg_locks` rows with `granted = false` while a session is blocked
2. `pg_catalog.pg_isolation_test_session_is_blocked(int, int[]) RETURNS bool`

`scripts/run_isolation.sh` keeps `ISOLATION_REQUIRES_PG_LOCKS=0`, so no
override is needed for normal pgrust runs.

## Prerequisites

One-time install of the PG build deps (used to build `isolationtester`):

```bash
# macOS
brew install meson ninja bison flex

# Ubuntu / Debian
sudo apt install meson ninja-build bison flex build-essential python3 pkg-config
```

A PostgreSQL source checkout is also required. The runner looks for it, in
order, at:

1. `$PGRUST_POSTGRES_DIR`
2. `$REPO_ROOT/postgres` (sibling to pgrust — the default)
3. `$HOME/postgres`, `$HOME/src/postgres`, `$HOME/dev/postgres`

If none exists:

```bash
git clone --depth 1 https://github.com/postgres/postgres.git ../postgres
```

Or point at an existing checkout:

```bash
export PGRUST_POSTGRES_DIR=/path/to/postgres
```

## Usage

```bash
# Full schedule (everything in src/test/isolation/isolation_schedule)
scripts/run_isolation.sh

# One spec
scripts/run_isolation.sh --test fk-deadlock

# Custom schedule file
scripts/run_isolation.sh --schedule path/to/schedule

# Skip pgrust rebuild between iterations
scripts/run_isolation.sh --skip-build
```

Key flags: `--port` (default 5434), `--timeout` (per-step wait, seconds),
`--skip-build`, `--skip-server`, `--test`, `--schedule`.

First run also builds `isolationtester` from the postgres/ source — takes
1–3 minutes. Subsequent runs reuse the cached binary (meson detects no
changes). To force rebuild after a `git pull` in postgres/:

```bash
scripts/build_pg_isolation_tools.sh --force
```

## Output

Results go to a unique `/tmp/pgrust_isolation_results.*` directory. Per-spec
output lives in `$RESULTS/output/<name>.out`, diffs on failure in
`$RESULTS/diff/<name>.diff`. The summary line at the end prints pass / fail
/ skip / missing counts.

## How it works

Pattern mirrors `scripts/run_regression.sh`:

1. `build_pg_isolation_tools.sh` — builds upstream's `isolationtester` C
   binary from the postgres/ source using meson. Target-only build; no PG
   server is compiled, only libpq + fe_utils + isolationtester.
2. `run_isolation.sh` — starts a fresh pgrust cluster on port 5434, then
   invokes `isolationtester "conninfo" < spec > result` per spec in the
   schedule, diffs against `expected/<name>.out`.

`isolationtester` is the upstream multi-connection libpq client. We skip
upstream's `pg_isolation_regress` driver and handle schedule / diffing
ourselves, same as `run_regression.sh` does with `psql` for `.sql` tests.

## macOS dynamic-linking note

`isolationtester` links `@rpath/libpq.5.dylib`. The runner exports
`DYLD_LIBRARY_PATH=$POSTGRES_DIR/build/src/interfaces/libpq` so it picks up
the libpq we just built, not a system one. On Linux the same thing happens
via `LD_LIBRARY_PATH`.

## Iterating on the harness against another server

To point the harness at a real PostgreSQL instance instead of pgrust:

```bash
scripts/run_isolation.sh --skip-server --port 5432
```
