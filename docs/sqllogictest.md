# sqllogictest Against pgrust

This repo now ships a runner for external `sqllogictest` corpora against
pgrust over pgwire.

The goal is different from `scripts/run_regression.sh`:

- `run_regression.sh` measures compatibility against PostgreSQL's own `.sql`
  suite.
- `run_sqllogictest.sh` measures breadth against third-party `.slt` corpora
  that can be pointed at any PostgreSQL-compatible endpoint.

This is the first external-suite path to wire up because it gives a large
amount of SQL-behaviour coverage per hour of effort.

## Prerequisites

You need:

1. `psql` available on `PATH` for the readiness probe.
2. Either `sqllogictest` installed:

```bash
cargo install sqllogictest-bin
```

3. Or a local checkout of `sqllogictest-rs`:

```bash
git clone https://github.com/risinglightdb/sqllogictest-rs ../sqllogictest-rs
```

The runner discovers the source checkout automatically from common sibling
paths, or you can point it at one explicitly with `--sqllogictest-dir`.

## Usage

Run a local corpus:

```bash
scripts/run_sqllogictest.sh --files './path/to/**/*.slt'
```

Run everything under a suite root:

```bash
scripts/run_sqllogictest.sh --suite-dir ../sqllogictest-rs/tests/slt
```

List the built-in presets:

```bash
scripts/run_sqllogictest.sh --list-presets
```

Run the first real upstream Postgres baseline:

```bash
scripts/run_sqllogictest.sh \
  --sqllogictest-dir ../sqllogictest-rs \
  --preset upstream-postgres-simple
```

Run the broader upstream Postgres engine coverage file:

```bash
scripts/run_sqllogictest.sh \
  --sqllogictest-dir ../sqllogictest-rs \
  --preset upstream-postgres-extended
```

Use an explicit sqllogictest checkout and a skip regex:

```bash
scripts/run_sqllogictest.sh \
  --sqllogictest-dir ../sqllogictest-rs \
  --suite-dir ../sqllogictest-rs/tests/slt \
  --skip 'postgres_extended|mysql'
```

The script starts a fresh pgrust server on port `5435` by default, writes logs
to a unique `/tmp/pgrust_sqllogictest_results.*` directory, and emits JUnit XML
under `junit/`.

Useful flags:

- `--skip-build`: reuse the existing `pgrust_server` binary.
- `--skip-server`: point at an already-running pgrust instance.
- `--jobs N`: let sqllogictest create one temporary database per file and run
  files in parallel. Start with serial mode until the corpus is known to be
  database-isolated and `CREATE DATABASE` is solid on the target branch.
- `--label NAME`: pass extra `onlyif` / `skipif` labels to the runner.
- `--skip REGEX`: filter out known-bad files or directories while bootstrapping.
- `--skip-file PATH`: apply a checked-in list of regex filters, one per line.
- `--override`: ask `sqllogictest` to rewrite expected output from the target
  database. Use this for controlled oracle-materialization steps, not normal
  pgrust regression checks.

## Harness Self-Test

Before pointing the runner at a larger corpus, you can validate the harness
itself against the upstream sqllogictest-rs fixtures:

```bash
scripts/run_sqllogictest.sh \
  --sqllogictest-dir ../sqllogictest-rs \
  --files '../sqllogictest-rs/tests/slt/**/*.slt'
```

Expect plenty of failures there. Many of those files are runner/fake-database
tests rather than a real PostgreSQL compatibility corpus. The immediate value is:

- a stable pass/fail number
- repro logs per run
- a path to introduce a skiplist instead of manually curated one-off tests

## Preset Baseline

The current best built-in baseline is:

- `upstream-postgres-simple`: passes on the current branch and validates the
  end-to-end harness against a real Postgres SQL test file.

The first intentionally failing comparison point is:

- `upstream-postgres-extended`: currently fails on float array text formatting
  (`Infinity` / `-Infinity` expected vs `inf` / `-inf` actual), which makes it
  a useful compatibility canary as type I/O changes land.

## Skip Files

For larger corpora, prefer checked-in skip files over ad hoc shell history:

```bash
scripts/run_sqllogictest.sh \
  --suite-dir ../some-corpus \
  --skip-file scripts/sqllogictest-skips/example.regex
```

Each non-comment line is passed through as a `--skip` regex to
`sqllogictest`.

## Current Shape

This runner is intentionally generic:

- it does not vendor any corpus into the pgrust repo
- it does not hardcode CockroachDB or SQLite-specific fixtures
- it treats corpora as external inputs so we can compare multiple sources with
  the same harness

The next likely step is choosing a larger weekly baseline corpus now that the
generic runner, presets, and skip-file support exist.
