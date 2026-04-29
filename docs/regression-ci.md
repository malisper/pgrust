# Regression CI

`regression-tests.yml` runs the PostgreSQL regression suite against
`pgrust_server` twice daily and on manual dispatch.

## Workflow

- Workflow: `.github/workflows/regression-tests.yml`
- Normal cadence: twice daily, with cron times in UTC.
- Runner: the org `32x` larger-runner label. This maps to a GitHub-hosted
  Ubuntu runner with 32 cores, 128 GB RAM, and 1.2 TB storage. The full suite
  runs multiple isolated `pgrust_server` processes and staged database-cluster
  copies, which can starve standard private-repo runners.
- Manual dispatch inputs:
  - `command_timeout`: wall-clock watchdog for each shard command. Normal runs
    use `75m`; short values are useful for abort-path smoke tests.
  - `jobs`: number of concurrent isolated regression workers per shard. Normal
    runs use `4`.
  - `shard_deadline_secs`: self-owned per-shard scheduling deadline. Normal runs
    use `3000` seconds, leaving time for summary/artifact upload before any
    GitHub job timeout can kill the process tree.
- Timeouts: CI runs use a `15s` SQL `statement_timeout` and a `300s`
  per-file wall-clock timeout. Base-cluster dependency setup also has its own
  longer setup timeout in `scripts/run_regression.sh`.
- PostgreSQL source is checked out at the pinned stable PG18 release tag in
  `POSTGRES_REF` (`REL_18_3` as of this writing). Bump this only when the team
  decides to move to a newer stable PG18 release corpus.

The workflow builds `pgrust_server` once, uploads it as a short-lived artifact,
runs four schedule-group shards, uploads one compact artifact bundle per shard,
aggregates all available shard artifacts, publishes full text results to the
`regression-history` orphan branch, and posts a Slack summary.

Each shard step is intentionally `continue-on-error`. The runner also owns a
deadline inside `scripts/run_regression.sh`: after the deadline, it stops
scheduling new files, marks unstarted files as timed out, writes a partial
`summary.json`, and exits cleanly. This is more reliable than depending on
GitHub to run later `always()` steps after a hard job cancellation.

The aggregate job uses whatever shard artifacts are present. Missing or
deadline-limited shards produce a `partial` run instead of producing no history
or Slack notification.

## Results

`scripts/run_regression.sh` writes:

- `summary.json`: machine-readable status, test counts, and query-block counts.
- `summary.md`: human-readable summary generated during history publishing.
- `output/`: actual `psql -a -q` output per test.
- `diff/`: best expected-vs-actual diff per failed test.
- `status/`: per-test status records used to aggregate parallel results.
- `fixtures/`: rewritten SQL/expected fixtures used by pgrust compatibility
  shims.
- `base/`: staged isolated base clusters.
- `workers/`: isolated worker database state.
- `tablespaces/`: tablespace state.

The Actions artifact intentionally uploads only the compact text/debug bundle:
summary files, exit code, `output/`, `diff/`, `status/`, and `fixtures/`.
It excludes `base/`, `workers/`, and `tablespaces/` because those are large
database directories used only to isolate in-run workers.

Shard artifacts are named `regression-shard-0` through `regression-shard-3`.
The aggregate artifact is named `regression-results` and has the same compact
layout expected by `regression-history`.

`regression-history` stores the text results under:

- `runs/<timestamp>/`
- `runs/latest/`
- `index.tsv`

Slack links to both the timestamped run and `runs/latest`.

## Isolation And Dependencies

PostgreSQL's `parallel_schedule` is stateful across schedule groups: later files
can depend on objects created by earlier files. At the same time, pgrust's CI
uses isolated parallelism so concurrent files do not share one `pgrust_server`.

With `--jobs > 1` in managed-server mode, each concurrent test file gets its own:

- `pgrust_server` process
- TCP port
- data directory
- tablespace directory
- cloned base-cluster state

The isolation boundary is the test file, not the SQL statement. Ordinary SQL
errors inside a file do not stop `psql`, so later statements in that same file
can be affected by earlier failed setup. That is intentional: PostgreSQL
regression files are mini-scenarios, and splitting them into independent
statements would change the coverage.

Before running isolated workers, the script stages reusable base clusters:

- `test_setup`: bootstrap fixture state only.
- `post_create_index`: a copy of `test_setup` after running `create_index.sql`.

Tests before the `create_index` schedule group clone the `test_setup` base.
Tests after that group clone the `post_create_index` base. This preserves
PostgreSQL's broad `create_index` setup assumption without replaying
`create_index.sql` before every later test.

For explicit named cross-file dependencies documented by PostgreSQL's schedule,
isolated workers replay the prerequisite SQL files in the same worker database
before running the dependent file. For example, `aggregates.sql` replays
`create_aggregate.sql` first in the same isolated worker.

When PostgreSQL adds or changes schedule dependency comments, update
`direct_test_dependencies` in `scripts/run_regression.sh` so isolated workers
keep matching the upstream schedule assumptions.

## Query Match Rate

The `Queries: matched/total` number is a heuristic compatibility signal, not a
PostgreSQL-native pass rate.

The script parses each regression `.sql` file into statement-like blocks, splits
expected and actual `psql -a` output by echoed statement text, normalizes
whitespace, and counts blocks whose output matches exactly.

The denominator can change if:

- the pinned PostgreSQL checkout changes;
- the schedule changes;
- the SQL corpus changes; or
- the statement-block parser changes.

For the previous pinned PG18 corpus, the first full isolated CI baseline was:

- `22/249` tests passed.
- `25,932/55,333` query blocks matched (`46.87%`).
