# Regression CI

`regression-tests.yml` runs the PostgreSQL regression suite against
`pgrust_server` twice daily and on manual dispatch.

## Workflow

- Workflow: `.github/workflows/regression-tests.yml`
- Normal cadence: twice daily, with cron times in UTC.
- Manual dispatch inputs:
  - `command_timeout`: wall-clock timeout for the whole regression command.
    Normal runs use `210m`; short values are useful for abort-path smoke tests.
  - `jobs`: number of concurrent isolated regression workers. Normal runs use
    `4`.
- PostgreSQL source is checked out at the pinned PG18 SHA in `POSTGRES_REF`.
  Bump this only when the team decides to track a newer upstream corpus.

The workflow builds `pgrust_server`, runs `scripts/run_regression.sh`, uploads a
compact artifact bundle, publishes full text results to the `regression-history`
orphan branch, posts a Slack summary, then fails or passes from the recorded
regression exit code.

The regression step is intentionally `continue-on-error`. It records
`exit_code.txt` and `summary.json`, then exits cleanly so artifact upload,
history publishing, Slack notification, and the final gate still run after
regression failures, timeouts, or catchable termination signals.

## Results

`scripts/run_regression.sh` writes:

- `summary.json`: machine-readable status, test counts, and query-block counts.
- `summary.md`: human-readable summary generated during history publishing.
- `output/`: actual `psql -a -q` output per test.
- `diff/`: best expected-vs-actual diff per failed test.
- `status/`: per-test status records used to aggregate parallel results.
- `fixtures/`: rewritten SQL/expected fixtures used by pgrust compatibility
  shims.
- `workers/`: isolated worker database state.
- `tablespaces/`: tablespace state.

The Actions artifact intentionally uploads only the compact text/debug bundle:
summary files, exit code, `output/`, `diff/`, `status/`, and `fixtures/`.
It excludes `workers/` and `tablespaces/` because those are large database
directories used only to isolate in-run workers.

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
- bootstrap setup output

The isolation boundary is the test file, not the SQL statement. Ordinary SQL
errors inside a file do not stop `psql`, so later statements in that same file
can be affected by earlier failed setup. That is intentional: PostgreSQL
regression files are mini-scenarios, and splitting them into independent
statements would change the coverage.

For explicit named cross-file dependencies documented by PostgreSQL's schedule,
isolated workers replay the prerequisite SQL files in the same worker database
before running the dependent file. For example, `aggregates.sql` replays
`create_aggregate.sql` first in the same isolated worker.

When PostgreSQL adds or changes schedule dependency comments, update
`direct_test_dependencies` in `scripts/run_regression.sh` so isolated workers
keep matching the upstream schedule assumptions.

PostgreSQL also documents a broader dependency class: many later tests can
depend on `create_index.sql` because their output order, `EXPLAIN` output, or
catalog-visible index objects differ when those indexes are absent. The current
isolated runner does not replay `create_index.sql` for every later test because
doing that naively would add a large setup cost to most workers. If those missing
indexes become a material source of noise, prefer adding a staged base-cluster
snapshot after `create_index.sql` over replaying the whole file before every
test.

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

For the current pinned PG18 corpus, the first full isolated CI baseline was:

- `22/249` tests passed.
- `25,932/55,333` query blocks matched (`46.87%`).
