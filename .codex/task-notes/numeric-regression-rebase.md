Goal:
Rebase this workspace branch onto malisper/foreign-key-regression-2 and try the numeric regression with output under /tmp/diffs.

Key decisions:
Renamed the branch to malisper/numeric-regression-run because malisper/numeric-regression already existed.
Rebased onto local branch malisper/foreign-key-regression-2 without conflicts.
Port 5433 was already in use, so retries used alternate ports.
The apparent numeric failure was the default 60s file timeout, not a semantic diff.
The `dependencies[@]` error came from wrapping the script in macOS `/bin/bash` 3.2 via `bash -lc`; direct shebang execution avoided it in this environment.

Files touched:
.codex/task-notes/numeric-regression-rebase.md

Tests run:
scripts/run_regression.sh --test numeric --results-dir /tmp/diffs
  Failed before running numeric: port 5433 in use or isolated base startup failed.
scripts/run_regression.sh --test numeric --results-dir /tmp/diffs --port 55434
  Failed during isolated test_setup base startup: pg_constraint_conparentid_index btree extend missing-file I/O error.
scripts/run_regression.sh --test numeric --results-dir /tmp/diffs --port 55435 --jobs 1
  Reached runner but errored before executing numeric: scripts/run_regression.sh line 1633 dependencies[@] unbound variable.
  Full terminal log: /tmp/diffs/numeric_regression_jobs1.log
  Summary: /tmp/diffs/summary.json
scripts/run_regression.sh --test numeric --results-dir /tmp/pgrust-numeric-direct.ZELt3X --port 55438 --jobs 1
  Timed out at the default 60s file timeout after 1057 queries started, with 1018 matched before truncation.
  The visible cutoff was SELECT SUM(9999::numeric) FROM generate_series(1, 100000), but that query runs in about 0.18s on a fresh server.
scripts/run_regression.sh --test numeric --results-dir /tmp/pgrust-numeric-timeout300.tiuLgP --port 55440 --jobs 1 --timeout 300
  PASS: 1057/1057 queries matched.
scripts/run_regression.sh --test numeric --results-dir /tmp/pgrust-numeric-isolated.PgN1ao --port 55441 --timeout 300
  PASS through the default isolated-parallel path: 1057/1057 queries matched.
  Copied final artifacts to /tmp/diffs/numeric-isolated-pass.

Remaining:
Use --timeout 300 for numeric on this branch. If the script must be invoked through /bin/bash 3.2, guard empty array expansions under set -u.

Numeric query performance follow-up:
Target query: select sum(9999::numeric) from generate_series(1, 100000)
Added benchmark binary: src/bin/numeric_query_bench.rs, registered under Cargo feature tools.
Validation: scripts/cargo_isolated.sh check --features tools --bin numeric_query_bench

pgrust direct debug profile, 100k rows, 10 iterations:
count: 49.156 ms avg
sum-int: 70.833 ms avg
target numeric const sum: 109.060 ms avg
sum generated i::numeric: 130.869 ms avg
series rows: 36.987 ms avg

pgrust direct release profile, 100k rows, 30 iterations:
count: 12.882 ms avg
sum-int: 16.317 ms avg
target numeric const sum: 19.518 ms avg
sum generated i::numeric: 25.169 ms avg
series rows: 12.876 ms avg

PostgreSQL EXPLAIN ANALYZE, 100k rows:
count: 4.547 ms
sum-int: 4.512 ms
target numeric const sum: 5.125 ms
sum generated i::numeric: 6.918 ms
series rows: 3.930 ms

pgrust release, 1M rows, 5 iterations:
count: 96.013 ms avg
sum-int: 106.605 ms avg
target numeric const sum: 138.844 ms avg
sum generated i::numeric: 194.143 ms avg
series rows: 75.886 ms avg

PostgreSQL EXPLAIN ANALYZE, 1M rows:
count: 70.747 ms
sum-int: 71.723 ms
target numeric const sum: 77.016 ms
sum generated i::numeric: 98.317 ms
series rows: 64.912 ms

Likely causes:
The regression harness uses an unoptimized dev build for --test numeric; that makes the 100k target query roughly 21x slower than PostgreSQL.
In release, most gap is executor/SRF overhead: FunctionScanState materializes all SRF rows as TupleSlot/MaterializedRow objects before scanning them.
AggregateState then clones/materializes slot values, resets expression bindings, evaluates aggregate args, and dispatches the transition on each row.
Numeric sum adds extra BigInt clone/normalize work in NumericValue::add; PostgreSQL's numeric transition uses compact in-place-style NumericVar code.
PostgreSQL also materializes FunctionScan into a tuplestore, but its tuple slots, function-call protocol, and numeric accumulator are much tighter than pgrust's Value/Vec/BigInt path.
