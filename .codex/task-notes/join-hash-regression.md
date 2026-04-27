Goal:
Investigate why `join_hash` regression is marked errored.

Key decisions:
Ran the regression on an explicit port because default port 5433 was already in use.
Captured a second run with `RUST_BACKTRACE=1` to identify the panic path.
PostgreSQL's `pull_varnos()` descends into bare SubLink subqueries and treats
their `varlevelsup = 1` Vars as relids of the current query. pgrust's
`expr_relids()` did not, so correlated sublinks were placed too low and could
panic during setrefs.

Files touched:
.codex/task-notes/join-hash-regression.md
src/backend/optimizer/joininfo.rs

Tests run:
scripts/run_regression.sh --test join_hash --timeout 180 --jobs 1 --results-dir /tmp/pgrust-join-hash-daegu
scripts/run_regression.sh --test join_hash --timeout 180 --jobs 1 --port 59433 --results-dir /tmp/pgrust-join-hash-daegu-port59433
RUST_BACKTRACE=1 scripts/run_regression.sh --test join_hash --timeout 180 --jobs 1 --skip-build --port 59434 --results-dir /tmp/pgrust-join-hash-daegu-backtrace
scripts/cargo_isolated.sh test --lib --quiet joininfo::tests
scripts/run_regression.sh --test join_hash --timeout 180 --jobs 1 --port 59435 --results-dir /tmp/pgrust-join-hash-daegu-after-relids
scripts/cargo_isolated.sh check

Remaining:
The server panic is fixed: join_hash now FAILs normally rather than ERRORing.
Remaining diffs include unsupported `EXPLAIN (ANALYZE, FORMAT 'json')`, some
subquery alias lookup errors in rescan sections, and plan-shape differences in
the hjtest hash-key section where pgrust still picks a Nested Loop plus upper
Filter instead of PostgreSQL's Hash Join.
