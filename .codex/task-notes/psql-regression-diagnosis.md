Goal:
Diagnose PostgreSQL regression test `psql` failures.

Key decisions:
Ran only `scripts/run_regression.sh --test psql` on a free high port.
Primary failure bucket is SQL PREPARE state split between `src/backend/tcop/postgres.rs`
and `src/pgrust/session.rs`: PREPARE is intercepted into `ConnectionState::prepared`,
while SQL EXECUTE resolves `Session::prepared_selects`.
Implemented fix by removing the SQL PREPARE tcop interception, keeping extended
protocol prepares in `ConnectionState`, and exposing session-prepared statements
to the pg_prepared_statements shim.

Files touched:
`src/backend/tcop/postgres.rs`
`src/pgrust/session.rs`
`.codex/task-notes/psql-regression-diagnosis.md`

Tests run:
`scripts/run_regression.sh --test psql --results-dir /tmp/pgrust_psql_regress --timeout 120 --port 56433 --skip-build`
Result: FAIL, 272/464 queries matched, 192 mismatched, 5867 diff lines.
`scripts/cargo_isolated.sh test --lib --quiet sql_prepare_execute_and_deallocate_use_session_state`
Result: PASS.
`scripts/run_regression.sh --test psql --results-dir /tmp/pgrust_psql_regress_fix2 --timeout 120 --port 58433`
Result: FAIL, 396/464 queries matched, 68 mismatched, 2533 diff lines.
`prepared statement "q" does not exist` count: 0.

Remaining:
Remaining buckets: \gdesc/Describe handling, missing `pg_catalog.pg_sequence`, table
access method syntax/catalog support, `pg_table_size`, catalog visibility functions,
and protocol error text cleanup.
