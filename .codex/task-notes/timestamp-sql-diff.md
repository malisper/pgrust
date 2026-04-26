Goal:
Explain the timestamp.sql regression diff.

Key decisions:
The first mismatch is not interval formatting. It is `timestamp(2) 'now'`
returning zero matches where PostgreSQL expects the two rows inserted inside the
explicit transaction. Likely cause is optimizer constant folding of text-to-
timestamp casts in `src/backend/optimizer/constfold.rs`, which calls
`cast_value` without the session/executor datetime config. That makes special
timestamp literals such as `now` use the wrong timestamp context.

Fixed by treating text-like casts to date/time/timestamp/timestamptz/timetz and
interval as unsafe to constant-fold in the catalog-free optimizer pass.

The large interval hunks are formatting-only. PostgreSQL's regression driver
sets `PGOPTIONS=-c intervalstyle=postgres_verbose`; pgrust's
`scripts/run_regression.sh` sets `PGDATESTYLE` and `PGTZ` but not intervalstyle,
so output uses `IntervalStyle::Postgres` instead of `PostgresVerbose`.

Fixed by adding `-c intervalstyle=postgres_verbose` to both regression runners'
PGOPTIONS export.

Files touched:
.codex/task-notes/timestamp-sql-diff.md
scripts/run_regression.sh
scripts/run_regression_one_by_one.sh
src/backend/optimizer/constfold.rs
src/pgrust/database_tests.rs

Tests run:
CARGO_TARGET_DIR=/tmp/pgrust-target-timestamp-sql cargo test --lib --quiet timestamp_now_literal_applies_declared_precision
CARGO_TARGET_DIR=/tmp/pgrust-target-timestamp-sql scripts/run_regression.sh --test timestamp --jobs 1 --timeout 120 --port 56433

Remaining:
None for the timestamp.sql diff.
