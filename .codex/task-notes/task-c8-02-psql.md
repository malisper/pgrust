Goal:
Fix psql describe metadata, \gdesc, unnamed prepared visibility, and psql-visible helper catalog/function gaps for TASK-C8-02.

Key decisions:
- Kept portal lifecycle/pipeline cleanup out of scope; remaining portal/cursor follow-on errors are blocked on protocol lifecycle work.
- Used PostgreSQL DescribeQuery behavior in ../postgres/src/bin/psql/common.c and pg_proc.dat as reference.
- Implemented narrow Describe Prepared metadata handling in tcop because psql gets \gdesc metadata via Parse/Describe, not execution.
- Hid unnamed protocol prepared statements from pg_prepared_statements.
- Added pg_collation_is_visible with the existing search-path visibility helper shape.

Files touched:
- src/backend/tcop/postgres.rs
- src/pgrust/session.rs
- src/include/nodes/primnodes.rs
- src/include/catalog/pg_proc.rs
- src/backend/parser/analyze/functions.rs
- src/backend/executor/exec_expr.rs
- src/pgrust/database_tests.rs

Tests run:
- TMPDIR='/Volumes/OSCOO PSSD/rust/tmp' scripts/cargo_isolated.sh check
- TMPDIR='/Volumes/OSCOO PSSD/rust/tmp' scripts/cargo_isolated.sh test --lib --quiet gdesc
- TMPDIR='/Volumes/OSCOO PSSD/rust/tmp' scripts/cargo_isolated.sh test --lib --quiet connection_describe_unnamed_prepared_statement_survives_sync
- TMPDIR='/Volumes/OSCOO PSSD/rust/tmp' scripts/cargo_isolated.sh test --lib --quiet catalog_visibility_functions_cover_psql_describe_helpers
- TMPDIR='/Volumes/OSCOO PSSD/rust/tmp' CARGO_INCREMENTAL=0 scripts/run_regression.sh --test psql --port 54140 --results-dir /tmp/pgrust-task-c8-02-psql-final4

Remaining:
- psql still fails overall at 416/464 matched. Remaining diffs include portal/cursor follow-on errors after Bind failures, which belong to protocol lifecycle work.
- The \gdesc syntax-error primary message now matches, but pgrust does not emit PostgreSQL's LINE/caret decoration for these Describe errors.
- Broad psql describe output still differs for unsupported access methods, role columns, missing pg_db_role_setting/tableoid behavior, timeout-prone access-method listings, and catalog listing shape. Those are outside this narrow psql describe/metadata slice or require broader catalog/DDL work.
