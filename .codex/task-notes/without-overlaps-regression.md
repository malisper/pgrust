Goal:
Diagnose and reduce failures in the upstream `without_overlaps` regression.

Key decisions:
Fixed the first real mismatch: temporal `WITHOUT OVERLAPS` constraints on custom
range types need to fall back to polymorphic `anyrange`/`anymultirange`
operators when exact custom-type operators are not present.
Added PERIOD foreign-key syntax to the parser/AST/analyzer, including temporal
referenced-key matching through `pg_constraint.conperiod`.
Added a narrow multi-action `ALTER TABLE` shim that splits top-level comma
actions into existing single-action statements.
Added `ALTER TABLE ... REPLICA IDENTITY USING INDEX` parsing/execution by
updating `pg_index.indisreplident`.
Changed non-unique `USING INDEX` errors to PostgreSQL's `"idx" is not a unique
index` message/detail.
Passed session DateStyle into ALTER TABLE ADD CONSTRAINT validation so exclusion
conflict details use the active formatting config.

Files touched:
src/backend/catalog/state.rs
src/backend/catalog/store/heap.rs
src/backend/executor/driver.rs
src/backend/executor/exec_expr.rs
src/backend/executor/foreign_keys.rs
src/backend/parser/analyze/constraints.rs
src/backend/parser/gram.pest
src/backend/parser/gram.rs
src/backend/parser/tests.rs
src/backend/tcop/postgres.rs
src/include/nodes/parsenodes.rs
src/pgrust/database/commands/constraint.rs
src/pgrust/database/commands/create.rs
src/pgrust/database/commands/execute.rs
src/pgrust/database/commands/index.rs
src/pgrust/database_tests.rs
src/pgrust/session.rs
src/bin/query_repl.rs

Tests run:
scripts/cargo_isolated.sh test --lib --quiet without_overlaps_accepts_custom_range_period_column
scripts/cargo_isolated.sh test --lib --quiet parse_alter_table_constraint_statements
scripts/cargo_isolated.sh test --lib --quiet parse_create_table_foreign_key_constraints
scripts/cargo_isolated.sh test --lib --quiet without_overlaps_replica_identity_using_index_marks_pg_index
scripts/cargo_isolated.sh test --lib --quiet alter_table_add_constraint_using_nonunique_index_matches_postgres_error
scripts/cargo_isolated.sh check
CARGO_INCREMENTAL=0 cargo build --bin pgrust_server
manual psql smoke on port 55446 for PERIOD FK, multi-action ALTER TABLE,
REPLICA IDENTITY USING INDEX, non-unique USING INDEX error text, and DateStyle
exclusion conflict details
CARGO_INCREMENTAL=0 scripts/run_regression.sh --port 55449 --jobs 1 --schedule .context/without_overlaps.schedule --test without_overlaps --timeout 180 --results-dir /tmp/pgrust_without_overlaps_after_fix7
cp /tmp/pgrust_without_overlaps_after_fix7/diff/without_overlaps.diff /tmp/diffs/without_overlaps_after_fix7.diff

Remaining:
The one-test regression now reaches `without_overlaps` and matches 541/643
queries. The requested unsupported syntax/error-text/DateStyle issues are fixed
in focused smoke output. Remaining diffs are broader: missing USING INDEX rename
notice, DROP COLUMN CASCADE dependency behavior, temporal FK debug-value detail
formatting, temporal FK update/delete semantics, and REFERENCES to partitioned
tables.
