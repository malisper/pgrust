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
2026-04-27:
scripts/run_regression.sh --test for_portion_of --results-dir /tmp/pgrust_regress_for_portion_of --timeout 60
  Result: failed before execution because local upstream checkout has no
  src/test/regress/sql/for_portion_of.sql.
scripts/run_regression.sh --test without_overlaps --results-dir /tmp/pgrust_regress_without_overlaps --timeout 120 --port 5543
  Result: without_overlaps FAIL, 556/643 queries matched, 87 mismatched,
  582 diff lines.
scripts/cargo_isolated.sh check
scripts/run_regression.sh --test without_overlaps --results-dir /tmp/pgrust_regress_without_overlaps_rangefmt --timeout 120 --port 5545
  Result after FK range/multirange detail formatting fix: without_overlaps
  FAIL, 568/643 queries matched, 75 mismatched, 498 diff lines. The
  `RangeValue {` / `MultirangeValue {` debug-format bucket dropped from 27 to
  0.
scripts/cargo_isolated.sh test --lib --quiet truncate_all_foreign_key_relations_together
scripts/run_regression.sh --test without_overlaps --results-dir /tmp/pgrust_regress_without_overlaps_truncatefk --timeout 120 --port 5547
  Result after target-set-aware TRUNCATE FK validation: without_overlaps FAIL,
  603/643 queries matched, 40 mismatched, 326 diff lines. The unexpected
  exclusion-conflict bucket dropped from 19 to 0, and the unexpected TRUNCATE
  FK-block bucket dropped from 11 to 0.
scripts/cargo_isolated.sh check

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
2026-04-27 current first mismatch: `ALTER TABLE temporal3 DROP COLUMN valid_at
CASCADE` still fails because `src/pgrust/database/commands/drop_column.rs`
always calls `reject_column_with_foreign_key_dependencies`, regardless of
`drop_stmt.cascade`. PostgreSQL drops the dependent period FK and emits a
NOTICE. This leaves later state dirty, causing duplicate constraints,
unexpected FK references, and exclusion conflicts. Fixed FK detail formatting
for range/multirange values by rendering `Value::Range` and `Value::Multirange`
through the SQL text renderers in
`src/backend/executor/foreign_keys.rs::render_key_value`.
Fixed multi-table TRUNCATE validation by allowing inbound FK references when
the referencing relation is also in the expanded TRUNCATE target set. This
matches PostgreSQL's behavior for `TRUNCATE parent, child` and removed the
dirty-state exclusion conflicts.

The one-test regression now reaches `without_overlaps` and matches 541/643
queries. The requested unsupported syntax/error-text/DateStyle issues are fixed
in focused smoke output. Remaining diffs are broader: missing USING INDEX rename
notice, DROP COLUMN CASCADE dependency behavior, temporal FK debug-value detail
formatting, temporal FK update/delete semantics, and REFERENCES to partitioned
tables.

2026-04-28 implementation update:
Goal:
Finish the remaining `without_overlaps` failures after commit `b6a13c229`.

Key decisions:
Prevalidated PERIOD FK actions before compound/multi ALTER execution so invalid
constraints are rejected before catalog writes. Used existing FK drop machinery
for DROP COLUMN CASCADE. Split deferred FK tracking into outbound child-row
checks and inbound parent-row checks so deferred parent update/delete reports
the PostgreSQL parent-side error. Bound partition leaf FKs/reference-side FKs
from partitioned parents and added display metadata for PostgreSQL-style
partitioned referenced-side FK names. Allowed partition-key UPDATE on
partitioned roots and implemented leaf row movement. Added statement-end
temporal/exclusion checks for updated rows before deferred outbound FK checks.
Threaded DateStyle into ALTER ADD CONSTRAINT FK validation and coerced range
string defaults through range input.

Files touched:
src/backend/commands/tablecmds.rs
src/backend/commands/upsert.rs
src/backend/executor/foreign_keys.rs
src/backend/executor/mod.rs
src/backend/parser/analyze/constraints.rs
src/backend/parser/analyze/modify.rs
src/pgrust/database.rs
src/pgrust/database/commands/alter_column_default.rs
src/pgrust/database/commands/constraint.rs
src/pgrust/database/commands/create.rs
src/pgrust/database/commands/drop_column.rs
src/pgrust/database/commands/partition.rs
src/pgrust/database/ddl.rs
src/pgrust/database/foreign_keys.rs
src/pgrust/database_tests.rs
src/pgrust/session.rs

Tests run:
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet without_overlaps_remaining
scripts/cargo_isolated.sh test --lib --quiet partitioned_root_dml_routes_rows_and_only_root_is_empty
scripts/run_regression.sh --test without_overlaps --results-dir /tmp/pgrust_regress_without_overlaps_final4 --timeout 180 --port 55480
  Result: PASS, 643/643 queries matched.

Remaining:
No remaining `without_overlaps` regression diffs in the focused run.
