Goal:
Fix the alter_table timeout, then improve ALTER object-kind routing, inherited constraint behavior, and partitioned-table compatibility.
Key decisions:
Ran focused and full alter_table regressions on alternate ports. The original timeout was fixed by rejecting circular ATTACH PARTITION before writing pg_inherits, matching PostgreSQL's error. Broader follow-up work kept to PostgreSQL compatibility shims: object-kind routing accepts partitioned indexes/views where needed, no-storage relkinds use logical lock tags, view column defaults are catalog-updatable through ALTER TABLE, and unsupported view column actions now return the PostgreSQL view-detail error.
This pass fixed DDL CHECK validation to use PostgreSQL validation wording instead of row-write CheckViolation errors, gave CHECK validation an executor catalog context so function checks can run, made ALTER TABLE VALIDATE CONSTRAINT recurse into inherited CHECK constraints, emitted merge notices when ALTER TABLE ADD CHECK merges an existing inherited child constraint, and propagated parent DROP NOT NULL through inherited child constraints.
This formatting pass cleared the remaining original "error text/detail/hint/caret/display formatting only" bucket by fixing literal SET DEFAULT coercion/caret behavior, dropped-column CREATE INDEX validation, ALTER TYPE DROP ATTRIBUTE IF EXISTS notices, single-column CHECK generated names, and basic float CHECK display casts. The six previously remaining query ids from that bucket (531, 532, 626, 876, 1091, 1183) all match in the latest run.
This partition pass fixed several attach/detach gaps: CREATE TABLE LIKE now preserves explicit collations through syscache relation rebuilds, attached partition constraint display uses pg_get_partition_constraintdef, inherited CHECK/NOT NULL drop guards match PostgreSQL, partitioned-root INSERT fires parent statement triggers before routing, PL/pgSQL dynamic CREATE TABLE runs through the database DDL path, dynamic EXECUTE errors include SQL-statement context, direct inherited ALTER COLUMN TYPE errors use the PostgreSQL wording, and partitioned-root UPDATE captures parent transition-table rows remapped from child partitions.
Files touched:
src/backend/catalog/state.rs
src/backend/catalog/store/heap.rs
src/backend/executor/exec_expr.rs
src/backend/parser/analyze/create_table_inherits.rs
src/backend/tcop/postgres.rs
src/backend/utils/cache/syscache.rs
src/pgrust/database.rs
src/pgrust/database/commands/alter_column_default.rs
src/pgrust/database/commands/alter_column_type.rs
src/pgrust/database/commands/create.rs
src/pgrust/database/commands/drop_column.rs
src/pgrust/database/commands/inheritance.rs
src/pgrust/database/commands/maintenance.rs
src/pgrust/database/commands/partition.rs
src/pgrust/database/commands/constraint.rs
src/pgrust/database/commands/rename.rs
src/pgrust/database/ddl.rs
src/pgrust/database/stats_import.rs
src/pgrust/database_tests.rs
src/pgrust/session.rs
src/pl/plpgsql/exec.rs
.codex/task-notes/alter-table-timeout.md
Tests run:
scripts/run_regression.sh --test alter_table --timeout 90 --port 55433 --results-dir /tmp/pgrust-alter-table-regression
scripts/run_regression_one_by_one.sh --test alter_table --port 55443 --results-dir /tmp/pgrust-alter-table-one-by-one
scripts/cargo_isolated.sh test --lib --quiet attach_partition_rejects_circular_inheritance_before_catalog_write
scripts/run_regression.sh --test alter_table --timeout 90 --port 55453 --results-dir /tmp/pgrust-alter-table-regression-after
scripts/cargo_isolated.sh test --lib --quiet relation_rename_accepts_alter_table_and_index_object_kind_mismatch
scripts/cargo_isolated.sh test --lib --quiet alter_index_rename_supports_if_exists_and_rename
scripts/cargo_isolated.sh test --lib --quiet partitioned_table_and_index_rename_use_distinct_lock_tags
scripts/cargo_isolated.sh test --lib --quiet alter_table_rejects_partition_key_column_drop_and_type_change
scripts/cargo_isolated.sh test --lib --quiet create_table_inherits_notices_column_check_constraint_merge
scripts/cargo_isolated.sh test --lib --quiet alter_table_add_check_not_enforced_skips_validation_and_write_enforcement
scripts/cargo_isolated.sh test --lib --quiet psql_describe_constraint_query_renders_check_state
scripts/cargo_isolated.sh test --lib --quiet create_table_local_not_valid_check_is_catalog_valid_immediately
scripts/cargo_isolated.sh test --lib --quiet alter_column_type_with_foreign_key_after_referenced_type_change_finishes
scripts/cargo_isolated.sh test --lib --quiet alter_table_view_alter_column_default_updates_view_catalog
scripts/cargo_isolated.sh test --lib --quiet alter_table_view_rejects_unsupported_column_actions_with_view_detail
scripts/cargo_isolated.sh test --lib --quiet create_table_partition_validation_matches_postgres_messages
cargo fmt
git diff --check
scripts/run_regression.sh --test alter_table --timeout 90 --port 55503 --results-dir /tmp/pgrust-alter-table-regression-final
scripts/cargo_isolated.sh test --lib --quiet alter_table_check_validation
scripts/cargo_isolated.sh test --lib --quiet alter_table_drop_parent_not_null_removes_inherited_child_constraint
scripts/cargo_isolated.sh test --lib --quiet alter_table_add_check_not_enforced_skips_validation_and_write_enforcement
scripts/cargo_isolated.sh test --lib --quiet not_null_inheritance_metadata_and_alter_constraint_inheritability
scripts/cargo_isolated.sh test --lib --quiet alter_table_set_and_drop_not_null_updates_enforcement_and_catalog
cargo fmt
git diff --check
scripts/run_regression.sh --test alter_table --timeout 90 --port 50901 --results-dir /tmp/pgrust-alter-table-regression-check-notnull
cargo fmt
scripts/cargo_isolated.sh check
scripts/run_regression.sh --test alter_table --timeout 150 --port 55871 --results-dir /tmp/pgrust-alter-table-regression-format-final
cargo fmt
scripts/cargo_isolated.sh check
git diff --check
scripts/run_regression.sh --test alter_table --timeout 150 --port 58070 --results-dir /tmp/pgrust-alter-table-regression-format-final2
cargo fmt
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet attach_partition_like_child_preserves_collation_before_overlap_check
scripts/cargo_isolated.sh test --lib --quiet inherited_partition_check_added_with_not_null_cannot_be_dropped_from_child
scripts/cargo_isolated.sh test --lib --quiet attach_partition_from_parent_statement_trigger_reports_active_query
scripts/cargo_isolated.sh test --lib --quiet partitioned_update_parent_transition_table_sees_attached_default_rows
scripts/cargo_isolated.sh test --lib --quiet partition::tests
scripts/cargo_isolated.sh test --lib --quiet transition_table_statement_triggers_can_read_statement_rows
scripts/run_regression.sh --test alter_table --timeout 150 --port 55620 --results-dir /tmp/pgrust-alter-table-partition-final12
scripts/run_regression.sh --test alter_table --timeout 150 --port 55640 --results-dir /tmp/pgrust-alter-table-partition-final13
Remaining:
The timeout is fixed. The latest full alter_table regression completes without timeout:
1386/1683 queries matched, 2602 diff lines.
Original timed-out statement was query 01511:
ALTER TABLE part_5 ATTACH PARTITION list_parted2 FOR VALUES IN ('b');
pgrust now errors with "circular inheritance not allowed" before creating the edge.
The CHECK validation wording/catalog-context cluster is now covered by focused tests and no longer appears in the alter_table diff for b_greater_than_ten, b_le_20, identity/boo(), or check_a_is_2. Parent DROP NOT NULL now removes inherited child enforcement in the simple inheritance case. The original formatting-only bucket is cleared. The latest partition pass brings alter_table to 1453/1683 matched, 230 mismatches, no timeout. The remaining partition-section items are mostly caret/context formatting, custom operator-class support for the at_test_sql_partop attach case, and unsupported DROP OPERATOR CLASS syntax; broader remaining failures include CLUSTER/clustered-index metadata, operator-class/schema support, rowtype dependency checks during ALTER TYPE, updatable-view INSERT rule execution, partitioned index rebuild behavior after ALTER COLUMN TYPE, and ALTER SET SCHEMA support for views.
