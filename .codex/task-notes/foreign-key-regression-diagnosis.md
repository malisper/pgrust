Goal:
Diagnose and fix selected foreign_key regression failures. First batch covered referenced-table SELECT ACL checks, pg_get_constraintdef FK action coverage, and FK-specific column errors. Follow-up batch covered DDL/error fidelity, NOT ENFORCED/ALTER CONSTRAINT behavior, FK action ordering, and an initial partitioned-FK support slice.

Key decisions:
Grouped output into root causes rather than treating cascading relation-missing errors as independent failures.
Partitioned FK support now includes dropped-column layout remapping, inherited child FK rows for existing/attached partitions, child check triggers, ENFORCED/NOT ENFORCED propagation, DROP propagation, VALIDATE propagation, and inline FKs on partitioned roots. Full PostgreSQL merge/validation fidelity is still partial.
Moved default NO ACTION parent checks to statement-end rechecks and kept RESTRICT immediate.
Added trigger-to-constraint dependencies so dropping an FK child removes internal RI triggers left on the referenced table.
COMMIT of an already-failed transaction now aborts without running deferred FK checks.

Files touched:
.codex/task-notes/foreign-key-regression-diagnosis.md
crates/pgrust_sql_grammar/src/gram.pest
src/backend/access/index/buildkeys.rs
src/backend/catalog/pg_depend.rs
src/backend/catalog/state.rs
src/backend/catalog/store/heap.rs
src/backend/commands/partition.rs
src/backend/commands/tablecmds.rs
src/backend/commands/upsert.rs
src/backend/executor/foreign_keys.rs
src/backend/executor/permissions.rs
src/backend/parser/analyze/constraints.rs
src/backend/parser/analyze/mod.rs
src/backend/parser/analyze/partition.rs
src/backend/tcop/postgres.rs
src/backend/parser/gram.rs
src/backend/parser/tests.rs
src/include/nodes/parsenodes.rs
src/pgrust/database/commands/constraint.rs
src/pgrust/database/commands/create.rs
src/pgrust/database/commands/drop_column.rs
src/pgrust/database/commands/index.rs
src/pgrust/database/commands/partition.rs
src/pgrust/database/commands/trigger.rs
src/pgrust/database/toast.rs
src/pgrust/session.rs
src/pgrust/database_tests.rs

Tests run:
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet foreign_key_insert_requires_select_on_referenced_table
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet pg_get_constraintdef_keeps_fk_actions_when_referenced_columns_are_omitted
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet pg_get_constraintdef_formats_foreign_key_actions_and_delete_columns
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet lower_create_table_uses_foreign_key_column_errors
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet lower_create_table_rejects_invalid_foreign_key_delete_set_columns
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/run_regression.sh --port 55433 --test foreign_key --timeout 120 --ignore-deps --results-dir /tmp/pgrust-foreign-key-regression-ignore-deps
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh check
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet parse_alter_table_constraint_statements
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet foreign_key_reports_postgres_unique_key_error
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet foreign_key_type_mismatch_reports_first_pair_only
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet foreign_keys_support_not_enforced_and_alter_enforced_state
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet alter_foreign_key_constraint_reports_postgres_option_errors
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet foreign_key_triggers_match_action_deferrability
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet foreign_key_update_actions_accumulate_on_same_child_row
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet self_referential_foreign_key_cascade_sees_updated_parent_row
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet self_referential_no_action_checks_final_statement_state
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet no_action_update_allows_same_statement_replacement_key
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet dropping_fk_child_removes_referenced_table_ri_triggers
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet foreign_key_partition_attach_matches_columns_by_name
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet foreign_key_can_reference_partitioned_primary_key
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/run_regression.sh --port 55433 --test foreign_key --timeout 120 --ignore-deps --results-dir /tmp/pgrust-foreign-key-followup4
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet failed_transaction_commit_skips_deferred_foreign_key_checks
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet exec_error_position_points_at_alter_constraint_fk_options
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet partitioned_table_drop_column_keeps_attach_layout_name_based
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet multi_level_partition_routing_remaps_dropped_column_layouts
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet foreign_key_on_partitioned_table_creates_child_constraints
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet alter_partitioned_foreign_key_enforced_updates_child_constraints
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet attach_partition_merges_existing_foreign_key_with_parent
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet only_foreign_key_on_partitioned_table_reports_postgres_error
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet dropping_partitioned_foreign_key_removes_child_constraints
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet create_partitioned_table_with_inline_foreign_key_propagates_on_attach
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet validate_partitioned_foreign_key_marks_child_constraints
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet "partitioned"
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh check
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/run_regression.sh --port 55433 --test foreign_key --timeout 120 --ignore-deps --results-dir /tmp/pgrust-foreign-key-followup8

Remaining:
Full foreign_key regression still times out: /tmp/pgrust-foreign-key-followup8 matched 683/1252 queries. Early remaining hunks are unsupported rule EXPLAIN/deletes, missing pending-trigger-event semantics for DROP CONSTRAINT, float -0 equality in cascade, lack of partition-key UPDATE routing, ALTER COLUMN TYPE storage rewrites for FK-referenced columns, and deeper partitioned-FK merge/attach-validation details. The partition block now gets much further, but still has duplicate/extra child constraints in some NOT VALID attach cases and unsupported range/list bound forms later in the file.
