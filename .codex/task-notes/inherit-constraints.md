Goal:
Reduce `inherit` regression drift across constraint inheritance, early tableoid/update state,
EXCLUDE DDL, partition pruning, and EXPLAIN display/plan-shape buckets.

Key decisions:
Implemented PostgreSQL-style merge/error behavior for inherited CHECK constraints where local
constraints have the same name/expression. Preserved NO INHERIT display, added child CHECK name
generation for single-column table checks, and made NOT NULL ALTER CONSTRAINT INHERIT create
missing inherited child rows.

Follow-up pass added CHECK failing-row detail, normalized stored CHECK/default expression display,
kept `tableoid` system-var bindings through old comma joins and update inputs, enabled auto-view
`UPDATE ... FROM` rewrites, allowed `ALTER TABLE ... ADD/DROP EXCLUDE` including the regression's
`EXCLUDE ((1) WITH =)`, merged compatible inherited `ADD COLUMN`, and kept range partitions for
`IS NOT NULL` pruning. Expression EXCLUDE enforcement is intentionally deferred with a `:HACK:`
because the current exclusion checker is column-based.

Files touched:
`crates/pgrust_sql_grammar/src/gram.pest`
`src/backend/catalog/pg_constraint.rs`
`src/backend/catalog/store/heap.rs`
`src/backend/executor/constraints.rs`
`src/backend/executor/exec_expr.rs`
`src/backend/executor/nodes.rs`
`src/backend/libpq/pqformat.rs`
`src/backend/optimizer/partition_prune.rs`
`src/backend/optimizer/setrefs.rs`
`src/backend/parser/analyze/constraints.rs`
`src/backend/parser/analyze/create_table_inherits.rs`
`src/backend/parser/analyze/modify.rs`
`src/backend/parser/gram.rs`
`src/backend/tcop/postgres.rs`
`src/backend/utils/sql_deparse.rs`
`src/include/nodes/parsenodes.rs`
`src/pgrust/database/commands/constraint.rs`
`src/pgrust/database/commands/create.rs`
`src/pgrust/database/commands/index.rs`
`src/pgrust/database/commands/inheritance.rs`
`src/pgrust/database/commands/maintenance.rs`
`src/pgrust/database/commands/partitioned_indexes.rs`
`src/pgrust/database/commands/partitioned_keys.rs`
`src/pgrust/database/commands/rules.rs`
`src/pgrust/database_tests.rs`

Tests run:
`scripts/cargo_isolated.sh test --lib --quiet alter_not_null_inherit_creates_missing_child_constraints`
`scripts/cargo_isolated.sh test --lib --quiet inherited_check_rejects_not_enforced_or_not_valid_child_conflicts`
`scripts/cargo_isolated.sh test --lib --quiet inherit`
`scripts/cargo_isolated.sh check`
`scripts/run_regression.sh --test inherit --timeout 120 --port 56463 --results-dir /tmp/diffs/inherit_constraints_current4`
`scripts/cargo_isolated.sh test --lib --quiet inherited_scan_tableoid_tracks_physical_child_relation`
`scripts/cargo_isolated.sh test --lib --quiet update_from_rewrites_auto_updatable_view_targets`
`scripts/cargo_isolated.sh test --lib --quiet update_from_updates_inherited_children`
`scripts/cargo_isolated.sh test --lib --quiet update_from_updates_partitioned_targets_from_appendrel_source`
`scripts/cargo_isolated.sh test --lib --quiet normalizes_check_expression_sql`
`scripts/cargo_isolated.sh test --lib --quiet alter_table_add_column_merges_existing_child_column_and_not_null`
`scripts/cargo_isolated.sh test --lib --quiet alter_table_drop_exclusion_constraint_drops_backing_index`
`scripts/cargo_isolated.sh test --lib --quiet alter_table_add_exclusion_constraint_accepts_expression_key`
`scripts/cargo_isolated.sh test --lib --quiet executor::tests::explain`
`scripts/cargo_isolated.sh test --lib --quiet optimizer::tests`
`scripts/run_regression.sh --port 56000 --jobs 1 --test inherit --timeout 300 --results-dir /tmp/diffs/inherit_after_remaining`

Remaining:
Latest completed regression artifact is `/tmp/diffs/inherit_after_remaining`: 753/884 queries
matched, 131 mismatched, 2006 diff lines. Remaining hunk buckets are roughly: planner/EXPLAIN
shape 40, NOT NULL inheritance 25, CHECK/expression display 15, UPDATE FROM/row assignment 15,
partition failing-row detail 7, ADD COLUMN/EXCLUDE residual 4, other parser/executor 8, raw display
fallback 9. The early tableoid Exec-param failures are gone.

2026-04-28 follow-up:
Implemented row assignment parsing for UPDATE/MERGE/ON CONFLICT by lowering multi-column
assignments into existing assignment nodes, fixed correlated set-operation derived tables seeing
outer aliases, coerced recursive CTE recursive terms to binary-compatible anchor types, projected
wider inherited child records to parent composite casts by name, split DDL CHECK validation errors
from row-write CHECK detail, remapped routed partition INSERT failing-row detail to parent logical
column order, and added the inherited NOT NULL NO INHERIT detail.

Additional tests run:
`scripts/cargo_isolated.sh test --lib --quiet row_assignment_updates_inherited_children`
`scripts/cargo_isolated.sh test --lib --quiet on_conflict_row_assignment_updates_columns`
`scripts/cargo_isolated.sh test --lib --quiet inherited_child_row_cast_projects_parent_composite_fields`
`scripts/cargo_isolated.sh test --lib --quiet recursive_cte_coerces_oid_recursive_term_to_regclass_anchor`
`scripts/cargo_isolated.sh test --lib --quiet correlated_set_operation_derived_table_can_see_outer_alias`
`scripts/cargo_isolated.sh test --lib --quiet routed_partition_constraint_detail_uses_parent_column_order`
`scripts/cargo_isolated.sh test --lib --quiet create_table_rejects_not_null_no_inherit_on_inherited_not_null_column`
`scripts/cargo_isolated.sh test --lib --quiet inherit`
`scripts/cargo_isolated.sh test --lib --quiet executor::tests::explain`
`scripts/cargo_isolated.sh test --lib --quiet optimizer::tests`
`scripts/cargo_isolated.sh check`
`scripts/run_regression.sh --port 56000 --jobs 1 --test inherit --timeout 300 --results-dir /tmp/diffs/inherit_after_final`

New regression artifact:
`/tmp/diffs/inherit_after_final`: 770/884 queries matched, 114 mismatched, 1794 diff lines.
Remaining buckets are now mostly planner/EXPLAIN shape and raw expression display, NOT NULL
inheritance/drop/coninhcount edge cases, CHECK connoinherit/localization display, direct leaf
partition UPDATE and partition-move error ordering, inherited column type rewrite/default message
drift, partitioned ON CONFLICT support, and relation-qualified/temp/permission error text.

2026-04-28 second follow-up:
Implemented direct leaf partition UPDATE constraint reporting and parent-ordered routed-update
failing-row detail, fixed NULL literal coercion inside CHECK expressions, added `bit_length`
builtin support and bound inherited ALTER TYPE `USING` expressions for descendants, normalized
several inherited DDL errors/notices, expanded `LIKE` before inherited CREATE TABLE column merge,
fixed ADD COLUMN NOT NULL inheritance metadata for child rows, set top-level index-backed
constraints `connoinherit = true`, returned SQL NULL for ordinary child `relpartbound` in psql
describe, and added wildcard support to the psql describe lookup shim.

Additional tests run in this slice:
`scripts/cargo_isolated.sh test --lib --quiet direct_leaf_partition_update_enforces_leaf_partition_constraint`
`scripts/cargo_isolated.sh test --lib --quiet routed_partition_update_constraint_detail_uses_parent_column_order`
`scripts/cargo_isolated.sh test --lib --quiet check_constraint_null_literal_coerces_to_peer_type`
`scripts/cargo_isolated.sh test --lib --quiet alter_table_alter_inherited_column_type_rewrites_children_with_using_expr`
`scripts/cargo_isolated.sh test --lib --quiet alter_table_alter_column_type_rewrites_rows_with_using_expr`
`scripts/cargo_isolated.sh test --lib --quiet create_table_like_inherits_reports_column_merge_notice`
`scripts/cargo_isolated.sh test --lib --quiet alter_table_add_column_merges_existing_child_column_and_not_null`
`scripts/cargo_isolated.sh test --lib --quiet alter_table_add_column_not_null_no_inherit_does_not_mark_children`
`scripts/cargo_isolated.sh test --lib --quiet index_backed_constraints_are_no_inherit_catalog_rows`
`scripts/cargo_isolated.sh test --lib --quiet psql_describe_child_tables_uses_null_bound_for_plain_inheritance`
`scripts/cargo_isolated.sh test --lib --quiet alter_table_set_not_null_rejects_existing_no_inherit_constraint`
`scripts/cargo_isolated.sh test --lib --quiet psql_describe_lookup_query_matches_wildcard_pattern`
`scripts/cargo_isolated.sh test --lib --quiet inherit`
`scripts/cargo_isolated.sh check`
`scripts/run_regression.sh --port 56000 --jobs 1 --test inherit --timeout 300 --results-dir /tmp/diffs/inherit_after_final2`
`scripts/run_regression.sh --port 56000 --jobs 1 --test inherit --timeout 300 --results-dir /tmp/diffs/inherit_after_final3`
`scripts/run_regression.sh --port 56000 --jobs 1 --test inherit --timeout 300 --results-dir /tmp/diffs/inherit_after_final5`
`scripts/run_regression.sh --port 56000 --jobs 1 --test inherit --timeout 300 --results-dir /tmp/diffs/inherit_after_final6`
`scripts/run_regression.sh --port 56000 --jobs 1 --test inherit --timeout 300 --results-dir /tmp/diffs/inherit_after_final7`
`scripts/run_regression.sh --port 56000 --jobs 1 --test inherit --timeout 300 --results-dir /tmp/diffs/inherit_after_final8`
`scripts/run_regression.sh --port 56000 --jobs 1 --test inherit --timeout 300 --results-dir /tmp/diffs/inherit_after_final9`

One contaminated regression run at `/tmp/diffs/inherit_after_final4` timed out and restart failed
because the filesystem was full. Removed stale `/tmp/diffs/tuplesort-after5`,
`/tmp/diffs/tuplesort-after6`, and `/tmp/diffs/tuplesort-after7`, restoring about 33G free.

Latest regression artifact:
`/tmp/diffs/inherit_after_final9`: 801/884 queries matched, 83 mismatched, 1513 diff lines.
Remaining buckets are dominated by planner/EXPLAIN shape and raw expression display, partitioned
ON CONFLICT support, multi-parent/deinherit NOT NULL `coninhcount`/`conislocal`, CHECK
connoinherit/localization edge cases, partition pruning/single-child Append collapse, partitioned
expression-index immutability/timing, and a few permission/join-filter display differences.

2026-04-28 third follow-up:
Added the PostgreSQL `pg_proc` row for immutable `left(text, int4)`, allowing the inherit
permission regression's partitioned expression index to be created. Render singleton `= ANY
(ARRAY[...])` predicates as plain equality for EXPLAIN, strip duplicate child aliases from
nonverbose index scan labels, render function calls containing OUTER/INNER join Vars without
`FuncExpr` debug fallback, and make planner sort-key display render common scalar functions such
as `abs(b)` recursively instead of `abs(Var(Var { ... }))`.

Additional tests run in this slice:
`scripts/cargo_isolated.sh test --lib --quiet explain_expr_renders_scalar_array_op_with_typed_array_literal`
`scripts/cargo_isolated.sh test --lib --quiet bootstrap_left_proc_row_matches_postgres_volatility`
`scripts/cargo_isolated.sh test --lib --quiet explain_partition_index_scan_does_not_duplicate_child_alias`
`scripts/cargo_isolated.sh test --lib --quiet explain_join_expr_renders_function_args_with_join_vars`
`scripts/cargo_isolated.sh test --lib --quiet explain_expr_matches_postgres_filter_formatting`
`scripts/cargo_isolated.sh test --lib --quiet executor::tests::explain`
`scripts/cargo_isolated.sh test --lib --quiet optimizer::tests`
`scripts/cargo_isolated.sh test --lib --quiet inherit`
`scripts/cargo_isolated.sh check`
`scripts/run_regression.sh --port 56000 --jobs 1 --test inherit --timeout 300 --results-dir /tmp/diffs/inherit_after_final10`
`scripts/run_regression.sh --port 56000 --jobs 1 --test inherit --timeout 300 --results-dir /tmp/diffs/inherit_after_final11`

Latest regression artifact:
`/tmp/diffs/inherit_after_final11`: 802/884 queries matched, 82 mismatched, 1310 diff lines.
`/tmp/diffs/inherit_after_final10` reported 809/884, but included a transient 5s timeout on a
400-row insert that changed later state, so use `final11` as the reliable baseline. Remaining
failures are still mainly planner path shape (`MergeAppend`/minmax/small-limit joins), partitioned
ON CONFLICT, NOT NULL inheritance metadata, CHECK localization/connoinherit, cascade detail order,
single-child partition Append collapse, and inherited composite-cast/projection display.

2026-04-28 fourth follow-up:
Parsed repeated `ALTER TABLE ... INHERIT` / `NO INHERIT` actions into typed AST nodes instead of
the generic SQL-string multi-action fallback, and executed all listed parents in one DDL
transaction. Applied inheritance catalog mutation effects immediately so follow-on metadata updates
see the new parent graph. Recomputed NOT NULL inheritance counts from direct parents using stable
relation shapes, fixed primary-key implied NOT NULL propagation to inheritors, preserved local
table-level NOT NULL state on inherited columns, avoided propagating drops from `NO INHERIT`
parent constraints, and aligned CHECK merge validation for enforced child rows.

Additional tests run in this slice:
`scripts/cargo_isolated.sh test --lib --quiet alter_table_inherit_statement`
`scripts/cargo_isolated.sh test --lib --quiet alter_table_no_inherit_statement`
`scripts/cargo_isolated.sh test --lib --quiet not_null_inheritability_recomputes_multi_parent_counts`
`scripts/cargo_isolated.sh test --lib --quiet inherited_table_level_not_null_remains_local`
`scripts/cargo_isolated.sh test --lib --quiet inherited_check_merge_validates_enforced_child_rows`
`scripts/cargo_isolated.sh test --lib --quiet inheritance_multi_parent_create_and_drop_clean_up_catalog_rows`
`scripts/cargo_isolated.sh test --lib --quiet alter_table_no_inherit_recomputes_multi_parent_column_and_not_null_metadata`
`scripts/cargo_isolated.sh test --lib --quiet not_null_inheritance_metadata_and_alter_constraint_inheritability`
`scripts/cargo_isolated.sh test --lib --quiet alter_not_null_inherit_creates_missing_child_constraints`
`scripts/cargo_isolated.sh test --lib --quiet no_inherit_not_null_blocks_primary_key`
`scripts/cargo_isolated.sh test --lib --quiet alter_table_inherit_validates_not_null_constraints`
`scripts/cargo_isolated.sh test --lib --quiet inherit`
`scripts/cargo_isolated.sh test --lib --quiet executor::tests::explain`
`scripts/cargo_isolated.sh test --lib --quiet optimizer::tests`
`scripts/cargo_isolated.sh check`
`scripts/run_regression.sh --port 56000 --jobs 1 --test inherit --timeout 300 --results-dir /tmp/diffs/inherit_after_final12`
`scripts/run_regression.sh --port 56000 --jobs 1 --test inherit --timeout 300 --results-dir /tmp/diffs/inherit_after_final13`
`scripts/run_regression.sh --port 56000 --jobs 1 --test inherit --timeout 300 --results-dir /tmp/diffs/inherit_after_final14`

Latest regression artifact:
`/tmp/diffs/inherit_after_final14`: 820/884 queries matched, 64 mismatched, 1159 diff lines.
This slice removed 18 mismatches from the reliable `final11` baseline. Remaining failures are now
mostly planner/EXPLAIN shape and display (`MergeAppend`, min/max, small LIMIT joins, raw sort-key
fallback), partitioned `ON CONFLICT DO UPDATE`, single-child partition Append collapse and default
pruning, inherited composite-cast projection display, cascade DETAIL ordering, ADD COLUMN merge
notice parity, and permission filter placement in inherited joins.
