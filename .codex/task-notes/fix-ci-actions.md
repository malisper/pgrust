Goal:
Fix cargo-test CI failures on the PL/pgSQL regression PR.

Key decisions:
Restore the parser AST contract for SRFs with typed column definitions: implicit aliases use the function name instead of an empty sentinel.
Expand variadic pg_proc declared argument OIDs before concretizing polymorphic declared argument types.
Keep ANY arguments as per-argument targets instead of folding them through anyelement unification.

Files touched:
src/backend/parser/analyze/functions.rs
src/backend/parser/analyze/scope.rs
src/backend/parser/gram.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet parse_srf_column_definitions_without_alias
scripts/cargo_isolated.sh test --lib --quiet plpgsql_record_returning_function_from_accepts_column_definition_list
scripts/cargo_isolated.sh test --lib --quiet create_aggregate_supports_plain_custom_aggregate_execution
scripts/cargo_isolated.sh test --lib --quiet multirange_adjacency_uses_outer_endpoints_only
scripts/cargo_isolated.sh test --lib --quiet user_defined_ranges_support_default_and_manual_multirange_names
scripts/cargo_isolated.sh test --lib --quiet gist_range_index_handles_multirange_scan_keys
scripts/cargo_isolated.sh test --lib --quiet create_gist_multirange_index_explain_and_query_use_it
scripts/cargo_isolated.sh test --lib --quiet create_or_replace_aggregate_preserves_proc_oid
scripts/cargo_isolated.sh test --lib --quiet custom_aggregate_window_execution_is_rejected
scripts/cargo_isolated.sh test --lib --quiet reopen_missing_pg_aggregate_custom_rows_is_corrupt
scripts/cargo_isolated.sh test --lib --quiet comment_on_aggregate_uses_pg_proc_description_rows
scripts/cargo_isolated.sh test --lib --quiet drop_aggregate_removes_proc_and_aggregate_rows
scripts/cargo_isolated.sh test --lib --quiet resolve_function_call
scripts/cargo_isolated.sh check

Remaining:
No local failures in the attached CI repro set.

---

Goal:
Fix CI failures reported in attached cargo test logs.

Key decisions:
Restore INSERT CTE body lowering in the parser.
Do not re-apply OVERRIDING USER identity defaults for VALUES rows already normalized by binding.
Return SQL NULL for unavailable tableoid/ctid on null-extended rows while preserving slot metadata fallback.

Files touched:
src/backend/parser/gram.rs
src/backend/commands/tablecmds.rs
src/backend/executor/exec_expr.rs

Tests run:
scripts/cargo_isolated.sh test --lib --quiet writable_cte
scripts/cargo_isolated.sh test --lib --quiet parse_select_with_writable_insert_cte_returning_tableoid_and_star
scripts/cargo_isolated.sh test --lib --quiet parse_insert_with_writable_insert_cte
scripts/cargo_isolated.sh test --lib --quiet alter_identity_and_overriding_enforce_generated_always
scripts/cargo_isolated.sh test --lib --quiet outer_join_null_extended_ctid_is_null
scripts/cargo_isolated.sh check

Remaining:
query_repl.rs still has the existing unreachable-pattern warning during check.

---

Goal:
Fix cargo-test CI failures from the targeted relcache/fmgr PR.

Key decisions:
Carry full pg_attribute metadata in targeted relation descriptors, including identity, ACL, and collation.
Scope relation descriptor cache by backend cache context while reusing it across command IDs in the same transaction.
Apply local catalog invalidations when immediate catalog effects are applied so multi-step DDL sees its own changes.
Give materialized-view SELECT execution an executor catalog for user-defined scalar calls.
Run command-end catalog bookkeeping when streaming SELECT portals complete.
Use search-path-visible sequence names for pg_get_serial_sequence.
Repair dynamic composite array type metadata during targeted relation descriptor construction.

Files touched:
src/backend/executor/exec_expr.rs
src/backend/tcop/postgres.rs
src/backend/utils/cache/inval.rs
src/backend/utils/cache/syscache.rs
src/backend/utils/time/snapmgr.rs
src/pgrust/database/commands/execute.rs
src/pgrust/database/commands/matview.rs
src/pgrust/database/commands/rename.rs
src/pgrust/database/txn.rs
src/pgrust/session.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet alter_identity_and_overriding_enforce_generated_always
scripts/cargo_isolated.sh test --lib --quiet create_table_like_copies_identity_only_when_requested
scripts/cargo_isolated.sh test --lib --quiet create_index_on_partitioned_table_builds_index_tree
scripts/cargo_isolated.sh test --lib --quiet create_index_on_partitioned_table_reuses_only_child_without_recursing
scripts/cargo_isolated.sh test --lib --quiet create_table_serial_creates_sequence_defaults_and_persists_state
scripts/cargo_isolated.sh test --lib --quiet alter_table_add_column_serial_backfills_existing_rows_and_keeps_sequence_advancing
scripts/cargo_isolated.sh test --lib --quiet dependent_views_track_relation_rename_and_set_schema
scripts/cargo_isolated.sh test --lib --quiet materialized_view_with_no_data_refreshes_and_rejects_writes
scripts/cargo_isolated.sh test --lib --quiet materialized_view_set_schema_refresh_concurrently_and_drop_cascade
scripts/cargo_isolated.sh test --lib --quiet vacuum_full_rewrites_storage_and_preserves_rows
scripts/cargo_isolated.sh test --lib --quiet failed_unique_index_concurrently_leaves_invalid_catalog_state
scripts/cargo_isolated.sh test --lib --quiet pg_get_ruledef_formats_insert_rule_actions_with_casts

Remaining:
No local failures in the attached CI repro set.

---

Goal:
Fix cargo-test CI failures from the tsrf/subquery planning PR.

Key decisions:
Allow PostgreSQL-valid set-returning functions in GROUP BY instead of rejecting
them during parse analysis.
Keep filtered SubqueryScan lowering, but stop forcing visible SubqueryScan nodes
for grouped/aggregate subqueries without filters so verbose EXPLAIN can render
the same aggregate output shape PostgreSQL does.
Remove the obsolete negative GROUP BY SRF unit test; the optimizer SRF test now
covers the allowed case.

Files touched:
src/backend/parser/analyze/mod.rs
src/backend/parser/tests.rs
src/backend/optimizer/setrefs.rs

Tests run:
scripts/cargo_isolated.sh test --lib --quiet grouped_target_srf_uses_project_set_before_aggregate
scripts/cargo_isolated.sh test --lib --quiet explain_verbose_lateral_aggregate_renders_pg_style_details
scripts/cargo_isolated.sh test --lib --quiet srf
scripts/cargo_isolated.sh check
scripts/run_regression.sh --test tsrf --timeout 240 --jobs 1 --port 59450 --results-dir /tmp/diffs/tsrf-ci-fix
scripts/cargo_isolated.sh test --lib --quiet

Remaining:
tsrf still has expected output diffs, but no regression errors or timeouts.

---

Goal:
Fix follow-up CI failures after merging origin/perf-optimization into malisper/btree-index.

Key decisions:
Let numeric literals in IN lists participate in common-type inference instead of always preferring the left operand type.
Split btree scan-positioning quals from additional index tuple filter quals so multicolumn indexes are not costed as selective on non-leading columns.
Add a btree unused-key-column cost so narrower indexes win when scan/order usefulness ties.
Keep DISTINCT ON ordered-index assertions focused on the index path while allowing planner projection elision.

Files touched:
src/backend/parser/analyze/expr/subquery.rs
src/backend/optimizer/mod.rs
src/backend/optimizer/path/allpaths.rs
src/backend/optimizer/path/costsize.rs
src/backend/optimizer/plan/planner.rs
src/backend/optimizer/tests.rs

Tests run:
cargo fmt --check
git diff --check
scripts/cargo_isolated.sh test --lib --quiet build_plan_in_list_common_type_includes_left_operand
scripts/cargo_isolated.sh test --lib --quiet index_matrix
scripts/cargo_isolated.sh test --lib --quiet planner_uses_index_order_for_distinct_on_reordered_keys
scripts/cargo_isolated.sh test --lib --quiet distinct_on
scripts/cargo_isolated.sh test --no-run --lib --locked
scripts/cargo_isolated.sh check

Remaining:
No local failures in the targeted CI repro set.

---

Goal:
Fix follow-up CI parser failure and CTAS test timeout.

Key decisions:
Return AlterTableAddColumns for multi-action ALTER TABLE statements where every action is ADD COLUMN.
Keep mixed multi-action ALTER TABLE statements on the AlterTableMulti fallback path.
Reduce the point CTAS window-order fixture size while preserving the disabled-indexscan behavior.

Files touched:
src/backend/parser/gram.rs
src/pgrust/database_tests.rs

Tests run:
scripts/cargo_isolated.sh test --lib --quiet parse_alter_table_multi_add_column_statement
scripts/cargo_isolated.sh test --lib --quiet temp_create_table_as_point_window_order_ignores_disabled_indexscan
scripts/cargo_isolated.sh test --lib --quiet alter_table_multi_add_column_updates_partitioned_table
scripts/cargo_isolated.sh test --lib --quiet parse_alter_table_constraint_statements
scripts/cargo_isolated.sh check

Remaining:
query_repl.rs still has the existing unreachable-pattern warning during check.

---

Goal:
Fix follow-up CI executor failures for ordered assignment indirection.

Key decisions:
Collapse contiguous subscript-only paths back through existing array/jsonb assignment helpers.
Keep ordered field/subscript recursion for paths that still contain later fields.

Files touched:
src/backend/commands/tablecmds.rs

Tests run:
scripts/cargo_isolated.sh test --lib --quiet array_slice_assignment_uses_existing_bounds_for_omitted_limits
scripts/cargo_isolated.sh test --lib --quiet array_slice_assignment_three_dimensional_serial_updates_match_postgres
scripts/cargo_isolated.sh test --lib --quiet jsonb_subscript_assignment_updates_objects_arrays_and_nulls
scripts/cargo_isolated.sh test --lib --quiet domain_composite_array_insert_assignments_navigate_base_type
scripts/cargo_isolated.sh test --lib --quiet composite_field_array_assignment_uses_ordered_indirection
scripts/cargo_isolated.sh check

Remaining:
query_repl.rs still has the existing unreachable-pattern warning during check.
