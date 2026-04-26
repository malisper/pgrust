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
