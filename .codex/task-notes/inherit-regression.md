Goal:
Implement fixes for PostgreSQL `inherit` regression parity without editing expected outputs.

Key decisions:
- Fixed the high-noise catalog `IN` leak by preserving base inner index quals when creating parameterized nested-loop inner index paths.
- Expanded scalar-array equality scan keys for bitmap index scans so `IN` predicates can drive bitmap scans correctly.
- Fixed inherited UPDATE/DELETE and SELECT constraint exclusion for simple constant equality/inequality contradictions, including parent-local `CHECK ... NO INHERIT` behavior.
- Fixed inherited CHECK drop propagation through multi-parent grandchildren by refreshing child constraint lookups during recursion, matching by constraint name like PostgreSQL, and avoiding the relation-specific constraint cache when it misses just-updated rows inside the same command.
- Added hint control for NOT NULL propagation conflicts so `ALTER CONSTRAINT ... INHERIT` can omit the hint where PostgreSQL does.
- Added the missing inherited CHECK merge notice for `ALTER TABLE parent ADD COLUMN ... CHECK` when a child receives the new constraint from multiple parents.

Files touched:
- `src/backend/executor/nodes.rs`
- `src/backend/commands/tablecmds.rs`
- `src/backend/optimizer/path/allpaths.rs`
- `src/backend/optimizer/path/costsize.rs`
- `src/pgrust/database/commands/constraint.rs`
- `src/pgrust/database/commands/maintenance.rs`
- `src/pgrust/database_tests.rs`

Tests run:
- `cargo fmt`
- `scripts/cargo_isolated.sh test --lib --quiet catalog_relname_in_filter_applies_to_joined_constraint_rows`
- `scripts/cargo_isolated.sh test --lib --quiet explain_update_accepts_inherited_update_statement`
- `scripts/cargo_isolated.sh test --lib --quiet alter_table_drop_check_constraint_updates_multi_parent_grandchildren`
- `scripts/cargo_isolated.sh test --lib --quiet alter_table_drop_check_constraint_updates_inherited_children`
- `scripts/cargo_isolated.sh test --lib --quiet inherited_table_level_not_null_remains_local`
- `scripts/cargo_isolated.sh test --lib --quiet alter_table_add_column_check_notices_multi_parent_constraint_merge`
- `scripts/run_regression.sh --test inherit --results-dir '/Volumes/OSCOO PSSD/pgrust/diffs/inherit-after-preserve-index-quals'`: `819/884`, `1188` diff lines.
- `scripts/run_regression.sh --test inherit --results-dir '/Volumes/OSCOO PSSD/pgrust/diffs/inherit-after-constraint-drop'`: `821/884`, `1184` diff lines.
- `scripts/run_regression.sh --test inherit --results-dir '/Volumes/OSCOO PSSD/pgrust/diffs/inherit-after-add-column-notice'`: `822/884`, `1176` diff lines.

Remaining:
- `ALTER COLUMN ... DROP NOT NULL` still errors in two inheritance cases, leaving inherited NOT NULL metadata displayed.
- Several `DROP ... CASCADE` notice-order mismatches remain.
- Larger remaining diff is planner/EXPLAIN parity: UPDATE plan shape, composite row casts, Append/MergeAppend/index path choices, min/max inherited optimization, partition pruning for list subpartitions, alias/rendering differences, and ACL qual placement in joins.
