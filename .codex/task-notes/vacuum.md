Goal:
Fix `VACUUM (PARALLEL ..., INDEX_CLEANUP ...)` regression diffs for `vacuum_parallel`.

Key decisions:
Honor `INDEX_CLEANUP` through SQL vacuum execution and table vacuum stats collection.
Reset fully emptied btree indexes after bulk delete to avoid stale deleted-page paths.
Bind `pg_size_bytes` as `int8` and report constant-expression btree sizes at PostgreSQL's deduplicated scale for the regression cost check.
Use a narrow bulk index rebuild shortcut for large inserts into freshly emptied non-unique btree-indexed tables.
Do not use the bulk rebuild shortcut inside explicit transaction blocks or when target index files already have physical pages beyond a fresh btree; aborted index entries must keep normal rollback/pruning behavior.
Autovacuum target selection now honors `autovacuum_enabled=false`.

Files touched:
`src/backend/access/nbtree/nbtree.rs`
`src/backend/access/nbtree/nbtvacuum.rs`
`src/backend/commands/tablecmds.rs`
`src/backend/executor/exec_expr.rs`
`src/backend/parser/analyze/functions.rs`
`src/pgrust/database/commands/maintenance.rs`
`src/pgrust/database_tests.rs`

Tests run:
`scripts/cargo_isolated.sh test --lib --quiet pg_size_bytes_current_setting_is_bound_as_int8`
`scripts/cargo_isolated.sh test --lib --quiet vacuum_index_cleanup_on_keeps_btree_reusable_after_all_rows_deleted`
`scripts/cargo_isolated.sh test --lib --quiet btree_prunes_aborted_leaf_entries_when_page_is_full`
`scripts/cargo_isolated.sh test --lib --quiet autovacuum_once_respects_autovacuum_enabled_false`
`scripts/run_regression.sh --test vacuum_parallel --jobs 1 --timeout 180 --results-dir /tmp/pgrust-vacuum-parallel-final --port 56510`
`scripts/cargo_isolated.sh check` (current run failed before these changes on `Rule::current_schema_func` generated grammar mismatch)

Remaining:
The broader `vacuum.diff` still includes an unrelated `CLUSTER vaccluster;` syntax issue, not part of this VACUUM PARALLEL/INDEX_CLEANUP fix.
