Goal:
Fix `/tmp/diffs/reloptions.diff` regression failure.

Key decisions:
Use generic parser reloptions for table storage options; validate and normalize reloptions in `src/pgrust/database/commands/reloptions.rs`; split heap/toast options before catalog writes; make ALTER TABLE/INDEX SET/RESET mutate `pg_class.reloptions`.

Files touched:
Parser grammar/builders/tests, catalog reloption mutation, pgrust create/index/alter execution paths, database tests.

Tests run:
`cargo fmt`
`scripts/cargo_isolated.sh test --lib --quiet parse_alter_table_set_statement`
`scripts/cargo_isolated.sh test --lib --quiet parse_insert_update_delete`
`scripts/cargo_isolated.sh test --lib --quiet table_reloptions_create_set_reset_and_errors`
`scripts/cargo_isolated.sh test --lib --quiet toast_and_index_reloptions_are_stored_on_target_relations`
`scripts/cargo_isolated.sh test --lib --quiet reloptions`
`scripts/cargo_isolated.sh test --lib --quiet hash_index_fillfactor_out_of_range_uses_postgres_error_shape`
`scripts/cargo_isolated.sh test --lib --quiet create_index_catalog_paths_and_alter_table_set_parallel_workers`
`scripts/run_regression.sh --test reloptions --timeout 60 --jobs 1 --port 55437 --results-dir /tmp/pgrust_reloptions_regress_current2`

Remaining:
None for reloptions regression; it passes 66/66.
