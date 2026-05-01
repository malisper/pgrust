Goal:
Fix TASK-C13-03 for the `vacuum` regression slice: VACUUM option forms,
`vacuum_index_cleanup` reloptions, and the `vac_option_tab_counts` helper-view
setup around `pg_stat_all_tables`.

Key decisions:
Accepted `vacuum_index_cleanup` as a heap and toast table reloption, normalizing
`auto` and boolean spellings to PostgreSQL-compatible stored reloption values.
Kept original SQL for views that reference `pg_stat_all_tables` because
deparsing the synthetic stats view can exceed the current un-toasted
`pg_rewrite.ev_action` tuple limit. Those views intentionally reparse from SQL
at execution time so live vacuum statistics are not frozen at CREATE VIEW time.

Files touched:
src/pgrust/database/commands/create.rs
src/pgrust/database/commands/reloptions.rs
src/pgrust/database_tests.rs
.codex/task-notes/task-c13-03-vacuum.md

Tests run:
cargo fmt
CARGO_TARGET_DIR=/tmp/pgrust-target-pool/beirut-v3-vacuum-test scripts/cargo_isolated.sh test --lib --quiet vacuum_process_counts_helper_view_is_creatable
CARGO_TARGET_DIR=/tmp/pgrust-target-pool/beirut-v3-vacuum-test scripts/cargo_isolated.sh test --lib --quiet vacuum_index_cleanup_reloptions_accept_heap_and_toast_values
CARGO_TARGET_DIR=/tmp/pgrust-target-pool/beirut-v3-vacuum-test scripts/cargo_isolated.sh check
CARGO_TARGET_DIR=/tmp/pgrust-target-pool/beirut-v3-vacuum-test scripts/run_regression.sh --test vacuum --port 55439 --results-dir /tmp/pgrust-task-c13-03-vacuum

Remaining:
Focused `vacuum` regression now matches 326/328 queries. Remaining diff is
outside this slice: `CLUSTER vaccluster` still has unsupported syntax, and the
expression-index ANALYZE error is consequently aligned later in the file.
