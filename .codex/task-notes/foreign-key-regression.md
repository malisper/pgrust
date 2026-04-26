Goal:
Diagnose the foreign_key regression diff in .context/attachments/pasted_text_2026-04-26_15-20-26.txt.

Key decisions:
Grouped failures by root subsystem instead of treating later hunks as independent; many late partitioned-table failures cascade from earlier unsupported DROP COLUMN / partitioned FK DDL.
Fixed the first two selected issues: referenced unique/primary keys can now match by key-column set instead of exact order, and insert-time self-referential FK checks run after statement heap + index insertion so same-row and same-statement references are visible.
Committed those fixes as 2ea2b7ab9.
Started the next two contained mismatches: pg_get_constraintdef() now renders FK match/action clauses plus ON DELETE SET column lists, and generated constraint names now keep PostgreSQL-style table/name/label parts so 63-byte clipping preserves labels and suffixes collisions.

Files touched:
.codex/task-notes/foreign-key-regression.md
src/backend/parser/analyze/constraints.rs
src/backend/executor/foreign_keys.rs
src/backend/executor/mod.rs
src/backend/commands/tablecmds.rs
src/backend/executor/exec_expr.rs
src/backend/tcop/postgres.rs
src/pgrust/database_tests.rs

Tests run:
scripts/cargo_isolated.sh test --lib --quiet foreign_keys_can_reference_unique_key_columns_out_of_order
scripts/cargo_isolated.sh test --lib --quiet self_referential_foreign_key_can_reference_inserted_row
scripts/cargo_isolated.sh test --lib --quiet create_table_foreign_keys_are_enforced_and_persisted
scripts/cargo_isolated.sh test --lib --quiet alter_table_add_foreign_key_supports_match_full
scripts/cargo_isolated.sh test --lib --quiet lower_create_table_resolves_self_referential_foreign_keys_against_pending_primary_key
scripts/cargo_isolated.sh test --lib --quiet foreign_keys_
scripts/cargo_isolated.sh test --lib --quiet create_temp_table_constraints_are_supported_with_postgres_persistence_rules
scripts/cargo_isolated.sh check
scripts/run_regression.sh --test foreign_key --timeout 60 --results-dir /tmp/pgrust_regress_foreign_key_fix (did not reach foreign_key; server died during shared test_setup bootstrap around CREATE TABLE tenk2 AS SELECT * FROM tenk1)
scripts/cargo_isolated.sh test --lib --quiet generated_foreign_key_names_preserve_label_when_truncated_and_suffix_collisions
scripts/cargo_isolated.sh test --lib --quiet pg_get_constraintdef_formats_foreign_key_actions_and_delete_columns
scripts/cargo_isolated.sh test --lib --quiet foreign_key
scripts/cargo_isolated.sh check
scripts/run_regression.sh --test foreign_key --timeout 60 --results-dir /tmp/pgrust_regress_foreign_key_formatting (did not reach foreign_key; pgrust setup bootstrap failed after CREATE TABLE tenk2 AS SELECT * FROM tenk1)

Remaining:
Likely fixes still span pg_trigger RI metadata, deferred transaction state, referential action ordering edge cases, dropped columns, partitioned FK support, and the shared regression setup crash before the foreign_key file runs.
