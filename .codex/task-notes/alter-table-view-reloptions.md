Goal:
Fix PostgreSQL-compatible ALTER TABLE/VIEW RESET behavior for relation storage reloptions on views, especially autovacuum_enabled in alter_table regression.

Key decisions:
PostgreSQL validates view reloptions after RESET removes matching stored entries, so RESET of storage options that cannot be SET on views is accepted as a no-op.
Keep SET validation strict for views.
Allow the hand-written ALTER VIEW RESET parser to accept one reloption namespace qualifier, matching PostgreSQL reloption grammar.

Files touched:
src/pgrust/database/commands/alter_column_options.rs
src/backend/parser/gram.rs
src/backend/parser/tests.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
CARGO_TARGET_DIR=/tmp/pgrust-target-pool/montpellier-v2-relopts scripts/cargo_isolated.sh test --lib --quiet view_reset_accepts_storage_reloptions_as_noops
CARGO_TARGET_DIR=/tmp/pgrust-target-pool/montpellier-v2-relopts scripts/cargo_isolated.sh test --lib --quiet parse_alter_table_set_statement
CARGO_TARGET_DIR=/tmp/pgrust-target-pool/montpellier-v2-relopts scripts/run_regression.sh --test alter_table --jobs 1 --timeout 120 --port 56431 --results-dir /tmp/pgrust-alter-table-view-relopts

Remaining:
The targeted autovacuum_enabled RESET hunk is fixed in /tmp/pgrust-alter-table-view-relopts/output/alter_table.out.
The full alter_table regression still times out later: 1206/1683 queries matched, with unrelated lock/dependency/output differences.
