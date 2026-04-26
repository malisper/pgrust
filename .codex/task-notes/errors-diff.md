Goal:
Implement PostgreSQL-compatible fixes for the errors regression diffs.
Key decisions:
Added explicit ParseError positions and made tcop prefer them over token rfind fallbacks. Kept parser/DDL behavior fixes in their owning layers, with a narrow :HACK: in tcop for exact semicolon/end-of-input compatibility cases still reduced too early by pgrust parsing.
Files touched:
src/include/nodes/parsenodes.rs
src/backend/parser/gram.pest
src/backend/parser/gram.rs
src/backend/parser/analyze/mod.rs
src/backend/parser/tests.rs
src/backend/tcop/postgres.rs
src/backend/libpq/pqformat.rs
src/pgrust/session.rs
src/pgrust/database/ddl.rs
src/pgrust/database/commands/create.rs
src/pgrust/database/commands/drop.rs
src/pgrust/database/commands/rename.rs
src/pgrust/database/commands/rules.rs
src/pgrust/database_tests.rs
Tests run:
cargo fmt
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet parser
scripts/cargo_isolated.sh test --lib --quiet tcop
scripts/cargo_isolated.sh test --lib --quiet database_tests (0 tests matched this filter)
scripts/cargo_isolated.sh test --lib --quiet alter_table_rename_column_reports_postgres_errors
scripts/cargo_isolated.sh test --lib --quiet drop_missing_index_reports_index_does_not_exist
scripts/cargo_isolated.sh test --lib --quiet abort_outside_transaction_emits_warning
scripts/run_regression.sh --schedule /tmp one-test errors schedule --test errors --port 56573 --timeout 300
Remaining:
errors regression passes 87/87. scripts/cargo_isolated.sh check still reports the pre-existing query_repl unreachable-pattern warning.
