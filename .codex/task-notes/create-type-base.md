Goal:
Support base CREATE TYPE definitions such as widget, city_budget, int42, text_w_default, and myvarchar enough to unblock the early create_type regression section.

Key decisions:
Parse CREATE TYPE name (...) into a base-type AST variant. Complete an existing shell pg_type row into a text-backed physical base type row, create the implicit array type, validate input/output proc signatures, record support dependencies, and keep type-level defaults in Database::base_types for table creation.

Files touched:
src/include/nodes/parsenodes.rs
src/backend/parser/gram.rs
src/backend/parser/tests.rs
src/backend/catalog/store/heap.rs
src/backend/catalog/rowcodec.rs
src/backend/parser/analyze/mod.rs
src/backend/parser/analyze/create_table.rs
src/backend/utils/cache/lsyscache.rs
src/pgrust/database.rs
src/pgrust/cluster.rs
src/pgrust/database/commands/typecmds.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
cargo test --lib --quiet parse_create_type_supports_base_enum_and_range_forms
cargo test --lib --quiet create_type_base_completes_shell_and_applies_type_default
cargo check
bash scripts/run_regression.sh --test create_type --timeout 120 --port 55433 --results-dir /tmp/diffs/create_type_base_55433

Remaining:
create_type regression still fails: 43/86 matched. Main remaining failures are pg_type compatibility columns, user-defined type runtime input/output behavior, ALTER TYPE SET, dependency/cascade behavior, COMMENT ON TYPE/COLUMN support, CREATE TEMP TABLE typmod forms, custom operator/type-literal SELECT syntax, notice caret lines, and existing format_type bpchar mismatch.
