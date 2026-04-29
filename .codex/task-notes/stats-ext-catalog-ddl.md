Goal:
Fix stats_ext catalog/display and DDL-support gaps after selectivity parity work.

Key decisions:
Expose real pg_statistic_ext_data rows, keep existing serialized stats payloads, and add only private/native display helpers. Allow ALTER COLUMN TYPE through plain dependent indexes but keep expression/partial index rejection as a compatibility shim.

Files touched:
src/backend/catalog/store/heap.rs
src/backend/executor/exec_expr.rs
src/backend/executor/srf.rs
src/backend/executor/value_io/array.rs
src/backend/parser/gram.rs
src/backend/parser/tests.rs
src/backend/statistics/build.rs
src/backend/statistics/types.rs
src/backend/tcop/postgres.rs
src/include/catalog/pg_proc.rs
src/pgrust/database/commands/alter_column_type.rs
src/pgrust/database/commands/create_statistics.rs
src/pgrust/database_tests.rs

Tests run:
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet pg_mcv_list_items_decodes_extended_statistics_payload
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet psql_list_statistics_query_formats_relation_names
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet parse_create_statistics_without_explicit_name_with_kinds
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet create_statistics_rejects_virtual_generated_and_system_column_expressions
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet create_statistics_reports_postgres_relation_kind_errors
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet alter_table_alter_column_type_rebuilds_plain_indexed_target_column
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh check
PGRUST_STATEMENT_TIMEOUT=30 CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/run_regression.sh --test stats_ext --timeout 300 --port 60663

Remaining:
stats_ext still has 74 mismatched queries: residual selectivity estimates, missing ANALYZE warnings, PL/pgSQL DECLARE reltoastrelid lookup, privilege/schema GRANT and custom operator gaps, pg_stats_ext view privilege/MCV rendering, pg_get_statisticsobjdef expression cast deparse, and EXPLAIN plan/format differences.
