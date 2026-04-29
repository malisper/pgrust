Goal:
Close remaining stats_ext catalog/display/DDL-adjacent gaps after the selectivity fix.

Key decisions:
Keep the follow-up narrow: warn and skip uncomputable extended stats during ANALYZE, suppress noisy CREATE STATISTICS unknown-column carets, support PL/pgSQL declaration defaults written as query expressions, and make dynamic CREATE STATISTICS throw catchable wrong_object_type errors for unsupported relation kinds.

Files touched:
src/backend/commands/analyze.rs
src/backend/executor/driver.rs
src/backend/executor/exec_expr.rs
src/backend/executor/expr_casts.rs
src/backend/tcop/postgres.rs
src/pgrust/database_tests.rs
src/pl/plpgsql/compile.rs
src/pl/plpgsql/exec.rs

Tests run:
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet plpgsql_decl_default_accepts_query_expression
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet analyze_warns_when_extended_statistics_cannot_be_computed
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet exec_error_position_
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh check
PGRUST_STATEMENT_TIMEOUT=30 CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/run_regression.sh --test stats_ext --timeout 300 --port 60668

Remaining:
stats_ext still fails with 70 mismatched SQL blocks and 538 diff lines. Remaining clusters include residual selectivity mismatches, MCV estimate drift, \dX expression cast formatting, schema privilege and pg_stats_ext visibility gaps, unsupported GRANT/REVOKE CREATE ON SCHEMA parsing, and EXPLAIN plan formatting/order differences.
