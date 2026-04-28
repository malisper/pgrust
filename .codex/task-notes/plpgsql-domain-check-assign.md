Goal:
Fix PL/pgSQL DO assignment to domain variables so catalog-backed domain checks run.

Key decisions:
Reuse the existing table-domain enforcement helper after PL/pgSQL assignment casts. Route executor-level DO handling through the context-aware PL/pgSQL path when a catalog and executor context are available.

Files touched:
src/backend/commands/tablecmds.rs
src/backend/executor/driver.rs
src/pl/plpgsql/exec.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet plpgsql_do_assignment_enforces_function_domain_checks
scripts/cargo_isolated.sh test --lib --quiet plpgsql
scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55434 --results-dir /tmp/diffs/plpgsql-domain-check-assign

Remaining:
Latest regression baseline is 2187/2271 matched with 1013 diff lines. Remaining clusters include PG_CONTEXT stack text, composite/record returns, WHERE CURRENT OF, transition tables, cursor/current-of syntax, and formatting mismatches.
