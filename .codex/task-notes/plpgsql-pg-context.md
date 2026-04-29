Goal:
Fix GET DIAGNOSTICS PG_CONTEXT live PL/pgSQL call stack output.

Key decisions:
Use a lightweight thread-local PL/pgSQL context stack populated from compiled
line wrapper frames. Omit block frames so PG_CONTEXT matches PostgreSQL output
for nested calls.

Files touched:
src/pl/plpgsql/exec.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet plpgsql_get_diagnostics_pg_context_reports_live_stack
scripts/cargo_isolated.sh test --lib --quiet plpgsql
scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55434 --results-dir /tmp/diffs/plpgsql-pg-context-final-rerun

Remaining:
Latest rerun reported 2191/2271 matched and 953 diff lines with unrelated
transition-table timeout noise. The targeted PG_CONTEXT hunk is clear. Remaining
clusters include declaration-default scoping/context, transition tables,
composite/record returns, WHERE CURRENT OF, cursor validation formatting,
SELECT INTO gaps, and exact error/warning/notice formatting.
