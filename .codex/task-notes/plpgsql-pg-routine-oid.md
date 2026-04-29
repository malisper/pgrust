Goal:
Implement `GET DIAGNOSTICS ... = PG_ROUTINE_OID` for PL/pgSQL functions and DO blocks.

Key decisions:
- Store the compiled function OID in `CompiledFunction` so non-stacked diagnostics can report the current routine.
- Return `0` for `PG_ROUTINE_OID` inside anonymous DO blocks, matching PostgreSQL behavior.
- Keep stacked diagnostics unchanged; this slice only covers current routine diagnostics.

Files touched:
- `src/pl/plpgsql/compile.rs`
- `src/pl/plpgsql/exec.rs`
- `src/pgrust/database_tests.rs`

Tests run:
- `cargo fmt`
- `scripts/cargo_isolated.sh test --lib --quiet plpgsql_get_diagnostics_pg_routine_oid_reports_current_function`
- `scripts/cargo_isolated.sh test --lib --quiet plpgsql`
- `scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55434 --results-dir /tmp/diffs/plpgsql-pg-routine-oid`

Remaining:
- Regression improved to `2176/2271` matched, `1134` diff lines.
- `regprocedure` output still prints only the function name in normal query output for this case.
