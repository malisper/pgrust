Goal:
- Reject composite PL/pgSQL GET DIAGNOSTICS targets during CREATE FUNCTION.

Key decisions:
- Reuse resolved CREATE FUNCTION argument SqlType metadata rather than resolving names again in the PL/pgSQL parser.
- Keep validation limited to named function arguments with composite or record types.
- Add a protocol position heuristic so the error caret points at the GET DIAGNOSTICS assignment target.

Files touched:
- src/pl/plpgsql/mod.rs
- src/pgrust/database/commands/create.rs
- src/pgrust/database_tests.rs
- src/backend/tcop/postgres.rs

Tests run:
- cargo fmt
- scripts/cargo_isolated.sh test --lib --quiet plpgsql_get_diagnostics_rejects_composite_target
- scripts/cargo_isolated.sh test --lib --quiet plpgsql
- scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55434 --results-dir /tmp/diffs/plpgsql-get-diagnostics-target-final

Remaining:
- Regression result is 2200/2271 with 863 diff lines.
- The GET DIAGNOSTICS composite-target hunk now matches, including LINE/caret output.
