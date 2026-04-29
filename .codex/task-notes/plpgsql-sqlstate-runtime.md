Goal:
Fix PL/pgSQL exception handlers that catch direct expression errors by SQLSTATE, especially WHEN SQLSTATE '22012'.

Key decisions:
Added PlannerConfig::fold_constants and disabled constant folding only for PL/pgSQL static query plans so errors like PERFORM 1/0 occur at function runtime, where exception handlers can catch them.
Kept normal session/query planning constant folding enabled by default.
Restored SQLSTATE and SQLERRM slot values after nested exception handlers so outer handler diagnostics survive inner handlers.

Files touched:
src/backend/parser/analyze/mod.rs
src/include/nodes/pathnodes.rs
src/pgrust/session.rs
src/pl/plpgsql/compile.rs
src/pl/plpgsql/exec.rs
src/pl/plpgsql/gram.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet plpgsql_exception_sqlstate_condition_matches_error_code
scripts/cargo_isolated.sh test --lib --quiet parse_exception_sqlstate_condition
scripts/cargo_isolated.sh test --lib --quiet plpgsql
scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55434 --results-dir /tmp/diffs/plpgsql-sqlstate-runtime-final

Remaining:
Clean regression baseline is 2167/2271 matched, 1227 diff lines.
Use port 55434 for this workspace if 55433 is occupied by another Conductor worktree.
