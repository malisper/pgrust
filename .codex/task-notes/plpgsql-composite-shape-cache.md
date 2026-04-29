Goal:
Close PL/pgSQL composite-return and RETURN QUERY rowtype diffs after table shape changes.
Key decisions:
Clear each session's PL/pgSQL function cache on catalog invalidation so cached static plans and row contracts recompile after DDL.
Use the caller-provided composite row shape when coercing fixed-row PL/pgSQL results.
Reject scalar values returned from composite-returning functions with PostgreSQL's non-composite return error.
Files touched:
src/backend/utils/cache/inval.rs
src/pgrust/database.rs
src/pgrust/database_tests.rs
src/pl/plpgsql/cache.rs
src/pl/plpgsql/exec.rs
Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet plpgsql_composite_return_handles_null_and_scalar_mismatch
scripts/cargo_isolated.sh test --lib --quiet plpgsql_return_query_uses_current_composite_shape
scripts/cargo_isolated.sh test --lib --quiet plpgsql
scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55434 --results-dir /tmp/diffs/plpgsql-composite-shape-cache-final
Remaining:
Regression is 2231/2271 matched with 470 diff lines. Remaining clusters include unknown-vs-varchar composite return typing, exception/RAISE context formatting, string escape warnings, dynamic DO exception loop notices, and transition table support.
