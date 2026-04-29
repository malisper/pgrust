Goal:
Preserve original PL/pgSQL error context when `RAISE;` rethrows an active exception.

Key decisions:
`PlpgsqlExceptionData` already stores the original context for diagnostics. `exception_data_to_error` now wraps the reconstructed error with that stored context so the statement wrapper does not replace it with the `RAISE;` line.

Files touched:
src/pl/plpgsql/exec.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
CARGO_INCREMENTAL=0 scripts/cargo_isolated.sh test --lib --quiet plpgsql_reraise_preserves_original_error_context
CARGO_INCREMENTAL=0 scripts/cargo_isolated.sh test --lib --quiet plpgsql
CARGO_INCREMENTAL=0 scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55434 --results-dir /tmp/diffs/plpgsql-reraise-context
PGRUST_STATEMENT_TIMEOUT=10 CARGO_INCREMENTAL=0 scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55434 --results-dir /tmp/diffs/plpgsql-reraise-context-10s

Remaining:
The standard 5s regression run timed out in the pre-existing transition-table bulk insert, producing a noisy 2236/2271 result. With 10s statement timeout, the slice reaches 2241/2271 matched and 372 diff lines.
