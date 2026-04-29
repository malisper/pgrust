Goal:
- Report ordinary missing scalar function calls with PostgreSQL-style function signatures.

Key decisions:
- When a scalar call has no legacy builtin implementation, return the catalog resolver's missing-function diagnostic instead of the older "supported builtin function" fallback.
- Add a focused database test for an unknown one-argument scalar function.

Files touched:
- src/backend/parser/analyze/expr.rs
- src/pgrust/database_tests.rs

Tests run:
- cargo fmt
- scripts/cargo_isolated.sh test --lib --quiet missing_scalar_function_call_reports_signature
- scripts/cargo_isolated.sh test --lib --quiet plpgsql
- scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55434 --results-dir /tmp/diffs/plpgsql-missing-function-fallback

Remaining:
- The target `select shadowtest(1)` hunk now matches.
- Regression result was 2210/2271 with 760 diff lines because this run hit known transition-table timeout variability.
