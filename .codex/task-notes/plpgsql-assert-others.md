Goal:
Match PostgreSQL exception matching for PL/pgSQL ASSERT failures.

Key decisions:
`WHEN others` should not catch `assert_failure` (`P0004`), matching PostgreSQL's special handling. Explicit `WHEN assert_failure` remains catchable.

Files touched:
src/pl/plpgsql/exec.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet plpgsql_exception_others_does_not_catch_assert_failure
scripts/cargo_isolated.sh test --lib --quiet plpgsql
scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55434 --results-dir /tmp/diffs/plpgsql-assert-others

Remaining:
Latest regression baseline is 2185/2271 matched with 1031 diff lines. Remaining clusters include domain check assignment errors, PG_CONTEXT stack text, composite/record returns, WHERE CURRENT OF, and transition tables.
