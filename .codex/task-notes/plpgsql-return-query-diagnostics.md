Goal:
Close the PL/pgSQL regression hunk where `FOUND` and `ROW_COUNT` were wrong after `RETURN QUERY`.

Key decisions:
Track the number of rows appended by `RETURN QUERY` in function state and expose it through `GET DIAGNOSTICS ... ROW_COUNT`. Keep this slice narrow instead of changing row count handling for every SQL statement.

Files touched:
src/pl/plpgsql/exec.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet plpgsql_return_query_updates_found
scripts/cargo_isolated.sh test --lib --quiet plpgsql
scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55434 --results-dir /tmp/diffs/plpgsql-return-query-diagnostics

Remaining:
Latest regression baseline is 2183/2271 matched with 1051 diff lines. Larger remaining clusters include composite/record returns, WHERE CURRENT OF, transition tables, context formatting, and output formatting.
