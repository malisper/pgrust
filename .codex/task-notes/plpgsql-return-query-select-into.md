Goal:
Make `RETURN QUERY SELECT ... INTO ...` report PostgreSQL's no-tuples error instead of pgrust's unsupported SELECT form.

Key decisions:
- Compile static `RETURN QUERY` SELECT-INTO forms into a deferred no-tuples source so the error is raised with PL/pgSQL statement context.
- For dynamic `RETURN QUERY EXECUTE`, evaluate the SQL first and map SELECT-INTO unsupported forms to `SELECT INTO query does not return tuples`.
- Add SQL statement context around dynamic SQL execution errors once the query string is known.

Files touched:
- `src/pl/plpgsql/compile.rs`
- `src/pl/plpgsql/exec.rs`
- `src/pgrust/database_tests.rs`

Tests run:
- `cargo fmt`
- `scripts/cargo_isolated.sh test --lib --quiet plpgsql_return_query_select_into_reports_no_tuples`
- `scripts/cargo_isolated.sh test --lib --quiet plpgsql`
- `scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55434 --results-dir /tmp/diffs/plpgsql-return-query-select-into`

Remaining:
- Regression improved to `2180/2271` matched, `1087` diff lines.
- Remaining clusters still include `WHERE CURRENT OF`, composite result coercion, PG_CONTEXT call stack, transition tables, and formatting/context caret details.
