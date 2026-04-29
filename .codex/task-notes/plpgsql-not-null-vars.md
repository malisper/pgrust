Goal:
Enforce PL/pgSQL `NOT NULL` variable assignments so NULL assignment raises and can be caught.

Key decisions:
- Treat declaration `NOT NULL` the same as the existing parsed `strict` variable flag.
- Carry assignment target name/nullability through direct assignments, `SELECT INTO`, cursor fetches, query loops, diagnostics targets, and DML-returning targets.
- Raise SQLSTATE `22004` with PostgreSQL-style variable text when an assignment would store NULL in a NOT NULL variable.

Files touched:
- `src/pl/plpgsql/gram.rs`
- `src/pl/plpgsql/compile.rs`
- `src/pl/plpgsql/exec.rs`
- `src/pgrust/database_tests.rs`

Tests run:
- `cargo fmt`
- `scripts/cargo_isolated.sh test --lib --quiet parse_not_null_var_declaration`
- `scripts/cargo_isolated.sh test --lib --quiet plpgsql_not_null_variable_assignment_raises_and_can_be_caught`
- `scripts/cargo_isolated.sh test --lib --quiet plpgsql`
- `scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55434 --results-dir /tmp/diffs/plpgsql-not-null-vars`

Remaining:
- Regression improved to `2182/2271` matched, `1064` diff lines.
- Remaining large clusters include `WHERE CURRENT OF`, composite result coercion/dropped columns, PG_CONTEXT stack reporting, transition tables, and final error/context formatting.
