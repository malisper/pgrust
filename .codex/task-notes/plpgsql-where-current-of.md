Goal:
Implement PL/pgSQL `WHERE CURRENT OF` cursor updates/deletes for the plpgsql regression `forc01` cluster.

Key decisions:
Parsed `WHERE CURRENT OF cursor` as update/delete statement metadata, then resolved it at PL/pgSQL execution time to a concrete `tableoid`/`ctid` predicate from the positioned cursor row. Preserved system bindings in PL/pgSQL cursor query rows. Added cursor shape tracking so `FETCH c INTO r` can type a following `r.field` reference after static `OPEN c FOR ...`.

Files touched:
`crates/pgrust_sql_grammar/src/gram.pest`, `src/backend/parser/gram.rs`, `src/include/nodes/parsenodes.rs`, `src/backend/parser/analyze/modify.rs`, `src/backend/commands/tablecmds.rs`, `src/backend/executor/exec_expr.rs`, `src/pgrust/database/commands/rules.rs`, `src/pl/plpgsql/compile.rs`, `src/pl/plpgsql/exec.rs`, `src/pgrust/database_tests.rs`.

Tests run:
`cargo fmt`
`scripts/cargo_isolated.sh test --lib --quiet plpgsql_where_current_of_updates_cursor_row`
`scripts/cargo_isolated.sh test --lib --quiet plpgsql`
`scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55434 --results-dir /tmp/diffs/plpgsql-where-current-of`

Remaining:
Regression now reports `2222/2271` matched and `605` diff lines. Remaining clusters are unrelated to current-of syntax/runtime support.
