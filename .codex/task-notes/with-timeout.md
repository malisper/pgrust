Goal:
Diagnose why the PostgreSQL `with` regression times out.

Key decisions:
Ran the `with` regression into `/tmp/diffs/with_regression` and copied the main
diff to `/tmp/diffs/diff/with.diff`. Reduced the timeout to nested CTE parsing:
`w3 -> w4 -> w5 -> recursive w6 -> shadowed w6 -> w8` returns, but adding a
`w2` wrapper hangs. Server sampling during the reduced query shows all samples
inside `pgrust_sql_grammar` Pest parsing `cte_body` / nested `cte_clause`, not
planner or executor code.

Fixed by removing the speculative `recursive_union_cte_body` grammar alternative
from `cte_body`. CTE bodies now parse through the normal `select_stmt` path, and
`src/backend/parser/gram.rs` reconstructs the existing `CteBody::RecursiveUnion`
shape from the parsed leftmost UNION. This keeps existing analyzer behavior
without forcing Pest to parse nested CTE bodies once as a possible recursive
union and again as an ordinary SELECT.

Files touched:
.codex/task-notes/with-timeout.md
crates/pgrust_sql_grammar/src/gram.pest
src/backend/parser/gram.rs
src/backend/parser/tests.rs

Tests run:
scripts/run_regression.sh --skip-build --test with --jobs 1 --port 55433 --results-dir /tmp/diffs/with_regression
manual psql repros on throwaway ports 55434-55438
macOS sample of pgrust_server during the reduced timeout query
scripts/cargo_isolated.sh test --lib --quiet parse_nested_cte_shadowing_regression_without_backtracking_timeout
scripts/cargo_isolated.sh test --lib --quiet parse_with_recursive_cte_union_all
scripts/run_regression.sh --test with --jobs 1 --port 55433 --results-dir /tmp/diffs/with_regression_after
scripts/cargo_isolated.sh check

Remaining:
Timeout is fixed. The `with` regression now completes as FAIL rather than
TIMEOUT: 139/312 matched, 173 mismatched, 0 timed out. Remaining failures are
separate SQL feature/semantic gaps such as `CREATE RECURSIVE VIEW`,
`pg_get_viewdef` formatting/support, SEARCH/CYCLE syntax, writable CTEs, and
some recursive CTE semantics.
