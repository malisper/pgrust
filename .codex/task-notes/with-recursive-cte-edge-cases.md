Goal:
Fix recursive CTE edge-case diffs from `/tmp/pgrust-regression-diffs-2026-05-01T2044Z/with.diff`.

Key decisions:
Hoist leading WITH clauses from parser-split recursive-union CTE bodies back onto the synthesized set-operation query so all UNION inputs can see them.
Mark unparenthesized chained UNION recursive terms as left-nested for SEARCH/CYCLE diagnostics.
Match PostgreSQL diagnostics for recursive FOR UPDATE and EXCEPT edge cases.

Files touched:
`src/backend/parser/analyze/mod.rs`
`src/backend/parser/gram.rs`
`src/pgrust/database_tests.rs`

Tests run:
`scripts/cargo_isolated.sh test --lib --quiet recursive_cte_nonrecursive_union_body_preserves_local_with_scope`
`scripts/cargo_isolated.sh test --lib --quiet recursive_cte_search_cycle_clauses_parse_and_validate_names`
`scripts/cargo_isolated.sh test --lib --quiet recursive_cte_rejects_unsupported_term_decorations`
`scripts/cargo_isolated.sh test --lib --quiet recursive_cte_term_local_with_can_read_worktable`
`scripts/cargo_isolated.sh test --lib --quiet recursive_cte_nested_union_ctes_inside_recursive_term_execute`
`scripts/run_regression.sh --test with --jobs 1 --timeout 180 --port 55433 --skip-build --results-dir /tmp/pgrust-with-after-recursive-cte-3`

Remaining:
`with` still has unrelated EXPLAIN, viewdef, and writable CTE diffs; targeted recursive CTE edge-case strings no longer appear in `/tmp/pgrust-with-after-recursive-cte-3/diff/with.diff`.
