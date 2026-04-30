Goal:
Fix remaining PostgreSQL `with.sql` mismatches around recursive CTEs, SEARCH/CYCLE, writable CTEs, MERGE, rules, and recursive views.

Key decisions:
- Lower `CREATE RECURSIVE VIEW` to an equivalent stored `WITH RECURSIVE` view query.
- Lower SEARCH/CYCLE during recursive CTE binding by adding generated search sequence, cycle mark, and cycle path columns.
- Keep current writable-CTE session/database materialization shims for now; remaining writable-CTE failures point to missing first-class analyzer/planner state.
- Allow recursive UNION DISTINCT to hash record/record-array generated columns because `Value` already supports record equality/hash.
- Preserve whether a recursive CTE came from a left-nested UNION so SEARCH/CYCLE can choose PostgreSQL's left/right error text.
- Treat qualified column references as table/CTE references for dependency and OLD/NEW-in-rule-CTE checks.
- Expand simple rule actions with writable CTEs into sequenced bound actions so unreferenced writable CTEs run before the main rule action.
- Use base-table defaults when rewriting auto-updatable view INSERT DEFAULT VALUES.
- Suppress non-ANALYZE EXPLAIN writable-CTE analyzer aborts with a placeholder plan until first-class writable CTE plan nodes exist.

Files touched:
- `crates/pgrust_sql_grammar/src/gram.pest`
- `src/backend/parser/gram.rs`
- `src/include/nodes/parsenodes.rs`
- `src/backend/parser/analyze/mod.rs`
- `src/backend/parser/analyze/expr.rs`
- `src/backend/parser/analyze/expr/ops.rs`
- `src/backend/parser/analyze/functions.rs`
- `src/backend/parser/analyze/rules.rs`
- `src/backend/parser/analyze/scope.rs`
- `src/backend/parser/analyze/modify.rs`
- `src/backend/commands/tablecmds.rs`
- `src/backend/executor/startup.rs`
- `src/backend/executor/exec_expr.rs`
- `src/backend/rewrite/views.rs`
- `src/pgrust/database/relation_refs.rs`
- `src/pgrust/database/commands/execute.rs`
- `src/pgrust/database/commands/rules.rs`
- `src/pgrust/session.rs`
- `src/pl/plpgsql/compile.rs`
- `src/backend/parser/tests.rs`
- `src/pgrust/database_tests.rs`

Tests run:
- `TMPDIR=/tmp CARGO_TARGET_DIR=/tmp/pgrust-target-ottawa-v2-check CARGO_BUILD_RUSTC_WRAPPER=/usr/bin/env RUSTC_WRAPPER=/usr/bin/env scripts/cargo_isolated.sh check` passed with pre-existing unreachable-pattern warnings.
- Focused parser/executor tests passed: `parse_union_distinct_select_chain`, `parse_create_table_rejects_unquoted_with_column_name`, `recursive_cte_search_cycle_clauses_parse_and_validate_names`, `recursive_cte_search_union_distinct_hashes_record_path`, `recursive_cte_type_mismatch_uses_postgres_diagnostic`, prior recursive SEARCH/CYCLE tests.
- `scripts/run_regression.sh --test with --jobs 1 --port 55444 --results-dir /tmp/diffs/with_full_fix4` completed without timeout: 197/312 queries matched, 1835 diff lines.
- Additional focused parser tests passed: `parse_top_level_with_merge_statement`, `parse_qualified_star_cast_expression`, `recursive_cte_search_cycle_clauses_parse_and_validate_names`.
- `TMPDIR=/tmp CARGO_TARGET_DIR=/tmp/pgrust-target-ottawa-v2-check CARGO_BUILD_RUSTC_WRAPPER=/usr/bin/env RUSTC_WRAPPER=/usr/bin/env scripts/cargo_isolated.sh check` passed with pre-existing unreachable-pattern warnings.
- Latest regression: `scripts/run_regression.sh --test with --jobs 1 --port 55450 --results-dir /tmp/diffs/with_full_fix10` completed without timeout: 206/312 queries matched, 1761 diff lines.

Remaining:
- Latest added SQL errors in `/tmp/diffs/with_full_fix10/diff/with.diff`: recursive type error selection (`text < integer` vs `text + integer`), recursive non-recursive-term context reported as subquery, outer aggregate/nested CTE still reported as missing relation, one recursive query timeout, two scalar-subquery cardinality errors from missing projection pruning, one recursive writable DELETE body missing CTE scope (`relation "t"`), one writable CTE + ON CONFLICT duplicate-update error, and one DELETE/USING CTE sublink reaches executor unplanned.
- Output-only mismatches remain for EXPLAIN/viewdef plan shape and formatting, including placeholder EXPLAIN output for writable CTEs.
