Goal:
Fix remaining PostgreSQL `with.sql` mismatches around recursive CTEs, SEARCH/CYCLE, writable CTEs, MERGE, rules, and recursive views.

Key decisions:
- Lower `CREATE RECURSIVE VIEW` to an equivalent stored `WITH RECURSIVE` view query.
- Lower SEARCH/CYCLE during recursive CTE binding by adding generated search sequence, cycle mark, and cycle path columns.
- Keep current writable-CTE session/database materialization shims for now; remaining writable-CTE failures point to missing first-class analyzer/planner state.
- Allow recursive UNION DISTINCT to hash record/record-array generated columns because `Value` already supports record equality/hash.

Files touched:
- `crates/pgrust_sql_grammar/src/gram.pest`
- `src/backend/parser/gram.rs`
- `src/include/nodes/parsenodes.rs`
- `src/backend/parser/analyze/mod.rs`
- `src/backend/parser/analyze/expr.rs`
- `src/backend/parser/analyze/expr/ops.rs`
- `src/backend/parser/analyze/functions.rs`
- `src/backend/executor/startup.rs`
- `src/backend/executor/exec_expr.rs`
- `src/backend/rewrite/views.rs`
- `src/pgrust/database/relation_refs.rs`
- `src/pgrust/session.rs`
- `src/backend/parser/tests.rs`
- `src/pgrust/database_tests.rs`

Tests run:
- `TMPDIR=/tmp CARGO_TARGET_DIR=/tmp/pgrust-target-ottawa-v2-check CARGO_BUILD_RUSTC_WRAPPER=/usr/bin/env RUSTC_WRAPPER=/usr/bin/env scripts/cargo_isolated.sh check` passed with pre-existing unreachable-pattern warnings.
- Focused parser/executor tests passed: `parse_union_distinct_select_chain`, `parse_create_table_rejects_unquoted_with_column_name`, `recursive_cte_search_cycle_clauses_parse_and_validate_names`, `recursive_cte_search_union_distinct_hashes_record_path`, `recursive_cte_type_mismatch_uses_postgres_diagnostic`, prior recursive SEARCH/CYCLE tests.
- `scripts/run_regression.sh --test with --jobs 1 --port 55444 --results-dir /tmp/diffs/with_full_fix4` completed without timeout: 197/312 queries matched, 1835 diff lines.

Remaining:
- 18 added ERRORs remain in `/tmp/diffs/with_full_fix4/diff/with.diff`.
- Largest blocker: 4 writable CTE analyzer/planner gaps still emit `writable CTE must be materialized before binding`, mainly EXPLAIN/rules/nested placement.
- Other categories: recursive CTE validation/message gaps, scalar subquery cardinality differences, MERGE WITH source unsupported path, DELETE/USING unplanned subquery, ON CONFLICT writable CTE rowcount/duplicate behavior, one statement timeout, and output-only EXPLAIN/viewdef plan text differences.
