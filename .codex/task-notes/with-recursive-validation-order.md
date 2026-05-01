Goal:
Fix the recursive validation/order bucket in the PostgreSQL `with` regression.

Key decisions:
- Carry parser locations through CTE, SELECT target, FROM table, set-operation, ORDER/LIMIT/OFFSET/locking, SEARCH, and CYCLE raw nodes.
- Position recursive validation errors through `ParseError::with_position` where PostgreSQL reports a caret.
- Keep recursive target-list binding ahead of qualifier binding so `text + integer` wins over later filter errors.
- Bind recursive-term local `WITH` items during target prevalidation so nested worktable CTEs stay visible.
- Reject nonnumeric operands in the numeric arithmetic resolver.
- Avoid hash joins for paths containing `WorkTableScan` as a compatibility preference for regression-visible recursive traversal order.
- Preserve relation-specific output expressions for `JOIN ... USING` merged columns so qualified `x.*`/`x.col` references use the null-extended side in outer joins.
- Treat an `ON CONFLICT DO UPDATE` conflict against a row already touched by a writable CTE in the same statement as skipped; duplicate input rows remain covered by the existing preflight cardinality check.
- Convert non-recursive CTE forward/self references into PostgreSQL's special undefined-table detail/hint when the unknown table name matches a current or later CTE.
- Derive scalar-subquery select-list names from the bound subquery target list so nested WITH subqueries preserve PostgreSQL labels like `foo` and `column1`.

Files touched:
- `src/include/nodes/parsenodes.rs`
- `src/backend/parser/gram.rs`
- `src/backend/parser/analyze/mod.rs`
- `src/backend/parser/analyze/coerce.rs`
- `src/backend/parser/analyze/scope.rs`
- `src/backend/parser/analyze/expr.rs`
- `src/backend/parser/analyze/expr/targets.rs`
- `src/backend/parser/analyze/modify.rs`
- `src/backend/parser/analyze/on_conflict.rs`
- `src/backend/parser/analyze/rules.rs`
- `src/backend/commands/upsert.rs`
- `src/backend/executor/tests.rs`
- `src/pl/plpgsql/compile.rs`
- `src/backend/optimizer/path/costsize.rs`
- `src/backend/parser/tests.rs`
- `src/pgrust/database_tests.rs`
- `src/pgrust/database/commands/rules.rs`
- `src/pgrust/session.rs`

Tests run:
- `cargo test --lib --quiet recursive_validation` with direct rustc/external target: passed, 6 tests.
- `cargo test --lib --quiet recursive_cte_search_cycle_clauses_parse_and_validate_names`: passed, 1 test.
- `cargo test --lib --quiet recursive`: passed, 71 tests.
- `scripts/run_regression.sh --test with --results-dir /tmp/diffs/with-recursive-validation-order-final --timeout 120 --jobs 1 --port 55433`: failed with 266/312 matched, 46 mismatches, 0 timeouts.
- `cargo fmt`: passed.
- `scripts/cargo_isolated.sh test --lib --quiet join_using_qualified_star`: passed, 1 test.
- `scripts/cargo_isolated.sh test --lib --quiet build_plan_rejects_forward_cte_references`: passed, 1 test.
- `scripts/cargo_isolated.sh test --lib --quiet on_conflict_update_skips_row_already_touched_by_writable_cte`: passed, 1 test.
- `scripts/cargo_isolated.sh test --lib --quiet on_conflict_do_update_rejects_duplicate_input_rows`: passed, 1 test.
- `scripts/cargo_isolated.sh test --lib --quiet on_conflict_do_update_duplicate_existing_conflicts_leave_row_unchanged`: passed on rerun after one dyld loader abort in a separate slot.
- `scripts/cargo_isolated.sh test --lib --quiet on_conflict_do_update_allows_duplicate_input_after_arbiter_key_changes`: passed, 1 test.
- `scripts/cargo_isolated.sh check`: passed with existing unreachable-pattern warnings.
- `CARGO_TARGET_DIR=/tmp/pgrust-target-spokane-v4-regress scripts/run_regression.sh --test with --results-dir /tmp/diffs/with-targeted-fixes --timeout 120 --jobs 1`: failed with 270/312 matched, 42 mismatches, 0 timeouts.
- `scripts/cargo_isolated.sh test --lib --quiet build_plan_propagates_nested_scalar_subquery_column_names`: passed, 1 test.
- `CARGO_TARGET_DIR=/tmp/pgrust-target-spokane-v4-regress scripts/run_regression.sh --test with --results-dir /tmp/diffs/with-column-labels --timeout 120 --jobs 1`: failed with 272/312 matched, 40 mismatches, 0 timeouts.

Remaining:
- `pg_get_viewdef`/ruleutils formatting for recursive CTEs, SEARCH, and CYCLE.
- EXPLAIN formatting differences around generated SEARCH/CYCLE expressions and WorkTableScan aliases.
- Some remaining recursive validation error caret positions differ.
- Writable CTE/DML semantics remain in other DML CTE scope cases; the CTE-fed `ON CONFLICT DO UPDATE` mismatch from this bucket is fixed.
- Some SEARCH/CYCLE output ordering still differs despite disabling hash joins around WorkTableScan.
- Remaining visible row-order mismatches are recursive CYCLE traversal order and writable CTE physical table order after update/insert interleaving. A planner nested-loop/worktable-side preference did not affect the recursive CYCLE order, so that likely needs deeper recursive-term path orientation or executor iteration work.
