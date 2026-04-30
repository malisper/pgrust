Goal:
Fix remaining PostgreSQL compatibility gaps in `with.sql`, focusing on CTE runtime semantics, writable CTE RETURNING behavior, rule interactions, CTE subquery pruning, and SQL-visible diagnostics.

Key decisions:
- Reset nested CTE materializations that depend on the recursive worktable at each recursive-union iteration.
- Let scalar subquery pruning look through CTE-backed subqueries instead of treating every SubLink/SubPlan as prune-volatile.
- Preserve side-specific hidden columns for qualified `JOIN ... USING` stars.
- Keep writable CTE duplicate-row ON CONFLICT behavior as a scoped `:HACK:` until command-id semantics are modeled more precisely.
- Match PostgreSQL rule rewrite behavior for `ON INSERT DO INSTEAD SELECT` by joining the rule action to the original INSERT source rows.
- Add CTE-specific error caret/detail compatibility in the protocol layer for the `with.sql` analyzer cases.
- Reject non-literal text arithmetic before numeric fallback so recursive diagnostics report the same target-list operator PostgreSQL reports.
- Derive scalar subquery output names from the bound subquery target so nested CTE scalar subqueries expose `foo`/`column1` like PostgreSQL.
- Preserve PostgreSQL's non-recursive-term classification for parenthesized left recursive CTE terms while still treating copied top-level WITH clauses as subquery context.

Files touched:
- `src/backend/executor/startup.rs`
- `src/backend/executor/nodes.rs`
- `src/include/nodes/execnodes.rs`
- `src/backend/optimizer/path/subquery_prune.rs`
- `src/backend/parser/analyze/scope.rs`
- `src/backend/parser/analyze/agg_scope.rs`
- `src/backend/parser/tests.rs`
- `src/backend/commands/tablecmds.rs`
- `src/backend/commands/upsert.rs`
- `src/pgrust/database/commands/rules.rs`
- `src/pgrust/session.rs`
- `src/backend/tcop/postgres.rs`
- `src/backend/parser/analyze/expr/ops.rs`
- `src/backend/parser/analyze/expr/targets.rs`

Tests run:
- `scripts/cargo_isolated.sh check`
- `scripts/cargo_isolated.sh test --lib --quiet recursive_cte_rematerializes_nested_iteration_ctes`
- `scripts/cargo_isolated.sh test --lib --quiet unused_subquery_output_prunes_scalar_cte_subquery`
- `scripts/cargo_isolated.sh test --lib --quiet planner_handles_recursive_cte_non_output_filter_column`
- `scripts/cargo_isolated.sh test --lib --quiet join_using`
- `scripts/cargo_isolated.sh test --lib --quiet delete_using`
- `scripts/cargo_isolated.sh test --lib --quiet writable_cte_insert_instead_select_rule_joins_original_source`
- `scripts/cargo_isolated.sh test --lib --quiet writable_cte_delete_statement_level_instead_rule_runs_once`
- `scripts/cargo_isolated.sh test --lib --quiet writable_cte_on_conflict_update_same_row_returns_no_outer_rows`
- `scripts/cargo_isolated.sh test --lib --quiet on_conflict_do_update_rejects_duplicate_input_rows`
- `scripts/cargo_isolated.sh test --lib --quiet on_conflict_do_update_duplicate_existing_conflicts_leave_row_unchanged`
- `scripts/cargo_isolated.sh test --lib --quiet outer_level_aggregate_rejects_nested_cte_reference`
- `scripts/cargo_isolated.sh test --lib --quiet aggregate_rejects_nested_subquery_reference_to_local_cte`
- `scripts/run_regression.sh --test with --port 55453 --results-dir /tmp/diffs/with-remaining-fix-7`
- `scripts/cargo_isolated.sh test --lib --quiet recursive_cte_reports_target_operator_error_before_filter_operator_error`
- `scripts/cargo_isolated.sh test --lib --quiet recursive_cte_rejects_parenthesized_left_with_self_reference_as_non_recursive_term`
- `scripts/cargo_isolated.sh test --lib --quiet select_cte_can_capture_outer_value_through_scalar_subquery`
- `scripts/cargo_isolated.sh test --lib --quiet select_cte_scalar_values_subquery_uses_values_column_name`
- `env -u CARGO_TARGET_DIR scripts/cargo_isolated.sh check`
- `TMPDIR=/Volumes/OSCOO\ PSSD/tmp/pgrust-sccache CARGO_TARGET_DIR=/Volumes/OSCOO\ PSSD/pgrust/daegu-v6-regression-target scripts/run_regression.sh --test with --port 55454 --results-dir /tmp/diffs/with-nonorder-fix`

Remaining:
- Final `with.sql` status: `267/312` matched, 45 mismatches, 1023 diff lines.
- Remaining runtime/semantic diffs are SEARCH/CYCLE recursive output ordering and data-modifying CTE heap/result ordering.
- Remaining formatting/planner diffs are mostly EXPLAIN CTE shape parity, MERGE-with-CTE EXPLAIN, writable DELETE CTE EXPLAIN, and ruleutils/pg_get_viewdef normalization for recursive SEARCH/CYCLE views.
- Remaining SQL-visible diagnostic diffs are caret/LINE-location only for recursive ORDER BY/OFFSET/type errors and nested CTE recursive-reference location.
