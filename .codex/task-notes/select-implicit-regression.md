Goal:
Diagnose and fix pasted `select_implicit` regression failures around implicit GROUP BY/ORDER BY targets.

Key decisions:
- Treat raw GROUP BY matches as a fast path, then compare the fully bound output expression against bound group keys so qualified and unqualified expressions match when they bind to the same Vars.
- Add `upper(text)` as a text builtin like `lower(text)` so `ORDER BY upper(d)` resolves to the text function instead of the range `upper` candidate.
- Add grouped `ORDER BY` position reporting and ambiguous-column position lookup so the regression's expected caret lines are emitted.

Files touched:
- `src/backend/parser/analyze/agg_output.rs`
- `src/include/nodes/primnodes.rs`
- `src/include/catalog/pg_proc.rs`
- `src/backend/executor/expr_string.rs`
- `src/backend/executor/exec_expr.rs`
- `src/backend/executor/nodes.rs`
- `src/backend/parser/analyze/{functions.rs,infer.rs,agg_output_special.rs,expr/func.rs}`
- `src/backend/parser/tests.rs`
- `src/backend/tcop/postgres.rs`
- `src/include/nodes/parsenodes.rs`

Tests run:
- `cargo fmt`
- `scripts/cargo_isolated.sh test --lib --quiet analyze_grouped_query_matches_bound_equivalent_group_exprs`
- `scripts/cargo_isolated.sh test --lib --quiet resolve_function_call_prefers_text_upper_for_text_arguments`
- `scripts/cargo_isolated.sh test --lib --quiet scalar_proc_oid_helpers_cover_real_and_synthetic_builtins`
- `scripts/cargo_isolated.sh check`
- `scripts/run_regression.sh --port 59020 --schedule .context/select_implicit.schedule --test select_implicit --timeout 30`

Remaining:
- The full upstream schedule path still trips unrelated `create_index` setup on unsupported GiST point strategies. The focused one-test schedule passes `select_implicit`.
