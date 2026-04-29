Goal:
Reduce the remaining PostgreSQL `join` regression diffs, prioritizing timeouts and semantic failures before order and caret-only formatting.

Key decisions:
- Kept existing uncommitted semantic fixes in place.
- Preserved scalar subquery target-width handling, but made prepared `EXISTS` avoid rescanning by using proven-empty checks, boolean short-circuiting, and a simple membership cache for safe equality shapes.
- Rewrote parameterized OR join clauses into runtime scalar-array index quals, including nested OR trees.
- Expanded runtime btree `= ANY(array)` index scan keys into ordinary equality probes in the executor so each outer row performs a few seeks instead of a full index walk.
- Expanded single `Record` rows returned by SQL set-returning functions into the declared composite/table descriptor for function scans.
- Allowed the default physical orientation for `RIGHT JOIN` when the left side has an ancestor lateral reference, fixing the PlaceHolderVar-style lateral/right-join binding failure without restoring the broader unsafe orientation relaxation.

Files touched:
- `src/backend/executor/driver.rs`
- `src/backend/executor/exec_expr.rs`
- `src/backend/executor/exec_expr/subquery.rs`
- `src/backend/executor/mod.rs`
- `src/backend/executor/nodes.rs`
- `src/backend/executor/sqlfunc.rs`
- `src/backend/executor/startup.rs`
- `src/backend/executor/tests.rs`
- `src/backend/optimizer/path/costsize.rs`
- `src/backend/optimizer/tests.rs`
- `src/include/nodes/execnodes.rs`
- `src/pgrust/session.rs`

Tests run:
- `scripts/cargo_isolated.sh check`
- `scripts/cargo_isolated.sh test --lib --quiet sql_function_scan_expands_named_composite_record_result`
- `scripts/cargo_isolated.sh test --lib --quiet prepared_exists_join_qual_uses_parameter_values_without_rescanning_inner -- --nocapture`
- `scripts/cargo_isolated.sh test --lib --quiet planner_uses_runtime_scalar_array_index_for_or_join_clause -- --nocapture`
- `scripts/cargo_isolated.sh test --lib --quiet lateral_right_join_placeholder_uses_outer_binding_at_join_level -- --nocapture`
- `scripts/cargo_isolated.sh test --lib --quiet right_full_join_lateral_cannot_reference_left_side -- --nocapture`
- `RUSTC_WRAPPER= PGRUST_STATEMENT_TIMEOUT=30 scripts/run_regression.sh --test join --jobs 1 --timeout 300 --port 55444 --results-dir /tmp/diffs/join-impl-19`

Remaining:
- Latest `join`: `/tmp/diffs/join-impl-19`, `601/641` matched, `40` mismatches, `643` diff lines.
- No remaining statement timeouts.
- No remaining direct `special executor Var referenced beyond the bound tuple width` failure.
- No remaining SQL-function composite expansion diff for `mki8`/`mki4`.
- Remaining groups are row-order diffs, nulling/outer-join semantic row-count diffs, and source-position/detail-only error diffs.
- Source/detail-only items visible in `join.diff`: 3 `FOR UPDATE OF` caret omissions, 2 invalid RIGHT/FULL LATERAL detail strings, and 1 ambiguous LATERAL alias caret omission.
