Goal:
Fix the remaining non-formatting subselect regression failures in the quantified subquery/IN, LATERAL outer-reference, and CTE/recursive CTE buckets, using PostgreSQL behavior as reference.

Key decisions:
Preserve quantified-subquery operator metadata from analysis through SubPlan execution so user-defined operators and coerced comparisons run with the operator PostgreSQL selected.
Block EXISTS/NOT EXISTS pull-up when the EXISTS WHERE clause does not reference the parent query; PostgreSQL keeps those as SubPlans/InitPlans rather than converting them into joins.
Keep LATERAL sibling subquery references from pruning subquery outputs that behave like PostgreSQL PlaceHolderVars. This is a conservative :HACK: until pgrust has full nullable-rel/PHV metadata.
Reset worktable-dependent CTEs between recursive iterations and reset lateral-right CTEs per rescan.
Add a Memoize-backed lateral right-side cache for the current LATERAL aggregate timeout shape; this is also marked as a PostgreSQL-compatibility shortcut until path pull-up/hash planning is stronger.
Parse quantified `ANY`/`ALL (VALUES ...)` as a VALUES subquery rather than a function/array expression.

Files touched:
crates/pgrust_sql_grammar/src/gram.pest
src/backend/parser/gram.rs
src/backend/parser/tests.rs
src/backend/parser/analyze/expr/subquery.rs
src/backend/parser/analyze/query.rs
src/backend/parser/analyze/agg_output_special.rs
src/include/nodes/primnodes.rs
src/include/nodes/execnodes.rs
src/backend/optimizer/sublink_pullup.rs
src/backend/optimizer/path/subquery_prune.rs
src/backend/optimizer/path/allpaths.rs
src/backend/optimizer/plan/subselect.rs
src/backend/optimizer/plan/planner.rs
src/backend/optimizer/root.rs
src/backend/optimizer/setrefs.rs
src/backend/optimizer/joininfo.rs
src/backend/optimizer/mod.rs
src/backend/optimizer/tests.rs
src/backend/executor/exec_expr/subquery.rs
src/backend/executor/nodes.rs
src/backend/executor/startup.rs
src/backend/executor/tests.rs

Tests run:
cargo fmt --check
CARGO_TARGET_DIR=/tmp/pgrust-target-puebla-subselect RUSTC_WRAPPER= scripts/cargo_isolated.sh check
CARGO_TARGET_DIR=/tmp/pgrust-target-puebla-subselect RUSTC_WRAPPER= scripts/cargo_isolated.sh test --lib --quiet scalar_in_subquery_coerces_comparison_types
CARGO_TARGET_DIR=/tmp/pgrust-target-puebla-subselect RUSTC_WRAPPER= scripts/cargo_isolated.sh test --lib --quiet lateral_join_output_expr
CARGO_TARGET_DIR=/tmp/pgrust-target-puebla-subselect-2 RUSTC_WRAPPER= scripts/cargo_isolated.sh test --lib --quiet lateral_recursive_cte_rescans_per_outer_row
CARGO_TARGET_DIR=/tmp/pgrust-target-puebla-subselect RUSTC_WRAPPER= scripts/cargo_isolated.sh test --lib --quiet recursive_cte_nested
CARGO_TARGET_DIR=/tmp/pgrust-target-puebla-subselect RUSTC_WRAPPER= scripts/cargo_isolated.sh test --lib --quiet planner_keeps_uncorrelated_not_exists_out_of_minmax_join_pullup
CARGO_TARGET_DIR=/tmp/pgrust-target-puebla-subselect RUSTC_WRAPPER= scripts/cargo_isolated.sh test --lib --quiet parse_any_all_subquery_expressions
CARGO_TARGET_DIR=/tmp/pgrust-target-puebla-subselect RUSTC_WRAPPER= scripts/cargo_isolated.sh test --lib --quiet lateral_subquery_output_expr_survives_sibling_lateral_pruning
CARGO_TARGET_DIR=/tmp/pgrust-target-puebla-subselect-regress RUSTC_WRAPPER= scripts/run_regression.sh --test subselect --results-dir /tmp/pgrust-subselect-after-fixes-5 --timeout 180 --jobs 1 --port 65433
CARGO_TARGET_DIR=/tmp/pgrust-target-puebla-subselect-final RUSTC_WRAPPER= scripts/cargo_isolated.sh check
CARGO_TARGET_DIR=/tmp/pgrust-target-puebla-ci-fix RUSTC_WRAPPER= scripts/cargo_isolated.sh test --lib --quiet any_and_all_subquery_propagate_nulls
CARGO_TARGET_DIR=/tmp/pgrust-target-puebla-ci-fix2 RUSTC_WRAPPER= scripts/cargo_isolated.sh test --lib --quiet in_and_not_in_propagate_nulls_like_postgres
CARGO_TARGET_DIR=/tmp/pgrust-target-puebla-ci-fix3 RUSTC_WRAPPER= scripts/cargo_isolated.sh test --lib --quiet explain_nested_exists_not_exists_pulls_up_semi_and_anti_joins
CARGO_TARGET_DIR=/tmp/pgrust-target-puebla-ci-fix4 RUSTC_WRAPPER= scripts/cargo_isolated.sh test --lib --quiet planner_keeps_uncorrelated_not_exists_out_of_minmax_join_pullup
CARGO_TARGET_DIR=/tmp/pgrust-target-puebla-ci-check RUSTC_WRAPPER= scripts/cargo_isolated.sh check

Remaining:
`subselect` still fails overall with 237/334 queries matched and 2206 diff lines in /tmp/pgrust-subselect-after-fixes-5/diff/subselect.diff.
No `ERROR:` lines, no statement timeouts, and no remaining LATERAL `ss2.*` row mismatches are present in that diff.
Remaining visible differences are mostly EXPLAIN plan shape, CTE inlining/materialization plan shape, VtA/index plan choices, and a few volatile NOTICE count differences outside the three requested buckets.
