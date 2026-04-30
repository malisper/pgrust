Goal:
Fix the remaining jsonb regression failures from
/private/tmp/pgrust-regression-artifact-2026-04-30T0340Z/diff/jsonb.diff
without updating expected output.

Key decisions:
- Canonicalize jsonb numeric GIN keys with jsonb numeric equality semantics so
  25 and 25.0 index to the same key.
- Add regression-correct, lossy GIN behavior for jsonpath @?/@@ and
  jsonb_path_ops, with heap recheck and :HACK: comments for future full
  jsonpath key extraction.
- Render jsonb_pretty with jsonb-aware 4-space formatting instead of serde
  pretty output.
- Validate jsonb subscripts during binding and align scalar/null assignment
  error details with PostgreSQL.
- Suppress caret positions for jsonb runtime path errors while keeping targeted
  positions for jsonb subscript binding errors.
- Route scalar builtins in FROM through the single-row Result + Projection path.
- Preserve left-to-right function cross join tie-breaking so unordered
  jsonb_agg over generate_series matches PostgreSQL's regression output.
- Use canonicalized hash lookup keys for aggregate/group-by jsonb values to
  avoid repeated decode/compare scans while preserving jsonb numeric equality.

Files touched:
- src/backend/access/gin/gin.rs
- src/backend/access/gin/jsonb_ops.rs
- src/backend/catalog/state.rs
- src/backend/catalog/store/heap.rs
- src/backend/executor/expr_json.rs
- src/backend/executor/jsonb.rs
- src/backend/executor/nodes.rs
- src/backend/executor/tests.rs
- src/backend/optimizer/bestpath.rs
- src/backend/optimizer/path/allpaths.rs
- src/backend/optimizer/path/costsize.rs
- src/backend/parser/analyze/coerce.rs
- src/backend/parser/analyze/expr/json.rs
- src/backend/parser/analyze/expr/ops.rs
- src/backend/parser/analyze/functions.rs
- src/backend/parser/analyze/modify.rs
- src/backend/parser/analyze/scope.rs
- src/backend/tcop/postgres.rs
- src/include/catalog/pg_amop.rs
- src/include/catalog/pg_amproc.rs
- src/include/catalog/pg_opclass.rs
- src/include/catalog/pg_operator.rs
- src/include/catalog/pg_opfamily.rs
- src/include/catalog/pg_proc.rs
- src/pgrust/database_tests.rs

Tests run:
- cargo fmt
- CARGO_TARGET_DIR=/tmp/pgrust-target-jsonb-las-vegas scripts/cargo_isolated.sh test --lib --quiet generate_series_sources_can_cross_join_each_other
- CARGO_TARGET_DIR=/tmp/pgrust-target-jsonb-las-vegas scripts/cargo_isolated.sh test --lib --quiet explain_expr_matches_postgres_filter_formatting
- CARGO_TARGET_DIR=/tmp/pgrust-target-jsonb-las-vegas scripts/cargo_isolated.sh test --lib --quiet jsonb
  Result: 82 passed.
- CARGO_TARGET_DIR=/tmp/pgrust-target-jsonb-las-vegas scripts/cargo_isolated.sh test --lib --quiet scalar_repeat_in_from_returns_single_row
- CARGO_TARGET_DIR=/tmp/pgrust-target-jsonb-las-vegas scripts/run_regression.sh --test jsonb --results-dir /tmp/pgrust-jsonb-regression-las-vegas-3 --timeout 180 --jobs 1 --port 55433
  Result: PASS, 1084/1084 queries matched.
- After rebasing onto origin/perf-optimization:
  - CARGO_TARGET_DIR=/tmp/pgrust-target-jsonb-las-vegas scripts/cargo_isolated.sh test --lib --quiet jsonb
    Result: 82 passed.
  - CARGO_TARGET_DIR=/tmp/pgrust-target-jsonb-las-vegas scripts/cargo_isolated.sh test --lib --quiet scalar_repeat_in_from_returns_single_row
  - CARGO_TARGET_DIR=/tmp/pgrust-target-jsonb-las-vegas scripts/run_regression.sh --test jsonb --results-dir /tmp/pgrust-jsonb-regression-las-vegas-pr-2 --timeout 180 --jobs 1 --port 55433
    Result: PASS, 1084/1084 queries matched.

Remaining:
None for the scoped jsonb regression file.
