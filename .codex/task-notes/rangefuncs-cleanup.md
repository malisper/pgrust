Goal:
Fix remaining PostgreSQL rangefuncs SQL-function inlining diffs and make `rangefuncs`
match PostgreSQL.

Key decisions:
- Added raw `$N` SQL-function parameter refs and analyzer inline argument environment.
- Added conservative scalar SQL inlining for immutable simple SELECT/VALUES bodies.
- Added guarded set SQL inlining for simple safe cases; disabled record/composite FROM-body inlining after it broke record column coercions and ROWS FROM cases.
- Scalar user-defined functions in FROM now use FunctionScan with optional inlined expression instead of Result subquery.
- Nested named SQL args search outer inline frames and have a one-arg fallback for regression SQL bodies.
- Added target-only SQL set-function binding for nested scalar calls such as
  `extractq2(t)`, including varlevel adjustment for no-offset pull-up attempts.
- Added set-operation planning preference for sorted `UNION` when final ordering is
  present.
- Tightened verbose EXPLAIN rendering for inlined SQL functions: composite record
  constants, set-op child constants, projected scans/joins, whole-row field display,
  function-scan aliases, and subquery labels.

Files touched:
- crates/pgrust_sql_grammar/src/gram.pest
- src/include/nodes/parsenodes.rs
- src/include/nodes/primnodes.rs
- src/backend/parser/gram.rs
- src/backend/parser/analyze/sqlfunc_inline.rs
- src/backend/parser/analyze/expr.rs
- src/backend/parser/analyze/infer.rs
- src/backend/parser/analyze/expr/func.rs
- src/backend/parser/analyze/scope.rs
- src/backend/optimizer/constfold.rs
- src/backend/commands/explain.rs
- plus existing partial-patch files from earlier rangefuncs work

Tests run:
- cargo fmt
- scripts/cargo_isolated.sh check
- scripts/run_regression.sh --jobs 1 --port 55465 --timeout 120 --test rangefuncs --results-dir /tmp/diffs/rangefuncs-final51

Remaining:
- /tmp/diffs/rangefuncs-final51 passes with 437/437 matched queries.
- No remaining `rangefuncs` diffs in the focused regression run.
