Goal:
Fix rowtypes regression failure where `fcompos3.v.*` inside an inlined SQL function was treated as an unknown column instead of the composite argument value.

Key decisions:
Track the inlined SQL function name alongside argument bindings, then resolve `function.arg` / `function.arg.*` only when the qualifier matches that function name. Preserve existing single-arg dotted-field fallback for other unknown dotted references.

Files touched:
src/backend/parser/analyze/expr.rs
src/backend/parser/analyze/expr/func.rs
src/backend/parser/analyze/scope.rs
src/backend/parser/analyze/sqlfunc_inline.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
env -u CARGO_TARGET_DIR PGRUST_TARGET_SLOT=6 scripts/cargo_isolated.sh test --lib --quiet sql_function_qualified_composite_arg_star_passes_whole_value
env CARGO_TARGET_DIR=/tmp/pgrust-rowtypes-target-baku scripts/run_regression.sh --test rowtypes --jobs 1 --timeout 180 --results-dir /tmp/pgrust-rowtypes-composite-wrappers-final

Remaining:
`rowtypes` still fails on pre-existing row comparison/plan/order mismatches, but the `fcompos3.v.*` hunk is gone and actual output inserts `(3, three)`.
