Goal:
Make create_function_sql pass against upstream expected output without updating expected files.

Key decisions:
Renamed branch to malisper/create-function-sql.
Used PostgreSQL behavior as reference and kept changes in parser/catalog/executor/system-view layers.
Used an isolated target dir for regression builds because other workspaces held the shared Cargo build lock.
Removed stale /tmp/diffs/create_function_sql.diff and .out after the final passing run.

Files touched:
src/include/nodes/parsenodes.rs
src/backend/parser/gram.rs
src/backend/parser/tests.rs
src/pgrust/database/commands/create.rs
src/pgrust/database/commands/routine.rs
src/pgrust/database/commands/drop.rs
src/backend/executor/sqlfunc.rs
src/backend/executor/srf.rs
src/backend/executor/expr_casts.rs
src/backend/executor/value_io/array.rs
src/backend/executor/exec_expr.rs
src/backend/parser/analyze/scope.rs
src/backend/parser/analyze/expr/func.rs
src/backend/parser/analyze/system_views.rs
src/backend/utils/cache/system_view_registry.rs
src/backend/tcop/postgres.rs
src/backend/catalog/store/heap.rs
src/backend/optimizer/path/allpaths.rs
src/backend/rewrite/views.rs
src/pgrust/database/commands/sequence.rs
src/pgrust/database/commands/opclass.rs

Tests run:
cargo fmt
CARGO_TARGET_DIR=/tmp/pgrust-target-create-function-sql scripts/cargo_isolated.sh check
Result: PASS with existing unreachable-pattern warnings.
CARGO_TARGET_DIR=/tmp/pgrust-target-create-function-sql scripts/cargo_isolated.sh test --lib --quiet create_function
Result: PASS, 24 passed.
CARGO_TARGET_DIR=/tmp/pgrust-target-create-function-sql scripts/run_regression.sh --test create_function_sql --jobs 1 --port 61344 --results-dir /tmp/pgrust_regress_create_function_sql
Result: PASS, 180/180 queries matched.

Remaining:
None for create_function_sql. The regression passes against ../postgres expected output.
