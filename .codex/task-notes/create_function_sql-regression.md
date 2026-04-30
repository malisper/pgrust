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

---

Goal:
Fix CI cargo-test failures reported after the create_function_sql PR was opened.

Key decisions:
Keep quoted LANGUAGE SQL body validation narrow: still reject syntax errors, bad
$n parameters, simple scalar return count/type mismatches, and empty bodies, but
do not reject supported quoted-body cases whose shape is validated at execution
time or depends on later catalog state.
Preserve SQL-standard runtime normalization separately from SQL-function inlining
so RETURN bodies can execute while non-SELECT inlining candidates are rejected.
Let scalar void SQL functions used in FROM produce a single function-scan row.
Keep catalog pg_get_functiondef output with raw $n references when no argument
names exist to deparse against.

Files touched:
src/pgrust/database/commands/create.rs
src/backend/executor/sqlfunc.rs
src/backend/executor/srf.rs
src/backend/executor/exec_expr.rs

Tests run:
cargo fmt
env -u RUSTC_WRAPPER CARGO_BUILD_RUSTC_WRAPPER= CARGO_TARGET_DIR=/tmp/pgrust-target-create-function-sql cargo test --lib --quiet sql_function
Result: PASS, 23 passed.
env -u RUSTC_WRAPPER CARGO_BUILD_RUSTC_WRAPPER= CARGO_TARGET_DIR=/tmp/pgrust-target-create-function-sql cargo test --lib --quiet create_function
Result: PASS, 24 passed.
env -u RUSTC_WRAPPER CARGO_BUILD_RUSTC_WRAPPER= CARGO_TARGET_DIR=/tmp/pgrust-target-create-function-sql scripts/run_regression.sh --test create_function_sql --jobs 1 --port 61344 --results-dir /tmp/pgrust_regress_create_function_sql
Result: PASS, 180/180 queries matched.

Remaining:
None for the attached CI repro set.

---

Goal:
Rebase PR #366 after GitHub reported the branch as merge-conflicted.

Key decisions:
Resolved base-branch conflicts by keeping the existing parser `WINDOW` field
rather than duplicating it.
Preserved SQL function multi-statement execution while carrying forward the
base branch's volatile snapshot handling.
Changed live `proc_rows_by_name` lookup to filter directly inside the backend
catcache; repeated regproc casts in create_function_sql were timing out when
they cloned or scanned proc rows per evaluated tuple.

Files touched:
src/backend/parser/gram.rs
src/backend/parser/tests.rs
src/backend/utils/cache/lsyscache.rs
src/include/nodes/parsenodes.rs
src/pgrust/database/commands/create.rs
src/pgrust/database/commands/sequence.rs

Tests run:
cargo fmt
env -u RUSTC_WRAPPER CARGO_BUILD_RUSTC_WRAPPER= CARGO_TARGET_DIR=/tmp/pgrust-target-create-function-sql cargo test --lib --quiet create_function
Result: PASS, 25 passed.
env -u RUSTC_WRAPPER CARGO_BUILD_RUSTC_WRAPPER= CARGO_TARGET_DIR=/tmp/pgrust-target-create-function-sql cargo test --lib --quiet sql_function
Result: PASS, 24 passed.
env -u RUSTC_WRAPPER CARGO_BUILD_RUSTC_WRAPPER= CARGO_TARGET_DIR=/tmp/pgrust-target-create-function-sql scripts/run_regression.sh --test create_function_sql --jobs 1 --port 61344 --results-dir /tmp/pgrust_regress_create_function_sql
Result: PASS, 180/180 queries matched after the lsyscache lookup fix.

Remaining:
Force-push rebased PR branch and re-check GitHub mergeability.

---

Goal:
Fix failed PR #366 merge-queue cargo-test-run (2/2).

Key decisions:
The failing CI test was `parse_drop_function_statement_with_multiple_names`.
The rebase left a stale parser branch that converted multi-item `DROP FUNCTION`
into generic `DropRoutine`, bypassing `DropFunctionStatement.additional_functions`.
Removed that branch so `DROP FUNCTION` always parses as `DropFunction`; `DROP
ROUTINE` still uses `DropRoutine`.

Files touched:
src/backend/parser/gram.rs

Tests run:
cargo fmt
env -u RUSTC_WRAPPER CARGO_BUILD_RUSTC_WRAPPER= CARGO_TARGET_DIR=/tmp/pgrust-target-create-function-sql scripts/cargo_isolated.sh test --lib --quiet parse_drop_function_statement_with_multiple_names
Result: PASS, 1 passed.
env -u RUSTC_WRAPPER CARGO_BUILD_RUSTC_WRAPPER= CARGO_TARGET_DIR=/tmp/pgrust-target-create-function-sql scripts/cargo_isolated.sh test --lib --quiet
Result: FAIL, 4028 passed, 4 failed, 1 ignored. The failures were database view tests unrelated to the parser change and all four passed when rerun individually.
env -u RUSTC_WRAPPER CARGO_BUILD_RUSTC_WRAPPER= CARGO_TARGET_DIR=/tmp/pgrust-target-create-function-sql scripts/cargo_isolated.sh test --lib --quiet auto_view_errors_preserve_postgres_column_specific_text
Result: PASS, 1 passed.
env -u RUSTC_WRAPPER CARGO_BUILD_RUSTC_WRAPPER= CARGO_TARGET_DIR=/tmp/pgrust-target-create-function-sql scripts/cargo_isolated.sh test --lib --quiet materialized_view_rejects_row_locks_and_renders_viewdef
Result: PASS, 1 passed.
env -u RUSTC_WRAPPER CARGO_BUILD_RUSTC_WRAPPER= CARGO_TARGET_DIR=/tmp/pgrust-target-create-function-sql scripts/cargo_isolated.sh test --lib --quiet nested_simple_views_auto_dml_returning_route_to_base_table
Result: PASS, 1 passed.
env -u RUSTC_WRAPPER CARGO_BUILD_RUSTC_WRAPPER= CARGO_TARGET_DIR=/tmp/pgrust-target-create-function-sql scripts/cargo_isolated.sh test --lib --quiet view_instead_of_triggers_fire_statement_triggers_and_return_rows
Result: PASS, 1 passed.

Remaining:
Push the parser fix and re-check PR #366 CI.

---

Goal:
Fix PR #366 cargo-test-archive failure after latest perf-optimization merge.

Key decisions:
Rebased onto current `origin/perf-optimization`.
Updated SQL-function dependency collection for the rebased parser AST: `SELECT
group_by` now stores `GroupByItem` instead of bare expressions, so dependency
and column-name collection recurse through grouping sets. Added handling for
`CteBody::Delete` in the same collector.

Files touched:
src/pgrust/database/commands/create.rs

Tests run:
cargo fmt
env -u RUSTC_WRAPPER CARGO_BUILD_RUSTC_WRAPPER= CARGO_TARGET_DIR=/tmp/pgrust-target-create-function-sql cargo test --no-run --lib --locked
Result: PASS with existing unreachable-pattern warnings.
env -u RUSTC_WRAPPER CARGO_BUILD_RUSTC_WRAPPER= CARGO_TARGET_DIR=/tmp/pgrust-target-create-function-sql scripts/cargo_isolated.sh test --lib --quiet parse_drop_function_statement_with_multiple_names
Result: PASS, 1 passed.

Remaining:
Force-push the rebased branch and re-check PR #366 CI.
