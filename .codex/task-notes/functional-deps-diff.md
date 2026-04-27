Goal:
Fix functional_deps regression diffs around grouped output validation and the
remaining direct-smoke blockers.

Key decisions:
PostgreSQL accepts non-grouped columns from a base relation when that
relation's primary key columns are all grouped. pgrust previously only accepted
exact grouped columns/expressions and errored on the first non-grouped column.

Implemented a PostgreSQL-style last-resort primary-key FD check in grouped
output binding. It uses catalog constraints with CONSTRAINT_PRIMARY only, so
UNIQUE NOT NULL remains rejected like PostgreSQL's current fail/todo case.
Base scope columns now carry source relation OID/attno metadata, and merged
JOIN USING columns carry all source columns so grouping by the merged key can
prove the left relation's primary key. Aggregate layout now preserves accepted
functionally-dependent Vars as passthrough expressions.

Grouped view creation now tracks the primary-key constraint OIDs used to prove
functional grouping and records normal pg_depend rows from the rewrite rule to
those constraints. ALTER TABLE DROP CONSTRAINT ... RESTRICT parses and rejects
constraint drops when dependent views exist. CASCADE remains explicitly
unsupported.

Added minimal SQL PREPARE/EXECUTE support at the Session layer for no-parameter
prepared statements. EXECUTE replans from the stored raw AST, so the prepared
functional-deps query succeeds before dropping the primary key and fails after
the key is dropped, matching PostgreSQL's regression behavior.

The final regression diff was missing PostgreSQL's LINE/caret context for
multi-line grouped-output errors. The tcop error-position finder now locates a
top-level FROM keyword across newlines/comments so SELECT-target grouped errors
emit the protocol position field consistently.

Files touched:
src/backend/parser/analyze/agg_output.rs
src/backend/parser/analyze/scope.rs
src/backend/parser/analyze/generated.rs
src/backend/parser/analyze/modify.rs
src/backend/parser/analyze/on_conflict.rs
src/backend/parser/analyze/rules.rs
src/backend/parser/analyze/mod.rs
src/backend/optimizer/groupby_rewrite.rs
src/backend/catalog/store/heap.rs
src/backend/executor/driver.rs
src/backend/parser/gram.pest
src/backend/parser/gram.rs
src/backend/parser/tests.rs
src/backend/tcop/postgres.rs
src/bin/query_repl.rs
src/include/nodes/parsenodes.rs
src/pgrust/database/commands/constraint.rs
src/pgrust/database/commands/create.rs
src/pgrust/database/commands/execute.rs
src/pgrust/database/commands/matview.rs
src/pgrust/database/ddl.rs
src/pgrust/database_tests.rs
src/pgrust/session.rs
src/pl/plpgsql/compile.rs
.codex/task-notes/functional-deps-diff.md

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet build_plan_allows_using_merged_primary_key_for_grouped_functional_dependency
scripts/cargo_isolated.sh test --lib --quiet parse_prepare_and_execute_statements
scripts/cargo_isolated.sh test --lib --quiet grouped_view_blocks_primary_key_constraint_drop_restrict
scripts/cargo_isolated.sh test --lib --quiet sql_prepare_execute_replans_after_primary_key_drop
scripts/cargo_isolated.sh test --lib --quiet simple_query_reports_position_for_grouped_output_error
scripts/cargo_isolated.sh test --lib --quiet functional_dependency
scripts/cargo_isolated.sh test --lib --quiet grouped
scripts/cargo_isolated.sh check
CARGO_TARGET_DIR=/tmp/pgrust-target-denver-fd CARGO_INCREMENTAL=0 cargo build --bin pgrust_server
Direct smoke: ../postgres/src/test/regress/sql/functional_deps.sql via psql
against /tmp/pgrust-target-denver-fd/debug/pgrust_server on port 55435.
scripts/run_regression.sh --test functional_deps --skip-build --jobs 1
--timeout 300 --port 55435 --results-dir
/tmp/pgrust-regress-functional-deps-denver --data-dir
/tmp/pgrust-regress-functional-deps-denver-data
git diff --check

Remaining:
functional_deps now passes the regression comparison: 40/40 queries matched.
The remaining SQL errors in the output are expected PostgreSQL regression
results, not diffs.
