Goal:
Fix targeted window.sql regression diff groups from .context/attachments/pasted_text_2026-04-27_10-50-41.txt.
Key decisions:
First pasted mismatch is CREATE TEMPORARY TABLE empsalary returning ERROR table "1545345412" does not exist, causing later empsalary misses to cascade. Could not reproduce that on current checkout: existing temp_namespace_reuse_cleans_stale_relations_before_create_table_lowering unit passes with LLVM codegen; local window regression gets through empsalary setup.
Local run with 30s timeout truncates at the "opexpr with different windows evaluation" query; replaying the file directly completed, so that was timeout/truncation, not a deterministic crash.
Remaining pasted diff groups include unsupported window frame exclusion, unsupported RESPECT/IGNORE NULLS, unsupported temp functions/views, row order/frame semantics differences for unordered window queries, and EXPLAIN shape differences.
Current branch already had working window frame exclusion execution after rebase. Added parser/analyzer/executor support for RESPECT/IGNORE NULLS on lag, lead, first_value, last_value, and nth_value; other window functions reject it. Aggregates reject RESPECT/IGNORE NULLS whether used as plain aggregates or window aggregates.
Adjusted non-verbose EXPLAIN traversal so a planner-only Projection directly below HashAggregate is hidden when it has no direct subplans. The requested regression block now renders WindowAgg -> HashAggregate -> Seq Scan with Group Key: (tenk1.ten + tenk1.four).
View deparsing now preserves IGNORE NULLS in pg_get_viewdef and omits explicit RESPECT NULLS as the default.
Files touched:
crates/pgrust_sql_grammar/src/gram.pest
src/include/nodes/parsenodes.rs
src/include/nodes/primnodes.rs
src/backend/parser/gram.rs
src/backend/parser/analyze/{expr.rs,agg_output.rs,agg.rs,agg_scope.rs,window.rs}
src/backend/executor/window.rs
src/backend/commands/explain.rs
src/backend/rewrite/views.rs
src/backend/tcop/postgres.rs
src/backend/executor/tests.rs
src/backend/optimizer/tests.rs
src/backend/parser/tests.rs
src/pl/plpgsql/compile.rs
.codex/task-notes/window-regression-investigation.md
Tests run:
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet temp_namespace_reuse_cleans_stale_relations_before_create_table_lowering (pass)
scripts/cargo_isolated.sh test --lib --quiet temp_tables_create_and_use_temp_toast_namespace (blocked by Cranelift llvm.aarch64.crc32cx abort)
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/run_regression.sh --test window --jobs 1 --timeout 30 --results-dir /tmp/pgrust_window_regress_probe (errored due per-file timeout/truncation)
Manual psql replay of window.sql against same data/server completed; output in /tmp/window_manual.out.
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet temp_tables_create_and_use_temp_toast_namespace (pass after rebase check)
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet temp_namespace_reuse_cleans_stale_relations_before_create_table_lowering (pass after rebase check)
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh check (pass)
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet window_functions_support_respect_and_ignore_nulls (pass)
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet window_frame_exclusion_filters_aggregate_and_value_frames (pass)
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet explain_window_over_grouped_subquery_hides_group_projection (pass)
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet psql_get_viewdef_renders_window_ignore_nulls (pass)
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/run_regression.sh --test window --jobs 1 --timeout 120 --port 5545 --results-dir /tmp/pgrust-window-regression --skip-build (overall fail: 249/388 queries matched, 2512 diff lines; requested EXPLAIN block matches and no window frame exclusion unsupported error remains)
Remaining:
The full upstream window regression still fails for unrelated pre-existing gaps: statement timeout in one early query, SQL-language CREATE FUNCTION bodies, custom window aggregate execution, row-order/frame semantic differences, and broader EXPLAIN formatting/planning differences. Full diff copied to /tmp/diffs/window.diff.
