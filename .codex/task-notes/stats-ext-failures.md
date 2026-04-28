Goal:
Explain why the stats_ext regression diff is failing.

Key decisions:
Grouped failures by root cause instead of treating every estimate mismatch as distinct.
The dominant cause is incomplete extended-statistics support: pg_statistic_ext rows can be created, but pg_statistic_ext_data, pg_stats_ext views, ndistinct/dependencies/MCV build, and planner selectivity use are missing or partial.

Files touched:
.codex/task-notes/stats-ext-failures.md
src/backend/parser/gram.rs
src/backend/parser/tests.rs
src/pgrust/database_tests.rs
src/backend/parser/analyze/mod.rs
src/backend/parser/analyze/system_views.rs
src/backend/utils/cache/system_view_registry.rs
src/backend/utils/cache/system_views.rs

Tests run:
scripts/cargo_isolated.sh test --lib --quiet parse_create_statistics
scripts/cargo_isolated.sh test --lib --quiet parse_drop_function_statement
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet drop_function_without_signature_drops_unique_match
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet pg_stats_ext_views_exist_and_bind_columns
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh check
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/run_regression.sh --test stats_ext --timeout 60
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/run_regression.sh --test stats_ext --timeout 300
sample 94222 15 -file /tmp/pgrust_stats_ext_timeout.sample.txt
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet pg_stats_ext_views_exist_and_bind_columns
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet btree_prunes_aborted_leaf_entries_when_page_is_full
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/run_regression.sh --test mvcc
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/run_regression.sh --test stats_ext --timeout 60
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh check
git fetch origin perf-optimization
git rebase origin/perf-optimization
sample 71772 15 -file /tmp/pgrust_stats_ext_late.sample.txt
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet build_plan_binds_stats_ext_any_and_function_predicates
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet build_plan_reports_postgres_any_all_array_errors
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet build_plan_in_list_common_type_includes_left_operand
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh check
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/run_regression.sh --test stats_ext --timeout 60 --port 5543
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/run_regression.sh --test stats_ext --timeout 300 --port 5543

Failed tests:
scripts/cargo_isolated.sh test --lib --quiet drop_function_without_signature_drops_unique_match hit an existing Cranelift llvm.aarch64.crc32cx panic before rerunning with LLVM.
scripts/run_regression.sh stats_ext was invoked with the wrong argument form; rerun with --test.
stats_ext regression timed out after 212/866 matching queries, but no pg_stats_ext or pg_stats_ext_exprs "relation does not exist" errors appeared in output/diff.
After rebasing on origin/perf-optimization, a 300s stats_ext run finished without timing out: 536/866 queries matched, 330 mismatched.
The 60s timeout stops at INSERT INTO functional_dependencies SELECT i, i, i, i FROM generate_series(1,5000) after TRUNCATE functional_dependencies.
The sample shows the active connection in execute_bound_insert_with_rules -> execute_insert -> maintain_indexes_for_row -> btinsert -> insert_tuple_into_page -> prune_aborted_leaf_items, not in stats view binding.
PostgreSQL difference for the timeout:
The timed-out stats_ext statement is the 5000-row insert into functional_dependencies after TRUNCATE, with two plain non-unique indexes.
PostgreSQL nbtree only heap-checks existing index tuples during unique conflict checking, or when deletion/dedup is needed to avoid a page split. It does not heap-fetch every item on every non-unique leaf insert.
pgrust calls prune_aborted_leaf_items for every leaf insert, and that retain() heap_fetches every existing tuple on the target leaf page to check aborted xmin.
This was introduced by 1558ff65fd (fix(mvcc): handle aborted DO index rows) as a temporary way to prevent aborted subtransaction index growth.
pgrust also decodes each leaf page into Vec<IndexTupleData> and rebuilds the page on normal insertion, while PostgreSQL inserts in-place with PageAddItem unless a split/delete/dedup is required. The sample shows pruning/heap_fetch is the largest specific avoidable cost.
Implemented the narrow B-tree pruning fix by trying the unpruned insert page first, pruning aborted leaf entries only after a leaf page image does not fit, and then splitting if the pruned image still does not fit.
The focused B-tree test passes, and cargo check passes.
mvcc still fails before exercising index-size comparison because DO cannot see transaction-local relation clean_aborted_self.
stats_ext no longer times out at the first 5000-row functional_dependencies insert; it advances from 212/866 to 264/866 matched queries and times out later at the expression ANY inequality query:
SELECT * FROM functional_dependencies WHERE (a * 2) < ANY (ARRAY[2, 102]) AND upper(b) > '1'
After rebasing on origin/perf-optimization, the branch rebased cleanly. The local B-tree edits were stashed before rebase and restored after rebase.
Late stats_ext profiling shows time in PL/pgSQL check_estimated_rows dynamic EXPLAIN planning, specifically bind_expr_with_outer_and_ctes -> bind_quantified_array_expr -> infer_sql_expr_type_with_ctes -> resolve_function_call/match_proc_signature, plus backend_catcache/CatCache/PgProcRow clone overhead.
PostgreSQL also executes PL/pgSQL dynamic EXECUTE without a saved plan, so the difference is not that PG avoids planning check_estimated_rows queries.
The concrete cost-shape difference is in expression transformation: PostgreSQL transforms ANY/ALL by recursively transforming left/right exactly once, then make_scalar_array_op reads exprType() from those already-transformed nodes and resolves the operator once. pgrust infers left and array types first, re-infers each array literal element to pick a target type, then binds the array, then binds the left side, causing repeated raw-expression walks and repeated function/type/catalog resolution.
For function calls, PostgreSQL transformFuncCall transforms arguments once, ParseFuncOrColumn gets actual arg OIDs from the transformed nodes, and func_get_detail resolves the function. pgrust function binding first infers every raw argument and resolves the function, then bind_scalar_function_call binds every argument and may infer again if the bound expression lacks a type hint.
Catalog lookup amplifies this: pgrust backend_catcache returns CatCache by value and clones the cached CatCache on every hit; CatCache itself owns large Vec/BTreeMap row collections, including proc_rows. The sample shows CatCache::clone and Vec<PgProcRow>::clone in the hot path. PostgreSQL syscache returns pinned catalog tuples and releases them after lookup instead of cloning the whole catalog cache per lookup.
Implemented the stats_ext planning hot-path optimization:
Added typed binding reuse for ordinary positional scalar/user-defined function calls.
Reworked quantified ARRAY literal binding to bind left/array elements once and coerce already-bound expressions.
Added with_backend_catcache for borrowed CatCache access and used it for type-by-oid, namespace-qualified type lookup, and cast lookup.
Added parser/analyzer test coverage for the stats_ext ANY + upper predicate shape.
After the first typed bind/cache pass, stats_ext --timeout 60 advanced from 264/866 to 367/866 and timed out in the UUID MCV section.
After the borrowed cast lookup, stats_ext --timeout 60 advanced to 440/866 and timed out later at the permission-test section after expression stats.
stats_ext --timeout 300 completes without timing out and remains a regression FAIL due expected semantic diffs: 536/866 matched, 2673 diff lines.

Remaining:
Inspect create_statistics, analyze extended-stat generation, extended-stat planner use, relkind/object support, the mvcc DO relation-visibility failure, and remaining stats_ext runtime. For remaining performance, sample the new 60s permission-test timeout point; likely remaining work is broader executor/planner/catalog overhead rather than the original ANY + upper binder hotspot.
