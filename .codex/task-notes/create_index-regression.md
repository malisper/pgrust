Goal:
Fix non-planner create_index regression failures from /tmp/pgrust-diffs-2026-04-30T0340Z/create_index.diff.

Key decisions:
Keep planner path-selection differences out of scope. Fixed SQL-visible semantics/catalog behavior for geometry predicates, PL/pgSQL dynamic CTAS, ALTER TABLE CLUSTER ON, reindex catalog effects, pg_depend rows, pg_attribute.attstattarget nullability, expression-index statistics, and pg_get_indexdef collation/cast rendering.

Files touched:
Parser/AST, cluster/reindex commands, PL/pgSQL exec, pg_depend/catalog row generation, pg_attribute row codec/cache loading, analyze expression-index stats, sql_deparse/pg_get_indexdef, and focused database/parser tests.

Tests run:
Focused cargo tests for geometry counts, cluster-on parsing/execution, PL/pgSQL dynamic CTAS, reindex invalid/toast behavior, pg_depend rows, text-collation deparse, attstattarget reset, and expression-index stats.
Regression: scripts/run_regression.sh --test create_index --timeout 300 --results-dir /tmp/pgrust_regress_create_index_florence. Result: 663/687 queries matched, 24 mismatches, 469 diff lines. Copied diff to /tmp/diffs/create_index.florence-v3.diff and linked it at /tmp/pgrust-diffs-2026-04-30T0340Z/create_index.florence-v3.after.diff.
CI follow-up: restored non-index relation-to-type pg_depend rows after cargo-test-run (2/2) failed create_type_nested_dependencies_and_named_composite_arrays_work, drop_enum_type_enforces_restrict_and_if_exists, and drop_range_type_enforces_restrict_and_if_exists.
Validation: scripts/cargo_isolated.sh test --lib --quiet create_type_nested_dependencies_and_named_composite_arrays_work; drop_enum_type_enforces_restrict_and_if_exists; drop_range_type_enforces_restrict_and_if_exists. Also ran scripts/cargo_isolated.sh check.
PR check follow-up: cargo-test-run (1/2) failed analyze_expression_index_reports_nested_sql_function_context due nondeterministic sampling; made the test force expression-index stats before ANALYZE. cargo-test-run (2/2) failed partition_index_pg_depend_rows_use_partition_deptypes because restored relation type deps included pinned builtin integer; filtered pinned type dependencies.
Validation: scripts/cargo_isolated.sh test --lib --quiet partition_index_pg_depend_rows_use_partition_deptypes; analyze_expression_index_reports_nested_sql_function_context; type_enforces_restrict; create_type_nested_dependencies_and_named_composite_arrays_work. Also reran scripts/cargo_isolated.sh check.

Remaining:
Residual create_index diffs are EXPLAIN/path-shape issues: GiST KNN/correlated display, Sort-key parentheses, OR/SAOP BitmapOr/BitmapAnd shape, index-vs-seq-scan choices, Memoize/join choices, row-comparison/index qual display, and bitmap_split_or plan grouping.
