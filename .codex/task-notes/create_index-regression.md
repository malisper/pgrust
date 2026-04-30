Goal:
Fix non-planner create_index regression failures from /tmp/pgrust-diffs-2026-04-30T0340Z/create_index.diff.

Key decisions:
Keep planner path-selection differences out of scope. Fixed SQL-visible semantics/catalog behavior for geometry predicates, PL/pgSQL dynamic CTAS, ALTER TABLE CLUSTER ON, reindex catalog effects, pg_depend rows, pg_attribute.attstattarget nullability, expression-index statistics, and pg_get_indexdef collation/cast rendering.

Files touched:
Parser/AST, cluster/reindex commands, PL/pgSQL exec, pg_depend/catalog row generation, pg_attribute row codec/cache loading, analyze expression-index stats, sql_deparse/pg_get_indexdef, and focused database/parser tests.

Tests run:
Focused cargo tests for geometry counts, cluster-on parsing/execution, PL/pgSQL dynamic CTAS, reindex invalid/toast behavior, pg_depend rows, text-collation deparse, attstattarget reset, and expression-index stats.
Regression: scripts/run_regression.sh --test create_index --timeout 300 --results-dir /tmp/pgrust_regress_create_index_florence. Result: 663/687 queries matched, 24 mismatches, 469 diff lines. Copied diff to /tmp/diffs/create_index.florence-v3.diff and linked it at /tmp/pgrust-diffs-2026-04-30T0340Z/create_index.florence-v3.after.diff.

Remaining:
Residual create_index diffs are EXPLAIN/path-shape issues: GiST KNN/correlated display, Sort-key parentheses, OR/SAOP BitmapOr/BitmapAnd shape, index-vs-seq-scan choices, Memoize/join choices, row-comparison/index qual display, and bitmap_split_or plan grouping.
