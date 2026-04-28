Goal:
Implement memoize-owned regression fixes for the PostgreSQL memoize regression.

Key decisions:
Added planner GUC support for enable_hashjoin, enable_mergejoin, and enable_memoize.
Changed Query.limit_offset to Option<usize> so explicit OFFSET 0 blocks EXISTS pull-up.
Added Plan::Memoize plus executor MemoizeState with hit/miss/eviction/overflow/memory instrumentation.
Wrapped parameterized nested-loop inner plans in Memoize when the inner depends on immediate nest params.
Generalized parameterized inner path creation across subquery/projection/limit/filter wrappers and added a fallback parameterized filter for non-seqscan inner paths.
PL/pgSQL dynamic EXECUTE now passes current planner GUCs through config-aware planning, including dynamic EXPLAIN.
Runtime index arguments now detect casts/functions under boolean clauses, which fixed expression-key Memoize planning.
EXPLAIN actual rows now reports per-loop row counts.
Follow-up pass fixed btree float comparison for -0.0/+0.0, added Memoize cache key labels and runtime index labels, added Heap Fetches reporting for index-only scans, charged Memoize cache memory for key/row/entry overhead, and added a broad unordered btree range-scan cost penalty.

Files touched:
src/include/nodes/pathnodes.rs
src/include/nodes/parsenodes.rs
src/include/nodes/plannodes.rs
src/include/nodes/execnodes.rs
src/pgrust/session.rs
src/pl/plpgsql/exec.rs
src/backend/parser/analyze/*
src/backend/rewrite/*
src/backend/optimizer/*
src/backend/executor/*
src/backend/commands/explain.rs
src/backend/commands/tablecmds.rs
src/pgrust/database/relation_refs.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet planner_memoizes_expression_key_nested_loop
scripts/run_regression.sh --test memoize --timeout 60 --port 55445

Remaining:
Latest memoize regression after the current pass: FAIL, 76/88 queries matched, 325 diff lines at /tmp/pgrust_memoize_after_textdecode/diff/memoize.diff.
No timeouts remain.
Fixed/mostly fixed: broad unique range scans now keep PostgreSQL's Seq Scan outer side, the first tenk memoize index-only hunks match, cache key labels for simple outer keys, expression key display for (t1.two + 1) and (t1.t)::numeric, runtime index cond labels, Heap Fetches lines, memoize eviction accounting, and float -0/+0 row counts.
Remaining hunks are planner-shape mismatches: explicit OFFSET 0 still leaves visible SubqueryScan/Limit wrappers in EXPLAIN, LEFT JOIN LATERAL strict quals are not reduced/pushed like PostgreSQL, expr_key still renders a non-index residual param as $0, partitionwise and union-all Append paths still pick the global Append/nested-loop orientation, join orientation differs for float and EXISTS cases, text inequality scan still reports too few child rows for the long text case, and the final parallel Gather/partial aggregate hunk remains out of scope.
