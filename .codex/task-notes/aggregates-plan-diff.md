Goal:
Implement PostgreSQL-style aggregate planner alignment for the `aggregates.diff`
regression, prioritizing real planner/executor behavior over EXPLAIN shims.

Key decisions:
- Added `AggAccum::presorted` and PG-like ordered/distinct aggregate input
  pathkey selection. Plain and grouped ordered aggregates can now request input
  Sort/IncrementalSort, and only the covered regular aggregates skip executor
  local sorting. Ordered-set and hypothetical aggregates keep executor-side
  ordered input collection.
- Added GROUP BY pathkey alternatives: normal order, outer ORDER BY prefix,
  existing input pathkey order, and bounded group-key permutations. Sorted
  aggregate input planning can substitute a matching ordered index path for a
  base scan.
- Moved partial aggregate planning toward catalog metadata by requiring
  `pg_aggregate.aggcombinefn`, while still gating unsupported executor partial
  state shapes. Added float8 variance/regr_count partial state combine support
  and custom aggregate combine support.
- Fixed custom partial aggregation semantics: Partial Aggregate emits raw
  transition states; Finalize Aggregate applies the combine function. This fixes
  PostgreSQL's `balk`-style combine-returning-NULL case.
- Fixed parallel Append row loss by adding `parallel_scan_id` to SeqScan plan and
  executor state and assigning unique scan ids to cloned parallel-aware Append
  children. Repeated UNION ALL scans of the same relation no longer share a
  block cursor.
- Took a narrow MinMax rewrite step: preserve/rewrite outer ORDER BY, keep
  `has_target_srfs`, and allow constant min/max args. Full MinMaxAggPath as an
  upper path is not implemented.

Files touched:
- `crates/pgrust_optimizer/src/plan/planner.rs`
- `crates/pgrust_optimizer/src/setrefs.rs`
- `crates/pgrust_optimizer/src/root.rs`
- `crates/pgrust_catalog_data/src/pg_aggregate.rs`
- `crates/pgrust_nodes/src/plannodes.rs`
- `crates/pgrust_nodes/src/primnodes.rs`
- `src/backend/executor/agg.rs`
- `src/backend/executor/nodes.rs`
- `src/backend/executor/startup.rs`
- `src/include/nodes/execnodes.rs`
- constructor/test callsites updated for `parallel_scan_id` and `presorted`
- focused tests in `src/backend/executor/tests.rs` and
  `src/pgrust/database_tests.rs`

Tests run:
- `scripts/cargo_isolated.sh check`
- `scripts/cargo_isolated.sh test --lib --quiet aggregate`
- `scripts/cargo_isolated.sh test --lib --quiet parallel_append_repeated_seqscan_returns_each_child_rows`
- `scripts/cargo_isolated.sh test --lib --quiet custom_aggregate_with_combine_uses_parallel_partial_aggregate`
- `scripts/cargo_isolated.sh test --lib --quiet custom_parallel_combine_returning_null_finalizes_to_null`
- `scripts/run_regression.sh --test aggregates --timeout 120 --results-dir /tmp/pgrust_regress_aggregates_fix`

Remaining:
- `aggregates` still has plan/display diffs. Fresh rerun after the custom
  combine fix matched 507/583 queries and showed 1100 diff lines in
  `/tmp/pgrust_regress_aggregates_fix_rerun`.
- Full MinMaxAggPath-style planning is still not implemented. Current early
  rewrite still causes plan-shape and alias/correlation differences, especially
  inherited min/max and correlated min/max subqueries.
- GROUP BY alternatives are bounded and pragmatic, not a complete PG
  `get_useful_group_keys_orderings` clone.
- Remaining regression hunks include EXPLAIN alias/parentheses differences,
  MinMax path-shape differences, some grouping-key display/order differences,
  and unsupported/known aggregate error text differences.
