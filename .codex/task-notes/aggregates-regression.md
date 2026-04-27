Goal:
- Investigate and fix the aggregates regression server error around outer references in aggregate FILTER clauses.

Key decisions:
- PostgreSQL includes aggregate FILTER expressions when computing aggregate semantic level.
- pgrust already classified `count(*) FILTER (WHERE outer_c <> 0)` as an outer aggregate, but rebound its filter expression at the owner level without raising varlevels for the child query.
- Raise visible outer aggregate args, direct args, ORDER BY expressions, and FILTER expressions by the visible aggregate scope depth before storing the `Aggref` in the child query.

Files touched:
- `src/backend/parser/analyze/expr.rs`
- `src/backend/parser/analyze/agg_output.rs`
- `src/backend/optimizer/tests.rs`

Tests run:
- `scripts/cargo_isolated.sh test --lib --quiet planner_lowers_outer_aggregate_filter_refs_in_scalar_subqueries`
- `scripts/cargo_isolated.sh test --lib --quiet planner_lowers_outer_aggregate_refs_in_correlated_subqueries`
- `scripts/cargo_isolated.sh check`
- `scripts/run_regression.sh --test aggregates --timeout 240 --jobs 1 --port 59447 --results-dir /tmp/pgrust-aggregates-daegu-fixed`

Remaining:
- `aggregates` now completes without an error, but still fails with ordinary output diffs.
