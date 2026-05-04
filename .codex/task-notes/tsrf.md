Goal:
Fix high-impact `tsrf` regression mismatches around target SRFs with grouping.

Key decisions:
Treat explicit grouping-set `GroupingKey(...)` wrappers as satisfying grouped SRF placement so `GROUP BY CUBE(..., generate_series(...))` does not get a second post-aggregate `ProjectSet`.
Add grouped aggregate-output lowering for single-argument `unnest(...)` so SRFs computed after aggregation do not fall through to scalar function execution.

Files touched:
crates/pgrust_optimizer/src/plan/planner.rs
crates/pgrust_analyze/src/agg_output.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh check
scripts/run_regression.sh --test tsrf --timeout 120 --results-dir /tmp/pgrust-tsrf-after
scripts/run_regression.sh --test tsrf --timeout 120 --results-dir /tmp/pgrust-tsrf-after-2

Remaining:
`tsrf` still fails on explain plan shape, SQLSTATE/caret/hint formatting for rejected SRFs, UPDATE SRF rejection, non-top-level SRF scalar function resolution, DISTINCT ON SRF support, SRF operator support, and some duplicate/nested SRF explain details.
