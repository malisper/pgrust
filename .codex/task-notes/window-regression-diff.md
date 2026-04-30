Goal:
Fix PostgreSQL `window` regression mismatches without expected-output edits.

Key decisions:
The original tenk1 peer-order failures were planner cost/path-choice issues: unordered, non-covering btree range probes looked too cheap and displaced bitmap heap order.
Runtime fixes were needed for GROUPS-frame underflow and moving aggregate FILTER handling.
Planner fixes added active-window ordering, native window-order index path handling, and run-condition plumbing.
SQL-visible compatibility fixes covered parser/tcop error diagnostics and EXPLAIN text for window-specific output, run conditions, frame rendering, and remaining tenk1 plan display.
`Query::columns()` now filters resjunk targets so stored view validation uses visible query width.

Files touched:
src/backend/commands/explain.rs
src/backend/executor/nodes.rs
src/backend/executor/startup.rs
src/backend/executor/tests.rs
src/backend/executor/window.rs
src/backend/optimizer/path/allpaths.rs
src/backend/optimizer/path/costsize.rs
src/backend/optimizer/plan/planner.rs
src/backend/optimizer/plan/subselect.rs
src/backend/optimizer/setrefs.rs
src/backend/parser/analyze/expr.rs
src/backend/parser/analyze/modify.rs
src/backend/parser/analyze/scope.rs
src/backend/parser/analyze/window.rs
src/backend/tcop/postgres.rs
src/include/nodes/execnodes.rs
src/include/nodes/parsenodes.rs
src/include/nodes/pathnodes.rs
src/include/nodes/plannodes.rs

Tests run:
cargo fmt
env -u RUSTC_WRAPPER CARGO_BUILD_RUSTC_WRAPPER= scripts/cargo_isolated.sh test --lib --quiet planner_prefers_bitmap_heap_for_unordered_btree_range_window_input
env -u RUSTC_WRAPPER CARGO_BUILD_RUSTC_WRAPPER= scripts/cargo_isolated.sh test --lib --quiet planner_uses_spgist_polygon_distance_ordering_for_window_input
env -u RUSTC_WRAPPER CARGO_BUILD_RUSTC_WRAPPER= scripts/cargo_isolated.sh test --lib --quiet pg_get_viewdef_renders_window_functions_and_function_rtes
env -u RUSTC_WRAPPER CARGO_BUILD_RUSTC_WRAPPER= scripts/cargo_isolated.sh test --lib --quiet window
env -u RUSTC_WRAPPER CARGO_BUILD_RUSTC_WRAPPER= CARGO_TARGET_DIR=/tmp/pgrust-target-cairo-v2-nosccache cargo build --bin pgrust_server
env -u RUSTC_WRAPPER CARGO_BUILD_RUSTC_WRAPPER= CARGO_TARGET_DIR=/tmp/pgrust-target-cairo-v2-nosccache scripts/run_regression.sh --test window --skip-build --port 6669 --results-dir /tmp/diffs/window-current24

Remaining:
`window` regression now passes: 388/388 queries matched in /tmp/diffs/window-current24.
No `window.diff` remains for that run.
