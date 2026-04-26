Goal:
Fix remaining PostgreSQL planner/explain parity gaps in the union regression.

Key decisions:
Kept the work in planner/explain paths instead of output shims. Added conservative qual pushdown through set-op inputs, disabled hash aggregate EXPLAIN metadata, xid group-key rendering, expression-index condition rendering, simple `var = var` simplification, and partial target-aware index-only path support.

Files touched:
`src/backend/optimizer/path/allpaths.rs`, `src/backend/optimizer/path/costsize.rs`, `src/backend/optimizer/path/mod.rs`, `src/backend/optimizer/setrefs.rs`, `src/backend/optimizer/constfold.rs`, `src/backend/commands/explain.rs`, `src/backend/executor/nodes.rs`, plus previously touched parser/planner/executor support files in this workspace.

Tests run:
`cargo fmt`
`PGRUST_TARGET_SLOT=1 scripts/cargo_isolated.sh check`
`PGRUST_TARGET_SLOT=1 scripts/cargo_isolated.sh build --bin pgrust_server`
`CARGO_TARGET_DIR=$(PGRUST_TARGET_SLOT=1 scripts/cargo_isolated.sh --print-target-dir) scripts/run_regression.sh --test union --jobs 1 --timeout 180 --skip-build --port 55447 --results-dir /tmp/pgrust-union-neg-render`

Remaining:
Latest `union` run is still failing at 175/197 matched with 443 diff lines. Remaining gaps are mostly real planner path parity: ordered index/index-only set-op children, MergeAppend through set-op/inheritance, parameterized append nested-loop paths, single-row VALUES as Result, and some sorted-vs-hash set-op choices. The xid and expression-index condition rendering differences are fixed.
