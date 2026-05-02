Goal:
Implement PostgreSQL-compatible system columns for combocid coverage, especially cmin and ctid behavior in /tmp/pgrust-regression-diffs-2026-05-01T2044Z/combocid.diff.

Key decisions:
Added cmin/cmax system-column binding and raw command-id evaluation. Kept ctid on the existing tuple-id path.
Separated heap-visible command ids from pgrust's internal catalog command counter so cmin/cmax match PostgreSQL command-counter behavior.
Added local combo-cid forward and reverse maps so heap visibility can decode cmin/cmax pairs while a transaction is active.
Updated heap visibility and unique-index probing to treat parent/subtransaction xids in the current snapshot as own transactions.

Files touched:
src/include/nodes/primnodes.rs
src/include/nodes/execnodes.rs
src/backend/parser/analyze/scope.rs
src/backend/parser/analyze/constraints.rs
src/backend/executor/exec_expr.rs
src/backend/executor/nodes.rs
src/backend/commands/explain.rs
src/backend/commands/tablecmds.rs
src/backend/access/heap/heapam.rs
src/backend/access/heap/heapam_visibility.rs
src/backend/access/index/unique.rs
src/backend/access/transam/xact.rs
src/pgrust/session.rs

Tests run:
cargo fmt
unset CARGO_TARGET_DIR; PGRUST_TARGET_SLOT=7 scripts/cargo_isolated.sh check
unset CARGO_TARGET_DIR; PGRUST_TARGET_SLOT=7 scripts/cargo_isolated.sh build --bin pgrust_server
env -u CARGO_TARGET_DIR PGRUST_TARGET_SLOT=7 scripts/run_regression.sh --test combocid --timeout 120 --port 57900 --skip-build --results-dir /tmp/pgrust_regress_combocid_cmin

Remaining:
None for combocid; focused regression passes all 62 queries.
