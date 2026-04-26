Goal:
Diagnose and fix box regression EXPLAIN showing << instead of <-> for SP-GiST box distance ORDER BY.

Key decisions:
Keep normalized scan strategy for execution, but make EXPLAIN lookup AMOP ordering strategy 15 before falling back for normalized strategy 1.

Files touched:
src/backend/executor/nodes.rs

Tests run:
cargo fmt
cargo check
CARGO_TARGET_DIR=/tmp/pgrust-box-target scripts/run_regression.sh --test box --results-dir /tmp/pgrust_regress_box_fix

Remaining:
None.
