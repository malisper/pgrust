Goal:
Diagnose polygon.diff regression mismatch.

Key decisions:
The row-count mismatch is a polygon containment semantic bug, not an index-only
loss of tuples. Reproducing the regression data shows ids 4726 and 5325 are
counted by PostgreSQL but rejected by pgrust because pgrust's
polygon_contains_polygon requires all inner vertices inside and no edge
intersection. PostgreSQL's poly_contain_poly checks each contained edge with
lseg_inside_poly, which allows boundary-touching contained segments.

The EXPLAIN mismatch is separate: pgrust plans an Index Scan for the <@
predicate and an explicit Sort for the window ORDER BY, while PostgreSQL uses
the SP-GiST ORDER BY distance operator directly. The seq/index result comparison
still returns zero rows because both sides use pgrust's 829-row semantics.

Fix:
Ported PostgreSQL-style segment-in-polygon containment into
src/backend/executor/expr_geometry.rs. Fixed SP-GiST/GiST ORDER BY matching for
geometry constructor constants and polygon opclasses whose storage key type is
box but operator type is polygon. Window planning now prefers generated native
ordered index paths over adding an explicit-sort competitor for unordered input
paths. EXPLAIN rendering now maps normalized ordering strategy 1 back to the
catalog ORDER BY strategy 15 operator.

Files touched:
.codex/task-notes/polygon-diff.md
src/backend/executor/expr_geometry.rs
src/backend/executor/nodes.rs
src/backend/optimizer/path/costsize.rs
src/backend/optimizer/plan/planner.rs
src/backend/optimizer/tests.rs

Tests run:
Read the pasted actual output and expected polygon.out. Ran a local script to
reproduce the generated polygon grid and identify the two boundary-touching ids.
CARGO_TARGET_DIR=$PWD/.context/cargo-target cargo test --lib --quiet polygon_contains_boundary_touching_regression_rows
CARGO_TARGET_DIR=$PWD/.context/cargo-target cargo test --lib --quiet planner_uses_spgist_polygon_distance_ordering_for_window_input
CARGO_TARGET_DIR=$PWD/.context/cargo-target scripts/run_regression.sh --test polygon --jobs 1 --timeout 120

Remaining:
None for polygon.diff.
