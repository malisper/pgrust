Goal:
Investigate current rangefuncs regression diffs under /tmp/diffs.

Key decisions:
Reran focused rangefuncs regression twice: once with existing shared binary and once with an isolated rebuilt CARGO_TARGET_DIR to rule out stale build artifacts.
The current failure is 5 explain-only mismatches; all corresponding SELECT result rows match PostgreSQL.
Diffs are in SQL-function inlining/explain formatting and planner shape for lateral function scans, not ROWS FROM execution or ordinality behavior.

Files touched:
None.

Tests run:
scripts/run_regression.sh --skip-build --jobs 1 --test rangefuncs --timeout 120 --port 55466 --results-dir /tmp/diffs/rangefuncs-current
CARGO_TARGET_DIR=/tmp/pgrust-target-rangefuncs-current scripts/run_regression.sh --jobs 1 --test rangefuncs --timeout 120 --port 55467 --results-dir /tmp/diffs/rangefuncs-current-rebuilt

Remaining:
Fixed in the follow-up patch. `rangefuncs` now passes 437/437 matched queries in `/tmp/diffs/rangefuncs-fix2`.
