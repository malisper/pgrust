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
rangefuncs still fails 432/437 matched, 110 diff lines. Mismatches: one verbose EXPLAIN constant rendering for set-op SQL function returning numeric composite, three nested lateral SQL-function EXPLAIN shape/rendering differences around extractq2, and one EXPLAIN Memoize difference around jsonb_to_recordset lateral function.
