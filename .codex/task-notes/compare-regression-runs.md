Goal:
Compare regression-history runs 2026-05-04T2042Z and 2026-05-04T1857Z to explain lower pass/query numbers.
Key decisions:
Used origin/regression-history run artifacts and compared meta, summaries, diff inventories, and selected outputs. Fixed the regression runner so access-method fixture rewriting preserves numbered PostgreSQL alternate expected files.
Files touched:
.codex/task-notes/compare-regression-runs.md
scripts/run_regression.sh
Tests run:
scripts/run_regression.sh --test compression --results-dir /tmp/pgrust_regress_compression_fix
Remaining:
Investigate real output regressions in arrays/brin/gin/jsonb_jsonpath/multirangetypes.
