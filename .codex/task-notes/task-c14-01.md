Goal:
Close TASK-C14-01 owned `stats` and `stats_import` regression failures.

Key decisions:
Expression index ANALYZE should treat `attstattarget = -1` as enabled/default and skip only `0`, matching PostgreSQL.
Partial BRIN bitmap paths were suppressed because hash-only partial-index gettuple support was applied to all partial integer indexes; restrict that helper to hash indexes.

Files touched:
src/backend/commands/analyze.rs
src/backend/optimizer/path/allpaths.rs

Tests run:
scripts/run_regression.sh --test stats --port 61234 --results-dir /tmp/pgrust-task-c14-01-stats
scripts/run_regression.sh --test stats_import --port 61236 --results-dir /tmp/pgrust-task-c14-01-stats-import
scripts/cargo_isolated.sh check

Remaining:
None for the owned stats/stats_import surfaces. Broader C14 sanity/catalog cleanup remains out of scope.
