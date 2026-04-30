Goal:
- Add PostgreSQL-like GIN pending-list support and gin_clean_pending_list.

Key decisions:
- Fastupdate inserts append to pending list pages; forced cleanup drains pending entries into main GIN pages.
- Main insertion now updates entry/posting pages incrementally, with an empty-main cleanup fast path for initial pending-list flushes.
- GIN reloptions persist fastupdate/gin_pending_list_limit and update on-disk metapage options.
- GIN page-image writes use RM_GIN_ID WAL records and recovery dispatch.
- Full GIN rewrites clear stale FSM entries before rewriting pages.

Files touched:
- src/backend/access/gin/gin.rs
- src/backend/access/gin/mod.rs
- src/backend/access/gin/wal.rs
- src/backend/access/transam/xlog.rs
- src/backend/access/transam/xlogrecovery.rs
- src/backend/storage/fsm.rs
- src/backend/catalog/store/heap.rs
- src/backend/executor/exec_expr.rs
- src/backend/parser/analyze/functions.rs
- src/include/catalog/pg_proc.rs
- src/include/nodes/primnodes.rs
- src/pgrust/database/commands/reloptions.rs
- src/pgrust/database_tests.rs

Tests run:
- CARGO_TARGET_DIR=/tmp/pgrust-target-pool/washington-v1/gin-slot RUSTC_WRAPPER=/usr/bin/env scripts/cargo_isolated.sh test --lib --quiet gin
- CARGO_TARGET_DIR=/tmp/pgrust-target-pool/washington-v1/gin-regress RUSTC_WRAPPER=/usr/bin/env scripts/run_regression.sh --test gin --timeout 180 --jobs 1 --port 55443 --results-dir /tmp/pgrust_regress_gin_washington6

Remaining:
- Upstream gin regression still has non-pending-list mismatches: seq scan vs bitmap plan text, EXPLAIN FORMAT json mismatch, temp schema qualification in plan output, and DELETE/index-recheck behavior after posting-tree vacuum. Latest diff copied to /tmp/diffs/gin.diff.
