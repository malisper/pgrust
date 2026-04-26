Goal:
Eliminate the `triggers` regression file timeout by reducing PostgreSQL-style
catalog and storage-manager overhead.

Key decisions:
Added a keyed per-backend syscache with catalog-kind invalidation, kept broad
catcache invalidation for compatibility, rewired CREATE TABLE range type checks
to avoid full auth catalogs, and changed md smgr close/LRU tracking to avoid
global scans.

Files touched:
src/backend/utils/cache/syscache.rs
src/backend/utils/cache/inval.rs
src/pgrust/database/commands/create.rs
src/backend/storage/smgr/md.rs

Tests run:
scripts/cargo_isolated.sh test --lib --quiet backend_syscache
scripts/cargo_isolated.sh test --lib --quiet syscache_query_key
scripts/cargo_isolated.sh test --lib --quiet target_fork_handles
scripts/cargo_isolated.sh test --lib --quiet close_preserves_other_forks
scripts/cargo_isolated.sh test --lib --quiet range_owner_and_usage_privileges_apply_to_multirange_columns
scripts/cargo_isolated.sh check
scripts/run_regression.sh --test triggers --port 55446 --jobs 1 --timeout 300

Remaining:
`triggers` still fails expected-output diffs; timeout is gone. Profile sample
is at `/tmp/pgrust_triggers_after_cache.sample.txt`.
