Goal:
Diagnose and fix the create_table regression timeout caused by event-trigger preflight work.
Key decisions:
Renamed branch to malisper/create-table-diffs as requested. The timeout was caused by repeated event-trigger preflight checks rebuilding the full backend catcache. PostgreSQL avoids this with evtcache.c, so pgrust now has a small event-trigger cache that reads only pg_event_trigger, caches rows by event, pre-normalizes tag filters, and invalidates on full reset or PgEventTrigger changes. PgClass/PgAttribute/PgType and relation-only relcache invalidations no longer clear this cache.
Files touched:
.codex/task-notes/create-table-diffs.md
src/backend/catalog/store/storage.rs
src/backend/utils/cache/evtcache.rs
src/backend/utils/cache/inval.rs
src/backend/utils/cache/mod.rs
src/backend/utils/cache/syscache.rs
src/pgrust/database/commands/event_trigger.rs
src/pgrust/database_tests.rs
Tests run:
find /tmp/diffs for create_table artifacts; rg for create_table mentions; CARGO_TARGET_DIR=/tmp/pgrust-target-madrid-create-table scripts/run_regression.sh --test create_table --results-dir /tmp/diffs/create_table-current --timeout 60.
scripts/cargo_isolated.sh test --lib --quiet event_trigger (with isolated target env): 8 passed.
scripts/cargo_isolated.sh test --lib --quiet backend_cache (with isolated target env): 0 matched, command succeeded.
CARGO_TARGET_DIR=/tmp/pgrust-target-madrid-create-table scripts/run_regression.sh --test create_table --results-dir /tmp/diffs/create_table-after-event-cache --timeout 60: completed as normal diff failure, not timeout; 260/330 matched, 70 mismatched, 431 diff lines; worker duration 26s.
CARGO_TARGET_DIR=/tmp/pgrust-target-madrid-create-table scripts/run_regression.sh --skip-build --test create_table --results-dir /tmp/diffs/create_table-after-event-cache-profile --timeout 60: completed as normal diff failure, not timeout; worker duration 31s. Sample file: /tmp/pgrust_create_table_after_event_cache.sample.txt.
CARGO_TARGET_DIR=/tmp/pgrust-target-madrid-create-table scripts/run_regression.sh --test create_table --results-dir /tmp/diffs/create_table-after-event-cache-final --timeout 60: completed as normal diff failure, not timeout; 260/330 matched, 70 mismatched, 431 diff lines; worker duration 34s.
Remaining:
create_table still has existing functional diffs: 260/330 query blocks matched, 70 mismatched. The timeout/performance issue is fixed. Artifacts are under /tmp/diffs/create_table-current, /tmp/diffs/create_table-timeout180, /tmp/diffs/create_table-timeout60-skipbuild, /tmp/diffs/create_table-after-event-cache, /tmp/diffs/create_table-after-event-cache-profile, and /tmp/diffs/create_table-after-event-cache-final.
Profile:
Sampled worker pgrust_server during create_table into /tmp/pgrust_create_table.sample.txt. Fresh profiled run /tmp/diffs/create_table-profile completed in 56s. Dominant client-thread path: Session::execute_internal -> statement_may_fire_event_triggers -> Database::event_trigger_may_fire -> backend_catcache -> load_backend_catcache -> catcache_with_snapshot -> scan_catalog_relation_visible -> decode catalog rows into Vec<Value>. Roughly 2342/5061 client-thread samples were in event-trigger eligibility and 2111 were under catcache_with_snapshot. WAL/background flushing was negligible.
After the event-trigger cache change, a 10s sample of the create_table worker found no statement_may_fire_event_triggers/event_trigger_may_fire/event_trigger_cache stack entries. backend_catcache still appears in ordinary parser/planner/catalog paths, but the former event-trigger preflight hotspot is gone.
