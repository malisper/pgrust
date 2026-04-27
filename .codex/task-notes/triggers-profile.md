Goal:
Profile why the triggers regression times out under the 90s file budget.

Key decisions:
The 90s triggers run timed out after 654/1265 matched queries. A focused
minimal repro of the last echoed ATTACH PARTITION statement returned
immediately, so the timeout marker is from cumulative file runtime, not that
statement hanging.

Sampling the actual regression worker showed repeated catalog-heavy DDL paths:
CREATE TRIGGER spends time in lookup_trigger_relation_for_ddl ->
lookup_any_relation -> backend_relcache -> backend_catcache ->
catcache_with_snapshot -> load_physical_catalog_rows_visible_scoped ->
scan_catalog_relation_visible. Later samples showed DROP TABLE cleanup in
drop_relation_by_oid_mvcc_with_extra_type_rows -> delete_catalog_rows_subset_mvcc
-> catalog_tuple_delete_matching -> find_catalog_tuple_tid -> row decoding, and
partition/FK table creation rebuilding catalog state while installing
constraints and index support metadata.

Files touched:
.codex/task-notes/triggers-profile.md

Tests run:
scripts/run_regression.sh --test triggers --timeout 90 --port 61800 --results-dir /tmp/diffs/triggers-timeout --skip-build
scripts/run_regression.sh --test triggers --timeout 300 --port 62000 --results-dir /tmp/diffs/triggers-profile-run --skip-build
sample 98171 10 -file /tmp/diffs/triggers-profile-run/pgrust_triggers_worker.sample.txt
sample 98171 10 -file /tmp/diffs/triggers-profile-run/pgrust_triggers_worker_late.sample.txt
sample 98171 8 -file /tmp/diffs/triggers-profile-run/pgrust_triggers_worker_convslot.sample.txt

Remaining:
The 300s run completed as FAIL, not TIMEOUT, matching 1125/1265 queries. The
next performance fix should target repeated catalog/relcache rebuild and
catalog tuple scan/delete costs in trigger DDL, partitioned constraint/index
setup, and drop cleanup.
