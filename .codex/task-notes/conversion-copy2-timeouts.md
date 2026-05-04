Goal:
Investigate conversion.diff UTF-8 validation statement timeouts and copy2.diff TRUNCATE MVCC wait errors.

Key decisions:
conversion timeout was reproducible in isolation and still timed out with statement_timeout=60s before the fix. The failing shape is the nested CTE query where test_padded depends on test_bytes and the final join also scans test_bytes. The root cause was nested PL/pgSQL SELECT execution clearing the caller's CTE tables/producers while the outer CTE was being materialized. Fix: save and restore cte_tables, cte_producers, and recursive_worktables in executor driver and PL/pgSQL planned-query execution.

copy2 TRUNCATE failure was reproducible in isolation with fresh XIDs. The second TRUNCATE after SAVEPOINT waited on the parent transaction that performed the first TRUNCATE. Root cause was catalog snapshots and catalog delete matching not treating all XIDs owned by the same client as own after savepoint/subxid transitions. Fix: expose TransactionWaiter holder XIDs by client, add them to catalog snapshots, and make catalog tuple deletion prefer exact visible rows before identity fallback.

Files touched:
crates/pgrust_storage/src/lmgr/proc.rs
src/backend/catalog/indexing.rs
src/backend/catalog/persistence.rs
src/backend/catalog/store.rs
src/backend/catalog/store/heap.rs
src/backend/catalog/store/roles.rs
src/backend/catalog/store/storage.rs
src/backend/executor/driver.rs
src/backend/utils/cache/syscache.rs
src/backend/utils/time/snapmgr.rs
src/pgrust/database_tests.rs
src/pl/plpgsql/exec.rs

Tests run:
PGRUST_STATEMENT_TIMEOUT=60 scripts/run_regression.sh --skip-build --port 5743 --jobs 1 --test conversion --timeout 180 --results-dir /tmp/pgrust-conversion-probe2
scripts/run_regression.sh --skip-build --port 5743 --jobs 1 --test copy2 --timeout 120 --results-dir /tmp/pgrust-copy2-probe
scripts/cargo_isolated.sh test --lib --quiet nested_cte_survives_plpgsql_internal_select
scripts/cargo_isolated.sh test --lib --quiet truncate_after_savepoint_uses_current_catalog_version
scripts/run_regression.sh --port 5743 --jobs 1 --test conversion --timeout 180 --results-dir /tmp/pgrust-conversion-final
scripts/run_regression.sh --skip-build --port 5743 --jobs 1 --test copy2 --timeout 120 --results-dir /tmp/pgrust-copy2-final2

Remaining:
conversion still has non-timeout encoding behavior diffs after the UTF-8 padding queries.
copy2 still has unrelated COPY behavior diffs, but the reported catalog delete wait errors are gone.
