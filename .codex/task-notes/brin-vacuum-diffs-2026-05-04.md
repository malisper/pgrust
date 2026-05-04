Goal:
Fix regression failures from brin, cluster, numeric_big, vacuum, and rowsecurity
diffs: index-build TOAST decoding, oversized catalog statistic rows, and
visible fetches from stale/invalid TIDs.

Key decisions:
- Added object-safe detoast hooks in pgrust_access services and kept root as
  the runtime provider for fetching external TOAST bytes.
- Moved access-method row materialization for generic index build and BRIN
  summary build onto the new TOAST-aware service path.
- Kept root-owned catalog physical persistence in root, but added a small
  pgrust_catalog_store bootstrap helper to identify catalog TOAST relations.
- Added catalog TOAST persistence/loading for bootstrap catalog rows. Catalog
  TOAST indexes are intentionally not required for v1; detoast scans toast heap.
- Changed only visible heap fetch APIs to treat invalid offsets, unused item ids,
  and out-of-range blocks as no visible tuple. Direct heap fetch/update/delete
  remain strict.
- Made CLUSTER consume a heap command id so rows reinserted into rewritten
  storage are visible to the next statement in the same transaction.

Files touched:
- crates/pgrust_access/src/services.rs
- crates/pgrust_access/src/index/indexam.rs
- crates/pgrust_access/src/brin/runtime.rs
- crates/pgrust_access/src/heap/heapam.rs
- crates/pgrust_access/src/table/toast_helper.rs
- crates/pgrust_catalog_store/src/bootstrap.rs
- src/backend/access/common/detoast.rs
- src/backend/access/services.rs
- src/backend/executor/value_io.rs
- src/backend/catalog/persistence.rs
- src/backend/catalog/loader.rs
- src/backend/catalog/rowcodec.rs
- src/backend/catalog/indexing.rs
- src/pgrust/database/commands/cluster.rs
- src/pgrust/session.rs
- src/pgrust/database_tests.rs

Tests run:
- cargo fmt
- scripts/cargo_isolated.sh check
- scripts/run_regression.sh --test cluster: PASS
- scripts/run_regression.sh --skip-build --test brin: PASS
- scripts/run_regression.sh --skip-build --test numeric_big: PASS
- scripts/run_regression.sh --skip-build --test vacuum: PASS
- scripts/run_regression.sh --skip-build --test rowsecurity: still has broad
  existing RLS/planner/output mismatches, but the invalid-TID heap error no
  longer occurs.
- scripts/cargo_isolated.sh test --lib --quiet cluster_rebuilds_index_over_external_toast_columns
- scripts/cargo_isolated.sh test --lib --quiet create_brin_index_detoasts_external_heap_columns
- scripts/cargo_isolated.sh test --lib --quiet catalog_statistics_rows_can_toast_and_reload
- scripts/cargo_isolated.sh test --lib --quiet tid_scan_ignores_invalid_visible_fetch_candidates
- scripts/cargo_isolated.sh test --lib --quiet policy_expressions_can_reference_ctid

Remaining:
- rowsecurity full regression still needs separate RLS/current-of/planner parity
  work if the goal is a clean full-file match.
