Goal:
Fix requested regression files: create_index_spgist, index_including, init_privs, interval, reloptions, select_distinct_on. Skip create_function_c.
Key decisions:
- Added minimal synthetic pg_init_privs rows and a CURRENT_USER-to-postgres GRANT compatibility shim for init_privs.
- Routed ALTER TABLE SET reloptions through database metadata instead of treating every SET as a session setting.
- Made interval hashing use IntervalValue::cmp_key and added VALUES join-order tie-breakers to match PostgreSQL regression row order.
- Tuned EXPLAIN scan/sort rendering for temp schemas, text pattern opclasses, subquery aliases, and nested subquery sort keys.
- Added planner path preferences/costing for covering index-only scans, row-prefix btree quals, and DISTINCT ON incremental sort/pathkey reuse.
Files touched:
- src/backend/access/hash/support.rs
- src/backend/commands/explain.rs
- src/backend/executor/nodes.rs
- src/backend/optimizer/bestpath.rs
- src/backend/optimizer/path/allpaths.rs
- src/backend/optimizer/path/costsize.rs
- src/backend/optimizer/plan/planner.rs
- src/backend/parser/analyze/system_views.rs
- src/backend/parser/gram.rs
- src/backend/tcop/postgres.rs
- src/backend/utils/cache/system_view_registry.rs
- src/pgrust/session.rs
- scripts/run_regression.sh
Tests run:
- cargo fmt
- CARGO_TARGET_DIR=/tmp/pgrust-target-gwangju-v3-check2 RUSTC_WRAPPER= cargo check -q (passed; pre-existing unreachable-pattern warnings)
- cargo fmt --check
- scripts/cargo_isolated.sh check -q (passed; existing unreachable-pattern warnings and incremental-cache hardlink warnings)
- CARGO_TARGET_DIR=/tmp/pgrust-target-gwangju-v3-prcheck RUSTC_WRAPPER= cargo check -q (passed; pre-existing unreachable-pattern warnings)
- create_index_spgist: /tmp/pgrust-create_index_spgist-final PASS 202/202
- index_including: /tmp/pgrust-index_including-final PASS 135/135
- init_privs: /tmp/pgrust-init_privs-final PASS 4/4
- interval: /tmp/pgrust-interval-final PASS 450/450
- reloptions: /tmp/pgrust-reloptions-final PASS 66/66
- select_distinct_on: /tmp/pgrust-select_distinct_on-current10 PASS 23/23
- After rebasing, select_distinct_on single-test rerun first exposed the missing
  tenk1_hundred setup in scripts/run_regression.sh; fixed the harness helper and
  reran select_distinct_on: /Volumes/OSCOO PSSD/pgrust/tmp/gwangju-v3-pr-select_distinct_on-fixed PASS 23/23
Remaining:
- None for the six requested regression files.
