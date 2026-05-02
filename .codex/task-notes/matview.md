Goal:
Fix `matview` regression failure around renaming a base-table column used by materialized views with explicit output column names.

Key decisions:
Treat materialized-view rule output-name mismatches as allowed aliases during stored query validation, while preserving width/type checks and stale composite-function attribute checks.

Files touched:
`src/backend/rewrite/views.rs`
`src/pgrust/database_tests.rs`

Tests run:
`env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool/pgrust/hamburg-v2 PGRUST_TARGET_POOL_SIZE=1 scripts/cargo_isolated.sh test --lib --quiet materialized_view_base_column_rename_preserves_alias_dependencies`
`env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool/pgrust/hamburg-v2 PGRUST_TARGET_POOL_SIZE=1 scripts/run_regression.sh --test matview --timeout 120`
`env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool/pgrust/hamburg-v2 PGRUST_TARGET_POOL_SIZE=1 scripts/cargo_isolated.sh check`

Remaining:
None.
