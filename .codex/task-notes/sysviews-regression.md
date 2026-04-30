Goal:
Fix PostgreSQL regression `sysviews` diffs by adding SRF-backed system views.

Key decisions:
Added PostgreSQL-shaped synthetic view metadata and `pg_proc` rows for the
missing sysview SRFs, using PostgreSQL OIDs. Bound those views as
`SetReturningCall::UserDefined` so normal planning handles projections,
filters, and aggregates. Removed the broad `pg_cursors` and
`pg_prepared_statements` wire shims. Added executor-native row producers for the
regression-visible views and a session snapshot for cursor/prepared statement
state.

Files touched:
`src/backend/utils/cache/system_view_registry.rs`
`src/backend/parser/analyze/system_views.rs`
`src/include/catalog/pg_proc.rs`
`src/backend/executor/srf.rs`
`src/pgrust/database.rs`
`src/pgrust/cluster.rs`
`src/pgrust/session.rs`
`src/backend/tcop/postgres.rs`
`src/backend/parser/tests.rs`
`.codex/task-notes/sysviews-regression.md`

Tests run:
`CARGO_TARGET_DIR=/tmp/pgrust-target-sysviews-check RUSTC_WRAPPER= CARGO_BUILD_RUSTC_WRAPPER= SCCACHE_DISABLE=1 cargo check --config 'build.rustc-wrapper=""'`
`CARGO_TARGET_DIR=/tmp/pgrust-target-sysviews-check RUSTC_WRAPPER= CARGO_BUILD_RUSTC_WRAPPER= SCCACHE_DISABLE=1 cargo test --lib --quiet --config 'build.rustc-wrapper=""' analyze_sysviews_srf`
`CARGO_TARGET_DIR=/tmp/pgrust-target-sysviews RUSTC_WRAPPER= CARGO_BUILD_RUSTC_WRAPPER= SCCACHE_DISABLE=1 scripts/run_regression.sh --test sysviews --results-dir /tmp/diffs/sysviews-current --timeout 120 --jobs 1`

Remaining:
Focused regression passes. Broader follow-up could replace the modeled fallback
rows with fuller PostgreSQL subsystem implementations where pgrust grows those
runtime features.
