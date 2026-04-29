Goal:
Make PostgreSQL's subscription regression pass with catalog-backed subscription DDL.

Key decisions:
- Added real raw-parser AST support for CREATE/ALTER/DROP/COMMENT ON SUBSCRIPTION.
- Replaced the object-address-only subscription shim with a focused command module for catalog-backed create, alter, drop, comment, validation, permissions, and transaction restrictions.
- Stored subscription rows in object-address state, exposed them through pg_subscription scans, and wired pg_description comments for pg_subscription.
- Added pg_create_subscription with OID 6304, pg_subscription.subfailover, pg_stat_subscription_stats, and pg_stat_reset_subscription_stats(oid).
- Matched PostgreSQL/libpq-visible connection-string error formatting, including the trailing blank line in conninfo syntax errors.
- Used /tmp/pgrust-target-subscription for verification because shared Cargo targets were busy with other workspaces.

Files touched:
- src/backend/parser/gram.rs
- src/include/nodes/parsenodes.rs
- src/pgrust/database/commands/subscription.rs
- src/pgrust/database/commands/mod.rs
- src/pgrust/database/commands/execute.rs
- src/pgrust/session.rs
- src/backend/catalog/object_address.rs
- src/backend/catalog/store/heap.rs
- src/include/catalog/pg_subscription.rs
- src/include/catalog/pg_authid.rs
- src/backend/executor/nodes.rs
- src/backend/executor/exec_expr.rs
- src/backend/executor/sqlfunc.rs
- src/backend/parser/analyze/mod.rs
- src/backend/parser/analyze/system_views.rs
- src/backend/utils/cache/lsyscache.rs
- src/backend/utils/cache/system_view_registry.rs
- src/backend/tcop/postgres.rs
- src/bin/query_repl.rs
- src/backend/parser/tests.rs

Tests run:
- cargo fmt
- CARGO_TARGET_DIR=/tmp/pgrust-target-subscription scripts/cargo_isolated.sh check
- CARGO_TARGET_DIR=/tmp/pgrust-target-subscription CARGO_PROFILE_DEV_OPT_LEVEL=0 CARGO_INCREMENTAL=0 cargo test --lib --quiet subscription
- CARGO_TARGET_DIR=/tmp/pgrust-target-subscription scripts/run_regression.sh --test subscription --skip-build --results-dir /tmp/diffs/subscription --port 56555 --timeout 120
- git diff --check

Remaining:
- None for subscription regression acceptance. /tmp/diffs/subscription/diff/subscription.diff is absent or empty and the regression summary reports 158/158 matched.
