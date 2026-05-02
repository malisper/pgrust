Goal:
Implement PostgreSQL-style default ACLs, GRANT/REVOKE ON ALL ... IN SCHEMA, and routine-aware function privilege checks.

Key decisions:
Default ACLs are stored as physical pg_default_acl rows and merged with hard-wired global defaults at object creation. Schema-local defaults start empty and overlay the global defaults. Function, procedure, aggregate, and routine defaults share the pg_proc function bucket. Bulk schema grants expand to concrete relation/proc OIDs before using the existing ACL mutation logic. Built-in pg_proc ACL changes use full ACL overrides so REVOKE FROM PUBLIC is observable. Trusted language ACLs use a small bootstrap override so privileges regression language USAGE checks match CREATE FUNCTION behavior until pg_language has a physical lanacl column.
Runtime function EXECUTE checks cache successful `(current_user_oid, proc_oid)` decisions per executor query and resolve bootstrap pg_proc rows directly before consulting live catalog state. This keeps explicit ACL overrides observable while avoiding repeated CatCache rebuilds in expression-heavy executor tests.

Files touched:
Parser/AST: src/include/nodes/parsenodes.rs, src/backend/parser/gram.rs, src/backend/parser/tests.rs.
Privilege/catalog execution: src/pgrust/database/commands/privilege.rs, create.rs, drop.rs, role.rs, schema.rs, sequence.rs, typecmds.rs, foreign_data_wrapper.rs, matview.rs, large_objects.rs.
Runtime/plumbing: src/backend/executor/exec_expr.rs, fmgr.rs, expr_agg_support.rs, driver.rs, src/pgrust/session.rs, src/bin/query_repl.rs.
Catalog/helpers: src/include/catalog/pg_proc.rs, pg_language.rs, src/backend/access/nbtree/nbtree.rs, src/backend/optimizer/constfold.rs, src/backend/parser/analyze/functions.rs, src/include/nodes/primnodes.rs.

Tests run:
Rebased with git rebase --autostash origin/perf-optimization and resolved exec_expr.rs conflict.
cargo fmt.
scripts/cargo_isolated.sh check passed before the external volume unmounted.
env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool RUST_SSD_ROOT=/tmp/pgrust-rust TMPDIR=/tmp scripts/cargo_isolated.sh check passed.
scripts/cargo_isolated.sh test --lib --quiet alter_default_privileges -- --test-threads=1 passed.
scripts/cargo_isolated.sh test --lib --quiet all_in_schema -- --test-threads=1 passed.
env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool RUST_SSD_ROOT=/tmp/pgrust-rust TMPDIR=/tmp scripts/cargo_isolated.sh test --lib --quiet privilege -- --test-threads=1 passed, 48 tests.
env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool RUST_SSD_ROOT=/tmp/pgrust-rust TMPDIR=/tmp PGRUST_SSD_ROOT=/tmp/pgrust PGRUST_DATA_DIR=/tmp/pgrust/data scripts/run_regression.sh --test privileges --timeout 120 --port 55433 ran; result dir /tmp/pgrust_regress_results.lusaka.SuhjGJ, 1194/1295 matched, 834 diff lines.
CI fix validation:
Fetched and rebased onto origin/perf-optimization at 56bae4ac1 after CI found the PR merge included a new alter_column_type ExecutorContext initializer.
env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool RUST_SSD_ROOT=/tmp/pgrust-rust TMPDIR=/tmp scripts/cargo_isolated.sh test --lib --quiet recursive_lsystem_points_query_executes -- --test-threads=1 passed, 1 test, 19.36s.
env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool RUST_SSD_ROOT=/tmp/pgrust-rust TMPDIR=/tmp scripts/cargo_isolated.sh test --lib --quiet privilege -- --test-threads=1 passed, 48 tests, 145.12s.
env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool RUST_SSD_ROOT=/tmp/pgrust-rust TMPDIR=/tmp scripts/cargo_isolated.sh test --lib --quiet pgrust::database::commands::role::tests::drop_role_reports_function_type_database_and_default_acl_dependencies -- --test-threads=1 passed, 1 test, 4.20s.
env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool RUST_SSD_ROOT=/tmp/pgrust-rust TMPDIR=/tmp scripts/cargo_isolated.sh check --quiet passed.
After rebase:
env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool RUST_SSD_ROOT=/tmp/pgrust-rust TMPDIR=/tmp scripts/cargo_isolated.sh test --lib --locked --no-run --quiet passed.
env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool RUST_SSD_ROOT=/tmp/pgrust-rust TMPDIR=/tmp scripts/cargo_isolated.sh test --lib --quiet recursive_lsystem_points_query_executes -- --test-threads=1 passed, 1 test, 19.31s.
env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool RUST_SSD_ROOT=/tmp/pgrust-rust TMPDIR=/tmp scripts/cargo_isolated.sh test --lib --quiet privilege -- --test-threads=1 passed, 48 tests, 144.08s.
env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool RUST_SSD_ROOT=/tmp/pgrust-rust TMPDIR=/tmp scripts/cargo_isolated.sh test --lib --quiet pgrust::database::commands::role::tests::drop_role_reports_function_type_database_and_default_acl_dependencies -- --test-threads=1 passed, 1 test, 4.23s.

Remaining:
Focused default-ACL, ON ALL ... IN SCHEMA, routine privilege classification, aggregate/procedure ownership, array cast EXECUTE, and language USAGE hunks are gone from privileges.diff. Remaining privileges regression diffs are pre-existing or adjacent gaps such as large-object owner-vs-permission text, ALTER DATABASE behavior, TRUNCATE inheritance, SRO/materialized-view behavior, stats/information_schema, grantor warnings, and other broader privilege semantics.
stash@{0} is the rebase autostash safety copy left after resolving the exec_expr.rs conflict.
