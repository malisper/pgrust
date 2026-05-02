Goal:
Implement PostgreSQL-style default ACLs, GRANT/REVOKE ON ALL ... IN SCHEMA, and routine-aware function privilege checks.

Key decisions:
Default ACLs are stored as physical pg_default_acl rows and merged with hard-wired global defaults at object creation. Schema-local defaults start empty and overlay the global defaults. Function, procedure, aggregate, and routine defaults share the pg_proc function bucket. Bulk schema grants expand to concrete relation/proc OIDs before using the existing ACL mutation logic. Built-in pg_proc ACL changes use full ACL overrides so REVOKE FROM PUBLIC is observable. Trusted language ACLs use a small bootstrap override so privileges regression language USAGE checks match CREATE FUNCTION behavior until pg_language has a physical lanacl column.

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

Remaining:
Focused default-ACL, ON ALL ... IN SCHEMA, routine privilege classification, aggregate/procedure ownership, array cast EXECUTE, and language USAGE hunks are gone from privileges.diff. Remaining privileges regression diffs are pre-existing or adjacent gaps such as large-object owner-vs-permission text, ALTER DATABASE behavior, TRUNCATE inheritance, SRO/materialized-view behavior, stats/information_schema, grantor warnings, and other broader privilege semantics.
stash@{0} is the rebase autostash safety copy left after resolving the exec_expr.rs conflict.
