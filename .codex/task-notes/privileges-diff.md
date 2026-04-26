Goal:
Diagnose why the privileges regression diff diverges from PostgreSQL.

Key decisions:
Primary causes are incomplete object GRANT/REVOKE grammar, missing runtime ACL enforcement, partial ACL helper functions, missing LOCK TABLE syntax, and several unsupported PostgreSQL privilege-adjacent features. The huge later diff mostly cascades from early ACL setup failures.

Files touched:
.codex/task-notes/privileges-diff.md
src/include/nodes/parsenodes.rs
src/backend/parser/gram.rs
src/backend/parser/tests.rs
src/pgrust/database/commands/privilege.rs

Tests run:
scripts/cargo_isolated.sh test --lib --quiet parse_grant
scripts/cargo_isolated.sh test --lib --quiet parse_revoke
scripts/cargo_isolated.sh test --lib --quiet table_grant_update_delete_and_revoke_delete_update_acl
Attempted scripts/run_regression.sh --test privileges --timeout 60 --results-dir /tmp/pgrust_privileges_regress_after_grant_acl, but it waited on Cargo locks for several minutes and was terminated before running.
Retried with CARGO_TARGET_DIR=/tmp/pgrust-target-privileges-retry. The normal upstream schedule failed before privileges while building the post_create_index base because create_index hit `index build failed: Io("GIN jsonb_ops can only index jsonb values")`. A one-test schedule (`test: privileges`) ran privileges from test_setup and timed out: 306/1295 query outputs matched, results in /tmp/pgrust_privileges_regress_after_grant_acl_retry_priv_only.

Remaining:
Implement table/column/function/schema/type/large-object ACL enforcement in binder/executor. Add LOCK TABLE and has_*_privilege helpers afterward.
