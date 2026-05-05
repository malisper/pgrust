Goal:
Diagnose the create_role.access.out regression diff.

Key decisions:
The diff is not one root cause. It points at four narrow gaps:
- ADMIN-option checks are direct-only in pgrust, but PostgreSQL's is_admin_of_role follows membership paths.
- ALTER ROLE permission detail only reports restricted attribute/password details, missing the generic CREATEROLE+ADMIN detail for ordinary attribute changes.
- SYSID is accepted as a noise word but notice plumbing is intentionally deferred.
- REVOKE CREATE ON DATABASE ... CASCADE ignores cascade and leaves dependent database_create_grants behind.

Files touched:
Implementation touched:
- crates/pgrust_commands/src/rolecmds.rs
- src/pgrust/auth.rs
- src/pgrust/database/commands/role.rs
- src/pgrust/database/commands/privilege.rs
- crates/pgrust_analyze/src/expr.rs
- crates/pgrust_analyze/src/functions.rs
- src/backend/tcop/postgres.rs

Investigation targets:
- src/pgrust/auth.rs
- src/pgrust/database/commands/role.rs
- src/pgrust/database/commands/privilege.rs
- crates/pgrust_commands/src/rolecmds.rs

Tests run:
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet auth
scripts/cargo_isolated.sh test --lib --quiet role
scripts/cargo_isolated.sh test -p pgrust_analyze --lib --quiet resolve_shobj_description_accepts_regrole_oid_alias
scripts/run_regression.sh --test create_role --port 5543 --timeout 90 --results-dir /tmp/pgrust-create-role-results-7
scripts/cargo_isolated.sh test --lib --quiet rewrite_shobj_description_handles_regrole_literal
scripts/run_regression.sh --test create_role --port 5544 --timeout 120 --results-dir /tmp/pgrust-create-role-results-10

The pasted diff hunks are gone. The follow-up shobj_description failures were caused by
rewrite_shobj_description_calls rewriting 'role'::regrole to a permission-checked
SELECT oid FROM pg_authid. PostgreSQL regrole input resolves through catalog lookup,
not a user-visible pg_authid scan, so the rewrite now preserves the regrole cast.

Remaining:
create_role passes: 144 matched, 0 mismatched. /tmp/diffs/create_role.diff is empty because there is no remaining regression diff.
