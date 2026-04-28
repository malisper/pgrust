Goal:
Fix role-membership GRANT/REVOKE semantics surfaced by privileges regression.

Key decisions:
Track whether ADMIN was specified separately from its boolean value.
Existing membership rows only update explicitly specified options.
Duplicate no-op GRANTs now emit PostgreSQL-style NOTICEs.
Missing grantor-specific REVOKEs now emit PostgreSQL-style WARNINGs and continue.
Explicit role grantors must have direct ADMIN option on the target role, including when grantor equals target role.
CREATE ROLE/GROUP membership clauses default grant-level INHERIT from the member role's rolinherit.

Files touched:
src/include/nodes/parsenodes.rs
src/backend/parser/gram.rs
src/backend/parser/tests.rs
src/pgrust/database/commands/privilege.rs
src/pgrust/database/commands/role.rs

Tests run:
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet role_membership
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh check
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/run_regression.sh --test privileges --timeout 60 --port 59433

Remaining:
privileges regression still times out later with unrelated privilege/LOCK/view/function/default-ACL gaps.
