Goal:
Match PostgreSQL warning behavior for no-op type privilege revokes seen in create_operator regression.
Key decisions:
Warn for type REVOKE based on grant-option authority, not whether the target ACL entry changed.
Files touched:
src/pgrust/database/commands/privilege.rs
Tests run:
scripts/cargo_isolated.sh test --lib --quiet owner_revoke_type_usage_from_ungranted_role_does_not_warn
scripts/run_regression.sh --test create_operator --timeout 60 --jobs 1
Remaining:
None.
