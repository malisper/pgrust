Goal:
- Fix type/domain/cast/composite-attribute privilege behavior in `privileges`.

Key decisions:
- Preserve `ON TYPE` vs `ON DOMAIN` in grant/revoke AST.
- Use `pg_type.typacl` plus owner/public defaults for type USAGE checks.
- Reject direct array ACL edits; map array/multirange privilege checks to the PostgreSQL ACL target.
- Apply type USAGE checks at DDL type-resolution sites rather than in executor-only code.
- Keep domain cast rows droppable while warning when source type is a domain.

Files touched:
- Parser AST/grammar/tests, pg_proc catalog rows, executor privilege builtin dispatch.
- Privilege commands, create/cast/operator/type/alter-table command paths.
- Default ACL object-address materialization.

Tests run:
- `env -u CARGO_TARGET_DIR -u PGRUST_TARGET_POOL_DIR PGRUST_TARGET_POOL_SIZE=16 PGRUST_TARGET_SLOT=11 scripts/cargo_isolated.sh check`
- Focused unit tests for type ACL blocking, array/domain target errors, default type ACLs.
- Parser tests for `ON DOMAIN` grant/revoke parsing.
- `env -u CARGO_TARGET_DIR -u PGRUST_TARGET_POOL_DIR scripts/run_regression.sh --test privileges --results-dir /tmp/diffs/privileges-type-acl --timeout 120 --jobs 1`

Remaining:
- Full `privileges` still fails in unrelated sections. The type/domain/cast/default-type-ACL hunks no longer appear in `/tmp/diffs/privileges-type-acl/diff/privileges.diff`.
