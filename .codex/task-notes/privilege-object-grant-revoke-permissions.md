Goal:
Add object GRANT/REVOKE parsing coverage and relation privilege enforcement for privileges regression work.

Key decisions:
- Parse table column privilege lists as structured per-privilege column specs.
- Accept object grant/revoke forms for TO/FROM GROUP, type/domain, language, and function/procedure/routine targets.
- Add relation privilege gates for planned SELECT paths, streaming SELECT, DML, COPY FROM, TRUNCATE, subplans, and SELECT FOR UPDATE.
- Preserve existing pg_attribute.attacl values when mutating a subset of column ACLs.
- Keep language ACL storage as a no-op compatibility shim because pg_language has no lanacl model here yet.

Files touched:
- src/backend/parser/gram.rs
- src/include/nodes/parsenodes.rs
- src/pgrust/database/commands/privilege.rs
- src/backend/commands/tablecmds.rs
- src/backend/executor/driver.rs
- src/backend/executor/exec_expr.rs
- src/pgrust/database/commands/execute.rs
- src/pgrust/session.rs
- src/include/catalog/pg_attribute.rs
- src/backend/catalog/store/heap.rs
- src/backend/parser/tests.rs
- src/pgrust/database_tests.rs

Tests run:
- cargo fmt
- CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh check
- CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet parse_grant
- CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet parse_revoke
- CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet mixed_column_privilege_grant_updates_attribute_acls
- CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet relation_privileges_gate_select_dml_copy_and_locking
- CARGO_TARGET_DIR=/tmp/pgrust-regression-target CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/run_regression.sh --test privileges --timeout 90 --port 59493 --results-dir /tmp/pgrust-privileges-results-7 --skip-build

Remaining:
- privileges regression still times out: 668/1295 queries matched.
- Remaining major gaps include exact column SELECT tracking, view owner/security-barrier semantics, GRANTED BY object-grant errors, PostgreSQL warning/no-op behavior for non-owner grants, default ACL catalog behavior, LOCK TABLE, function/type/domain privilege enforcement, large object privilege enforcement, and pg_auth_members/system catalog visibility exceptions.
