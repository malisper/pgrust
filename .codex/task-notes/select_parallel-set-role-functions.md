Goal:
Fix SET ROLE / caller identity behavior inside SQL and PL/pgSQL functions, driven by select_parallel and privileges regression diffs.

Key decisions:
Apply SECURITY DEFINER and function proconfig identity changes in the function call wrapper. Treat function SET role as a temporary active-role/current-user change with session-user permission checks, and restore identity after success or error. Route GRANT/REVOKE inside SQL functions through the database executor under the temporary function identity.

Files touched:
src/backend/executor/function_guc.rs
src/backend/executor/sqlfunc.rs
src/backend/executor/exec_expr.rs
src/pl/plpgsql/compile.rs
src/pl/plpgsql/exec.rs
src/pgrust/auth.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
CARGO_TARGET_DIR=/tmp/pgrust-target-sacramento-role CARGO_INCREMENTAL=0 cargo test --lib --quiet sql_function_set_role_option_changes_identity_temporarily
CARGO_TARGET_DIR=/tmp/pgrust-target-sacramento-role CARGO_INCREMENTAL=0 cargo test --lib --quiet security_definer_sql_function_uses_owner_identity_and_restores_caller
CARGO_TARGET_DIR=/tmp/pgrust-target-sacramento-role CARGO_INCREMENTAL=0 cargo test --lib --quiet security_definer_sql_function_grants_roles_as_owner
CARGO_TARGET_DIR=/tmp/pgrust-target-sacramento-role CARGO_INCREMENTAL=0 cargo test --lib --quiet session_user_and_current_role_are_sql_visible
CARGO_TARGET_DIR=/tmp/pgrust-target-sacramento-role CARGO_INCREMENTAL=0 cargo test --lib --quiet security_definer_plpgsql_function_uses_owner_identity_and_restores_caller

Remaining:
select_parallel still has unrelated diff hunks in the provided result set; run the full regression file after neighboring unsupported-feature work lands.
