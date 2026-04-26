Goal:
Diagnose and fix create_operator regression diffs around role/schema/type/function privilege setup inside BEGIN blocks.

Key decisions:
Use active transaction catalog context for GRANT/REVOKE object privileges, role membership changes, SET ROLE, and SET SESSION AUTHORIZATION.
Split type/function ACL helpers into autocommit wrappers plus transaction-aware helpers.
For duplicate visible pg_namespace rows with the same OID, prefer the row carrying an ACL so public schema CREATE grants from test_setup are honored.

Files touched:
src/pgrust/session.rs
src/pgrust/database/commands/privilege.rs
src/pgrust/database/commands/session_auth.rs
src/backend/utils/cache/catcache.rs
src/backend/utils/cache/lsyscache.rs

Tests run:
CARGO_TARGET_DIR=.context/cargo-target cargo check
CARGO_TARGET_DIR=.context/cargo-target scripts/run_regression.sh --test create_operator --port 5533 --results-dir /tmp/pgrust-create-operator-austin-v3-3

Remaining:
The isolated cargo check reports an existing unreachable-pattern warning in src/bin/query_repl.rs.
