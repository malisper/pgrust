Goal:
Implement PostgreSQL-style large object support and get largeobject regression green.

Key decisions:
Store pg_largeobject and pg_largeobject_metadata as heap-backed physical catalog rows, with LargeObjectRuntime only managing per-client descriptors.
Execute large-object SQL functions against catalog rows and descriptor state, with PostgreSQL ACL/read-only/lo_compat_privileges behavior.
Support GRANT/REVOKE ON LARGE OBJECT, ALTER LARGE OBJECT OWNER, COMMENT ON LARGE OBJECT, and ALTER DEFAULT PRIVILEGES ON LARGE OBJECTS.
Handle psql/libpq large-object fastpath FunctionCall messages directly through Session instead of rewriting each call to SELECT SQL.
Update page writes to replace only touched pg_largeobject pages so repeated libpq lowrite calls do not rewrite the whole object.

Files touched:
Catalog physical row pipeline, parser/analyzer/function catalogs, executor LO builtins, session/protocol fastpath, database LO helpers, privilege handling, PL/pgSQL undefined_file mapping, bytea escape output, and targeted database/executor tests.

Tests run:
cargo fmt
CARGO_TARGET_DIR=/tmp/pgrust-target-regression-philadelphia RUSTC_WRAPPER= cargo check --lib --quiet
CARGO_TARGET_DIR=/tmp/pgrust-target-regression-philadelphia RUSTC_WRAPPER= cargo test --lib --quiet large_object
CARGO_TARGET_DIR=/tmp/pgrust-target-regression-philadelphia RUSTC_WRAPPER= cargo test --lib --quiet bytea_text_output_supports_hex_and_escape
CARGO_TARGET_DIR=/tmp/pgrust-target-regression-philadelphia RUSTC_WRAPPER= cargo test --lib --quiet has_privilege_builtins_match_missing_object_and_largeobject_edges
CARGO_TARGET_DIR=/tmp/pgrust-target-regression-philadelphia RUSTC_WRAPPER= cargo test --lib --quiet large_object_acl_and_default_privileges_follow_metadata_rows
CARGO_TARGET_DIR=/tmp/pgrust-target-regression-philadelphia RUSTC_WRAPPER= scripts/run_regression.sh --test largeobject --jobs 1 --port 5566
CARGO_TARGET_DIR=/tmp/pgrust-target-regression-philadelphia RUSTC_WRAPPER= scripts/run_regression.sh --test privileges --jobs 1 --port 5567

Remaining:
largeobject regression passes 129/129.
privileges regression timed out before its large-object section; first diffs are pre-existing unrelated role/planner/function privilege gaps, so LO ACL/default-ACL behavior is covered by targeted database tests instead.
