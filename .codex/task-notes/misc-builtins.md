Goal:
Fix miscellaneous built-in regression gaps for WAL helpers, regression C helper shims, materialized-view security-restricted refresh, and replica identity/publication metadata.

Key decisions:
Added executor security_restricted state and routed REFRESH MATERIALIZED VIEW evaluation through owner/SRO contexts with side-effecting SQL-function support for INSERT/MERGE/GRANT role paths.
Kept regression C helper support intentionally narrow to test_canonicalize_path and test_relpath with a nearby :HACK: comment.
Used pg_index/pg_class catalog rows for psql describe replica identity flags where bound index metadata can be stale.
Left unrelated regression display noise alone: publication filter parenthesization, partial-index predicate rendering, broad privileges diffs, and duplicate DROP CONSTRAINT error wording.

Files touched:
WAL/SRF built-ins, SQL function execution, trigger parsing/runtime, materialized-view refresh, replica identity validation/catalog describe paths, parser tests, and focused database tests.

Tests run:
cargo fmt
CARGO_TARGET_DIR=/tmp/pgrust-target-pool/chicago-v3/0 scripts/cargo_isolated.sh check
CARGO_TARGET_DIR=/tmp/pgrust-target-pool/chicago-v3/0 scripts/cargo_isolated.sh test --lib --quiet misc_builtin_wal_helpers_and_regression_c_shims
CARGO_TARGET_DIR=/tmp/pgrust-target-pool/chicago-v3/0 scripts/cargo_isolated.sh test --lib --quiet materialized_view_refresh_uses_security_restricted_operation_guards
CARGO_TARGET_DIR=/tmp/pgrust-target-pool/chicago-v3/0 scripts/cargo_isolated.sh test --lib --quiet replica_identity_using_index_validates_candidate_key_metadata
CARGO_TARGET_DIR=/tmp/pgrust-target-pool/chicago-v3/0 scripts/cargo_isolated.sh test --lib --quiet alter_table_add_primary_key_index_is_visible_for_replica_identity
CARGO_TARGET_DIR=/tmp/pgrust-target-pool/chicago-v3/0 scripts/cargo_isolated.sh test --lib --quiet parse_create_constraint_trigger_statement
scripts/run_regression.sh --port 55433 --test misc_functions: FAIL, remaining unrelated pg_timezone_names/EXPLAIN plan-shape diffs.
scripts/run_regression.sh --port 55463 --test privileges: FAIL, SRO role-grant hunk fixed; remaining broad privilege/index maintenance and PL/pgSQL context formatting diffs.
scripts/run_regression.sh --port 55473 --skip-build --test publication: FAIL, remaining row-filter parenthesization diffs only.
CARGO_INCREMENTAL=0 scripts/run_regression.sh --port 55553 --test replica_identity: FAIL, remaining partial-index predicate rendering, partitioned parent index validity after explicit ATTACH, and duplicate DROP CONSTRAINT wording.

Remaining:
Consider separate follow-ups for psql predicate deparse parity, partitioned-index ATTACH validity propagation, duplicate DROP CONSTRAINT wording, timezone names, and EXPLAIN Memoize/hash join plan shape.
