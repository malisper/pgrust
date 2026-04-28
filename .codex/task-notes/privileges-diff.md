Goal:
Diagnose and fix the `privileges.sql` lock-permission cascade caused by
unsupported `LOCK TABLE`.

Key decisions:
Implemented direct named-relation `LOCK [TABLE] ... [IN mode MODE] [NOWAIT]`
support for all PostgreSQL table lock modes. Inheritance/view-recursive locking
is intentionally deferred with a nearby `:HACK:` in the parser. PostgreSQL's
lock conflict matrix now drives table lock compatibility and `pg_locks` mode
names. `LOCK TABLE` is restricted to active transactions and uses the PostgreSQL
permission bands: SELECT only for ACCESS SHARE, INSERT through ROW EXCLUSIVE,
and UPDATE/DELETE/TRUNCATE/MAINTAIN for all modes. Added bootstrap
`pg_maintain`; predefined read/write/maintain roles satisfy matching relation
privileges, but write/maintain roles do not bypass protected catalog/toast
write checks.

Files touched:
Grammar/parser/AST, session/database execution routing, lock manager,
permission helpers, command tags, auth bootstrap roles, and focused parser,
lock-manager, and session tests.

Tests run:
`cargo fmt`
`scripts/cargo_isolated.sh test --lib --quiet parse_lock_table`
`scripts/cargo_isolated.sh test --lib --quiet table_lock_conflicts_match_postgres_matrix`
`scripts/cargo_isolated.sh test --lib --quiet lock_table`
`scripts/cargo_isolated.sh test --lib --quiet autovacuum_once_skips_locked_table_without_blocking`
`scripts/cargo_isolated.sh test --lib --quiet pg_write_all_data_does_not_allow_system_catalog_writes`
`scripts/cargo_isolated.sh test --lib --quiet bootstrap_toast_relations_resolve_before_privilege_checks`
`scripts/cargo_isolated.sh check`
`scripts/run_regression.sh --test privileges --timeout 120 --jobs 1 --results-dir /tmp/pgrust_privileges_lock_results2`

Remaining:
`privileges` now completes without error and the `LOCK TABLE` section is not in
the diff. Remaining mismatches are unrelated privilege/function/operator/large
object gaps; latest run matched 960/1295 queries with 2332 diff lines.
