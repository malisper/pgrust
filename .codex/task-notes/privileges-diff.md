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

CI exposed that FK runtime locks were depending on the old non-PostgreSQL
ShareUpdateExclusive/RowExclusive conflict. Kept PostgreSQL's table-lock matrix
for user-visible relation locks and switched the coarse FK partner-lock shim to
ShareLock, which still conflicts with RowExclusive DML until real row-level
KEY SHARE/UPDATE FK locks exist.

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
`scripts/cargo_isolated.sh test --lib --quiet foreign_key_locking_blocks_parent_delete_until_child_insert_finishes`
`scripts/cargo_isolated.sh test --lib --quiet lock_table`
`scripts/cargo_isolated.sh check`
`scripts/cargo_isolated.sh test --lib --quiet foreign_key`

Remaining:
`privileges` now completes without error and the `LOCK TABLE` section is not in
the diff. Remaining mismatches are unrelated privilege/function/operator/large
object gaps; latest run matched 960/1295 queries with 2332 diff lines.
PR #324 CI had one failing lib test in `cargo-test-run (2/2)`; the local
targeted rerun now passes.

Follow-up diagnosis 2026-05-05:
The added error after
`UPDATE atest2 SET col2 = true FROM atest1 WHERE atest1.a = 5` is caused by
pgrust requiring SELECT on the UPDATE target relation while planning/executing
the joined row source. PostgreSQL allows this statement for
`regress_priv_user3`: that role has UPDATE on `atest2` and SELECT on `atest1`,
and the target table is not read by the assignment or WHERE expression. The
likely code path is `bind_update_from` in `crates/pgrust_analyze/src/modify.rs`,
which builds the input plan as a projected query over target + source, then
`collect_query_relation_privileges` gives relation RTEs SELECT permissions by
default. `execute_update_with_waiter` tries to skip target OIDs when checking
input-plan privileges; inspect why the target `atest2` requirement is not
excluded or why it is also present in `stmt.required_privileges`.

Fix implemented:
`bind_update_from` and `bind_delete_using` now strip generic relation SELECT
permissions from the internal DML target row-source before wrapping it with
hidden `ctid`/`tableoid` identity columns. The explicit DML privilege
requirements still decide whether target columns require SELECT because they
appear in SET/WHERE/RETURNING. Added a binder regression test for UPDATE FROM
where the target is written but not read.
