Goal:
Implement PostgreSQL-compatible tablespace support needed for `tablespace.sql`.

Key decisions:
Used PostgreSQL-visible catalog behavior as the contract: `pg_class.reltablespace`
and relation locators are both updated for non-default tablespaces, while
`pg_default` stores as zero.
Kept storage changes narrow by using existing `RelFileLocator.spc_oid` and
`pg_tblspc/<oid>/PG_18_202406281/<db_oid>/<relfilenode>` paths.
Added a tablespace-specific regression bootstrap that creates `regress_tblspace`
as an in-place tablespace so `pg_tablespace_location()` matches upstream output.
Made relation flush synchronous before tablespace rewrites so file-level storage
copy preserves uncheckpointed heap and index pages.

Files touched:
Parser/AST/analyzer support for tablespace DDL, relation tablespace clauses,
constraint index tablespaces, grants/revokes, and REINDEX options.
Catalog persistence for `pg_tablespace.spcacl` and `spcoptions`.
Database command implementations for create/alter/drop tablespace, relation
placement, ALTER/REINDEX tablespace moves, and tablespace privileges.
Relcache/loader/syscache locator construction now honors `pg_class.reltablespace`.
Regression harness setup for the upstream `tablespace` file.

Tests run:
`cargo fmt`
`env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool/lyon-v3-tablespace-check scripts/cargo_isolated.sh check`
Focused psql repro for `ALTER TABLE/INDEX SET TABLESPACE` data preservation and
unique enforcement.
`env -u CARGO_TARGET_DIR PGRUST_TARGET_POOL_DIR=/tmp/pgrust-target-pool/lyon-v3-tablespace-check scripts/run_regression.sh --test tablespace --timeout 120 --results-dir /tmp/diffs/tablespace-after7 --port 5652`

Remaining:
`tablespace` regression passes: 205/205 queries matched.
