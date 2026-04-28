Goal:
Clear the remaining PostgreSQL `publication` regression diffs while keeping the PG-like targeted syscache/catalog-scan direction.

Key decisions:
Implemented minimal catalog-backed `CREATE COLLATION ... FROM ...`, `DROP COLLATION`, and `ALTER SCHEMA ... RENAME TO ...`; collation DDL now applies catalog effects immediately and invalidates backend cache state.
Matched publication semantics for schema/all-tables owner transfer, `publish_via_partition_root` validation, schema column-list errors, inherited child listing, FOR ALL TABLES detail text, and relation-not-in-publication names.
Extended publication DML enforcement through UPDATE, DELETE, MERGE update/delete, and `ON CONFLICT DO UPDATE`, including schema/ancestor publication membership and partition root column/filter mapping.
Added owned serial sequence pg_depend cleanup so table/schema drops remove owned sequences without extra cascade notice lines or stale role ownership dependencies.
Kept regression-only compatibility shims explicit with `:HACK:` comments for pgrust's `postgres` database name vs PostgreSQL regression's `regression` name.

Files touched:
crates/pgrust_sql_grammar/src/gram.pest
src/include/nodes/parsenodes.rs
src/backend/parser/gram.rs
src/backend/parser/analyze/modify.rs
src/backend/tcop/postgres.rs
src/backend/executor/driver.rs
src/backend/commands/tablecmds.rs
src/backend/catalog/pg_depend.rs
src/backend/catalog/store/heap.rs
src/backend/utils/cache/lsyscache.rs
src/bin/query_repl.rs
src/pgrust/session.rs
src/pgrust/database/commands/collation.rs
src/pgrust/database/commands/create.rs
src/pgrust/database/commands/drop.rs
src/pgrust/database/commands/execute.rs
src/pgrust/database/commands/maintenance.rs
src/pgrust/database/commands/mod.rs
src/pgrust/database/commands/owner.rs
src/pgrust/database/commands/publication.rs
src/pgrust/database/commands/sequence.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet drop_sequence_restrict_and_cascade_respect_serial_dependencies
scripts/cargo_isolated.sh test --lib --quiet create_table_serial_creates_sequence_defaults_and_persists_state
scripts/cargo_isolated.sh build --release --bin pgrust_server
scripts/run_regression.sh --skip-build --schedule .context/publication-only.schedule --port 5583 --timeout 180

Remaining:
`publication` now passes: 710/710 queries matched, 0 diff lines. `/tmp/diffs/publication.out` contains the passing output; no `publication.diff` remains from the final run.
