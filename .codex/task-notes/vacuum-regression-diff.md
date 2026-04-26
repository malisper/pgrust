Goal:
Close the largest semantic gaps behind the PostgreSQL vacuum.out regression diff.

Key decisions:
Implemented SRF lowering for INSERT ... VALUES by routing VALUES rows containing
set-returning expressions through the normal ProjectSet query path.
Implemented PostgreSQL-shaped VACUUM option validation and warning/skip behavior
for key options and ownership checks.
Implemented a focused VACUUM FULL relfilenode rewrite path that copies live rows,
rebuilds indexes, handles toast rewrites, and uses a post-rewrite command id for
FULL+ANALYZE.
Added pg_class.relhasindex storage/codec/cache plumbing and pg_stat_all_tables.

Files touched:
src/backend/catalog/*
src/backend/commands/tablecmds.rs
src/backend/executor/tests.rs
src/backend/parser/analyze/*
src/backend/parser/gram.rs
src/backend/parser/tests.rs
src/backend/utils/cache/*
src/include/catalog/pg_class.rs
src/include/nodes/parsenodes.rs
src/pgrust/database/commands/maintenance.rs
src/pgrust/database/commands/vacuum_full.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet backend::parser::tests
scripts/cargo_isolated.sh test --lib --quiet insert_values_
scripts/cargo_isolated.sh test --lib --quiet vacuum_full_
scripts/cargo_isolated.sh test --lib --quiet pg_class_relhasindex_tracks_create_and_drop_index
scripts/cargo_isolated.sh test --lib --quiet pg_stat_all_tables_includes_catalog_relations
scripts/cargo_isolated.sh test --lib --quiet vacuum_option_validation_and_permission_warnings
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh build --bin pgrust_server
CARGO_TARGET_DIR=/tmp/pgrust-target-pool/delhi-v3/2 .context/run_regression_timeout30.sh --skip-build --port 5649 --timeout 240 --schedule .context/vacuum_only_schedule --test vacuum

Remaining:
This was an earlier checkpoint. See `vacuum-regression.md` for the final state:
focused vacuum regression is down to 326/328 query matches, with only
unsupported `ALTER TABLE ... CLUSTER ON ...` and `CLUSTER table` remaining.
