Goal:
Investigate and reduce failures in PostgreSQL misc_sanity regression.

Key decisions:
Registered pg_shdepend as a shared bootstrap catalog with PostgreSQL OID 1214.
Added bootstrap catalog toast OIDs from PostgreSQL DECLARE_TOAST metadata so toast sanity no longer reports all catalogs.
Marked PostgreSQL PKEY system catalog indexes as primary and synthesized pg_constraint rows for unique system catalog indexes.
Added missing primary index descriptors for pg_conversion, pg_foreign_data_wrapper, and pg_largeobject_metadata.
Fixed regtype output for array types and aclitem[] catalog metadata used by pg_class.relacl and pg_largeobject_metadata.lomacl.
Kept pg_largeobject_metadata on the virtual scan path even after adding its physical catalog index, because large-object metadata rows come from LargeObjectRuntime rather than the bootstrap heap.

Files touched:
src/include/catalog/bootstrap.rs
src/backend/catalog/bootstrap.rs
src/include/catalog/pg_shdepend.rs
src/include/catalog/mod.rs
src/backend/catalog/loader.rs
src/backend/catalog/rowcodec.rs
src/include/catalog/indexing.rs
src/backend/catalog/indexing.rs
src/include/catalog/pg_constraint.rs
src/backend/executor/expr_reg.rs
src/backend/catalog/catalog.rs
src/backend/utils/cache/catcache.rs
src/include/catalog/pg_attribute.rs
src/include/catalog/pg_class.rs
src/include/catalog/pg_largeobject_metadata.rs
src/include/catalog/pg_largeobject.rs
src/include/catalog/pg_replication_origin.rs
src/include/catalog/pg_authid.rs
src/backend/catalog/roles.rs
src/backend/catalog/rows.rs
src/backend/catalog/store/heap.rs
src/backend/commands/rolecmds.rs
src/backend/parser/tests.rs
src/pgrust/auth.rs
src/backend/optimizer/path/allpaths.rs
src/backend/optimizer/path/costsize.rs

Tests run:
cargo fmt
CARGO_TARGET_DIR=.context/cargo-target-misc-sanity cargo check
CARGO_TARGET_DIR=.context/cargo-target-misc-sanity scripts/run_regression.sh --test misc_sanity --jobs 1 --port 62433
CARGO_TARGET_DIR=.context/cargo-target-misc-sanity scripts/run_regression.sh --test misc_sanity --jobs 1 --port 63433
CARGO_TARGET_DIR=.context/cargo-target-misc-sanity cargo test --lib --quiet core_bootstrap_
CARGO_TARGET_DIR=.context/cargo-target-misc-sanity cargo test --lib --quiet large_object_metadata_tracks_create_and_unlink
CARGO_TARGET_DIR=.context/cargo-target-misc-sanity cargo test --lib --quiet

Remaining:
misc_sanity now passes 5/5 queries, and the GitHub-failing unit tests pass locally. The last fix added PostgreSQL-compatible catalog metadata for pg_attribute nullable tail columns, pg_authid.rolpassword, pg_largeobject, and pg_replication_origin.
