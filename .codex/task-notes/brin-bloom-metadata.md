Goal:
Add BRIN bloom opclass metadata needed by the brin_bloom regression.

Key decisions:
Expose PostgreSQL-compatible BRIN bloom opfamilies/opclasses/amops and keep scan behavior lossy until native bloom summaries exist.
Validate bloom opclass options in both current and legacy index command paths.

Files touched:
crates/pgrust_catalog_data/src/pg_opfamily.rs
crates/pgrust_catalog_data/src/pg_opclass.rs
crates/pgrust_catalog_data/src/pg_amop.rs
crates/pgrust_commands/src/reloptions.rs
crates/pgrust_access/src/brin/runtime.rs
src/pgrust/database/commands/index.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh check -q
scripts/cargo_isolated.sh test -q -p pgrust_catalog_data --lib
scripts/run_regression.sh --test brin_bloom --port 55444 --timeout 120 --jobs 1

Remaining:
Native BRIN bloom summaries are still not implemented; bloom scans remain lossy and rely on heap recheck.
