Goal:
Try replacing generated catalog row constructors with embedded data blobs to reduce release compile time.

Key decisions:
Started with `pg_proc`, the largest generated catalog module and the observed release-codegen bottleneck.
Generated `crates/pgrust_catalog_data/data/pg_proc.json` from the existing `bootstrap_pg_proc_rows()` output, then changed `build_bootstrap_pg_proc_rows()` to decode that payload with `serde_json::from_slice(include_bytes!(...))`.
Removed obsolete production row-builder helpers from `pg_proc.rs`; kept only a small test helper for oid-vector formatting.

Files touched:
`crates/pgrust_catalog_data/src/pg_proc.rs`
`crates/pgrust_catalog_data/data/pg_proc.json`

Tests run:
`cargo fmt --all -- --check`
`scripts/cargo_isolated.sh test -p pgrust_catalog_data --quiet`
`scripts/cargo_isolated.sh check --message-format short`
`scripts/cargo_isolated.sh test --lib --quiet parser`
Release timing: clean `cargo build --release -p pgrust_catalog_data --lib` in 6.70s; touched `pg_proc.rs` rebuild in 2.88s.

Remaining:
Consider moving other large generated catalogs (`pg_operator`, `pg_type`, `pg_opclass`, etc.) to the same payload pattern.
Consider a compact binary payload instead of JSON if first-use decode time or repository size matters.
