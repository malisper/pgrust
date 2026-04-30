Goal:
Speed up the `triggers` regression expensive catalog DDL path enough to avoid
the default 30s timeout.

Key decisions:
- Reused system catalog index insert state per catalog kind, mirroring
  PostgreSQL's `CatalogTupleInsertWithInfo` amortization pattern.
- Changed ordinary btree insert WAL to log insert offset plus tuple data, like
  PostgreSQL's `XLOG_BTREE_INSERT_*` path, instead of forcing a full page image
  for every tuple insert.
- Kept full page-image WAL for btree splits, page initialization, vacuum, and
  other page rewrites.

Files touched:
- `src/backend/catalog/indexing.rs`
- `src/backend/catalog/persistence.rs`
- `src/backend/access/nbtree/nbtree.rs`
- `src/backend/access/nbtree/nbtxlog.rs`
- `src/backend/access/nbtree/nbtvacuum.rs`
- `src/backend/access/transam/xlog.rs`
- `src/backend/storage/page/bufpage.rs`

Tests run:
- `cargo check --features tools --bin catalog_ddl_profile`
- `cargo test --lib --quiet reader_roundtrip_insert_delta`
- `cargo test --lib --quiet partitioned_primary_key_propagates_to_nested_and_attached_partitions`
- `cargo test --lib --quiet reopening_database_replays_btree_wal`
- release `catalog_ddl_profile --iterations 20 --children 4 --triggers`
- release `scripts/run_regression.sh --test triggers --skip-build --timeout 30 --jobs 1`

Remaining:
- `triggers` still has existing semantic diffs: 1156/1265 matched, 109
  mismatched, 891 diff lines in `/tmp/pgrust_triggers_after_btree_delta`.
