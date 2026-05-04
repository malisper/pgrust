Goal:
Move generic index dispatch for extracted AM runtimes into `pgrust_access`,
leaving BRIN as a root fallback.

Key decisions:
- `pgrust_access::index::indexam` owns dispatch for btree, hash, GIN, GiST,
  and SP-GiST.
- Root `indexam` remains a compatibility adapter and handles only BRIN fallback.
- Unique probing now uses the access dispatcher with root transaction services.

Files touched:
- `crates/pgrust_access/src/index/indexam.rs`
- `crates/pgrust_access/src/index/unique.rs`
- `src/backend/access/index/indexam.rs`
- `src/backend/access/index/unique.rs`

Tests run:
- `cargo fmt --all -- --check`
- `scripts/cargo_isolated.sh check --message-format short`
- `scripts/cargo_isolated.sh check --features lz4 --message-format short`
- `scripts/cargo_isolated.sh test -p pgrust_access --quiet`
- `scripts/cargo_isolated.sh test -p pgrust_storage --quiet`
- Focused root filters: `index`, `unique`, `btree`, `hash`, `gist`,
  `spgist`, `brin`, `catalog`
- `gin` passed with the unrelated writable CTE test skipped.

Remaining:
- `scripts/cargo_isolated.sh test --lib --quiet gin` still also matches
  `writable_cte_insert_instead_select_rule_joins_original_source`, which fails
  independently with `planning requires root analyze services`.
