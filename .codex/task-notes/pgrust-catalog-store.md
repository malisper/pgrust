Goal:
Extract portable catalog data/cache internals into `pgrust_catalog_store`.

Key decisions:
Kept root storage/MVCC/catalog-store orchestration in `src/backend/catalog/store*`.
Moved portable `Catalog`, row helpers, catalog sort/derived helpers, `CatCache`,
`RelCache`, bootstrap/toast helpers, and catalog-shape analyzer impls.
Moved `InterruptReason` into `pgrust_core` with a root re-export.
Finished the next catalog slice by moving portable rowcodec, syscache identity/
tuple decoding, and broad `PhysicalCatalogRows -> Catalog` materialization into
`pgrust_catalog_store`. Root keeps only the `HeapTuple` rowcodec bridge, heap/
index scans, storage metapage reads, backend cache lifetime, and MVCC write code.
Moved non-runtime durable store state into `CatalogStoreCore`; root `CatalogStore`
now wraps/derefs into that core while root mutation methods remain in place.
Added portable `CatalogReadRuntime`/`CatalogWriteRuntime` traits and a root
`CatalogWriteContext` implementation for interrupt checks and MVCC row effects.
Wrapped up the hot-read migration by routing lsyscache broad row helpers through
single-catalog syscache/index scans where a catalog index already exists, adding
test-only broad materialization counters, and guarding relation lookup, simple
SELECT, and extended-protocol describe against broad `CatCache` loads.
Cached the connected database name on `Database` so executor context creation no
longer materializes shared `pg_database` on every statement.

Files touched:
`crates/pgrust_catalog_store/**`, root catalog/cache shim modules,
`crates/pgrust_analyze/src/catalog_store_lookup.rs`,
`crates/pgrust_core/src/interrupts.rs`.
Current slice added `materialize.rs`, `rowcodec.rs`, and `syscache.rs` under
`crates/pgrust_catalog_store/src`, plus root adapters in `loader.rs`,
`rowcodec.rs`, `syscache.rs`, and catalog store state wrappers.
Wrap-up slice touched root `syscache`, `lsyscache`, protocol describe helpers,
`Database`/`Cluster` construction, and database guard tests.

Tests run:
`cargo fmt --all -- --check`
`scripts/cargo_isolated.sh check --message-format short`
`scripts/cargo_isolated.sh test -p pgrust_catalog_store --quiet`
`scripts/cargo_isolated.sh test --lib --quiet catalog`
`scripts/cargo_isolated.sh test --lib --quiet parser`
`scripts/cargo_isolated.sh test --lib --quiet optimizer`
`scripts/cargo_isolated.sh test --lib --quiet sequence`
`scripts/cargo_isolated.sh test --lib --quiet plpgsql`
Guard tests:
`scripts/cargo_isolated.sh test --lib --quiet lookup_any_relation_uses_targeted_relation_cache_without_catcache`
`scripts/cargo_isolated.sh test --lib --quiet simple_select_uses_keyed_catalog_without_broad_catcache`
`scripts/cargo_isolated.sh test --lib --quiet extended_protocol_describe_statement_reports_gdesc_columns`
Boundary:
`rg "crate::backend::|crate::include::|crate::pgrust::|crate::pl::" crates/pgrust_catalog_store/src`

Remaining:
Root storage/index/MVCC runtime hooks, system-view producers, and explicit broad
compatibility paths remain. Remaining `backend_catcache()` uses are mostly psql
describe/catalog-view compatibility, text search/FDW row-set APIs without keyed
helpers yet, and direct test/compatibility calls.
