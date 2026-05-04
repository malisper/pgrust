Goal:
Make durable catalog open and hot catalog reads closer to PostgreSQL: lightweight database-local open, keyed syscache lookups, and per-relation relcache construction instead of startup-wide catalog/relcache work.

Key decisions:
- Existing durable CatalogStore opens now keep only control/cache metadata and mark the in-memory Catalog as not materialized.
- Fresh bootstrap still materializes a full Catalog so it can seed physical catalog heaps and indexes.
- Full Catalog/CatCache/RelCache builders remain compatibility escape hatches and are marked with :HACK: comments.
- Savepoint snapshots materialize the broad catalog when needed so rollback semantics preserve legacy write paths.
- Function lookup by name now uses PostgreSQL-style PROCNAMEARGSNSP prefix syscache scans; prefix operator lookup has an operator-name lookup hook to avoid broad CatCache in the analyzer path.

Files touched:
- crates/pgrust_analyze/src/expr/ops.rs
- crates/pgrust_analyze/src/lib.rs
- src/backend/catalog/store.rs
- src/backend/catalog/store/heap.rs
- src/backend/catalog/store/roles.rs
- src/backend/catalog/store/storage.rs
- src/backend/utils/cache/lsyscache.rs
- src/backend/utils/cache/syscache.rs

Tests run:
- cargo fmt --all -- --check
- scripts/cargo_isolated.sh check --message-format short
- scripts/cargo_isolated.sh test --lib --quiet catalog
- scripts/cargo_isolated.sh test --lib --quiet parser
- scripts/cargo_isolated.sh test --lib --quiet optimizer
- scripts/cargo_isolated.sh test --lib --quiet sequence
- scripts/cargo_isolated.sh test --lib --quiet create_database_clones_template1_and_persists_across_reopen
- scripts/cargo_isolated.sh test --lib --quiet durable_prepared_transaction_survives_reopen_then_finishes
- scripts/cargo_isolated.sh test --lib --quiet proc_name_lookup_uses_syscache_list_without_catcache
- scripts/cargo_isolated.sh test --lib --quiet relation_lookup_uses_keyed_syscache_and_one_relcache_entry
- /usr/bin/time -p scripts/cargo_isolated.sh test --lib --quiet plpgsql

Remaining:
- Shared-catalog connect still has broad compatibility scans in a few paths.
- DDL, catalog views, and some row-list APIs still intentionally materialize CatCache until their callers are converted.
