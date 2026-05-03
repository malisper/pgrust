Goal:
Make broad catalog cache guard tests parallel-safe by scoping test counters to the database/store under test.

Key decisions:
- Replaced process-global backend CatCache load counter with a test-only Database-owned counter.
- Replaced process-global CatalogStore::catcache counter with a test-only per-CatalogStore counter.
- Database test helpers now sum backend, local store, and shared store broad loads for the specific Database.
- Added a focused test proving one database's direct catcache call does not increment another database's counters.

Files touched:
- src/pgrust/database.rs
- src/pgrust/cluster.rs
- src/backend/catalog/store.rs
- src/backend/catalog/store/storage.rs
- src/backend/utils/cache/syscache.rs
- src/pgrust/database_tests.rs
- src/backend/tcop/postgres.rs

Tests run:
- cargo fmt --all
- cargo fmt --all -- --check
- scripts/cargo_isolated.sh check --message-format short
- scripts/cargo_isolated.sh test --lib --quiet simple_select_uses_keyed_catalog_without_broad_catcache
- scripts/cargo_isolated.sh test --lib --quiet broad_catalog_load_counters_are_database_scoped
- scripts/cargo_isolated.sh test --lib --quiet extended_protocol_describe_statement_reports_gdesc_columns
- scripts/cargo_isolated.sh test --lib --quiet catalog

Remaining:
- None for this fix.
