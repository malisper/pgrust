Goal:
Fix CI failures after the create_table regression branch merge.

Key decisions:
- Keep unknown as a catalog-visible pseudo-type, but skip it in fallback type-OID lookup maps so plain text resolves to oid 25.
- Encode pg_class reloptions/relacl with the existing typed text-array helper.
- Let unqualified CREATE TABLE/CTAS target pg_temp when pg_temp appears in search_path, while preserving the unlogged-temp-schema error.

Files touched:
- src/backend/catalog/rowcodec.rs
- src/backend/executor/value_io/array.rs
- src/backend/parser/analyze/mod.rs
- src/backend/utils/cache/lsyscache.rs
- src/pgrust/database/catalog_access.rs

Tests run:
- scripts/cargo_isolated.sh test --lib --quiet analyze_populates_pg_stats_view_and_anyarray_columns
- scripts/cargo_isolated.sh test --lib --quiet create_view_persists_security_reloptions
- scripts/cargo_isolated.sh test --lib --quiet create_table_uses_pg_temp_search_path_for_unqualified_creation
- scripts/cargo_isolated.sh test --lib --quiet create_table_as_uses_pg_temp_search_path_for_unqualified_creation
- scripts/cargo_isolated.sh test --lib --quiet anyarray_payload_roundtrips_directly
- scripts/cargo_isolated.sh test --lib --quiet pg_statistic_anyarray_catalog_tuple_roundtrips
- scripts/cargo_isolated.sh check

Remaining:
- CI should be rerun on the pushed branch to confirm the queue is green.
