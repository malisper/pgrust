Goal:
Fix TASK-C13-04 compression regression slice: column compression DDL, pg_attribute storage/compression metadata, psql display, and pg_column_compression after inheritance Append scans.

Key decisions:
- Reset attstorage and attcompression to target type defaults on ALTER COLUMN TYPE, matching PostgreSQL tablecmds.c behavior.
- Store and expose per-column attstorage from relation metadata instead of substituting pg_type.typstorage when building pg_attribute rows.
- Accept ALTER MATERIALIZED VIEW ... ALTER COLUMN ... SET COMPRESSION through the same compression DDL path, allowing materialized views and preserving PostgreSQL missing-relation behavior.
- Report unknown column compression names as invalid compression method errors with SQLSTATE 22023.
- Preserve only compressed-inline/on-disk-toast raw attribute bytes through Append virtual slots so pg_column_compression/related raw inspection still works without copying ordinary large values.

Files touched:
- crates/pgrust_sql_grammar/src/gram.pest
- src/backend/parser/gram.rs
- src/backend/parser/tests.rs
- src/pgrust/database/commands/alter_column_compression.rs
- src/pgrust/database/ddl.rs
- src/backend/catalog/rows.rs
- src/backend/catalog/store/heap.rs
- src/backend/utils/cache/catcache.rs
- src/include/catalog/pg_attribute.rs
- src/include/nodes/execnodes.rs
- src/backend/executor/exec_expr.rs
- src/backend/executor/nodes.rs
- src/pgrust/database_tests.rs

Tests run:
- cargo fmt
- git diff --check
- scripts/cargo_isolated.sh test --lib --quiet parse_alter_table_set_statement
- scripts/cargo_isolated.sh test --lib --quiet alter_column_type_resets_storage_and_compression_metadata
- scripts/cargo_isolated.sh test --lib --quiet create_table_like_copies_storage_and_compression_only_when_requested
- scripts/cargo_isolated.sh test --lib --quiet pg_column_compression_reports_compressed_values_after_inheritance_append
- scripts/cargo_isolated.sh check
- scripts/run_regression.sh --test compression --port <free-port> --results-dir /tmp/pgrust-task-c13-04-compression

Remaining:
- compression regression now matches 82/87 queries. Remaining 5 mismatches are shared error reporting/wording, not compression metadata/display:
  - missing LINE/caret for three relation-missing errors
  - "table ... does not exist" vs "relation ... does not exist" for ATTACH PARTITION and CREATE INDEX on a missing relation
- Prompt referenced .codex/task-notes/regression-failure-landscape-v2.md, but that file was not present in this checkout.
