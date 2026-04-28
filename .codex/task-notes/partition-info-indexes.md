Goal:
Diagnose and fix the partition_info regression failure for partitioned indexes.

Key decisions:
PostgreSQL treats relkind `I` as a relation kind that has partitions. pgrust's
partition-info helpers only recognized `pg_partitioned_table` metadata, so
partitioned indexes were filtered out. Index children also need
`relispartition = true` when linked through pg_inherits.

Files touched:
src/backend/commands/partition.rs
src/pgrust/database/commands/partitioned_indexes.rs
src/pgrust/database_tests.rs

Tests run:
scripts/cargo_isolated.sh test --lib --quiet create_index_on_partitioned_table_builds_index_tree
scripts/run_regression.sh --test partition_info --timeout 30 --results-dir /tmp/pgrust_partition_info_after_fix

Remaining:
None for partition_info.
