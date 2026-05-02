Goal:
Fix partitioned index attach/detach catalog metadata and validity propagation for `indexing` regression.

Key decisions:
Only emit index partition `P`/`S` dependency rows when an index has an inheritance parent.
On partition detach, rewrite detached child index metadata so `relispartition` and partition dependencies are cleared.
Propagate partitioned-index validity recalculation upward for both valid and invalid outcomes.

Files touched:
src/backend/catalog/pg_depend.rs
src/pgrust/database/commands/partition.rs
src/pgrust/database/commands/partitioned_indexes.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
env -u CARGO_TARGET_DIR scripts/cargo_isolated.sh test --lib --quiet partitioned_index
env -u CARGO_TARGET_DIR scripts/cargo_isolated.sh test --lib --quiet detach_partition
env -u CARGO_TARGET_DIR scripts/cargo_isolated.sh test --lib --quiet alter_table_only_partitioned_primary_key_does_not_reconcile_children
env -u CARGO_TARGET_DIR scripts/cargo_isolated.sh test --lib --quiet partition_index_pg_depend_rows_use_partition_deptypes
env -u CARGO_TARGET_DIR scripts/run_regression.sh --test indexing --jobs 1 --timeout 600 --results-dir /tmp/pgrust-indexing-partition-index-long

Remaining:
Short 120s `indexing` regression run times out before target hunks; long-timeout run passes.
