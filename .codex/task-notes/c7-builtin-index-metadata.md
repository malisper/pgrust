Goal:
- C7.4 builtin index metadata and index eligibility foundation for create_index,
  indexing, index_including, cluster, and indirect_toast.

Key decisions:
- Kept custom access methods out of scope.
- Fixed INCLUDE index key/residual handling and partition child index-only
  eligibility without broad cost model changes.
- Implemented PostgreSQL-compatible CLUSTER command forms and metadata behavior
  needed by cluster regression coverage, including partitioned relation handling.
- Added focused compatibility shims for pg_class lookup under heavy partitioned
  index DDL and bootstrap toast index metadata.
- Fixed partitioned index reconciliation to reuse matching invalid concurrent
  child indexes during root CREATE INDEX while keeping attach-partition paths
  strict.
- Treated indirect_toast's make_tuple_indirect/PLpgSQL NEW assignment failures as
  outside this C7.4 index metadata slice.

Files touched:
- src/backend/access/nbtree/nbtree.rs
- src/backend/catalog/rowcodec.rs
- src/backend/catalog/store/heap.rs
- src/backend/commands/partition.rs
- src/backend/executor/nodes.rs
- src/backend/optimizer/path/allpaths.rs
- src/backend/optimizer/path/costsize.rs
- src/backend/parser/analyze/partition.rs
- src/backend/parser/gram.rs
- src/backend/parser/tests.rs
- src/backend/utils/cache/lsyscache.rs
- src/pgrust/database/commands/cluster.rs
- src/pgrust/database/commands/drop_column.rs
- src/pgrust/database/commands/execute.rs
- src/pgrust/database/commands/index.rs
- src/pgrust/database/commands/partition.rs
- src/pgrust/database/commands/partitioned_indexes.rs
- src/pgrust/database/commands/partitioned_keys.rs
- src/pgrust/database_tests.rs
- src/pgrust/session.rs

Tests run:
- cargo fmt
- git diff --check
- CARGO_TARGET_DIR=/tmp/pgrust-c7-target scripts/cargo_isolated.sh check
- Focused unit tests:
  - drop_table_drops_constraint_backed_include_index_for_name_reuse
  - parse_cluster
  - partitioned_exclusion_rejects_non_equal_partition_key_operator
  - alter_table_attach_partition_on_index_reports_not_partitioned_table
  - partition_child_index_only_attrs_include_parent_target
  - reset_search_path_keeps_public_partitioned_table_visible_to_create_index
  - drop_partitioned_column_drops_dependent_child_indexes
  - alter_table_replica_identity_using_index_works_in_transaction
  - large_pg_index_expression_metadata_reindexes_and_drops
  - partitioned_index_creation_reuses_invalid_concurrent_child_index
- Regression:
  - create_index: FAIL 659/687 matched, 28 mismatches, 514 diff lines
    (/tmp/pgrust-c7-create-index-5; copied to /tmp/diffs/create_index.diff)
  - indexing: PASS 570/570 matched
    (/tmp/pgrust-c7-indexing-8)
  - index_including: PASS 135/135 matched
    (/tmp/pgrust-c7-index-including-7)
  - cluster: PASS 204/204 matched
    (/tmp/pgrust-c7-cluster-7)
  - indirect_toast: FAIL 22/30 matched, 8 mismatches, 100 diff lines
    (/tmp/pgrust-c7-indirect-toast-4; copied to /tmp/diffs/indirect_toast.diff)

Remaining:
- create_index still has planner-shape differences around bitmap/OR/SAOP row
  comparisons and array overlap NULL behavior; these are broader planner/runtime
  issues than the C7.4 metadata foundation.
- indirect_toast still depends on unsupported make_tuple_indirect and PL/pgSQL
  trigger-row assignment behavior.
