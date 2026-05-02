Goal:
Implement the foreign key referential-action, partition trigger, and replica identity feature cluster from the 2026-05-01 regression diffs.

Key decisions:
- Use root FK constraint rows for referenced-side clone display names.
- Remap tuple-movement values back into the source leaf layout before inbound FK actions.
- Queue deferrable user constraint trigger events in the existing deferred constraint tracker.
- Resolve named SET CONSTRAINTS targets against user constraint triggers as well as pg_constraint rows.
- Centralize REPLICA IDENTITY USING INDEX validation and keep system catalog relreplident at n.
- Validate attached partitions against published partition roots after child indexes are reconciled.

Files touched:
- FK/partition updates: src/backend/commands/tablecmds.rs, src/backend/parser/analyze/constraints.rs
- Triggers: src/backend/parser/gram.rs, src/include/nodes/parsenodes.rs, src/pgrust/database/commands/trigger.rs, src/backend/commands/trigger.rs, src/backend/executor/mod.rs, src/pgrust/database/foreign_keys.rs
- Replica identity/catalog: src/pgrust/database/commands/execute.rs, src/pgrust/database/commands/constraint.rs, src/backend/catalog/*, src/backend/utils/cache/*
- Tests: src/backend/parser/tests.rs, src/pgrust/database_tests.rs

Tests run:
- cargo fmt
- CARGO_TARGET_DIR=/tmp/pgrust-target-pool/zurich-v2/7 scripts/cargo_isolated.sh check
- CARGO_TARGET_DIR=/tmp/pgrust-target-pool/zurich-v2/7 scripts/cargo_isolated.sh test --lib --quiet parse_create_constraint_trigger_statement
- CARGO_TARGET_DIR=/tmp/pgrust-target-pool/zurich-v2/7 scripts/cargo_isolated.sh test --lib --quiet lower_create_table_rejects_invalid_key_constraints
- CARGO_TARGET_DIR=/tmp/pgrust-target-pool/zurich-v2/7 scripts/cargo_isolated.sh test --lib --quiet alter_table_replica_identity_using_index_works_in_transaction
- CARGO_TARGET_DIR=/tmp/pgrust-target-pool/zurich-v2/7 scripts/cargo_isolated.sh test --lib --quiet without_overlaps_replica_identity_using_index_marks_pg_index
- CARGO_TARGET_DIR=/tmp/pgrust-target-pool/zurich-v2/7 scripts/cargo_isolated.sh test --lib --quiet partitioned_table_row_triggers_clone_to_existing_new_and_attached_partitions
- CARGO_TARGET_DIR=/tmp/pgrust-target-pool/zurich-v2/7 scripts/cargo_isolated.sh test --lib --quiet publication_update_requires_replica_identity_even_without_rows
- CARGO_TARGET_DIR=/tmp/pgrust-target-pool/zurich-v2/7 scripts/cargo_isolated.sh test --lib --quiet attach_partition_under_published_root_requires_child_replica_identity
- CARGO_TARGET_DIR=/tmp/pgrust-target-pool/zurich-v2/7 scripts/cargo_isolated.sh test --lib --quiet pending_trigger_events_include_partition_children
- CARGO_TARGET_DIR=/tmp/pgrust-target-pool/zurich-v2/7 scripts/cargo_isolated.sh test --lib --quiet replica_identity_validation_matches_pg_edge_cases
- CARGO_TARGET_DIR=/tmp/pgrust-target-pool/zurich-v2/7 scripts/cargo_isolated.sh test --lib --quiet deferred_user_constraint_trigger_fires_at_commit

Remaining:
- Full PostgreSQL-style regression files were not rerun locally.
- Partitioned transition-table aggregation is not separately covered here.
