Goal:
Fix partitioned-index attach/reconciliation mismatches in indexing regression.

Key decisions:
Used PostgreSQL tablecmds behavior as reference for partitioned index attach,
partitioned key reconciliation, CREATE TABLE LIKE INCLUDING INDEXES, and
constraint-backed index inheritance. Auto-reconciliation must not reuse
constraint-backed local indexes unless attaching a matching constraint-backed
index explicitly.

Files touched:
.codex/task-notes/indexing-regression-diagnosis.md
src/backend/parser/analyze/create_table.rs
src/backend/parser/analyze/create_table_inherits.rs
src/backend/parser/gram.rs
src/backend/parser/tests.rs
src/pgrust/database/commands/constraint.rs
src/pgrust/database/commands/create.rs
src/pgrust/database/commands/partitioned_indexes.rs
src/pgrust/database/commands/partitioned_keys.rs
src/pgrust/database_tests.rs

Tests run:
env -u CARGO_TARGET_DIR scripts/cargo_isolated.sh test --lib --quiet create_table_like_including_indexes_copies_partitioned_source_indexes
env -u CARGO_TARGET_DIR scripts/cargo_isolated.sh test --lib --quiet alter_table_only_partitioned_primary_key_does_not_reconcile_children
env -u CARGO_TARGET_DIR scripts/cargo_isolated.sh test --lib --quiet attach_partition_rejects_mismatched_existing_primary_key
env -u CARGO_TARGET_DIR scripts/cargo_isolated.sh test --lib --quiet partition_child_primary_key_allowed_without_parent_primary_key
env -u CARGO_TARGET_DIR scripts/cargo_isolated.sh test --lib --quiet dropping_parent_primary_key_drops_inherited_child_keys
env -u CARGO_TARGET_DIR scripts/cargo_isolated.sh test --lib --quiet parse_create_table_partition_of_with_column_key_options
env -u CARGO_TARGET_DIR scripts/cargo_isolated.sh test --lib --quiet partitioned_key_coverage_checks_fire_for_root_partition_of_and_attach_partition
env -u CARGO_TARGET_DIR scripts/run_regression.sh --test indexing --timeout 300 --port 57436 --results-dir /tmp/diffs/indexing-fix3

Remaining:
indexing regression still fails 44 queries, 526/570 matched, 323 diff lines.
Remaining categories are separate from the requested attach/reconciliation fix:
pg_get_indexdef through aggregate/lateral execution, EXCLUDE/GiST coverage and
operator semantics, SP-GiST bitmap-vs-index plan output, covering partitioned
indexes, dropped-column index cleanup, invalid expression index handling,
REPLICA IDENTITY in transaction, and TOASTed pg_index index expressions.
