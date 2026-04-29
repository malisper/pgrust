Goal:
Fix remaining foreign_key regression issues around ENFORCED caret output,
ALTER TYPE storage metadata, and partitioned FK propagation/action routing.

Key decisions:
- ENFORCED/NOT ENFORCED duplicate/conflict diagnostics now point at the
  conflicting enforcement keyword.
- ALTER COLUMN TYPE now rewrites index relation descriptors/metadata so FK
  checks against rewritten keys do not decode int8 storage as int4.
- FK actions against partitioned referencing tables now carry the leaf relation,
  leaf tuple descriptor, and leaf column mapping with each TID before
  update/delete.
- ATTACH PARTITION FK reconciliation now matches existing child constraints by
  FK structure, rejects enforceability conflicts, validates child rows when the
  parent constraint is valid, and avoids duplicating already-inherited matching
  constraints on reattach/nested children.
- Referenced-side FK clones are now created for existing and newly attached
  referenced partitions, including nested partition trees. Action triggers are
  installed on referenced partitions only, and outbound check binding skips
  referenced-side clones so inserts do not require matching rows in every
  referenced partition.
- Child-side attach reconciliation skips referenced-side clone rows when
  inheriting outbound FKs. Inherited child-side FK trigger creation now advances
  with the normal command id instead of leaking CommandId::MAX into the
  referenced-side clone pass.

Files touched:
- src/backend/catalog/store/heap.rs
- src/backend/commands/tablecmds.rs
- src/backend/tcop/postgres.rs
- src/pgrust/database/commands/alter_column_type.rs
- src/pgrust/database/commands/constraint.rs
- src/pgrust/database/commands/create.rs
- src/pgrust/database/commands/drop.rs
- src/pgrust/database/commands/partition.rs
- src/pgrust/database/commands/trigger.rs
- src/backend/parser/analyze/constraints.rs
- src/pgrust/database_tests.rs

Tests run:
- cargo fmt
- scripts/cargo_isolated.sh check
- scripts/cargo_isolated.sh test --lib --quiet exec_error_position_points_at_alter_constraint_fk_options
- scripts/cargo_isolated.sh test --lib --quiet alter_table_alter_column_type_rewrites_referenced_index_metadata
- scripts/cargo_isolated.sh test --lib --quiet drop_table_allows_explicit_partitioned_fk_table_with_referenced_parent
- scripts/cargo_isolated.sh test --lib --quiet partitioned_foreign_key
- scripts/cargo_isolated.sh test --lib --quiet foreign_keys_apply_referential_actions
- scripts/cargo_isolated.sh test --lib --quiet partitioned_foreign_key_cascade_updates_leaf_partition_rows
- scripts/cargo_isolated.sh test --lib --quiet attach_partition_merges_existing_foreign_key_with_parent
- scripts/cargo_isolated.sh test --lib --quiet attach_partition_rejects_foreign_key_enforceability_conflict
- scripts/cargo_isolated.sh test --lib --quiet reattach_partition_reuses_inherited_foreign_key_constraint
- scripts/cargo_isolated.sh test --lib --quiet referenced_partition_foreign_key -- --nocapture
- scripts/cargo_isolated.sh test --lib --quiet self_referencing_partitioned_foreign_key_adds_referenced_clones -- --nocapture
- scripts/run_regression.sh --test foreign_key --timeout 120 --port 55441
- scripts/run_regression.sh --test foreign_key --timeout 120 --port 55780
- scripts/run_regression.sh --test foreign_key --timeout 300 --port 55820

Remaining:
- foreign_key completes with a 300s file timeout at 1141/1252 matched queries
  in /var/folders/tc/1psz8_jd0hnfmgyyr0n2wtzh0000gn/T/pgrust_regress_results.honolulu-v4.Nxn7d4.
- The old referenced-side root validation/invalid-attnum failure is gone. The
  120s run still times out early at 546/1252 because the file is slow enough to
  hit the shorter budget before the partitioned FK tail.
- Remaining mismatches are mostly: MATCH FULL validation on partitioned
  referencing roots, partitioned FK action/default-column behavior, inherited FK
  psql/drop/alter presentation on child partitions, referenced-side clone
  naming/dependency detail differences, and later fkpart11 statement-timeout
  cascade.

Remaining frequency snapshot:
- Harness: 1252 queries total, 1141 matched, 111 mismatched, no timeout with
  `--timeout 300`.
- Unified diff: 796 lines.
- Grouped root causes: about 7 buckets.
- Highest-frequency buckets:
  - inherited FK presentation/drop/alter protections on partition children and
    missing psql "TABLE parent CONSTRAINT" lines.
  - partitioned FK action/default column-list behavior and wrong action trigger
    binding details.
  - referenced-side clone naming/dependency detail differences for detach/drop
    partition cases.
  - MATCH FULL validation on partitioned referencing roots.
  - missing validation while adding/attaching partitioned referencing tables.
  - pending trigger event timing: 1 hunk.
  - drop schema notice ordering/aggregation and unsupported CTE/constraint
    trigger fallout near the tail.

Update 2026-04-28 partition FK follow-up:
- Implemented partition-aware validation for referencing roots/leaves,
  inherited FK drop/alter rejection, ancestor-aware psql constraint queries,
  referenced partition default-last clone ordering, pg_describe_object support
  for pg_constraint, SET DEFAULT outbound recheck ancestor matching, and
  pending-trigger checks across FK constraint families.
- Added focused tests:
  match_full_validation_scans_partition_leaves,
  attach_partition_validates_inherited_foreign_key_rows,
  inherited_foreign_key_drop_and_alter_are_rejected,
  referenced_partition_clone_names_default_last,
  partitioned_foreign_key_actions_remap_leaf_rows,
  pending_trigger_events_include_partition_children.
- Validation run:
  cargo fmt; scripts/cargo_isolated.sh check; focused tests above;
  scripts/cargo_isolated.sh test --lib --quiet partitioned_foreign_key;
  scripts/cargo_isolated.sh test --lib --quiet referenced_partition_foreign_key -- --nocapture;
  scripts/cargo_isolated.sh test --lib --quiet foreign_keys_apply_referential_actions.
- Full regression completed before a reverted experimental validation tweak:
  scripts/run_regression.sh --test foreign_key --timeout 300 --port 55991
  matched 1178/1252 queries, 74 mismatches, 609 diff lines, no timeout, in
  /var/folders/tc/1psz8_jd0hnfmgyyr0n2wtzh0000gn/T/pgrust_regress_results.honolulu-v4.REgAsV.
- A later experiment that skipped root validation for partitioned referenced
  tables timed out at the old partition-update statement and was reverted.
  The remaining first mismatch is the false FK validation error when enabling
  fk_notpartitioned_fk_a_b_fkey2 against fk_partitioned_pk.
