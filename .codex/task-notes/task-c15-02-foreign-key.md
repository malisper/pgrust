Goal:
Fix TASK-C15-02 for the `foreign_key` regression: partitioned/self-referential
FK catalog rows, generated referenced-partition FK names, detach validation
names, and drop dependency messages.

Key decisions:
- Kept the fix local to FK partition/catalog/drop paths. Shared object address
  helpers exist, but the dependency reporting issue could be handled in the
  current drop planner.
- Matched PostgreSQL's namespace-wide referenced-side FK clone name allocation
  and reserved just-dropped clone names during DETACH so detached FKs choose the
  same suffixes as PostgreSQL.
- Recreated referenced-side clones when an inherited FK becomes independent on
  detach, and skipped the detached subtree when cloning action rows.
- Normalized integer keys in subquery membership caches and cleared subquery
  eval caches for streaming SELECT startup so `pg_partition_tree()` regclass
  OIDs compare correctly across server protocol queries.

Files touched:
- src/backend/executor/exec_expr/subquery.rs
- src/backend/executor/srf.rs
- src/backend/parser/analyze/constraints.rs
- src/pgrust/database/commands/constraint.rs
- src/pgrust/database/commands/drop.rs
- src/pgrust/database/commands/execute.rs
- src/pgrust/database/commands/partition.rs
- src/pgrust/database_tests.rs

Tests run:
- cargo fmt
- scripts/cargo_isolated.sh test --lib --quiet self_referencing_partitioned_foreign_key_matches_pg_catalog_rows
- scripts/cargo_isolated.sh test --lib --quiet partitioned_foreign_key_drop_dependency_uses_root_constraint_name
- scripts/cargo_isolated.sh test --lib --quiet streaming_select_clears_subquery_membership_cache_between_statements
- scripts/cargo_isolated.sh test --lib --quiet referenced_partition_foreign_key
- scripts/cargo_isolated.sh test --lib --quiet self_referencing_partitioned_foreign_key_adds_referenced_clones
- scripts/cargo_isolated.sh check
- scripts/run_regression.sh --test foreign_key --port 60055 --results-dir /tmp/pgrust-task-c15-02-foreign-key

Remaining:
- The requested full `foreign_key` regression still fails:
  1180/1252 queries matched, 407 diff lines. The targeted self-referential
  partitioned FK catalog rows now appear in the full output, and the detach
  violation uses `parted_self_fk_id_abc_fkey_5`.
- Remaining diffs include pre-existing error cursor text, duplicate relation
  wording for a missing constraint, a later `fkpart3` catalog error that
  depends on earlier regression-file state, non-root partition FK update
  behavior, and unsupported `CREATE CONSTRAINT TRIGGER ... INITIALLY`.
- The source landscape note requested by the task,
  `.codex/task-notes/regression-failure-landscape-v2.md`, was not present in
  this checkout; the CI artifact diff was used as the source of truth.
