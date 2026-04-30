Goal:
Investigate planner/setrefs-looking errors seen in sibling Conductor workspaces.

Key decisions:
- The concrete CI failures were in `damascus-v4` logs for
  `create_index_on_partitioned_table_builds_index_tree` and
  `create_hash_index_catalog_and_equality_scan`.
- Both failed on `SELECT indexdef FROM pg_indexes ...` with
  `special executor Var referenced beyond the bound tuple width`
  (`OUTER_VAR`, `varattno=5`, `tuple_width=4`).
- This was not a hash/index planner failure. The `rules` parity branch had
  accidentally mixed four-column synthetic `pg_rules` metadata rows into
  `pg_indexes`, so the planned fifth-column projection (`indexdef`) executed
  against a four-value row.
- `damascus-v4/.codex/task-notes/rules-regression.md` records the branch fix:
  remove synthetic `pg_rules` metadata rows from `pg_indexes`, plus teach
  delete-rule EXPLAIN substitution about `RULE_OLD_VAR`.
- Added a defensive synthetic-system-view binding filter in `zagreb-v3` so
  malformed rows are dropped before they become planned `VALUES` rows. This
  turns this class of bug into a missing-row issue at the row builder boundary
  instead of a misleading setrefs/executor tuple-width failure.

Files touched:
- `src/backend/parser/analyze/system_views.rs`
- `.codex/task-notes/investigate-setref-errors.md`

Tests run:
- `scripts/cargo_isolated.sh test --lib --quiet create_hash_index_catalog_and_equality_scan -- --nocapture`
  in `zagreb-v3` before the code change: passed on `origin/perf-optimization`.
- Same focused test in `damascus-v4`: passed on that workspace's current tree.
- `cargo fmt`
- `scripts/cargo_isolated.sh test --lib --quiet synthetic_system_view_drops_malformed_rows_before_planning -- --nocapture`
- Earlier shared-target repro retries stalled behind unrelated cargo jobs.
- Clean external-target rerun:
  `CARGO_TARGET_DIR='/Volumes/OSCOO PSSD/pgrust/zagreb-repro-target' cargo test --lib synthetic_system_view_drops_malformed_rows_before_planning -- --nocapture`
  passed.
- Clean external-target original repro:
  `CARGO_TARGET_DIR='/Volumes/OSCOO PSSD/pgrust/zagreb-repro-target' cargo test --lib create_hash_index_catalog_and_equality_scan -- --nocapture`
  passed.
- Clean external-target original repro:
  `CARGO_TARGET_DIR='/Volumes/OSCOO PSSD/pgrust/zagreb-repro-target' cargo test --lib create_index_on_partitioned_table_builds_index_tree -- --nocapture`
  passed.

Remaining:
- No reproduced setrefs/tuple-width failure after the guard.
