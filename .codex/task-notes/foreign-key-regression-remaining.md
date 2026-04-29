Goal:
Fix remaining `foreign_key` regression issues after the partition-routing crash.

Key decisions:
Implemented focused support for `EXPLAIN DELETE` rule expansion for delete
actions, pending deferred child FK checks, savepoint restore of deferred FK
state, pending-trigger rejection for FK constraint drops, and physical-value FK
change detection so `-0` to `0` cascades.

Files touched:
`src/backend/commands/tablecmds.rs`
`src/backend/executor/foreign_keys.rs`
`src/backend/executor/mod.rs`
`src/pgrust/database/foreign_keys.rs`
`src/pgrust/session.rs`
`src/pgrust/database_tests.rs`

Tests run:
`cargo fmt`
`scripts/cargo_isolated.sh check`
`scripts/cargo_isolated.sh test --lib --quiet explain_delete_expands_delete_rule_actions`
`scripts/cargo_isolated.sh test --lib --quiet deferred_foreign_key_checks_survive_savepoint_rollback`
`scripts/cargo_isolated.sh test --lib --quiet alter_table_drop_foreign_key_rejects_pending_trigger_events`
`scripts/cargo_isolated.sh test --lib --quiet foreign_key_cascade_update_treats_negative_zero_as_changed`
`git diff --check`
`scripts/run_regression.sh --test foreign_key --timeout 120 --port 55433`

Remaining:
The first `foreign_key` regression rerun reached the targeted sections and
showed them fixed, then timed out later in unrelated partitioned-FK coverage.
A later rerun did not reach `foreign_key`; it failed while building the
`create_index` dependency base.
