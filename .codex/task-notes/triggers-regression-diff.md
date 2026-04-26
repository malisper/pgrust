Goal:
Implement the prioritized fixes for the triggers.out regression drift.

Key decisions:
Model foreign keys as internal RI trigger rows and make FK checks/actions consult
the existing trigger enable/session-replication rules. Keep internal RI trigger
rows out of user trigger execution. Fix the targeted parser, PL/pgSQL,
partition SRF, and DML RETURNING command-tag gaps without trying to make the
full triggers regression pass.

Files touched:
Catalog/proc bootstrap, FK catalog mutation paths, trigger DDL/runtime,
foreign-key executor/tablecmds enforcement, PL/pgSQL parser/runtime, parser
trigger syntax, partition SRF planning, libpq/tcop/session/portal command tags,
and focused tests.

Tests run:
cargo fmt
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet parse_create_trigger_statement_for_statement_without_each
scripts/cargo_isolated.sh test --lib --quiet execute_do_raise_sqlstate_uses_literal_message_and_handler
scripts/cargo_isolated.sh test --lib --quiet foreign_key_actions_respect_disabled_internal_triggers
scripts/cargo_isolated.sh test --lib --quiet foreign_key_checks_respect_disabled_internal_triggers
scripts/cargo_isolated.sh test --lib --quiet partition_trigger_state_propagates_to_clones_unless_only
scripts/cargo_isolated.sh test --lib --quiet trigger_relid_regclass_assignment_uses_relation_name
scripts/cargo_isolated.sh test --lib --quiet partition_ancestors_supports_with_ordinality
scripts/cargo_isolated.sh test --lib --quiet dml_returning_uses_dml_command_tag
scripts/run_regression.sh --test triggers --jobs 1 --port 57643

Remaining:
The single-job triggers regression still times out. Final artifact:
/var/folders/tc/1psz8_jd0hnfmgyyr0n2wtzh0000gn/T//pgrust_regress_results.sarajevo-v2.vNb7LQ
Copied diff: /tmp/diffs/triggers.sarajevo-v2.final.diff
The latest timeout occurs in the session_replication_role trigger-enable block
after 152/1265 matched queries. A prior run with the same patch scope timed out
later at view INSERT ... RETURNING after 290/1265 matched queries; the full file
still contains larger deferred trigger/view/partition gaps.
