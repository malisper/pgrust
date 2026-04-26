Goal:
Diagnose pasted insert regression diff from .context/attachments/pasted_text_2026-04-26_08-04-23.txt.

Key decisions:
First mismatch is expected/actual INSERT arity error wording and caret output. Main cascades are unsupported or incomplete PostgreSQL INSERT features: parenthesized SELECT insert source, assignment target indirection for composite/array fields, partition routing/constraint validation across multi-level partition trees, SRF lowering for INSERT SELECT, pg_get_partkeydef/psql describe support, role/grant syntax, COPY FROM STDOUT, and some ALTER TABLE forms.
Follow-up fixes: `INSERT INTO t (select ...)` was a grammar ambiguity because the parenthesized SELECT was only accepted as a normal `select_stmt` source, after the parser had already tried the optional target column list. Added an explicit parenthesized insert select source that still lowers to `InsertSource::Select`.
Follow-up fixes: missing `LINE 1` caret output for INSERT arity and DEFAULT-indirection errors was caused by `exec_error_position` returning no position for those parse/analyze errors. Added INSERT-specific position lookup for the unmatched target/value and for the target paired with a `DEFAULT` value.

Files touched:
src/include/nodes/parsenodes.rs
src/backend/parser/gram.pest
src/backend/parser/gram.rs
src/backend/parser/tests.rs
src/backend/parser/analyze/mod.rs
src/backend/parser/analyze/modify.rs
src/backend/parser/analyze/on_conflict.rs
src/backend/commands/tablecmds.rs
src/backend/commands/upsert.rs
src/backend/executor/tests.rs
src/backend/tcop/postgres.rs
.codex/task-notes/insert-regression-diff.md

Tests run:
scripts/cargo_isolated.sh test --lib --quiet parse_array_subscript_expressions_and_targets
scripts/cargo_isolated.sh test --lib --quiet composite_field_array_assignment_uses_ordered_indirection
scripts/cargo_isolated.sh test --lib --quiet composite_array_field_assignment_and_selection_work
scripts/cargo_isolated.sh test --lib --quiet parse_insert_update_delete
scripts/cargo_isolated.sh test --lib --quiet exec_error_position_points_at_insert_arity_mismatch
scripts/cargo_isolated.sh test --lib --quiet exec_error_position_points_at_default_indirection_target
scripts/cargo_isolated.sh test --lib --quiet simple_query_reports_position_for_insert_arity_error
scripts/cargo_isolated.sh test --lib --quiet simple_query_reports_position_for_default_indirection_error
scripts/cargo_isolated.sh check
scripts/run_regression.sh --test insert --timeout 120
CARGO_TARGET_DIR=/tmp/pgrust-target-stuttgart-v2-insert scripts/run_regression.sh --test insert --timeout 120 --port 56643

Remaining:
Last completed insert regression before the follow-up fixes had 280/390 queries matched. Remaining known groups after removing the now-fixed caret/parenthesized-SELECT buckets include pg_relation_size(2 args), rule display, domain checks on assignment into domains, partition routing/describe support, grant/revoke parsing, COPY FROM STDOUT, and unsupported ALTER TABLE forms.
Full follow-up insert regression did not reach `insert.sql`: the default run blocked on shared Cargo target locks, and the isolated-target rerun failed during `test_setup` bootstrap with `ERROR: canceling statement due to statement timeout` after `VACUUM ANALYZE tenk2`.

Follow-up 2026-04-26:
Fixed multi-action `ALTER TABLE ... ADD ..., ADD ...`, allowed ADD COLUMN on partitioned tables, remapped partition-routed RETURNING rows back through the parent row layout, and made table GRANT/REVOKE lookup accept partitioned tables. Insert regression improved to 322/390 matched. The requested buckets no longer show `ALTER TABLE form`, `returningwrtest`, or partitioned-table GRANT/REVOKE object-missing failures. Remaining prominent groups are domain cascade/checks, `pg_get_partkeydef` and partition describe formatting, partition constraint rechecks after BEFORE triggers, writable CTE support, and partition detail expression formatting.
