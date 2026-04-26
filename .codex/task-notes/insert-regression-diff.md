Goal:
Diagnose pasted insert regression diff from .context/attachments/pasted_text_2026-04-26_08-04-23.txt.

Key decisions:
First mismatch is expected/actual INSERT arity error wording and caret output. Main cascades are unsupported or incomplete PostgreSQL INSERT features: parenthesized SELECT insert source, assignment target indirection for composite/array fields, partition routing/constraint validation across multi-level partition trees, SRF lowering for INSERT SELECT, pg_get_partkeydef/psql describe support, role/grant syntax, COPY FROM STDOUT, and some ALTER TABLE forms.

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
.codex/task-notes/insert-regression-diff.md

Tests run:
scripts/cargo_isolated.sh test --lib --quiet parse_array_subscript_expressions_and_targets
scripts/cargo_isolated.sh test --lib --quiet composite_field_array_assignment_uses_ordered_indirection
scripts/cargo_isolated.sh test --lib --quiet composite_array_field_assignment_and_selection_work
scripts/cargo_isolated.sh check
scripts/run_regression.sh --test insert --timeout 120

Remaining:
insert regression still fails overall: 280/390 queries matched after this fix. Remaining groups include error caret formatting, parenthesized SELECT insert source, pg_relation_size(2 args), rule display, domain checks on assignment into domains, partition routing/describe support, grant/revoke parsing, COPY FROM STDOUT, and unsupported ALTER TABLE forms.
