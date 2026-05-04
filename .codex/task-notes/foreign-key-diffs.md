Goal:
Fix foreign_key.diff and without_overlaps.diff failures around partitioned FKs and temporal PERIOD/WITHOUT OVERLAPS FKs by sharing referenced-index lookup and extracting pure FK command helpers.

Key decisions:
Shared referenced-index lookup now lives in pgrust_analyze and matches PostgreSQL-style FK index eligibility: valid, ready, immediate, non-partial, no expressions, exact key count; normal FKs use btree unique indexes with attnums matching as a set, temporal FKs use exclusion indexes with the PERIOD attnum last. Pure FK DDL planning helpers now live in pgrust_commands; Database code only orchestrates catalog writes, triggers, and transaction effects.
Referenced-side partition FK clones now plan against the referenced partition's local index and preserve/remap conperiod, confrelid, conindid, confkey, enforcement, validation, actions, match type, and deferrability. FK action updates refresh inbound FK bindings at write time so ALTER CONSTRAINT state is not missed, and immediate NO ACTION parent checks exclude the tuple being updated while still accepting same-statement replacement keys.
Dropping a referencing table no longer treats FK conindid as an owned index; only primary/unique/exclusion constraints recurse into conindid. This fixed the without_overlaps key loss after child FK table drops.

Files touched:
crates/pgrust_analyze/src/foreign_keys.rs
crates/pgrust_analyze/src/lib.rs
crates/pgrust_analyze/src/constraints.rs
crates/pgrust_commands/src/foreign_keys.rs
crates/pgrust_commands/src/lib.rs
src/backend/catalog/store/heap.rs
src/backend/commands/tablecmds.rs
src/backend/commands/upsert.rs
src/backend/executor/foreign_keys.rs
src/pgrust/database/commands/constraint.rs
src/pgrust/database_tests.rs
.context/without_overlaps_only_schedule
.codex/task-notes/foreign-key-diffs.md

Tests run:
scripts/cargo_isolated.sh test -p pgrust_analyze --lib --quiet foreign_keys
scripts/cargo_isolated.sh test -p pgrust_commands --lib --quiet foreign_keys
scripts/cargo_isolated.sh test --lib --quiet foreign_key
scripts/cargo_isolated.sh test --lib --quiet without_overlaps
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet attach_partition_merges_existing_foreign_key_with_parent
scripts/run_regression.sh --port 5552 --schedule .context/without_overlaps_only_schedule --test without_overlaps --results-dir /tmp/pgrust_without_overlaps_regress6: PASS, 643/643.
scripts/run_regression.sh --port 5551 --test foreign_key --results-dir /tmp/pgrust_foreign_key_regress12: FAIL, 1228/1252 matched, 243 diff lines; copied diff to /tmp/diffs/foreign_key.diff.

Remaining:
without_overlaps is clean with the focused schedule. foreign_key improved but still has broader RI/action/display diffs: parser caret truncation, RI-induced action ordering for the row-updated-earlier-in-transaction case, partitioned SET DEFAULT column-list row movement, duplicate display of a merged local FK on ATTACH, one extra self-referencing partition clone, pg_constraint dependency parent selection for a partition child, RESTRICT/deferred parent-side error names, regclass qualification in fkpart11 tableoid output, leaf-FK cascade state in fkpart11, and trigger notice formatting for cross-partition moves.
The earlier post_create_index/create_index crash did not reproduce in repeated foreign_key runs on alternate port 5551; base setup completed cleanly there.
