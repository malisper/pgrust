Goal:
Implement staged ALTER TABLE handling for ALTER COLUMN TYPE, multi-column USING,
generated-column expression actions, and dependent index/constraint rewrite behavior.

Key decisions:
- Route compound/multi ALTER TABLE actions that need shared state through one batch planner.
- Bind ALTER COLUMN TYPE USING expressions against the original row descriptor, then rewrite rows once against the final staged descriptor.
- Use staged descriptors for generated ADD/SET/DROP EXPRESSION and type changes, including multi-ADD generated-column dependencies.
- Rebind dependent index expressions and predicates against the final descriptor, refresh index column metadata/opclasses, and update exclusion constraint operators.
- Validate rewritten rows with generated values, check constraints, not-null constraints, and outbound foreign keys before catalog mutation.

Files touched:
- src/pgrust/database/commands/alter_column_type.rs
- src/pgrust/database/commands/execute.rs
- src/pgrust/session.rs
- src/pgrust/database/ddl.rs
- src/backend/catalog/store/heap.rs
- src/backend/commands/tablecmds.rs
- src/backend/tcop/postgres.rs
- src/backend/utils/sql_deparse.rs
- src/pgrust/database_tests.rs

Tests run:
- cargo fmt
- git diff --check
- scripts/cargo_isolated.sh test --lib --quiet alter_table_alter_column_type
- scripts/cargo_isolated.sh test --lib --quiet alter_table_generated (before final multi-ADD dispatch patch)
- scripts/cargo_isolated.sh test --lib --quiet alter_table_generated_set_expression_rewrites_and_validates_rows
- scripts/cargo_isolated.sh test --lib --quiet normalizes_simple_text_predicates
- scripts/cargo_isolated.sh test --lib --quiet alter_table_generated_columns_support_staged_type_batches
- scripts/cargo_isolated.sh test --lib --quiet alter_table_alter_column_type_rewrites_dependent_indexes
- TMPDIR=/tmp CARGO_TARGET_DIR=/tmp/pgrust-pr485-target scripts/cargo_isolated.sh test --lib --quiet concurrent_indexed_updates_and_deletes_keep_index_results_correct -- --nocapture
- TMPDIR=/tmp CARGO_TARGET_DIR=/tmp/pgrust-pr485-target scripts/cargo_isolated.sh test --lib --quiet alter_table_generated_columns_support_staged_type_batches
- TMPDIR=/tmp CARGO_TARGET_DIR=/tmp/pgrust-pr485-target scripts/cargo_isolated.sh test --lib --quiet alter_table_alter_column_type_rewrites_dependent_indexes
- CARGO_TARGET_DIR=/tmp/pgrust-release-muscat-v7 scripts/run_regression.sh --test alter_table --jobs 1 --timeout 180 --results-dir /tmp/pgrust-alter-table-type-regress-alter-5
- CARGO_TARGET_DIR=/tmp/pgrust-release-muscat-v7 scripts/run_regression.sh --test generated_stored --jobs 1 --timeout 180 --port 15435 --results-dir /tmp/pgrust-alter-table-type-regress-stored-4
- CARGO_TARGET_DIR=/tmp/pgrust-release-muscat-v7 scripts/run_regression.sh --test generated_virtual --jobs 1 --timeout 180 --port 15436 --results-dir /tmp/pgrust-alter-table-type-regress-virtual

Remaining:
- alter_table still has unrelated broad-file diffs: formatting, partition/storage metadata, typed-table messages, FK row-type dependency behavior, and other catalog gaps.
- generated_stored/generated_virtual still have broader generated-column diffs around error wording, partition inheritance, trigger restrictions, privileges, virtual DML/check enforcement, and other unsupported PostgreSQL edge cases.
- Some direct cargo filtered test sessions became unreliable after an interrupted tool session; final validation for the multi-ADD generated-column dispatch came from the generated_stored/generated_virtual regression reruns, which compiled the final code.
