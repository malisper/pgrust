Goal:
Diagnose and fix selected foreign_key regression failures: referenced-table SELECT ACL checks, pg_get_constraintdef FK action coverage, and FK-specific column errors.

Key decisions:
Grouped output into root causes rather than treating cascading relation-missing errors as independent failures.
Used direct diff inspection because the listed local diff skill path was unavailable.
Added focused unit coverage for the requested fixes because the full foreign_key regression still times out on unrelated remaining FK gaps.

Files touched:
.codex/task-notes/foreign-key-regression-diagnosis.md
src/backend/executor/foreign_keys.rs
src/backend/executor/permissions.rs
src/backend/parser/analyze/constraints.rs
src/backend/parser/tests.rs
src/pgrust/database_tests.rs

Tests run:
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet foreign_key_insert_requires_select_on_referenced_table
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet pg_get_constraintdef_keeps_fk_actions_when_referenced_columns_are_omitted
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet pg_get_constraintdef_formats_foreign_key_actions_and_delete_columns
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet lower_create_table_uses_foreign_key_column_errors
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet lower_create_table_rejects_invalid_foreign_key_delete_set_columns
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/run_regression.sh --port 55433 --test foreign_key --timeout 120 --ignore-deps --results-dir /tmp/pgrust-foreign-key-regression-ignore-deps

Remaining:
Full foreign_key regression still times out with remaining mismatches unrelated to these three fixes; see /tmp/pgrust-foreign-key-regression-ignore-deps.
First regression attempt without --ignore-deps did not reach foreign_key because create_index dependency setup failed; see /tmp/pgrust-foreign-key-regression/output/base_post_create_index_create_index.out.
