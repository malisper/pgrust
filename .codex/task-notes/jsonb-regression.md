Goal:
Close the jsonb regression failures around jsonb_populate_record,
jsonb_populate_recordset, jsonb_to_record, jsonb_to_recordset, domain cleanup,
and PostgreSQL-compatible record/array error wording.

Key decisions:
Keep JSONB-style whitespace local to populate/record conversion paths so compact
JSON functions keep their existing behavior.
Allow anonymous record column-definition lists without inventing a visible user
alias by deriving the function name as the internal alias.
Represent DROP DOMAIN names as a list while keeping domain_name for older call
sites, then drop all requested domains and dependent range entries together.
Parse `(record_expr).*` into the existing FieldSelect `*` AST shape because the
analyzer already owns record expansion.

Files touched:
crates/pgrust_sql_grammar/src/gram.pest
src/backend/executor/expr_json.rs
src/backend/executor/tests.rs
src/backend/executor/value_io/array.rs
src/backend/optimizer/plan/planner.rs
src/backend/parser/analyze/scope.rs
src/backend/parser/gram.rs
src/backend/parser/tests.rs
src/include/nodes/parsenodes.rs
src/pgrust/database/commands/drop.rs
src/pgrust/database_tests.rs

Tests run:
env CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm CARGO_TARGET_DIR=/tmp/pgrust-target-llvm cargo test --lib --quiet parse_field_star_select_from_record_expression
env CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm CARGO_TARGET_DIR=/tmp/pgrust-target-llvm cargo test --lib --quiet jsonb_populate_record_reuses_temp_table_type_info
env CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm CARGO_TARGET_DIR=/tmp/pgrust-target-llvm cargo test --lib --quiet jsonb_record_expansion_functions_work
env CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm CARGO_TARGET_DIR=/tmp/pgrust-target-llvm cargo test --lib --quiet jsonb_to_record_allows_json_array_elements_inside_json_array_column
env CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm CARGO_TARGET_DIR=/tmp/pgrust-target-llvm cargo test --lib --quiet parse_srf_column_definitions_without_alias
env CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm CARGO_TARGET_DIR=/tmp/pgrust-target-llvm cargo test --lib --quiet jsonb_populate_record_enforces_domain_constraints
env CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm CARGO_TARGET_DIR=/tmp/pgrust-target-llvm cargo test --lib --quiet jsonb_populate_record_coerces_text_jsonb_and_reports_array_errors
env CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm CARGO_TARGET_DIR=/tmp/pgrust-target-llvm cargo check --quiet
env CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm CARGO_TARGET_DIR=/tmp/pgrust-target-llvm scripts/run_regression.sh --test jsonb --timeout 120 --jobs 1 --port 55435 --results-dir /tmp/pgrust-jsonb-regress-3
env CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm CARGO_TARGET_DIR=/tmp/pgrust-target-llvm cargo test --lib --quiet build_plan_for_project_set_keeps_scalar_target_name
env CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm CARGO_TARGET_DIR=/tmp/pgrust-target-llvm cargo test --lib --quiet analyze_jsonb_populate_recordset_rejects_mismatched_query_rowtype

Remaining:
The full jsonb regression still fails overall at 1017/1084 matched queries.
The remaining diff no longer contains the requested populate/recordset/domain
patterns. Remaining categories are broader jsonb behavior: containment/numeric
comparison counts, JSON path/Gin explain and missing opclass support, jsonb
ordering/distinct counts, jsonb_pretty display width, and jsonb concatenation.
