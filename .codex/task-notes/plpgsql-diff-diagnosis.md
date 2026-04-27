Goal:
Diagnose and fix the requested plpgsql regression diffs from .context/attachments/pasted_text_2026-04-27_11-12-54.txt.

Key decisions:
Fixed the requested PL/pgSQL record issues by preserving labeled block variables through synthetic slot aliases, so "outer".rec.field no longer binds to an inner shadowing rec.
Added assignment-expression rewriting for PL/pgSQL shorthand `target := expr FROM ...`, compiling it as a scalar subquery.
The WSlot/PSlot backlink rows, Hub HSlot integer comparison, and PField_v1 rows now match in the targeted plpgsql regression.
Added lazy transaction finalization for streaming SELECTs that execute PL/pgSQL writes, so trigger/function heap writes allocate and commit an xid instead of failing during autocommit streaming.
Accepted RETURN NEXT composite expressions for set-returning rowtype/record functions and function column-definition-list syntax without an explicit alias.
Kept polymorphic call binding concrete while preserving declared signatures in PL/pgSQL context output; anycompatible range anchors, polymorphic OUT row metadata, and string-literal coercions now match the relevant PL/pgSQL cases.
Canonicalized COMMENT ON FUNCTION missing-signature type names and rejected RETURN expr in functions with OUT parameters during CREATE FUNCTION validation.
Remaining plpgsql diffs are separate: PL/pgSQL source line numbers, raw executor TypeMismatch/operator error formatting, anyarray pseudo-type diagnostics, cursor/refcursor support, RAISE validation, strict/execute behavior, FOREACH, transition-table coverage, and later timeout-covered sections.

Files touched:
.codex/task-notes/plpgsql-diff-diagnosis.md
crates/pgrust_sql_grammar/src/gram.pest
src/backend/parser/analyze/functions.rs
src/backend/parser/analyze/scope.rs
src/backend/parser/gram.rs
src/backend/tcop/postgres.rs
src/pgrust/database/commands/create.rs
src/pgrust/database/commands/execute.rs
src/pgrust/database/commands/maintenance.rs
src/pgrust/session.rs
src/pl/plpgsql/compile.rs
src/pl/plpgsql/exec.rs
src/pl/plpgsql/mod.rs
src/pgrust/database_tests.rs

Tests run:
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet pl::plpgsql::compile::tests
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet plpgsql_select_into_record_preserves_field_types
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet plpgsql_labeled_record_reference_uses_outer_slot_when_shadowed
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet plpgsql_assignment_query_expr_from_clause_uses_sql_scope
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet plpgsql_after_trigger_update_keeps_new_row_and_reciprocal_update
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/run_regression.sh --test plpgsql --timeout 120 --jobs 1 --port 55434 --results-dir /tmp/pgrust-plpgsql-after2
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh check
git diff --check
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet plpgsql_write_inside_streaming_select_allocates_and_commits_xid
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet plpgsql_return_next_accepts_composite_expression_for_setof_rowtype
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet plpgsql_record_returning_function_from_accepts_column_definition_list
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet plpgsql_anycompatible_range_calls_coerce_anchor_arguments
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet plpgsql_polymorphic_out_arguments_compile_concrete_types
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet comment_on_function_missing_signature_uses_canonical_type_names
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet plpgsql_context_uses_declared_polymorphic_signature
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet plpgsql_return_expr_with_out_parameter_is_compile_error
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh check
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/run_regression.sh --test plpgsql --port 55443

Remaining:
The targeted plpgsql regression still times out, now at 1791/2271 matched queries. The original requested hunks are gone; next focused work should start with source line tracking or cursor/refcursor support.
