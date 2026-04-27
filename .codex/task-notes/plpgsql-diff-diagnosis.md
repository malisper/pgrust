Goal:
Diagnose and fix the requested plpgsql regression diffs from .context/attachments/pasted_text_2026-04-27_11-12-54.txt.

Key decisions:
Fixed the requested PL/pgSQL record issues by preserving labeled block variables through synthetic slot aliases, so "outer".rec.field no longer binds to an inner shadowing rec.
Added assignment-expression rewriting for PL/pgSQL shorthand `target := expr FROM ...`, compiling it as a scalar subquery.
The WSlot/PSlot backlink rows, Hub HSlot integer comparison, and PField_v1 rows now match in the targeted plpgsql regression.
Remaining plpgsql diffs are separate: COMMENT ON FUNCTION type-name display, context line numbers/signatures, missing lazy transaction state for heap writes in later functions, unsupported RETURN NEXT/record FROM-call forms, and polymorphic/range gaps.

Files touched:
.codex/task-notes/plpgsql-diff-diagnosis.md
src/pl/plpgsql/compile.rs
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

Remaining:
The targeted plpgsql regression still fails overall with unrelated remaining PL/pgSQL gaps, but the requested hunks are gone.
