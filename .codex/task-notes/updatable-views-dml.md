Goal:
Fix updatable-view DML gaps called out by the updatable_views regression diff: MERGE view target binding, ON CONFLICT on auto-updatable views, and old/new RETURNING pseudo rows.

Key decisions:
Use existing auto-updatable-view rewrite metadata for MERGE target binding and for rewriting view RETURNING/ON CONFLICT expressions onto base-table old/new rows. Keep old/new hidden and qualified-only so unqualified RETURNING * is unchanged while old.*, new.*, old, and new work.

Files touched:
src/backend/parser/analyze/modify.rs
src/backend/parser/analyze/on_conflict.rs
src/backend/parser/analyze/expr/targets.rs
src/backend/commands/tablecmds.rs
src/backend/commands/upsert.rs
src/backend/parser/tests.rs
src/pgrust/database_tests.rs

Tests run:
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet returning_expose
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet insert_on_conflict_works_for_auto_updatable_views
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet dml_returning_old_new_pseudo_rows
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet insert_on_conflict_returning_rows
scripts/cargo_isolated.sh test --lib --quiet parse_merge_returning_clause

Remaining:
MERGE RETURNING still depends on the existing MERGE executor returning behavior; this change focuses MERGE on rewriting view targets and view-specific errors.
