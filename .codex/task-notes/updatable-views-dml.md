Goal:
Fix updatable-view DML gaps called out by the updatable_views regression diff: MERGE view target binding, ON CONFLICT on auto-updatable views, old/new RETURNING pseudo rows, and MERGE RETURNING result rows.

Key decisions:
Use existing auto-updatable-view rewrite metadata for MERGE target binding and for rewriting view RETURNING/ON CONFLICT expressions onto base-table old/new rows. Keep old/new hidden and qualified-only so unqualified RETURNING * is unchanged while old.*, new.*, old, and new work.
Bind MERGE action clauses and RETURNING against the compact runtime slot layout: target columns, then source columns, then merge_action(). This avoids source columns reading same-numbered target attrs during MERGE execution.
Allow raised old/new executor-special Vars inside scalar subqueries so setrefs can lower them to subplan params.
Teach setrefs to recognize parent append Vars while lowering bitmap scan quals for inherited children, and to rewrite immediate projections over subquery scans against the subquery input expressions without polluting reusable join tlists.

Files touched:
src/backend/parser/analyze/modify.rs
src/backend/parser/analyze/on_conflict.rs
src/backend/parser/analyze/expr/targets.rs
src/backend/parser/analyze/expr.rs
src/backend/optimizer/setrefs.rs
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
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet merge_returning_projects_action_old_new_and_source
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet security_barrier_inheritance_view_filters_through_subquery_scan
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh check
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/run_regression.sh --test updatable_views --timeout 120 --jobs 1
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/run_regression.sh --test updatable_views --timeout 300 --jobs 1

Remaining:
updatable_views now completes without server crash/error under --timeout 300 and fails as a normal diff: 705/1139 queries matched, 3674 diff lines. The 120s run times out after 700/1139 queries.
