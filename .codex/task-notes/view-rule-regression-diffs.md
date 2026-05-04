Goal:
Diagnose regression diffs for select_views, create_view, updatable_views, and returning. Implement the first select_views fix for missing path ?# path.

Key decisions:
Grouped failures by behavior: view deparse/ruleutils, view DML rewrite, security-barrier/check-option handling, MERGE through views, RETURNING old/new binding, and a separate geometric path operator setup failure.
Moved geometry callers off the root executor expr_geometry shim and into pgrust_expr directly. Implemented path/path intersects in pgrust_expr using path bounding-box rejection plus pairwise segment intersection. Mapped real geometry proc OIDs/names for path/lseg/line intersects and whitelisted the upstream public C helper interpt_pp to use the native GeoIntersection runtime.

Files touched:
.codex/task-notes/view-rule-regression-diffs.md
crates/pgrust_catalog_data/src/pg_proc.rs
crates/pgrust_catalog_ids/src/lib.rs
crates/pgrust_analyze/src/functions.rs
crates/pgrust_expr/src/backend/executor/expr_geometry.rs
src/backend/access/services.rs
src/backend/commands/tablecmds.rs
src/backend/executor/*
src/backend/libpq/pqformat.rs
src/backend/optimizer/mod.rs
src/pgrust/session.rs
src/pl/plpgsql/exec.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet path_inter
scripts/cargo_isolated.sh test --lib --quiet path_intersects_operator_resolves_and_executes
scripts/cargo_isolated.sh test -p pgrust_expr --lib --quiet path_intersects
scripts/cargo_isolated.sh test -p pgrust_catalog_data --lib --quiet indexed_builtin_proc_helpers_match_bootstrap_rows
scripts/cargo_isolated.sh check
scripts/run_regression.sh --test select_views --jobs 1 --timeout 180 --results-dir /tmp/diffs/select_views-path-inter
scripts/run_regression.sh --test select_views --jobs 1 --timeout 180 --ignore-deps --results-dir /tmp/diffs/select_views-path-inter-ignore-deps
scripts/run_regression.sh --test select_views --jobs 1 --timeout 180 --port 56473 --results-dir /tmp/diffs/select_views-path-inter-final

Remaining:
select_views now reaches the body and matches 40/52 queries; the path ?# path and interpt_pp geometry blockers are gone. Remaining diff starts in security-barrier/leaky-view EXPLAIN and NOTICE behavior. Broader view/rewrite fixes still likely span crates/pgrust_rewrite/src/views.rs, crates/pgrust_rewrite/src/view_dml.rs, crates/pgrust_analyze/src/modify.rs, src/backend/commands/tablecmds.rs, src/backend/commands/explain.rs, and error-location formatting.
