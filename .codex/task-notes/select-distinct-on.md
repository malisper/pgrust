Goal:
Implement SELECT DISTINCT ON support for the select_distinct_on regression.

Key decisions:
Carry DISTINCT ON separately from bare DISTINCT through parser/analyzer/planner.
Use explicit Unique key indices so DISTINCT ON can keep the first row by ORDER
BY while deduplicating only the ON key. Add a constant-key LIMIT rewrite for
WHERE-constrained DISTINCT ON keys.

Files touched:
.codex/task-notes/select-distinct-on.md
src/backend/parser/gram.pest
src/backend/parser/gram.rs
src/include/nodes/parsenodes.rs
src/include/nodes/pathnodes.rs
src/include/nodes/plannodes.rs
src/include/nodes/execnodes.rs
src/backend/parser/analyze/mod.rs
src/backend/optimizer/plan/planner.rs
src/backend/executor/nodes.rs
src/backend/executor/startup.rs
and constructor/plumbing/test files.

Tests run:
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet distinct_on
scripts/run_regression.sh --test select_distinct_on --upstream-setup --timeout 120
scripts/run_regression.sh --test select_distinct_on --upstream-setup --ignore-deps --timeout 120 --port 55433

Remaining:
Regression harness did not reach select_distinct_on because create_index base
setup failed first. This implementation does not add a generic Incremental Sort
plan node; DISTINCT ON cases use existing sort paths plus index-order reuse.
