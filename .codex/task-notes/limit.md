Goal:
Fix PostgreSQL limit regression failures for LIMIT/OFFSET clauses that use full
expressions, nullable volatile expressions, and correlated OFFSET expressions.

Key decisions:
Bind LIMIT/OFFSET as analyzed expressions coerced to int8, evaluate them once per
Limit scan, treat NULL LIMIT as unbounded and NULL OFFSET as zero, and keep
constant LIMIT estimates available to planner costing.

Files touched:
Parser grammar and AST, select analysis, plan/path/exec Limit nodes, setrefs,
const folding, view deparse, explain tests, and executor/parser tests.

Tests run:
cargo fmt
scripts/cargo_isolated.sh check
cargo test --lib --quiet limit_
cargo test --lib --quiet correlated_offset_expression_evaluates_per_outer_scan
scripts/run_regression.sh --test limit --jobs 1 --timeout 180 --port 56561

Remaining:
The isolated limit regression still fails on unrelated FETCH FIRST support and
EXPLAIN/setup-dependent plan output; the targeted original LIMIT/OFFSET failure
hunks are gone.
