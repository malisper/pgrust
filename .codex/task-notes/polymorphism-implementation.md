Goal:
Fix PostgreSQL polymorphism regression behavior without changing expected output.

Key decisions:
Preserve untyped NULL/string literals as unknown for function-call resolution.
Resolve implicit variadic polymorphic calls against expanded element pseudotypes.
Pass concrete call-site type OIDs into SQL-function literal rendering.
Resolve custom aggregate anyarray state types from aggregate call input types.
Validate unanchored anyarray aggregate transition states before support-proc lookup.

Files touched:
src/backend/parser/analyze/functions.rs
src/backend/parser/analyze/infer.rs
src/backend/parser/analyze/expr.rs
src/backend/parser/analyze/expr/func.rs
src/backend/parser/analyze/expr/targets.rs
src/backend/parser/analyze/scope.rs
src/backend/parser/analyze/agg_output.rs
src/backend/executor/sqlfunc.rs
src/backend/executor/expr_agg_support.rs
src/backend/executor/agg.rs
src/backend/executor/exec_expr.rs
src/include/catalog/pg_proc.rs
src/pgrust/database/commands/create.rs
src/pgrust/database/commands/drop.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh check
scripts/run_regression.sh --test polymorphism --port 55473

Remaining:
Final polymorphism run still fails: 392/455 queries matched, 645 diff lines.
Final diff copied to /tmp/diffs/polymorphism.final.diff.
Largest remaining groups are dfunc default/named binding, custom aggregate window execution, array_in/anyrange_in special input functions, built-in concat shadowing for user variadic concat, PL/pgSQL polymorphic anyarray transition values, and CASE SQL-function eager argument evaluation.
