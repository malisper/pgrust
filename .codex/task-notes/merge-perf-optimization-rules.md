Goal:
Merge origin/perf-optimization into the rules regression parity branch and keep rules green.

Key decisions:
Preserve the upstream create-function SQL changes while adding the missing MERGE syntax used by rules: INSERT OVERRIDING, assignment targets in MERGE INSERT columns, and RETURNING WITH OLD/NEW aliases.
Use existing DML/MERGE RETURNING validation where practical, with a scoped SQL-standard table-returning DML shortcut until SQL-function bodies have a statement-local binding environment.

Files touched:
crates/pgrust_sql_grammar/src/gram.pest
src/backend/parser/gram.rs
src/include/nodes/parsenodes.rs
src/backend/parser/analyze/modify.rs
src/pgrust/session.rs
src/pl/plpgsql/compile.rs
src/pgrust/database/commands/create.rs
src/backend/executor/exec_expr.rs
src/backend/parser/tests.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet parse_merge
RUST_BACKTRACE=full scripts/run_regression.sh --test rules --timeout 300 --port 57033

Remaining:
No remaining rules regression diff after the merge fix.
