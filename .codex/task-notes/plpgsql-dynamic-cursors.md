Goal:
Implement PL/pgSQL dynamic SQL and cursor syntax gaps from the plpgsql regression slice.

Key decisions:
Represent RETURN QUERY EXECUTE, OPEN FOR EXECUTE, declared cursor calls, and cursor movement explicitly in the PL/pgSQL AST and compiled statements. Substitute declared cursor parameters as SQL literals as a temporary compatibility shim until cursor parameter binding exists in the SQL planner.

Files touched:
crates/pgrust_plpgsql_grammar/src/gram.pest
src/pl/plpgsql/ast.rs
src/pl/plpgsql/gram.rs
src/pl/plpgsql/compile.rs
src/pl/plpgsql/exec.rs
src/pl/plpgsql/mod.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet plpgsql
scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55433 --results-dir /tmp/diffs/plpgsql-dynamic-cursors

Remaining:
plpgsql still has 134 mismatched queries. Remaining cursor-related gaps include WHERE CURRENT OF behavior and exact LINE pointer formatting for create-time cursor argument errors.
