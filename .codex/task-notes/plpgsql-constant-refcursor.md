Goal:
Close the PL/pgSQL constant refcursor regression cluster.

Key decisions:
Parse `CONSTANT` as a declaration modifier instead of as part of the type name.
Track constant variables in the PL/pgSQL compile scope and reject assignments.
Allow `OPEN` on a constant refcursor only when the variable already contains a
portal name; otherwise report PostgreSQL's CONSTANT variable error.

Files touched:
src/pl/plpgsql/ast.rs
src/pl/plpgsql/gram.rs
src/pl/plpgsql/compile.rs
src/pl/plpgsql/exec.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet plpgsql
scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55433 --results-dir /tmp/diffs/plpgsql-constant-refcursor

Remaining:
Regression is at 2156/2271 matched with 1371 diff lines. Refcursor persistence
for returned cursor portals is still open, along with diagnostics/context,
record/composite return coercion, SELECT INTO, transition tables, and final
formatting.
