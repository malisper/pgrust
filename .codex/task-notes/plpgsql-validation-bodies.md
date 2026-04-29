Goal:
Reduce remaining PL/pgSQL regression diffs around create-time validation, return-shape checks, exception variable scoping, and malformed static SQL.

Key decisions:
Keep static SQL validation deliberately narrow: catch obviously invalid non-SQL statements and the regression's invalid SELECT alias form, but defer PL/pgSQL-specific rewrites such as INTO, $n parameters, transition-table queries, assignment subscripts, and WHERE CURRENT OF to later semantic slices.

Files touched:
crates/pgrust_plpgsql_grammar/src/gram.pest
src/pgrust/database/commands/create.rs
src/pgrust/database_tests.rs
src/pl/plpgsql/compile.rs
src/pl/plpgsql/exec.rs
src/pl/plpgsql/gram.rs
src/pl/plpgsql/mod.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet plpgsql
scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55433 --results-dir /tmp/diffs/plpgsql-validate-function-bodies-narrow2

Remaining:
plpgsql is at 2144/2271 matched with 1504 diff lines. Remaining failures are mostly record/composite assignment and return coercion, diagnostics/context, refcursor/WHERE CURRENT OF, SELECT INTO and transition tables, plus exact formatting.
