Goal:
Match PostgreSQL declaration-order visibility for PL/pgSQL variable defaults.

Key decisions:
Compile each variable default expression before defining that variable in the
current declaration scope. This keeps outer and earlier variables visible, hides
the variable from its own default, and makes later declarations see it.

Files touched:
src/pl/plpgsql/compile.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet plpgsql_declare_default_scope_matches_declaration_order
scripts/cargo_isolated.sh test --lib --quiet plpgsql
scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55434 --results-dir /tmp/diffs/plpgsql-declare-default-scope

Remaining:
Regression reported 2192/2271 matched and 943 diff lines. Declaration-default
success cases now match; unresolved default expressions still report a
compile-time line/caret instead of PostgreSQL's QUERY plus PL/pgSQL local
variable initialization context.
