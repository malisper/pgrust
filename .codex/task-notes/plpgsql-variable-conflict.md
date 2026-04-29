Goal:
Implement PL/pgSQL variable/table-column conflict handling for static SELECTs.

Key decisions:
Add a compiler-level variable conflict mode with PostgreSQL defaults, honor
#variable_conflict directives, detect ambiguous unqualified table/variable
references in static SELECTs, and rewrite unqualified variable references to
internal slot aliases for use_variable mode.

Files touched:
src/pl/plpgsql/compile.rs
src/pl/plpgsql/exec.rs
src/pl/plpgsql/mod.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet plpgsql_variable_conflict_modes_control_static_select_resolution
scripts/cargo_isolated.sh test --lib --quiet plpgsql
scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55434 --results-dir /tmp/diffs/plpgsql-variable-conflict

Remaining:
Regression reported 2198/2271 matched and 878 diff lines. The conflict_test
semantic behavior now matches, but the ambiguity error still lacks PostgreSQL's
internal QUERY/caret and exact PL/pgSQL statement context formatting.
