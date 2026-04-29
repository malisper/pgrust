Goal:
Close the remaining PL/pgSQL regression diff where PostgreSQL emits notices for function signatures using relation-column `%TYPE`, including partitioned table columns.

Key decisions:
Emit CREATE FUNCTION notices after resolving argument, return, and RETURNS TABLE raw types when the original raw type name was a relation-column `%TYPE` reference.
Deduplicate notices with a BTreeSet so repeated signature references do not produce unstable output.

Files touched:
src/pgrust/database/commands/create.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet plpgsql_partitioned_table_percent_type_signatures
scripts/cargo_isolated.sh test --lib --quiet plpgsql
scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55434 --results-dir /tmp/diffs/plpgsql-partitioned-percent-type-notices

Remaining:
PL/pgSQL regression is at 2218/2271 matched with 699 diff lines. Remaining clusters include WHERE CURRENT OF, composite return coercion, context/error formatting, escape-string warnings, anyarray pseudotype behavior, and transition table gaps.
