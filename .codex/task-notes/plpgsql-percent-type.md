Goal:
Resolve SQL-visible `%TYPE` references used by PL/pgSQL function signatures,
especially partitioned table column references.

Key decisions:
Preserve `%TYPE` and `%ROWTYPE` suffixes in `parse_type_name` instead of letting
the generic type grammar drop them. Resolve relation-column `%TYPE` and relation
`%ROWTYPE` in the analyzer using catalog relation metadata. PL/pgSQL declaration
resolution already had a parallel path; this slice fixes function argument and
return type positions.

Files touched:
src/backend/parser/gram.rs
src/backend/parser/analyze/mod.rs
src/backend/parser/tests.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet plpgsql
scripts/cargo_isolated.sh test --lib --quiet parse_record_and_named_type_names
scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55433 --results-dir /tmp/diffs/plpgsql-percent-type

Remaining:
Regression is at 2160/2271 matched with 1308 diff lines. The partitioned-table
behavior now matches, except PostgreSQL emits compatibility NOTICE lines for
the `%TYPE` conversion that pgrust does not yet emit.
