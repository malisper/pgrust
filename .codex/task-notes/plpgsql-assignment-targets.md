Goal:
Close PL/pgSQL assignment-target diffs for array subscripts and record-field array updates.

Key decisions:
Added PL/pgSQL assignment target indirection for fields and one-dimensional array subscripts.
Runtime assignment reuses the variable's declared composite type when anonymous row values need named field updates.
Unknown record field reads in PL/pgSQL expressions fall back to runtime field selection when the descriptor is only known after assignment.

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
scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55433 --results-dir /tmp/diffs/plpgsql-assignment-targets-rerun2

Remaining:
plpgsql regression now reports 2148/2271 matched and 1466 diff lines.
The nearby orderedarray failures are domain constraint enforcement, not assignment-target parsing.
