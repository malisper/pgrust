Goal:
Fix PL/pgSQL assignment through nested subscripts on multidimensional arrays.

Key decisions:
- Preserve `PgArray` multidimensional shape by assigning through nested slice values and rebuilding the array with original lower bounds.
- Keep the existing one-dimensional assignment path unchanged.
- Add a focused regression-shaped test using non-simple subquery expressions in both subscripts.

Files touched:
- `src/pl/plpgsql/exec.rs`
- `src/pgrust/database_tests.rs`

Tests run:
- `cargo fmt`
- `scripts/cargo_isolated.sh test --lib --quiet plpgsql_multidimensional_array_element_assignment_preserves_shape`
- `scripts/cargo_isolated.sh test --lib --quiet plpgsql`
- `scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55434 --results-dir /tmp/diffs/plpgsql-multidim-array-assign`

Remaining:
- Regression improved to `2181/2271` matched, `1073` diff lines.
- The second `nonsimple_expr_test` still differs because NOT NULL PL/pgSQL variable assignment does not raise and recover like PostgreSQL.
