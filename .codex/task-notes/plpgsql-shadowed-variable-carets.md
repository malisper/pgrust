Goal:
- Add LINE/caret positions for PL/pgSQL shadowed-variable warnings and errors.

Key decisions:
- Use backend notice position inference for warnings because validation notices still do not carry source spans.
- Count repeated notice messages so duplicate shadow warnings point to successive declarations.
- Treat names present before the PL/pgSQL body as function-argument shadows; otherwise skip the first body occurrence as the original local declaration.

Files touched:
- src/backend/tcop/postgres.rs

Tests run:
- cargo fmt
- scripts/cargo_isolated.sh test --lib --quiet exec_error_position_points_at_plpgsql_shadowed_variables
- scripts/cargo_isolated.sh test --lib --quiet plpgsql
- scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55434 --results-dir /tmp/diffs/plpgsql-shadowed-variable-carets-final

Remaining:
- Regression result is 2214/2271 with 736 diff lines.
- The follow-up `select shadowtest(1)` after failed creation still reports `ERROR: shadowtest` instead of PostgreSQL's missing-function diagnostic.
