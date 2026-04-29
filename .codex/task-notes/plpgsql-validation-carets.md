Goal:
- Add LINE/caret positions for PL/pgSQL create-time validation errors that already had matching ERROR text.

Key decisions:
- Keep this as protocol-side position mapping because these validation errors currently carry message text but not parser source spans.
- Cover RETURN validation, declared cursor duplicate argument errors, and declared cursor missing-argument errors.

Files touched:
- src/backend/tcop/postgres.rs

Tests run:
- cargo fmt
- scripts/cargo_isolated.sh test --lib --quiet exec_error_position_points_at_plpgsql
- scripts/cargo_isolated.sh test --lib --quiet plpgsql
- scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55434 --results-dir /tmp/diffs/plpgsql-validation-carets

Remaining:
- Regression result is 2203/2271 with 829 diff lines.
- Shadowed-variable warnings/errors still lack LINE/caret positions because validation notices do not yet carry source spans.
