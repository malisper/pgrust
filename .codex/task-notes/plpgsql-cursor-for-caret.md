Goal:
- Add LINE/caret positioning for unbound cursor FOR-loop validation errors.

Key decisions:
- Keep this as protocol-side error positioning to match the surrounding validation-caret fixes.
- Point at the cursor expression after `IN` in `FOR ... IN c LOOP`.

Files touched:
- src/backend/tcop/postgres.rs

Tests run:
- cargo fmt
- scripts/cargo_isolated.sh test --lib --quiet exec_error_position_points_at_unbound_plpgsql_cursor_for_loop
- scripts/cargo_isolated.sh test --lib --quiet plpgsql
- scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55434 --results-dir /tmp/diffs/plpgsql-cursor-for-caret

Remaining:
- Regression result is 2216/2271 with 715 diff lines.
