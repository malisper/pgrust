Goal:
Fix missing cursor position output in regproc regression errors.
Key decisions:
Recover error positions for reg* lookup failures in the existing tcop error response layer, alongside existing reg* syntax-error handling.
Files touched:
src/backend/tcop/postgres.rs
Tests run:
cargo fmt
cargo fmt --check
git diff --check
Attempted cargo test --lib --quiet tcop::postgres::tests::exec_error_position_points_at_reg_object_lookup_argument, but stopped it after it waited behind concurrent shared-target cargo jobs for over seven minutes.
Remaining:
Run the focused unit test or scripts/run_regression.sh --test regproc when the shared cargo target is not saturated.
