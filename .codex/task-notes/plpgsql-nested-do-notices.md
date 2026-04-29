Goal:
Preserve PL/pgSQL notices when a DO block dynamically executes nested DO statements.
Key decisions:
Keep top-level DO behavior clearing the notice queue, but add an internal context path that does not clear notices for nested dynamic DO execution.
Route PL/pgSQL dynamic EXECUTE of parsed DO statements through the preserving path.
Files touched:
src/pl/plpgsql/mod.rs
src/pl/plpgsql/exec.rs
src/pgrust/database_tests.rs
Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet plpgsql_nested_dynamic_do_preserves_outer_notices
scripts/cargo_isolated.sh test --lib --quiet plpgsql
scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55434 --results-dir /tmp/diffs/plpgsql-nested-do-notices
Remaining:
Regression is 2232/2271 matched with 454 diff lines. Remaining clusters include unknown-vs-varchar composite return typing, RAISE/context formatting, string escape warnings, polymorphic anyarray messages, and transition table support.
