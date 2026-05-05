Goal:
Diagnose /tmp/diffs insert_conflict regression output.

Key decisions:
The diffs are EXPLAIN compatibility issues, not execution-row mismatches. The
fixed gaps were missing DO NOTHING arbiter index rendering and missing conflict
predicate subplan rendering for VALUES-source INSERT. Remaining gaps are extra
parentheses in conflict filters and structured JSON key ordering/formatting
differences.

Files touched:
crates/pgrust_commands/src/explain.rs
src/backend/commands/tablecmds.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh check
scripts/run_regression_plan_diff_queries.sh --test insert_conflict --timeout 180 --copy-diffs /tmp/diffs-insert-conflict-check
scripts/run_regression.sh --test insert_conflict --timeout 180 --jobs 1 --port 55445 --results-dir /tmp/pgrust-insert-conflict-check3
scripts/cargo_isolated.sh test --lib --quiet explain_insert_on_conflict_do_nothing_shows_arbiter_indexes
scripts/run_regression.sh --test insert_conflict --timeout 180 --jobs 1 --port 55445 --results-dir /tmp/pgrust-insert-conflict-fix

Remaining:
Focused insert_conflict regression now fails 262/266, with the original missing
DO NOTHING arbiter index hunks gone from /tmp/diffs/insert_conflict.diff.
