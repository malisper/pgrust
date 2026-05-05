Goal:
Fix ALTER FUNCTION/AGGREGATE unqualified lookup so duplicate signatures in later schemas do not cause false ambiguity.

Key decisions:
Unqualified routine resolution now walks effective search_path and uses the first schema with a matching signature. Explicitly qualified names still resolve directly to that schema.

Files touched:
src/pgrust/database/commands/routine.rs
src/pgrust/database_tests.rs
scripts/run_regression.sh

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet uses_search_path_for_unqualified_resolution
scripts/run_regression.sh --test alter_generic

Remaining:
None for alter_generic. Latest focused run passes 332/332 queries.
