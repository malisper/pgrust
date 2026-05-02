Goal:
Fix PostgreSQL-compatible handling for `CAST(... AS text COLLATE "C")`.

Key decisions:
Detect `COLLATE` in the explicit cast type slot during parser prechecks and
return the PostgreSQL syntax error at that token. Keep expression-level
`CAST(... AS text) COLLATE "C"` valid.

Files touched:
src/backend/parser/gram.rs
src/backend/parser/tests.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet parse_select_rejects_collate_inside_cast_type
CARGO_TARGET_DIR=/tmp/pgrust-target-lahore-collate scripts/run_regression.sh --port 62001 --test collate

Remaining:
The full collate regression still fails on broader pre-existing collation
semantics, but the explicit-cast hunk is fixed and no longer appears in the new
collate diff.
