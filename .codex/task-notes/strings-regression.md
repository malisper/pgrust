Goal:
Diagnose and fix the `strings` regression mismatch for `CAST(f1 AS char(n))` from `TEXT_TBL`.

Key decisions:
PostgreSQL marks `text -> bpchar` as binary coercible, but pgrust must not strip casts when the target has a character typmod because the executor cast enforces truncation and padding.

Files touched:
src/backend/parser/analyze/coerce.rs

Tests run:
cargo test --lib --quiet binary_coercible_preserves_character_typmod_coercion
scripts/run_regression.sh --test strings --timeout 60

Remaining:
None.
