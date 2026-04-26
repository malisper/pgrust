Goal:
Fix unicode regression failures and expected-output selection.

Key decisions:
pgrust always reports UTF8, so unicode regression failures should compare
against unicode.out rather than the short non-UTF8 unicode_1.out alternate.
Implemented PostgreSQL-compatible lowering for normalize(..., form) and
expr IS [NOT] [form] NORMALIZED into builtin function calls, with runtime
support for unicode_version, unicode_assigned, normalize, and is_normalized.

Files touched:
Cargo.toml
Cargo.lock
scripts/run_regression.sh
src/backend/parser/gram.pest
src/backend/parser/gram.rs
src/backend/parser/analyze/functions.rs
src/include/catalog/pg_proc.rs
src/include/nodes/primnodes.rs
src/backend/executor/exec_expr.rs
src/backend/executor/expr_string.rs
src/backend/parser/tests.rs
src/backend/executor/tests.rs
.codex/task-notes/unicode-regression.md

Tests run:
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR="$PWD/.context/cargo-target" cargo test --lib --quiet unicode_normalization
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR="$PWD/.context/cargo-target" scripts/run_regression.sh --test unicode --results-dir "$PWD/.context/regress-results/unicode" --timeout 120

Remaining:
None for the reported unicode regression.
