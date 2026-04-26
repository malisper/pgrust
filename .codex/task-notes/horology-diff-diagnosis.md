Goal:
Close real remaining horology diffs against PostgreSQL behavior, using dependency-inclusive regression setup.

Key decisions:
Implemented compatibility in parser/analyzer/executor rather than masking bare single-file setup noise. The regression now runs with date/time/timetz/timestamp/timestamptz/interval fixture setup. Added targeted SQL features and runtime semantics for horology instead of changing expected output.

Files touched:
.codex/task-notes/horology-diff-diagnosis.md
src/backend/parser/gram.pest
src/backend/parser/gram.rs
src/backend/parser/tests.rs
src/backend/parser/analyze/*
src/backend/executor/exec_expr.rs
src/backend/executor/expr_casts.rs
src/backend/executor/expr_date.rs
src/backend/executor/expr_ops.rs
src/backend/executor/tests.rs
src/backend/utils/misc/guc_datetime.rs
src/backend/utils/time/date.rs
src/backend/utils/time/datetime.rs
src/backend/utils/time/timestamp.rs
src/include/catalog/pg_proc.rs
src/include/nodes/parsenodes.rs
src/include/nodes/primnodes.rs
expression traversal call sites for SqlExpr::Overlaps
Follow-up slice also touched:
src/backend/parser/analyze/expr/ops.rs
src/backend/executor/nodes.rs

Tests run:
cargo fmt
Focused parser/analyzer/executor tests for RESET/SHOW TIME ZONE, OVERLAPS, date/time arithmetic, timestamptz constructors, mixed datetime comparisons, interval signed fields, DateStyle hints, POSIX timezone/DST arithmetic, to_timestamp(text,text), time/interval casts, insert-select timestamp->timestamptz coercion, and timetz default timezone offsets.
scripts/cargo_isolated.sh check
scripts/run_regression.sh --test horology --timeout 120 --results-dir /tmp/diffs/horology-tokyo-v1
scripts/run_regression.sh --test horology --timeout 120 --results-dir /tmp/diffs/horology-tokyo-v1-2
scripts/run_regression.sh --test horology --timeout 120 --results-dir /tmp/diffs/horology-tokyo-v1-3
scripts/run_regression.sh --test horology --timeout 120 --results-dir /tmp/diffs/horology-tokyo-v1-5
Committed baseline as 525d095d2 (fix: align horology datetime behavior).
Follow-up focused tests:
scripts/cargo_isolated.sh test --lib --quiet analyze_mixed_date_timestamp_comparisons_keep_cross_type_ops
scripts/cargo_isolated.sh test --lib --quiet parse_not_between_lowers_like_postgres
scripts/cargo_isolated.sh test --lib --quiet mixed_date_timestamp_comparisons_execute_with_common_types
scripts/cargo_isolated.sh test --lib --quiet mixed_date_timestamp_comparisons_do_not_cast_out_of_range_dates
scripts/cargo_isolated.sh test --lib --quiet to_timestamp_text_format_supports_horology_templates
scripts/cargo_isolated.sh test --lib --quiet to_timestamp_fractional_template_edges
scripts/cargo_isolated.sh test --lib --quiet to_date_uses_postgres_template_parser_cases
scripts/cargo_isolated.sh test --lib --quiet formats_bc_timestamp_in_iso_and_sql_styles
scripts/cargo_isolated.sh check
Bare horology follow-up:
CARGO_TARGET_DIR=/tmp/pgrust-target-tokyo-horology scripts/run_regression.sh --test horology --jobs 1 --port 55446 --skip-build --timeout 300 --results-dir /tmp/diffs/horology-tokyo-v1-7

Remaining:
Latest bare horology run is /tmp/diffs/horology-tokyo-v1-7: 303/399 queries matched, 2495 diff lines. Diff-line count is inflated by missing dependency tables in bare mode, but the match count improved. The dependency-inclusive retry at /tmp/diffs/horology-tokyo-v1-6 timed out during timestamptz fixture setup after 120s, not on a Rust error; rerun with more disk headroom and longer timeout for final validation. Remaining real clusters include PostgreSQL-specific to_timestamp/to_date error wording and field-conflict rules, some template edge cases, and missing caret-location lines for unsupported casts.
