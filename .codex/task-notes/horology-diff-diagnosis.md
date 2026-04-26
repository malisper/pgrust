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

Tests run:
cargo fmt
Focused parser/analyzer/executor tests for RESET/SHOW TIME ZONE, OVERLAPS, date/time arithmetic, timestamptz constructors, mixed datetime comparisons, interval signed fields, DateStyle hints, POSIX timezone/DST arithmetic, to_timestamp(text,text), time/interval casts, insert-select timestamp->timestamptz coercion, and timetz default timezone offsets.
scripts/cargo_isolated.sh check
scripts/run_regression.sh --test horology --timeout 120 --results-dir /tmp/diffs/horology-tokyo-v1
scripts/run_regression.sh --test horology --timeout 120 --results-dir /tmp/diffs/horology-tokyo-v1-2
scripts/run_regression.sh --test horology --timeout 120 --results-dir /tmp/diffs/horology-tokyo-v1-3
scripts/run_regression.sh --test horology --timeout 120 --results-dir /tmp/diffs/horology-tokyo-v1-5

Remaining:
Latest useful horology run is /tmp/diffs/horology-tokyo-v1-5: 295/399 queries matched, 1223 diff lines. Remaining clusters are mostly to_timestamp/to_date template coverage and specific error messages, date-vs-timestamp/timestamptz comparisons for out-of-range dates, BC timestamp display under ISO/SQL DateStyle, EXPLAIN rendering for BETWEEN/NOT BETWEEN rewrites, and missing caret location lines for a couple unsupported cast errors.
