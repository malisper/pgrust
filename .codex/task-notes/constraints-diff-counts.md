Goal:
Count failure reasons in the constraints regression diff artifact and fix the missing row detail for check constraint violations.

Key decisions:
Used `/tmp/diffs/constraints.diff` because `/tmp/diffs/constraints` does not exist and this file matches `/tmp/diffs/next_failures_20260428/constraints.diff`.
Counted visible mismatching output occurrences, mostly by changed statement/error rather than raw diff lines.
Added `detail` to `ExecError::CheckViolation` and populate it with `format_failing_row_detail` wherever row values are available.
Bound CHECK expressions with a base-relation scope so `tableoid` can be used, while rejecting other system columns like `ctid` with PostgreSQL-compatible check-constraint diagnostics.

Files touched:
`src/backend/commands/tablecmds.rs`
`src/backend/executor/mod.rs`
`src/backend/executor/constraints.rs`
`src/backend/libpq/pqformat.rs`
`src/backend/parser/analyze/constraints.rs`
`src/backend/tcop/postgres.rs`
`src/pgrust/database/commands/constraint.rs`
`src/pgrust/database_tests.rs`
`.codex/task-notes/constraints-diff-counts.md`

Tests run:
`cargo fmt`
`scripts/cargo_isolated.sh test --lib --quiet check_constraints_allow_tableoid_and_reject_other_system_columns`
`scripts/cargo_isolated.sh test --lib --quiet create_table_check_and_named_not_null_constraints_are_enforced_and_persisted`
`scripts/run_regression.sh --test constraints --results-dir /tmp/diffs/constraints-detail-fix --timeout 120 --jobs 1`
`scripts/run_regression.sh --test constraints --results-dir /tmp/diffs/constraints-system-fix-2 --timeout 120 --jobs 1`
`scripts/cargo_isolated.sh check`

Remaining:
The constraints regression still fails for unrelated buckets. Missing `DETAIL: Failing row contains` lines in the new diff dropped from 25 to 0; the `SYS_COL_CHECK_TBL` tableoid/ctid block no longer appears in the diff.
