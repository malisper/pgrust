Goal:
Diagnose and clear the errors regression diffs under /tmp/diffs.

Key decisions:
Kept the fix scoped to existing SQL-visible error compatibility handling in
src/backend/tcop/postgres.rs. The mismatches were presentation-only: ALTER
TABLE RENAME missing-relation wording, inherited column-conflict relation name,
and extra cursor positions on DROP AGGREGATE/OPERATOR missing-type errors.

Files touched:
src/backend/tcop/postgres.rs

Tests run:
CARGO_TARGET_DIR=/tmp/pgrust-target-errors scripts/run_regression.sh --test errors --port 56333 --results-dir /tmp/diffs/errors

Remaining:
None for errors regression; it passes 87/87 queries.
