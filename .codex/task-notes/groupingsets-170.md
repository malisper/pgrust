Goal:
Diagnose remaining regression failures in /tmp/pgrust-diffs-2026-04-30T0340Z/groupingsets.diff and cover the unsupported SELECT fallback bucket.

Key decisions:
- Treat early gstest1 failures as cascading from the initial CREATE TEMP VIEW ... AS VALUES parse error.
- Group repeated "feature not supported: SELECT form" entries as parser fallback failures, not executor failures.
- Separate EXPLAIN-only syntax errors from direct SELECT unsupported fallbacks.
- Current parser already accepts the representative grouping-set SELECT shapes; added regression coverage so these do not fall back to Statement::Unsupported again.
- Fresh groupingsets run has no +ERROR lines and no "feature not supported: SELECT form" entries. Remaining failures are plan/result diffs.

Files touched:
- .codex/task-notes/groupingsets-170.md
- crates/pgrust_sql_grammar/src/lib.rs
- src/backend/parser/tests.rs

Tests run:
- CARGO_TARGET_DIR=/tmp/pgrust-grammar-target-davis-v4 cargo test --manifest-path crates/pgrust_sql_grammar/Cargo.toml --lib --quiet parses_grouping_sets_query_shapes
- CARGO_TARGET_DIR=/tmp/pgrust-target-davis-gsets cargo test --lib --quiet parse_grouping_sets_query_shapes_do_not_fallback
- CARGO_TARGET_DIR=/tmp/pgrust-target-davis-gsets scripts/run_regression.sh --test groupingsets --results-dir /tmp/pgrust-davis-gsets-regression --timeout 120 --jobs 1 --port 55470

Remaining:
- groupingsets still fails overall: 172/219 queries matched, 984 diff lines.
- Remaining diff has no SQL errors; focus next on plan/result mismatches, especially GROUPING() with lateral/subquery grouping keys and EXPLAIN plan shape/index path differences.
