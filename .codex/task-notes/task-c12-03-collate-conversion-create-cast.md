Goal:
Fix TASK-C12-03 for the owned regression files: collate, conversion, create_cast.

Key decisions:
- Removed function-resolution fallback that treated text input functions as implicit text casts; CREATE CAST now matches PostgreSQL for explicit/assignment/implicit cast visibility.
- Wired pg_rust_test_enc_conversion through aggregate/support expression execution so conversion tests no longer fail on an unsupported builtin in PL/pgSQL record expansion.
- Tightened conversion invalid-source handling to report byte details and avoid no-error partial-conversion panics.
- Added scalar-expression parsing and binding for COLLATION FOR, including column/subquery collation lookup where pgrust already carries expression collation hints.
- Rejected explicit index COLLATE clauses on non-collatable index key types.

Files touched:
- crates/pgrust_sql_grammar/src/gram.pest
- src/backend/executor/expr_agg_support.rs
- src/backend/executor/expr_string.rs
- src/backend/parser/analyze/expr.rs
- src/backend/parser/analyze/functions.rs
- src/backend/parser/gram.rs
- src/include/nodes/primnodes.rs
- src/pgrust/database/commands/index.rs

Tests run:
- cargo fmt
- scripts/cargo_isolated.sh check
- CARGO_TARGET_DIR=/tmp/pgrust-target-c12-03 scripts/run_regression.sh --test collate --port 57651 --results-dir /tmp/pgrust-task-c12-03-collate
  - FAIL: 105/144 queries matched, 405 diff lines.
- CARGO_TARGET_DIR=/tmp/pgrust-target-c12-03 scripts/run_regression.sh --test conversion --port 57661 --results-dir /tmp/pgrust-task-c12-03-conversion
  - FAIL: 116/147 queries matched, 534 diff lines.
- CARGO_TARGET_DIR=/tmp/pgrust-target-c12-03 scripts/run_regression.sh --test create_cast --port 57671 --results-dir /tmp/pgrust-task-c12-03-create-cast
  - PASS: 24/24 queries matched.

Remaining:
- collate still needs foundational collation propagation through query outputs, set operations, aggregate ORDER BY, subqueries, comparison/hash consumers, EXPLAIN/view deparse, and CREATE COLLATION option/dependency behavior.
- conversion still needs PostgreSQL-specific conversion tables/streaming behavior for EUC_JIS_2004, SHIFT_JIS_2004, MULE_INTERNAL, Big5/GB18030 edge cases, and exact conversion-error messages. The current encoding_rs-backed implementation cannot fully match those PostgreSQL conversion routines.
- The requested source note .codex/task-notes/regression-failure-landscape-v2.md was not present in this workspace.
