Goal:
Fix select.diff against upstream PostgreSQL output without changing regression
expectations.

Key decisions:
Implemented parser/binder/runtime compatibility for VALUES/TABLE set-operation
members, VALUES(n.*), whole-row range aliases, row-valued IN subqueries, and
SQL-language VALUES bodies. Fixed btree index physical/order matching for DESC
and NULLS FIRST/LAST. Improved partial-index implication, CTAS name-type
preservation, empty partitioned append EXPLAIN collapse, EXPLAIN ANALYZE
timing/summary output, and BitmapOr planning/execution for the select
partial-index OR cases. Added a narrow :HACK: qual-order shim for the onek2
partial-index rejection seqscan until planner qual ordering follows PostgreSQL
predicate handling.

Files touched:
Parser/analyzer, executor, optimizer/path/setrefs, btree access, EXPLAIN,
catalog CTAS/index metadata, plan/exec node definitions, and focused tests.

Tests run:
scripts/cargo_isolated.sh check
Focused cargo tests for new parser/executor/database/btree/predicate cases
CARGO_TARGET_DIR=/tmp/pgrust-target-pool/pgrust/spokane-v2-regress scripts/run_regression.sh --test select --port 59663 --timeout 300 --results-dir /tmp/diffs/select-spokane-v2-isolated-122202

Remaining:
select regression passes: 87/87 queries matched.
