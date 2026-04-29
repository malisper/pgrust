Goal:
Make progress on /tmp/diffs/partition_prune.diff without expected-file edits.

Key decisions:
- Kept PostgreSQL regression output as authoritative.
- Added prepared UPDATE support, but view UPDATE still needs automatically-updatable view handling.
- Added array hash, enum, and record/composite partition-key support exposed by the regression.
- Backed out a PL/pgSQL dynamic EXPLAIN EXECUTE bridge because it made the regression hit the 60s file timeout.

Files touched:
- Partition/catalog/type work: src/include/catalog/pg_opclass.rs, pg_opfamily.rs, pg_amop.rs, pg_amproc.rs, src/backend/parser/analyze/partition.rs, src/backend/executor/expr_casts.rs, src/backend/parser/analyze/expr.rs, src/backend/parser/analyze/expr/ops.rs.
- Prepared statement work: crates/pgrust_sql_grammar/src/gram.pest, src/include/nodes/parsenodes.rs, src/backend/parser/gram.rs, src/backend/parser/tests.rs, src/pgrust/session.rs.
- Earlier slices also touched planner/pruning/explain/PLpgSQL files.

Tests run:
- cargo fmt
- scripts/cargo_isolated.sh test --lib --quiet partition_prune
- scripts/cargo_isolated.sh test --lib --quiet parse_prepare_and_execute_statements
- scripts/cargo_isolated.sh test --lib --quiet sql_prepare_execute_substitutes_parameters
- scripts/cargo_isolated.sh test --lib --quiet sql_prepare_execute_supports_update_returning
- scripts/run_regression.sh --test partition_prune --jobs 1 --port 55941 --results-dir /tmp/diffs/partition_prune.after-slice41

Remaining:
- Latest regression: 638/750 queries matched, 2573 diff lines, no timeout.
- Remaining notable failures: PL/pgSQL dynamic EXPLAIN EXECUTE prepared SELECTs, EXPLAIN ANALYZE UPDATE, scalar subquery column-shape bug, automatically updatable view UPDATE, later MERGE/JOIN grammar/forms, plus many plan-shape/rendering mismatches.
