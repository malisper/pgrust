Goal:
Count failure reasons in `/tmp/diffs/rangefuncs`, investigate cursor/view/table creation failures, add `ROWS FROM` support, and fix the user-requested SRF ordinality, SQL-function `INSERT ... RETURNING`, and OUT/polymorphic failures.

Key decisions:
Used `/tmp/diffs/rangefuncs.diff`; it is identical to `/tmp/diffs/next_failures_20260428/rangefuncs.diff`.
Counted added `ERROR:` lines by reason, and separately noted hunk-level non-error output mismatches.
Follow-up investigation found cursor/view creation failures are parser-level:
pgrust supports single `srf_from_item = function(...) [WITH ORDINALITY]` but has no `ROWS FROM(...)` grammar, AST, or planner representation.
Standalone unsupported `SELECT ... ROWS FROM(...)` is converted by parser fallback into `feature not supported: SELECT form`; nested `SELECT` inside `DECLARE CURSOR` or `CREATE TEMP VIEW` surfaces as syntax error before object creation.
Non-temp `CREATE VIEW ... ROWS FROM(...)` can hit the whole-statement unsupported fallback as `CREATE VIEW form`; temp view variants do not because fallback does not match `create temp view`.
The `temporary table` error in the users rowtype section is unrelated object creation: `ALTER TABLE DROP COLUMN` explicitly rejects temporary tables, so the intended dropped column setup never happens.
Implemented `ROWS FROM(function [, ...]) [WITH ORDINALITY]` by adding raw AST/parser support, a `SetReturningCall::RowsFrom` plan node, executor zipping/null-padding, scalar one-row function item support, view deparse, dependency/rewrite/optimizer traversal, and focused parser/executor tests.
Fixed user-defined SQL SRF `WITH ORDINALITY` row-width mismatch by passing base function output columns into the SQL-function runtime and letting the common SRF wrapper append ordinality exactly once.
Added SQL-function execution support for single-statement `INSERT ... RETURNING` by dispatching parsed `INSERT` bodies through the existing insert binder/executor with `ensure_write_xid`.
Adjusted non-`SETOF` SQL functions to use the first returned row, including when invoked from `FROM`.
Packed scalar SQL-function multi-column results into `record` values for OUT-parameter, anonymous record, and named composite return shapes.
Accepted PostgreSQL's `name OUT type` function-argument spelling, validated unresolved polymorphic OUT results, rejected unknown literals for ordinary polymorphic inputs, and added `anycompatible` integer widening for mixed `integer`/`bigint[]` calls.
Tightened `CREATE OR REPLACE FUNCTION` so an existing function's return type/shape cannot change.

Files touched:
`crates/pgrust_sql_grammar/src/gram.pest`
`src/include/nodes/parsenodes.rs`
`src/include/nodes/primnodes.rs`
`src/backend/parser/gram.rs`
`src/backend/parser/analyze/scope.rs`
`src/backend/executor/srf.rs`
`src/backend/executor/sqlfunc.rs`
`src/backend/parser/analyze/functions.rs`
`src/backend/parser/analyze/expr/func.rs`
`src/pgrust/database/commands/create.rs`
plus traversal/deparse/explain/dependency callers and focused tests.

Tests run:
`scripts/cargo_isolated.sh check`
`scripts/cargo_isolated.sh test --lib --quiet rows_from`
`scripts/run_regression.sh --jobs 1 --test rangefuncs --timeout 60 --port 55434 --results-dir /tmp/diffs/rangefuncs-rowsfrom-final`
`scripts/cargo_isolated.sh test --lib --quiet insert_returning`
`scripts/cargo_isolated.sh test --lib --quiet sql_scalar_function_`
`scripts/cargo_isolated.sh test --lib --quiet parse_create_function_statement_with_mode_after_arg_name`
`scripts/cargo_isolated.sh test --lib --quiet sql_set_returning_function_with_ordinality_has_declared_width`
`scripts/cargo_isolated.sh test --lib --quiet sql_polymorphic_out_function_resolves_anyelement_outputs`
`scripts/run_regression.sh --jobs 1 --port 55440 --timeout 120 --test rangefuncs --results-dir /tmp/diffs/rangefuncs-after-out-replace`

Remaining:
`rangefuncs` still fails overall: 347/437 queries matched, 1112 diff lines. The requested buckets are cleared from the fresh diff: no unexpected protocol field-count errors, no `only single SELECT or VALUES` SQL-function errors for `INSERT ... RETURNING`, and no `dup`/polymorphic resolution failures. Remaining failures are separate: row/view deparse formatting, cursor backward positioning for `ROWS FROM`, missing `sin`, temp-table DDL restrictions around dropped columns, column-definition-list diagnostics, SQL-function row-shape coercion checks for `array_to_set`, and trigger/rule side effects for `INSERT ... RETURNING`.
