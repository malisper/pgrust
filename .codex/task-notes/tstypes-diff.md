Goal:
Make the upstream PostgreSQL tstypes regression pass for pgrust.

Key decisions:
Completed the SQL-visible tsvector/tsquery surface used by tstypes without adding pgrust-specific SQL. Kept runtime behavior under backend/executor tsearch modules, with node files limited to data structures plus text parse/render helpers already used by the codebase.

Added PostgreSQL-compatible parsing/rendering for the exercised tsvector and tsquery syntax, tsearch builtin catalog rows, coercions for unknown string literals, tsquery phrase/operator support, tsvector editing functions, rank functions, and unnest(tsvector). Final fixes matched PostgreSQL tsquery ordering by item count/storage size/structural order, allowed !! on unknown string literals coerced to tsquery, parenthesized right-nested phrase output, and treated omitted position weight as D during weighted matches.

Files touched:
src/include/nodes/tsearch.rs
src/include/nodes/primnodes.rs
src/include/catalog/pg_proc.rs
src/include/catalog/pg_type.rs
src/backend/parser/analyze/functions.rs
src/backend/parser/analyze/infer.rs
src/backend/parser/analyze/expr/func.rs
src/backend/parser/analyze/expr/ops.rs
src/backend/parser/analyze/expr/targets.rs
src/backend/parser/analyze/geometry.rs
src/backend/parser/analyze/scope.rs
src/backend/executor/exec_expr.rs
src/backend/executor/expr_ops.rs
src/backend/executor/mod.rs
src/backend/executor/srf.rs
src/backend/executor/tsearch/mod.rs
src/backend/executor/tsearch/ts_execute.rs
src/backend/executor/tsearch/tsquery_io.rs
src/backend/executor/tsearch/tsquery_op.rs
src/backend/executor/tsearch/tsvector_io.rs
src/backend/executor/tsearch/tsvector_op.rs
src/backend/tsearch/to_tsany.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet tsearch
scripts/cargo_isolated.sh check
CARGO_TARGET_DIR=/tmp/pgrust-target-porto-tstypes scripts/run_regression.sh --test tstypes --timeout 300 --port 55446

Remaining:
tstypes passes 238/238 queries. cargo check still prints the pre-existing unreachable Statement::ReindexIndex warning in src/bin/query_repl.rs.
