Goal:
Fix sqljson_queryfuncs regression failures.

Key decisions:
Implemented real SQL/JSON query-function expression support for JSON_EXISTS,
JSON_VALUE, and JSON_QUERY instead of relying on the old two-argument builtin
function shim. The new node carries context, path, PASSING variables, RETURNING,
wrapper/quotes, and ON EMPTY/ON ERROR behavior through parser, analyzer,
optimizer walkers, rewrite/dependency walkers, and executor evaluation.

Added early analyzer rejection for illegal SQL/JSON DEFAULT expressions so SRFs,
column refs, window/aggregate expressions, and subqueries fail before planning.
Used LLVM dev codegen for regression reruns because the repo's Cranelift dev
profile aborts locally on aarch64 crc32c.

Files touched:
crates/pgrust_sql_grammar/src/gram.pest
src/backend/parser/gram.rs
src/include/nodes/parsenodes.rs
src/include/nodes/primnodes.rs
src/backend/parser/analyze/expr.rs
src/backend/parser/analyze/infer.rs
src/backend/executor/expr_json.rs
src/backend/executor/exec_expr.rs
src/backend/executor/jsonpath.rs
src/backend/executor/expr_casts.rs
src/backend/executor/expr_ops.rs
plus expression visitor/rewrite/dependency exhaustiveness updates across
parser/analyze, optimizer, rewrite, executor startup, catalog dependency, and
database command modules.

Tests run:
cargo fmt
scripts/cargo_isolated.sh check
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/run_regression.sh --test sqljson_queryfuncs --timeout 120 --port 5580 --results-dir /tmp/pgrust_regress_sqljson_queryfuncs_final

Remaining:
sqljson_queryfuncs now completes without server panic but still fails:
208/314 queries matched, 649 diff lines. Remaining mismatches are mostly
pre-existing or deeper compatibility gaps: psql caret formatting differences,
date input/date arithmetic/display gaps, SQL/JSON datetime JSON rendering,
JSON_QUERY typed coercion for arrays/composites/ranges, domain drop/dependency
cleanup, expression-index immutability validation and CREATE INDEX parsing with
PASSING, information_schema.check_constraints, and pg_get_expr/\d canonical
rendering differences.
