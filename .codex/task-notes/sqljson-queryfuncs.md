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

Follow-up slice added CREATE INDEX parsing for SQL/JSON query functions with
PASSING, recursive immutable-expression validation for expression indexes,
PostgreSQL-like jsonpath datetime mutability checks, SQL/JSON datetime PASSING
rendering, ISO date+time input truncation for date literals, JSON_QUERY
structured coercion to arrays/composites, and SQL/JSON DEFAULT/result coercion
rules for bit/varbit and domains.

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
src/backend/executor/jsonb.rs
src/backend/utils/time/date.rs
src/pgrust/database/commands/index.rs
plus expression visitor/rewrite/dependency exhaustiveness updates across
parser/analyze, optimizer, rewrite, executor startup, catalog dependency, and
database command modules.

Tests run:
cargo fmt
scripts/cargo_isolated.sh check
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/run_regression.sh --test sqljson_queryfuncs --timeout 120 --port 5580 --results-dir /tmp/pgrust_regress_sqljson_queryfuncs_final
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/run_regression.sh --test sqljson_queryfuncs --timeout 120 --port 5591 --results-dir /tmp/pgrust_regress_sqljson_queryfuncs_query_default_fix

Remaining:
sqljson_queryfuncs now completes without server panic but still fails:
259/314 queries matched, 400 diff lines. Remaining mismatches are mostly
deeper or broader compatibility gaps: SQL-visible LINE/caret formatting,
JSON_QUERY composite display and composite-array unnest column expansion,
jsonpath input error wording, DROP DOMAIN dependency cleanup, DROP FUNCTION
without explicit args, information_schema.check_constraints, check-violation
failing-row details, and pg_get_expr/\d canonical rendering differences.
