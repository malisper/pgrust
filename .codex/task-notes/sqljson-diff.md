Goal:
Fix the sqljson regression failures where PostgreSQL treats SQL/JSON constructors
and predicates as special syntax, but pgrust parsed them as ordinary functions,
casts, or unsupported SELECT forms.

Key decisions:
Lower the special syntax into internal parser-only function names rather than
adding public catalog functions. Bind those internal names directly to builtin
scalar/aggregate implementations so ordinary user functions named json/json_array
do not mask the special forms. Preserve RETURNING via the existing cast wrapper
for now.

Implemented syntax for JSON(), JSON_SCALAR(), JSON_SERIALIZE(), JSON_OBJECT(),
JSON_ARRAY(), JSON_ARRAYAGG(), JSON_OBJECTAGG(), and IS JSON. FORMAT JSON now
marks JSON value expressions as JSON input and validates ENCODING clauses.
JSON_ARRAY(SELECT ...) is distinguished from an ordinary array argument in the
executor. JSON_OBJECT WITH UNIQUE and non-scalar/json object keys now error.

Files touched:
.codex/task-notes/sqljson-diff.md
src/backend/parser/gram.pest
src/backend/parser/gram.rs
src/backend/parser/tests.rs
src/backend/parser/analyze/expr.rs
src/backend/parser/analyze/functions.rs
src/backend/parser/analyze/infer.rs
src/include/nodes/primnodes.rs
src/backend/executor/exec_expr.rs
src/backend/executor/expr_json.rs
src/backend/executor/expr_string.rs
src/backend/executor/nodes.rs

Tests run:
scripts/cargo_isolated.sh test --lib --quiet parse_sql_json_special_syntax
scripts/cargo_isolated.sh check
git diff --check
scripts/run_regression.sh --test sqljson --timeout 300 --jobs 1 --port 56446 --results-dir /tmp/pgrust_sqljson_results_object_unique

Latest sqljson result:
115/221 queries matched, 1013 diff lines.

Remaining:
Major remaining gaps are deparsing/view reconstruction for internal SQL/JSON
forms, JSON_ARRAYAGG/JSON_OBJECTAGG SQL/JSON-specific null and uniqueness
semantics, precise EXPLAIN text, duplicate-key detection inside parsed JSON text
for WITH UNIQUE KEYS, exact caret locations, and fixed-length RETURNING/domain
coercion behavior.
