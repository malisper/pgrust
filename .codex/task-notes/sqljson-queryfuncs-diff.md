Goal:
Diagnose diffs in sqljson_queryfuncs regression output.

Key decisions:
The original failures were not result mismatches. pgrust lacked SQL/JSON query function support for JSON_EXISTS, JSON_VALUE, and JSON_QUERY. Plain two-argument calls parse as ordinary scalar functions, so they now lower through temporary legacy scalar builtins. Calls with SQL/JSON clauses like RETURNING, PASSING, ON ERROR, wrappers, or quotes are still not accepted by the SELECT grammar and fall through to Statement::Unsupported with feature "SELECT form".

Files touched:
src/include/nodes/primnodes.rs
src/backend/parser/analyze/functions.rs
src/include/catalog/pg_proc.rs
src/backend/executor/expr_json.rs
src/backend/executor/tests.rs

Tests run:
scripts/cargo_isolated.sh test --lib --quiet sql_json_plain_query_functions_work
scripts/cargo_isolated.sh check
scripts/run_regression.sh --test sqljson_queryfuncs --timeout 60 (blocked during shared setup bootstrap before the target test ran)

Remaining:
Full SQL/JSON clause support still requires parser AST support for PostgreSQL JsonFuncExpr/JsonExpr style nodes, analyzer/binder support for RETURNING/PASSING/ON EMPTY/ON ERROR semantics, and executor behavior handling.
