Goal:
Implement SQL/JSON JSON_TABLE support against the sqljson_jsontable regression.
Key decisions:
- Diff is dominated by missing SQL/JSON JSON_TABLE table expression support, not a small semantic regression.
- pgrust has legacy JSON SRFs named JsonTableFunction (json_each/jsonb_array_elements/etc.), which is unrelated to SQL/JSON JSON_TABLE.
- CREATE TEMP TABLE ... AS (VALUES ...) in the test setup is also unsupported, causing additional cascade failures.
- Added separate SQL/JSON JSON_TABLE raw/planned nodes and kept legacy JSON SRFs separate.
- Implemented parser/analyzer/executor/view/explain paths directly rather than rewriting JSON_TABLE to SQL.
- Added targeted compatibility shims for psql \sv JSON_TABLE view deparse and simple CTAS VALUES used by the regression.
- Latest regression state: `/tmp/pgrust-json-table-results21`, 85/117 queries matched, 387 diff lines.
Files touched:
- .codex/task-notes/sqljson-jsontable.md
- src/include/nodes/parsenodes.rs
- src/include/nodes/primnodes.rs
- src/backend/parser/gram.pest
- src/backend/parser/gram.rs
- src/backend/parser/analyze/scope.rs
- src/backend/executor/expr_json.rs
- src/backend/rewrite/views.rs
- src/backend/commands/explain.rs
- plus planner/executor/rewrite/tcop plumbing for function scans, relation refs, and psql describe handling.
Tests run:
- cargo fmt
- scripts/cargo_isolated.sh check
- scripts/cargo_isolated.sh test --lib psql_get_create_view_query_handles_sql_json_table_keywords -- --nocapture
- scripts/run_regression.sh --jobs 1 --timeout 240 --test sqljson_jsontable --results-dir /tmp/pgrust-json-table-results21
Remaining:
- Full regression is not green yet.
- Remaining large gaps: nested JSON_TABLE column ordering/output shape, jsonpath filters with arithmetic/PASSING variables, sibling nested row order, and JSON_TABLE-specific error propagation for path/scalar failures.
- Remaining smaller gaps: PostgreSQL caret line rendering for several parser errors, exact WRAPPER/QUOTES error wording, dynamic path expression error, and DROP DOMAIN cleanup after domain-dependent JSON_TABLE cases.
