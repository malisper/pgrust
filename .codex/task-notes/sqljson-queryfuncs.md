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
scripts/cargo_isolated.sh test --lib --quiet jsonpath_exists_propagates_non_silent_errors
scripts/cargo_isolated.sh test --lib --quiet jsonpath
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/run_regression.sh --test sqljson_queryfuncs --timeout 120 --port 5580 --results-dir /tmp/pgrust_regress_sqljson_queryfuncs_final
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/run_regression.sh --test sqljson_queryfuncs --timeout 120 --port 5591 --results-dir /tmp/pgrust_regress_sqljson_queryfuncs_query_default_fix

Remaining:
2026-04-28 PR 291 update: merged current origin/perf-optimization into the
PR branch, resolved JSONPath/SQL-JSON conflicts, preserved base fatal jsonpath
variable errors, covered new SQL/JSON nodes in generated/publication walkers,
and kept JSONPath datetime mutability exhaustive for newer filter/string method
variants. Focused JSONPath tests and cargo check pass on the merged tree.

sqljson_queryfuncs now completes without server panic but still fails:
259/314 queries matched, 400 diff lines. Remaining mismatches are mostly
deeper or broader compatibility gaps: SQL-visible LINE/caret formatting,
JSON_QUERY composite display and composite-array unnest column expansion,
jsonpath input error wording, DROP DOMAIN dependency cleanup, DROP FUNCTION
without explicit args, information_schema.check_constraints, check-violation
failing-row details, and pg_get_expr/\d canonical rendering differences.

2026-04-28 diagnostics slice: fixed the two LINE/caret buckets in
sqljson_queryfuncs by suppressing inferred positions for runtime SQL/JSON
query-function input/coercion errors and adding formatter-side positions for
unpositioned SQL/JSON validation errors. The remaining regression diff is 128
lines / 14 mismatched queries and no longer contains the requested extra or
missing SQL/JSON diagnostic-position failures.

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet exec_error_position
scripts/run_regression.sh --test sqljson_queryfuncs --results-dir /tmp/pgrust_sqljson_queryfuncs_fix
scripts/cargo_isolated.sh check

2026-04-28 remaining-failures implementation:

Goal:
Finish the remaining sqljson_queryfuncs mismatches after the diagnostic-position
slice.

Key decisions:
Expanded composite-array unnest from named/anonymous composite element metadata
in FROM and select-list field expansion paths, and made single-arg composite
unnest execution return multi-column tuple slots. Added failing-row DETAIL text
for check violations in executor and validation paths. Added a synthetic
information_schema.check_constraints view for check constraints.

Implemented canonical SQL/JSON query-function deparse for stored defaults,
check constraints, pg_get_expr, pg_get_constraintdef, and information_schema
check_clause output, including jsonpath canonicalization, RETURNING,
PASSING aliases, wrapper/quotes, and ON EMPTY/ON ERROR clauses. Preserved
jsonpath parser errors for RETURNING jsonpath coercion, improved trailing-token
jsonpath errors, and carried named composite row identities through whole-row
type inference so bad SQL/JSON path-type errors name the base relation type.

Files touched:
src/backend/executor/constraints.rs
src/backend/executor/exec_expr.rs
src/backend/executor/jsonpath.rs
src/backend/executor/mod.rs
src/backend/executor/srf.rs
src/backend/executor/tests.rs
src/backend/libpq/pqformat.rs
src/backend/parser/analyze/create_table.rs
src/backend/parser/analyze/expr.rs
src/backend/parser/analyze/expr/targets.rs
src/backend/parser/analyze/infer.rs
src/backend/parser/analyze/scope.rs
src/backend/parser/analyze/system_views.rs
src/backend/rewrite/mod.rs
src/backend/rewrite/views.rs
src/backend/tcop/postgres.rs
src/backend/utils/cache/system_view_registry.rs
src/pgrust/database/commands/constraint.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet unnest_composite_array_expands_record_fields
scripts/cargo_isolated.sh test --lib --quiet check_constraint_violation_includes_failing_row_detail
scripts/cargo_isolated.sh test --lib --quiet sql_json_check_constraint_deparse_matches_pg_style
scripts/cargo_isolated.sh check
scripts/run_regression.sh --test sqljson_queryfuncs --port 55473 --results-dir /tmp/pgrust_sqljson_queryfuncs_final_after_tests

Remaining:
None for sqljson_queryfuncs; the final focused regression run passed all 314
queries.

2026-04-28 CI jsonpath numeric-literal fix:

Goal:
Fix failing cargo-test-run job on
backend::executor::tests::jsonpath_numeric_literals_error after the trailing
jsonpath token diagnostic change.

Key decisions:
Kept near-token diagnostics for general trailing jsonpath tokens, but restored
PostgreSQL-compatible "syntax error at end of jsonpath input" for malformed
numeric literal tails that survive number scanning without whitespace.

Files touched:
src/backend/executor/jsonpath.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet jsonpath_numeric_literals_error
scripts/cargo_isolated.sh test --lib --quiet jsonpath_numeric_pg_input_error_info
scripts/cargo_isolated.sh test --lib --quiet jsonpath_numeric
scripts/cargo_isolated.sh check
scripts/run_regression.sh --test sqljson_queryfuncs --ignore-deps --port 55503 --results-dir /tmp/pgrust_sqljson_queryfuncs_jsonpath_ci_fix

Remaining:
None locally; let GitHub Actions rerun after the amended PR commit is pushed.
