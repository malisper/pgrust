Goal:
Fix case regression failures around CASE/NULLIF with arrdomain PL/pgSQL values.

Key decisions:
Preserve domain OIDs on dynamic domain SqlType rows, including domains over arrays.
Allow visible_type_oid_for_sql_type to return an explicit dynamic type OID before treating array-shaped domain types as array types.
Use resolved target type for array literal casts to named types/domains instead of raw_type_name_hint, which guessed Composite for user-defined names.
Lower NULLIF through the normal comparison binder while keeping the result type as the first argument type; coerce NULL right operand to the left type for comparison.

Files touched:
src/pgrust/database.rs
src/backend/utils/cache/lsyscache.rs
src/backend/parser/analyze/expr.rs
src/pgrust/database_tests.rs

Tests run:
scripts/cargo_isolated.sh test --lib --quiet case_and_nullif_preserve_array_domain_function_results (pass)
scripts/cargo_isolated.sh check (pass, existing query_repl unreachable-pattern warning)
scripts/run_regression.sh --schedule .context/case-only.schedule --test case --port 5668 --timeout 120 --results-dir /tmp/pgrust-case-regress-final4 --ignore-deps (pass, 67/67 queries)

Remaining:
None for the case regression.
