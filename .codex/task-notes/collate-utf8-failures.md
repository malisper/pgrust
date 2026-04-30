Goal:
Fix remaining regression failure 59, collate.utf8.

Key decisions:
The downloaded diff compares actual output to collate.utf8_1.out, the non-UTF8 skip variant. Actual runs past the \quit guard because pgrust reports UTF8, so the useful comparison is also against expected/collate.utf8.out.
Added PostgreSQL-compatible builtin pg_collation rows/metadata, CREATE COLLATION option syntax for builtin provider locales, casefold(text), and runtime support for C, PG_C_UTF8, and PG_UNICODE_FAST text/regex behavior.
Preserved implicit column collation through Var and FuncExpr metadata, generated relation output expressions, and setrefs slot Vars.
The final regression-only mismatch was the SELECT header for top-level ~*/!~* expressions rewritten through regexp_like; select-list naming now keeps ?column? for those operator spellings.
CI exposed setrefs panics where intermediate path tlists dropped Var collation metadata. Matched PostgreSQL setrefs behavior more closely: Var lookup uses varno/varattno identity rather than collation, and replacement slot Vars preserve collation from the expression being lowered when available.
After origin/perf-optimization advanced to f609345, the GitHub synthetic merge found two new Var initializers without collation_oid. Merged the new base and set JSONB test Vars to no collation plus subquery pathkey display Vars to the pathkey collation.

Files touched:
Parser/catalog/planner/executor collation paths, plus focused parser/catalog/database/executor tests.

Tests run:
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet parse_create_collation
scripts/cargo_isolated.sh test --lib --quiet text_case_functions_respect_builtin_utf8_collations
scripts/cargo_isolated.sh test --lib --quiet regex_classes_respect_builtin_utf8_collation_semantics
scripts/cargo_isolated.sh test --lib --quiet catalog_store_persists_pg_collation_rows
scripts/cargo_isolated.sh test --lib --quiet create_builtin_provider_collations_from_options
scripts/cargo_isolated.sh test --lib --quiet builtin_utf8_collations_drive_text_and_regex_functions
scripts/run_regression.sh --test collate.utf8 --jobs 1 --timeout 120 --port 55433
scripts/cargo_isolated.sh test --lib --quiet recursive_cte_search_depth_first_executes
scripts/cargo_isolated.sh test --lib --quiet tablesample_system_accepts_lateral_expressions_in_explain
scripts/cargo_isolated.sh test --lib --quiet recursive_cte_cycle_tracking_returns_record_arrays
scripts/cargo_isolated.sh test --lib --quiet update_from_updates_rows_and_returns_source_columns
scripts/cargo_isolated.sh test --lib --quiet text_tsearch_match_operator_accepts_tsquery
scripts/run_regression.sh --test collate.utf8 --jobs 1 --timeout 120 --port 55434
scripts/cargo_isolated.sh test --lib --no-run --locked

Remaining:
No remaining collate.utf8 failures; regression passed 59/59 queries.
