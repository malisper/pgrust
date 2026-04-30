Goal:
Fix remaining regression failure 59, collate.utf8.

Key decisions:
The downloaded diff compares actual output to collate.utf8_1.out, the non-UTF8 skip variant. Actual runs past the \quit guard because pgrust reports UTF8, so the useful comparison is also against expected/collate.utf8.out.
Added PostgreSQL-compatible builtin pg_collation rows/metadata, CREATE COLLATION option syntax for builtin provider locales, casefold(text), and runtime support for C, PG_C_UTF8, and PG_UNICODE_FAST text/regex behavior.
Preserved implicit column collation through Var and FuncExpr metadata, generated relation output expressions, and setrefs slot Vars.
The final regression-only mismatch was the SELECT header for top-level ~*/!~* expressions rewritten through regexp_like; select-list naming now keeps ?column? for those operator spellings.

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

Remaining:
No remaining collate.utf8 failures; regression passed 59/59 queries.
