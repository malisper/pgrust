Goal:
Fix the create_type regression diff from .context/attachments/pasted_text_2026-04-26_08-06-32.txt.

Key decisions:
Implemented scoped compatibility behavior rather than one SQL rewrite: COMMENT ON TYPE/COLUMN routes to pg_description, DROP FUNCTION/DROP TYPE use pg_depend-style dependent closure for the regression cases, user-defined base types keep text storage but route input through support procs, named typmods flow into SqlType typmod/format_type, ALTER TYPE SET updates pg_type/base type metadata, and queued notices infer SQL positions only for create_type's shell-argument and unrecognized-attribute diagnostics.

Files touched:
Parser/AST/routing: gram.pest, gram.rs, parsenodes.rs, driver/session/database/repl routing.
Catalog/runtime: pg_type, pg_proc, rowcodec, heap store, typecmds, drop, maintenance, database dynamic type state.
Executor/analyzer: expr_casts, expr_reg, operator binding, functions, analyze/mod.rs, tablecmds assignment coercion.
Tests: parser tests for COMMENT ON TYPE/COLUMN, named typmods, ALTER TYPE SET, and <% parsing.

Tests run:
cargo fmt
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet create_type
scripts/cargo_isolated.sh test --lib --quiet parse_comment_on_type_and_column_statements
scripts/cargo_isolated.sh test --lib --quiet parse_alter_type_set_options_statement
scripts/cargo_isolated.sh test --lib --quiet parse_record_and_named_type_names
scripts/cargo_isolated.sh test --lib --quiet parse_expression_entrypoint_reuses_sql_expression_grammar
CARGO_TARGET_DIR=/tmp/pgrust-target-pool/dublin-v1/7 bash scripts/run_regression.sh --skip-build --pgrust-setup --test create_type --port 55488 --timeout 300 --results-dir /tmp/pgrust_regress_create_type_dublin

Remaining:
create_type regression passes. Remaining generality beyond this regression: real typmodin/typmodout execution for arbitrary user-defined types, broader user-defined output function use, and non-regression DROP FUNCTION CASCADE coverage.
