Goal:
Make `scripts/run_regression.sh --test xml --jobs 1` match PostgreSQL for the remaining XML regression mismatches.

Key decisions:
Used PostgreSQL regression output as the reference. Fixed the XML behavior in the parser/analyzer/executor where possible, and kept formatting-only compatibility in EXPLAIN/view/protocol rendering paths.

Files touched:
XML grammar/parser/analyzer/executor, XMLTABLE EXPLAIN and view deparse, prepared statement substitution, PL/pgSQL RAISE LOG parsing, builtin function/catalog wiring, and related exhaustive AST walkers.

Tests run:
scripts/cargo_isolated.sh check
scripts/cargo_isolated.sh test --lib --quiet backend::executor::expr_xml::tests
scripts/cargo_isolated.sh test --lib --quiet backend::parser::tests::parse_prepare_and_execute_statements
scripts/cargo_isolated.sh test --lib --quiet include::catalog::pg_proc::tests::scalar_proc_oid_helpers_cover_real_and_synthetic_builtins
scripts/cargo_isolated.sh test --lib --quiet backend::executor::agg::tests::xmlagg_finalizes_with_xmlconcat_semantics
scripts/cargo_isolated.sh test --lib --quiet exec_error_position_points_at_execute_xml_argument
scripts/cargo_isolated.sh test --lib --quiet backend::parser::tests::parse_xmlserialize_expression
scripts/run_regression.sh --test xml --jobs 1 --timeout 60 --port 55433 --results-dir /tmp/pgrust_xml_remaining5

Remaining:
XML regression passed: 281/281 queries matched.
