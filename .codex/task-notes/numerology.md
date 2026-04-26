Goal:
Fix numerology regression diffs for PostgreSQL numeric literal scanning, GROUP BY target alias fallback, and grouped aggregate arithmetic coercion.

Key decisions:
Lexer-style numeric and parameter validation runs before unsupported-statement fallback, so PREPARE parameter junk still reports PostgreSQL-like scanner errors without implementing PREPARE. GROUP BY bare names prefer input columns, then unique SELECT aliases. Grouped aggregate arithmetic reuses numeric common-type coercion and leaves executor arithmetic unchanged.

Files touched:
src/backend/parser/gram.pest
src/backend/parser/gram.rs
src/backend/parser/analyze/mod.rs
src/backend/parser/analyze/agg_output.rs
src/backend/tcop/postgres.rs
src/backend/parser/tests.rs
src/backend/executor/tests.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet parse_rejects_numeric_and_parameter_junk
scripts/cargo_isolated.sh test --lib --quiet exec_error_position_points_at_numeric_and_parameter_lexer_errors
scripts/cargo_isolated.sh test --lib --quiet numeric_literals_and_arithmetic_bind_as_numeric_values
CARGO_TARGET_DIR=/tmp/pgrust-target-philadelphia-numerology scripts/run_regression.sh --test numerology --results-dir /tmp/pgrust_regress_numerology --port 57439 --timeout 180

Remaining:
None for the requested numerology slice.
