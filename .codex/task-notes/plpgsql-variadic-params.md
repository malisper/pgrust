Goal:
Fix PL/pgSQL variadic function parameters so `$1` resolves to the variadic array inside the function body.

Key decisions:
Treat pg_proc argument mode `v` as an input parameter when compiling PL/pgSQL function slots.
This registers the positional parameter alias and lets expressions such as `$1[i]`, `array_lower($1, 1)`, and variadic numeric array loops bind normally.

Files touched:
src/pl/plpgsql/compile.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet plpgsql_variadic_parameter_is_visible_as_positional_array
scripts/cargo_isolated.sh test --lib --quiet plpgsql
scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55434 --results-dir /tmp/diffs/plpgsql-variadic-params

Remaining:
Clean regression baseline is 2175/2271 matched, 1151 diff lines.
