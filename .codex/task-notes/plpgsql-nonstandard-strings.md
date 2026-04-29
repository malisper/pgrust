Goal:
Decode PL/pgSQL string expressions correctly when standard_conforming_strings is off.
Key decisions:
Track standard_conforming_strings in the PL/pgSQL compile environment.
Normalize expression SQL snippets to E'' form before parsing when nonstandard string literals are active.
Decode RAISE format strings for doubled-backslash and octal escapes without reinterpreting already-decoded E'' control escapes.
Files touched:
src/pl/plpgsql/compile.rs
src/pgrust/database_tests.rs
Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet plpgsql_nonstandard_string_literals_decode_backslashes
scripts/cargo_isolated.sh test --lib --quiet plpgsql
scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55434 --results-dir /tmp/diffs/plpgsql-nonstandard-strings-reraise
Remaining:
Regression is 2232/2271 matched with 448 diff lines. The string-literal hunk now only misses PostgreSQL warning/caret output for nonstandard backslashes.
