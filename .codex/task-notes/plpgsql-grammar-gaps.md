Goal:
Fix PL/pgSQL grammar gaps from the plpgsql regression diff, especially RAISE USING forms, compiler directives, bare LOOP/EXIT, and FOREACH.

Key decisions:
Accepted PostgreSQL PL/pgSQL syntax in the parser instead of adding protocol shims. Added runtime support for bare LOOP/EXIT, FOREACH array iteration, RAISE USING message/detail/hint/errcode, SQLSTATE/SQLERRM exception state, and re-RAISE inside handlers.

Files touched:
crates/pgrust_plpgsql_grammar/src/gram.pest
src/pl/plpgsql/ast.rs
src/pl/plpgsql/gram.rs
src/pl/plpgsql/compile.rs
src/pl/plpgsql/exec.rs
src/pl/plpgsql/mod.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet plpgsql
scripts/cargo_isolated.sh check
scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 180 --port 55472 --results-dir /tmp/diffs/plpgsql-grammar-fix2

Remaining:
The targeted plpgsql regression still fails overall at 2075/2271 queries matched with 2318 diff lines, but parser-arrow grammar failures dropped to zero. Remaining buckets are cursor semantics, dynamic SQL support, composite/record row-shape handling, PL/pgSQL expression binding gaps, and other runtime behavior.
