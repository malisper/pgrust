Goal:
Close the PL/pgSQL regression cluster where unreserved keyword variables and
COMMENT ON FUNCTION inside a function body diverged from PostgreSQL.

Key decisions:
Parse assignment statements before RETURN-family statements so a variable named
return can be assigned with `return := ...` while `return expr` still parses as
RETURN. Execute COMMENT ON FUNCTION through the existing database catalog
mutation API using the current PL/pgSQL transaction and search path.

Files touched:
crates/pgrust_plpgsql_grammar/src/gram.pest
src/pl/plpgsql/gram.rs
src/pl/plpgsql/compile.rs
src/pl/plpgsql/exec.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet plpgsql
scripts/run_regression.sh --test plpgsql --jobs 1 --timeout 360 --port 55433 --results-dir /tmp/diffs/plpgsql-unreserved-comment

Remaining:
Regression is at 2154/2271 matched with 1398 diff lines. Next visible clusters
include polymorphic SRF/EXPLAIN output and remaining record/diagnostic/refcursor
semantics.
