Goal:
Fix sqljson_jsontable regression mismatches from the rerun diff artifact.

Key decisions:
Bind JSON_TABLE column groups in PostgreSQL order: current non-nested columns first, then nested paths.
Make JSON_TABLE scalar/formatted execution honor column-specific ERROR/EMPTY behavior and PostgreSQL error text.
Parse EXISTS columns separately so EXISTS ... ON EMPTY is a syntax error at EMPTY.
Map JSON_TABLE validation/runtime errors to PostgreSQL-compatible cursor positions.
Render JSON_TABLE view/EXPLAIN output with PostgreSQL-compatible aliasing, target quoting, LATERAL, and comma FROM lists.
Skip temporary schemas when resolving unqualified domain names for permanent domain operations.

Files touched:
crates/pgrust_sql_grammar/src/gram.pest
src/backend/parser/gram.rs
src/backend/parser/analyze/scope.rs
src/backend/executor/expr_json.rs
src/backend/tcop/postgres.rs
src/backend/rewrite/views.rs
src/backend/commands/explain.rs
src/backend/optimizer/bestpath.rs
src/backend/optimizer/path/allpaths.rs
src/pgrust/database.rs
src/pgrust/database_tests.rs
.codex/task-notes/sqljson-regression.md

Tests run:
cargo fmt
scripts/cargo_isolated.sh check --lib --quiet
scripts/cargo_isolated.sh test --lib --quiet json_table
scripts/cargo_isolated.sh test --lib --quiet sqljson
scripts/cargo_isolated.sh test --lib --quiet drop_json_table_view_preserves_referenced_domain -- --nocapture
scripts/run_regression.sh --test sqljson_jsontable --timeout 60 --jobs 1

Remaining:
sqljson_jsontable passes 117/117. Check output still includes existing unreachable-pattern warnings unrelated to this change.
