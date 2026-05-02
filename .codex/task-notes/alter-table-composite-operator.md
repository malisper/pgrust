Goal:
Fix the `alter_table` regression section that creates a schema-qualified equality operator and hash operator class over a composite type.

Key decisions:
Resolve schema-qualified function names against arbitrary namespace OIDs, not only `public`/`pg_catalog`.
Allow PostgreSQL's `CREATE OPERATOR CLASS name DEFAULT FOR TYPE ...` ordering.
Resolve operator-class operator members with optional schema and explicit arg types, e.g. `alter1.=(alter1.ctype, alter1.ctype)`.

Files touched:
src/backend/parser/gram.rs
src/backend/parser/tests.rs
src/pgrust/database/commands/opclass.rs
src/pgrust/database/commands/operator.rs
src/pgrust/database_tests.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet create_operator_class (passed once before the final missing-schema guard)

Remaining:
`scripts/run_regression.sh --test alter_table --timeout 120` was attempted, but the release server build stalled in the main crate compile and was terminated after about 12 minutes.
