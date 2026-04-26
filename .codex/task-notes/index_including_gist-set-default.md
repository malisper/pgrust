Goal:
Diagnose and fix the `index_including_gist` regression diff where `SET enable_bitmapscan TO default` emitted `ERROR: unrecognized parameter "default"`.

Key decisions:
Represent unquoted `SET ... TO DEFAULT` as `SetStatement.value = None`, matching PostgreSQL's reset semantics. Keep quoted `'default'` as a normal string value.

Files touched:
`src/include/nodes/parsenodes.rs`, `src/backend/parser/gram.rs`, `src/backend/parser/tests.rs`, `src/pgrust/session.rs`, `src/pgrust/database_tests.rs`.

Tests run:
`cargo fmt`
`cargo test --lib --quiet parse_set_statement_to_default`
`cargo test --lib --quiet set_guc_to_default_resets_runtime_value`

Remaining:
Focused regression file was not rerun; the failing statement is covered by parser and runtime tests.
