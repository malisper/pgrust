Goal:
- Fix the `alter_table` regression region around `ALTER OPERATOR CLASS/FAMILY ... SET SCHEMA`.

Key decisions:
- The SET SCHEMA catalog update path already worked for simple operator classes/families.
- The regression hunk cascaded from setup failures: PostgreSQL `CREATE OPERATOR CLASS name DEFAULT FOR TYPE ...`, schema-qualified operator procedures, and schema-qualified operator class items with explicit arg types.

Files touched:
- `src/backend/parser/gram.rs`
- `src/backend/parser/tests.rs`
- `src/pgrust/database/commands/operator.rs`
- `src/pgrust/database/commands/opclass.rs`
- `src/pgrust/database_tests.rs`

Tests run:
- `env -u CARGO_TARGET_DIR PGRUST_TARGET_SLOT=5 scripts/cargo_isolated.sh test --lib --quiet parse_operator_family_and_class_alter_statements`
- `env -u CARGO_TARGET_DIR PGRUST_TARGET_SLOT=6 scripts/cargo_isolated.sh test --lib --quiet alter_operator_class`
- `env -u CARGO_TARGET_DIR PGRUST_TARGET_SLOT=5 scripts/cargo_isolated.sh check`

Remaining:
- Full `alter_table` regression still has unrelated failures in the provided diff.
