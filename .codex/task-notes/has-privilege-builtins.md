Goal:
- Close direct `has_*_privilege` builtin gaps in the `privileges` regression:
  table, column, any-column, sequence, largeobject, and role checks.

Key decisions:
- Added PostgreSQL-compatible `pg_proc` rows and scalar builtin dispatch for the
  builtin overloads exercised by `privileges`.
- Implemented executor-side ACL helpers for role resolution, privilege string
  parsing, ACL bit/grant-option matching, relation/column/large object checks,
  and `pg_has_role`.
- Preserved name-vs-OID missing-object behavior: name variants error; OID
  variants return `NULL` when the referenced object is absent.
- Added minimal sequence relation privilege grant/revoke support for `USAGE`,
  `SELECT`, and `UPDATE` so sequence ACL tests can drive the builtins.

Files touched:
- `src/include/nodes/primnodes.rs`
- `src/include/catalog/pg_proc.rs`
- `src/backend/parser/analyze/functions.rs`
- `src/backend/executor/exec_expr.rs`
- `src/backend/parser/gram.rs`
- `src/pgrust/database/commands/privilege.rs`
- `src/pgrust/database/large_objects.rs`
- `src/pgrust/database_tests.rs`

Tests run:
- `cargo fmt`
- `git diff --check`
- `scripts/cargo_isolated.sh check`
- `scripts/cargo_isolated.sh test --lib --quiet has_relation_column_sequence_and_role_privilege_builtins_use_catalog_acls`
- `scripts/cargo_isolated.sh test --lib --quiet has_privilege_builtins_match_missing_object_and_largeobject_edges`
- `scripts/run_regression.sh --schedule /tmp/privileges_schedule --jobs 1 --port 55433 --results-dir /tmp/diffs`

Remaining:
- `/tmp/diffs` is updated. `privileges` still fails overall: 901/1295 queries
  matched, 394 mismatched, 2794 diff lines.
- Direct missing-builtin errors for these six families are gone: 0 occurrences
  for `has_table_privilege`, `has_column_privilege`,
  `has_any_column_privilege`, `has_sequence_privilege`,
  `has_largeobject_privilege`, and `pg_has_role`.
- Remaining diff buckets are follow-on privilege coverage: catalog visibility
  for subqueries over `pg_class`, unsupported default privilege forms, large
  object grant/runtime behavior, missing system stats relations, and unsupported
  PL/pgSQL/function privilege setup used by later assertions.
