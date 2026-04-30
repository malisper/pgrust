Goal:
Diagnose `/tmp/diffs/full-run/diff/drop_if_exists.diff`.

Key decisions:
The failure is not one bug. It is a mix of object-specific DROP diagnostics,
missing grammar for some PostgreSQL DROP forms, and `IF EXISTS` code paths that
still resolve missing schemas/types as hard errors.

Files touched:
`.codex/task-notes/drop_if_exists-diff.md`

Tests run:
No tests run. Inspected `/tmp/diffs/full-run/diff/drop_if_exists.diff`,
`/tmp/diffs/full-run/output/drop_if_exists.out`, upstream SQL, and related
parser/session/drop handlers.

Remaining:
Fix should start in parser DROP grammar/builders, then database command DROP
handlers for missing notices and tolerant type/schema lookup under `IF EXISTS`.
