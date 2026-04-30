Goal:
Diagnose why the PostgreSQL `identity` regression fails.

Key decisions:
No `identity.diff` was present under `/tmp/diffs`, so ran a focused local
regression and wrote results to `/tmp/diffs/identity-local`.

Files touched:
`.codex/task-notes/identity-regression-diff.md`

Tests run:
`env -u CARGO_TARGET_DIR PGRUST_TARGET_SLOT=7 scripts/run_regression.sh --test identity --results-dir /tmp/diffs/identity-local --timeout 120`

Remaining:
Main likely causes are incomplete identity metadata in
`information_schema.columns` / `information_schema.sequences`, sequence
dependency kind/display for identity-owned sequences, identity propagation and
ALTER behavior for partitions/inheritance, privilege handling for identity
sequences, unsupported `OVERRIDING` in MERGE INSERT actions, and unsupported
ALTER TABLE SET LOGGED/UNLOGGED for ordinary tables.
