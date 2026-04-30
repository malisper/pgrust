Goal:
Make pgrust match PostgreSQL behavior for the upstream `identity` regression.

Key decisions:
- Store serial owned-sequence dependencies as `DEPENDENCY_AUTO` and identity owned-sequence dependencies as `DEPENDENCY_INTERNAL`.
- Bind identity defaults through an internal `IdentityNextVal` builtin so table `INSERT` privilege is enough.
- Read identity metadata for `information_schema.columns` from `pg_sequence` and hide internally owned identity sequences from `information_schema.sequences`.
- Normalize identity insert behavior for base tables, auto-updatable views, and MERGE `INSERT ... OVERRIDING`.
- Treat partition identity metadata as inherited from the partitioned parent, sharing the parent sequence and clearing it on detach.

Files touched:
- Parser/AST: grammar, parser, parse node fields, MERGE overriding syntax, explicit identity NULL tracking.
- Analyzer/executor: identity default binding, view/MERGE insert normalization, internal nextval execution.
- Catalog/system views: dependency kind handling, sequence metadata lookup, information_schema rows.
- DDL/storage commands: ALTER IDENTITY, ALTER TYPE, SET LOGGED/UNLOGGED, partition attach/detach propagation.
- Tests: parser coverage plus focused `identity_regression_` database tests.

Tests run:
- `cargo fmt`
- `env -u CARGO_TARGET_DIR PGRUST_TARGET_SLOT=7 scripts/cargo_isolated.sh test --lib --quiet identity_regression_`
- `env -u CARGO_TARGET_DIR PGRUST_TARGET_SLOT=7 scripts/cargo_isolated.sh check --lib`
- `env -u CARGO_TARGET_DIR PGRUST_TARGET_SLOT=7 scripts/run_regression.sh --test identity --results-dir /tmp/diffs/identity-final3 --timeout 120`
- `git diff --check`

Remaining:
None for the focused `identity` regression; `/tmp/diffs/identity-final3` passed 271/271 queries.
