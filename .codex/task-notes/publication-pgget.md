Goal:
Add pg_publication_tables and pg_get_publication_tables support, plus small publication regression error-text cleanup.

Key decisions:
Model pg_publication_tables as a synthetic system view backed by shared publication expansion helpers.
Expose pg_get_publication_tables as native SRF oid 6119 with PostgreSQL OUT columns.
Keep FOR ALL TABLES detail text from the sequence-support slice; only adjusted non-member table/schema wording.

Files touched:
src/backend/utils/cache/system_view_registry.rs
src/backend/utils/cache/system_views.rs
src/backend/parser/analyze/system_views.rs
src/backend/parser/analyze/mod.rs
src/backend/utils/cache/lsyscache.rs
src/backend/utils/cache/visible_catalog.rs
src/include/catalog/pg_proc.rs
src/backend/executor/srf.rs
src/pgrust/database/commands/publication.rs

Tests run:
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet publication
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh check
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/run_regression.sh --test publication --timeout 120 --jobs 1 --port 55433 --results-dir /tmp/pgrust-publication-pgget-55433
git diff --check

Remaining:
publication regression still fails broadly from unsupported publication/replica-identity/collation/generated-column behavior and some caret/text drift. Useful diff copied to /tmp/diffs/publication-pgget.diff.
