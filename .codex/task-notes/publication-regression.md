Goal:
Diagnose publication regression failures from .context/attachments/pasted_text_2026-04-27_10-46-46.txt.

Key decisions:
The failures are mostly feature skew. Expected output includes publication sequence support, publication EXCEPT clauses, and newer psql describe wording. Added publication-wide sequence support as a puballsequences flag parallel to puballtables. Added FOR ALL TABLES EXCEPT support by storing excluded relations in pg_publication_rel with prexcept=true and leaving normal relation rows as prexcept=false.

Files touched:
.codex/task-notes/publication-regression.md
src/include/nodes/parsenodes.rs
src/backend/parser/gram.rs
src/include/catalog/pg_publication.rs
src/include/catalog/pg_publication_rel.rs
src/backend/catalog/rowcodec.rs
src/pgrust/database/commands/publication.rs
src/backend/parser/tests.rs
src/backend/tcop/postgres.rs

Tests run:
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh check
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet publication
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/cargo_isolated.sh test --lib --quiet psql_publication
scripts/run_regression.sh --test publication --timeout 60 --jobs 1 --results-dir /tmp/pgrust-publication-regress timed out after 388/710 queries matched.
CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm scripts/run_regression.sh --test publication --timeout 90 --jobs 1 --results-dir /tmp/pgrust-publication-regress-except-final failed without timeout: 487/710 queries matched, 1401 diff lines.

Remaining:
Publication regression still has gaps outside the EXCEPT path: replica identity DDL and update/delete checks, row-filter validation edge cases, generated-column validation/dependency behavior, DROP COLLATION, partition publication details, pg_publication_tables/pg_get_publication_tables support, and some SQL-visible error text/position differences.
