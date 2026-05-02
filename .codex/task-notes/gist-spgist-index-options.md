Goal:
Fix GiST/SP-GiST regression diffs for index reloptions and text-compatible opclass coverage.
Key decisions:
Accept GiST fillfactor, SP-GiST fillfactor, PostgreSQL-like reloption errors, and text-family opclasses for domain/base text kinds.
Files touched:
src/include/access/gist.rs; src/backend/access/gist/build.rs; src/backend/commands/tablecmds.rs; src/backend/catalog/loader.rs; src/include/catalog/pg_opclass.rs; src/backend/executor/tests.rs.
Tests run:
cargo fmt; git diff --check. Targeted cargo test attempts were blocked by idle Cargo processes waiting behind other concurrent Cargo work.
Remaining:
Run targeted create_index_executor tests once Cargo locks clear.
