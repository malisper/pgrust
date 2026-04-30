Goal:
Fix named regression diffs from /private/tmp/pgrust-regression-artifact-2026-04-30T0340Z for alter_generic, copy, copyselect, create_aggregate, misc_sanity, predicate, sanity_check, tid, tuplesort, without_overlaps.
Key decisions:
- Added missing catalog index/toast metadata instead of weakening sanity queries.
- Tightened aggregate support proc coercion validation for resolved polymorphic args.
- Made nonverbose EXPLAIN sort keys re-render when relation aliases are present.
- Hid implicit GiST range opclass display and used unqualified relnames in currtid2 errors.
- Reordered DROP SCHEMA cascade details so operator families/tables precede text search objects, and skipped opclass family notices when the family belongs to another schema.
- Kept pg_stat_progress_copy as a runtime SRF-backed view so cached PL/pgSQL trigger plans see the current COPY progress snapshot.
- Restored PostgreSQL-style nonverbose EXPLAIN formatting for scan filters, aliased sort keys, temp relation labels, presorted keys, and join NULL tests.
Files touched:
- src/include/catalog/indexing.rs
- src/include/catalog/bootstrap.rs
- src/pgrust/database/commands/create.rs
- src/backend/commands/explain.rs
- src/backend/executor/exec_expr.rs
- src/backend/executor/srf.rs
- src/backend/executor/startup.rs
- src/backend/optimizer/constfold.rs
- src/backend/optimizer/path/costsize.rs
- src/backend/optimizer/plan/subselect.rs
- src/backend/optimizer/root.rs
- src/backend/optimizer/setrefs.rs
- src/backend/parser/analyze/agg_output.rs
- src/backend/parser/analyze/system_views.rs
- src/backend/rewrite/mod.rs
- src/backend/rewrite/views.rs
- src/include/nodes/primnodes.rs
- src/include/catalog/pg_opclass.rs
- src/pgrust/database/commands/drop.rs
- src/pgrust/database/relation_refs.rs
Tests run:
- cargo fmt
- cargo fmt --check
- git diff --check
- cargo check with CARGO_TARGET_DIR=/tmp/pgrust-target-davao-regression-check was stopped after several minutes without diagnostics
- scripts/cargo_isolated.sh check was stopped while queued behind a shared target lock
- CARGO_TARGET_DIR="/Volumes/OSCOO PSSD/pgrust-davao-named-regress/target" RUSTC_WRAPPER= scripts/cargo_isolated.sh check
- CARGO_TARGET_DIR="/Volumes/OSCOO PSSD/pgrust-davao-named-regress/target" RUSTC_WRAPPER= scripts/cargo_isolated.sh build --bin pgrust_server
- PGRUST_STATEMENT_TIMEOUT=30 scripts/run_regression.sh for remaining four: alter_generic, copy, predicate, tuplesort; 4/4 passed.
- PGRUST_STATEMENT_TIMEOUT=30 scripts/run_regression.sh for original ten named files; 10/10 passed, 1367/1367 queries matched.
Remaining:
- None for the named regression files.
