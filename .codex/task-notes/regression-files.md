Goal:
Fix named regression diffs from /private/tmp/pgrust-regression-artifact-2026-04-30T0340Z for alter_generic, copy, copyselect, create_aggregate, misc_sanity, predicate, sanity_check, tid, tuplesort, without_overlaps.
Key decisions:
- Added missing catalog index/toast metadata instead of weakening sanity queries.
- Tightened aggregate support proc coercion validation for resolved polymorphic args.
- Made nonverbose EXPLAIN sort keys re-render when relation aliases are present.
- Hid implicit GiST range opclass display and used unqualified relnames in currtid2 errors.
Files touched:
- src/include/catalog/indexing.rs
- src/include/catalog/bootstrap.rs
- src/pgrust/database/commands/create.rs
- src/backend/commands/explain.rs
- src/backend/executor/exec_expr.rs
- src/include/catalog/pg_opclass.rs
- src/pgrust/database/commands/drop.rs
Tests run:
- cargo fmt
- cargo fmt --check
- git diff --check
- cargo check with CARGO_TARGET_DIR=/tmp/pgrust-target-davao-regression-check was stopped after several minutes without diagnostics
- scripts/cargo_isolated.sh check was stopped while queued behind a shared target lock
Remaining:
- Run focused regression files after compile passes; COPY may still need a psql-variable/progress follow-up.
