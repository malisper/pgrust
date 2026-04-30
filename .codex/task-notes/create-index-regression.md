Goal:
Fix create_index regression failures against ../postgres and keep current diffs in /tmp/diffs.

Key decisions:
- Branch is malisper/create-index-reg.
- Kept expected files unchanged.
- /tmp/diffs/create_index_after is now a symlink to /Volumes/OSCOO PSSD/pgrust/tmp/create_index_after.amman because the full regression result tree stores worker data and /tmp ran out of space.
- RUSTC_WRAPPER=sccache fails locally with os error 45; use RUSTC_WRAPPER=/usr/bin/env. Use CARGO_INCREMENTAL=0 for regression runs to avoid stale incremental object copy failures.
- Added BitmapAnd plan/executor node plumbing and bitmap-tree walkers, but scalar-array common-qual BitmapAnd generation was backed out after it regressed the run.
- Added a narrow GiST KNN tie-break for equal infinite/NaN distance keys; this removed the two swapped point result hunks.

Files touched:
- src/backend/commands/explain.rs
- src/backend/commands/tablecmds.rs
- src/backend/executor/exec_expr/subquery.rs
- src/backend/executor/nodes.rs
- src/backend/executor/startup.rs
- src/backend/optimizer/bestpath.rs
- src/backend/optimizer/constfold.rs
- src/backend/optimizer/mod.rs
- src/backend/optimizer/path/allpaths.rs
- src/backend/optimizer/path/costsize.rs
- src/backend/optimizer/pathnodes.rs
- src/backend/optimizer/plan/planner.rs
- src/backend/optimizer/plan/subselect.rs
- src/backend/optimizer/rewrite.rs
- src/backend/optimizer/setrefs.rs
- src/include/access/relscan.rs
- src/include/access/tidbitmap.rs
- src/include/nodes/execnodes.rs
- src/include/nodes/pathnodes.rs
- src/include/nodes/plannodes.rs
- src/pgrust/database/relation_refs.rs

Tests run:
- cargo fmt
- CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/pgrust-target-amman-create-index RUSTC_WRAPPER=/usr/bin/env scripts/cargo_isolated.sh check
- CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/pgrust-target-amman-create-index RUSTC_WRAPPER=/usr/bin/env scripts/run_regression.sh --test create_index --timeout 300 --port 57373 --results-dir /tmp/diffs/create_index_after
- Latest verified create_index run: 643/687 matched, 44 mismatches, 800 diff lines.

Remaining:
- Correlated GiST KNN EXPLAIN still has an extra Projection node and renders the runtime point argument against gpolygon_tbl instead of outer alias x.
- Planner still needs OR/SAOP path selection for InitPlan arrays, BitmapAnd composition for common AND scalar-array predicates, row-comparison index quals, and bitmap_split_or parity.
- Unique btree IN/range queries still choose bitmap heap scan instead of index-only scan; a broad path preference was tried and backed out.
- REINDEX/catalog drift remains: pg_depend/deptype rows, _ccnew invalid artifact behavior, toast/system-catalog behavior, verbose notices, and attstattarget/stat rows.
- Deparse/display drift remains for mixed integer casts, collation/operator display, row comparison debug output, and final error caret formatting.
