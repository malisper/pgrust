Goal:
Fix create_index regression failures against ../postgres and keep current diffs in /tmp/diffs.

Key decisions:
- Branch is malisper/create-index-reg.
- Baseline complete run was 569/687 matched, 118 mismatches, 1749 diff lines.
- Focused first on semantic/runtime blockers: geometry GiST/KNN support, bitmap residual filters, PL/pgSQL dynamic CTAS, btree NULL scan keys, and REINDEX invalid/exclusion handling.
- Kept expected files unchanged. Added :HACK: comments only for existing/full compatibility shortcuts around GiST geometry opckeytype behavior and concurrent schema REINDEX state.
- Latest rebuilt create_index run is 612/687 matched, 75 mismatches, 1179 diff lines. Artifacts are under /tmp/diffs/create_index_after.

Files touched:
- src/backend/access/gist/state.rs
- src/backend/access/gist/support/mod.rs
- src/backend/access/gist/support/point_ops.rs
- src/backend/access/nbtree/nbtree.rs
- src/backend/executor/expr_geometry.rs
- src/backend/executor/nodes.rs
- src/backend/optimizer/path/allpaths.rs
- src/backend/optimizer/path/costsize.rs
- src/backend/parser/analyze/constraints.rs
- src/backend/utils/sql_deparse.rs
- src/include/catalog/pg_amop.rs
- src/include/catalog/pg_amproc.rs
- src/include/catalog/pg_opclass.rs
- src/include/catalog/pg_proc.rs
- src/include/nodes/parsenodes.rs
- src/pgrust/database/commands/index.rs
- src/pl/plpgsql/exec.rs

Tests run:
- cargo fmt
- CARGO_TARGET_DIR=/tmp/pgrust-target-amman-create-index RUSTC_WRAPPER=sccache scripts/cargo_isolated.sh check
- rm -rf /tmp/diffs/create_index_after && CARGO_TARGET_DIR=/tmp/pgrust-target-amman-create-index RUSTC_WRAPPER=sccache scripts/run_regression.sh --test create_index --timeout 300 --port 57373 --results-dir /tmp/diffs/create_index_after

Remaining:
- GiST polygon/circle KNN still explains as Index Only Scan; needs deeper index returnability/opclass metadata investigation.
- Planner still lacks PostgreSQL-equivalent BitmapAnd composition, row comparison index quals, and full OR-to-SAOP path selection for InitPlan arrays.
- Correlated KNN subquery still falls back to Sort + disabled Seq Scan and prints debug Var structures.
- REINDEX CONCURRENTLY still lacks _ccnew invalid artifact behavior and several toast/system-catalog permission/catalog cases.
- Catalog/dependency fidelity still emits extra type/schema dependency rows for indexes/materialized views.
