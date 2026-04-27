Goal:
Fix `btree_index` regression failures from pasted diff without changing upstream expected output.

Key decisions:
- Preserve PostgreSQL row `=`/`<>` pairwise lowering, but bind row `<`, `<=`, `>`, `>=` as row-vs-row operators after scalar coercion.
- Coerce unknown string literals to fixed catalog vector types (`oidvector`, `int2vector`) for comparisons and `IN`.
- Keep `tenk1_*` setup out of global bootstrap; make the regression harness run `create_index` as a `btree_index` dependency, including non-isolated `--jobs 1` runs.
- Treat btree `deduplicate_items` as metadata-only with a nearby `:HACK:`; honor btree `fillfactor` during bulk build.
- Reuse existing row-prefix and LIKE prefix index qual machinery rather than adding new plan nodes.
- Harden nbtree insert/scan paths around deleted right siblings after vacuum so the fast-root split section completes.
- Avoid per-row visible-catalog cloning on ordinary inserts; text coercion and partition rechecks were local bottlenecks for the 80k-row `delete_test_table` setup.
- Keep the metadata-only `deduplicate_items=on` compatibility shim from tripping dev-profile statement timeouts on duplicate-heavy btree split loops.

Files touched:
- `scripts/run_regression.sh`
- `src/backend/parser/analyze/*`, `src/backend/parser/gram.*`, `src/include/nodes/parsenodes.rs`
- `src/backend/optimizer/path/*`, `src/backend/optimizer/plan/planner.rs`, `src/backend/optimizer/setrefs.rs`
- `src/backend/executor/*`
- `src/backend/access/nbtree/*`, `src/include/access/nbtree.rs`
- catalog/index metadata and index DDL command paths
- `.codex/task-notes/btree-index.md`

Tests run:
- `cargo fmt`
- `scripts/cargo_isolated.sh check`
- `scripts/cargo_isolated.sh test --lib --quiet planner_keeps_unique_for_ordered_select_distinct`
- `scripts/cargo_isolated.sh test --lib --quiet planner_keeps_unique_for_ordered_select_distinct_saop_index_path`
- `CARGO_TARGET_DIR=/tmp/pgrust-target-casablanca-btree-final scripts/run_regression.sh --test btree_index --timeout 900 --jobs 1 --port 58634 --results-dir /tmp/pgrust-casablanca-btree-index-final4`
- `CARGO_TARGET_DIR=/tmp/pgrust-target-casablanca-btree-final scripts/run_regression.sh --test btree_index --timeout 900 --jobs 1 --port 58636 --results-dir /tmp/pgrust-casablanca-btree-index-rebased2`
- `CARGO_TARGET_DIR=/tmp/pgrust-target-casablanca-btree scripts/run_regression.sh --test btree_index --timeout 420 --port 58612 --results-dir /tmp/pgrust-casablanca-btree-index-current4 --ignore-deps`
- `CARGO_TARGET_DIR=/tmp/pgrust-target-casablanca-btree scripts/run_regression.sh --test btree_index --timeout 420 --jobs 1 --port 58630 --results-dir /tmp/pgrust-casablanca-btree-index-nonisolate`
- `CARGO_TARGET_DIR=/tmp/pgrust-target-casablanca-btree scripts/run_regression.sh --test create_index --timeout 180 --port 58620 --results-dir /tmp/pgrust-casablanca-create-index-current`
- `git diff --check`

Remaining:
- `btree_index` passes in non-isolated dependency mode with the final run above.
- `create_index` still has broad unrelated failures in this checkout, mainly unsupported GiST/geometry behavior and pre-existing output/error-text gaps; it is not a useful green guard for this btree-focused change yet.
- `scripts/cargo_isolated.sh check` still reports the pre-existing unreachable `Statement::ReindexIndex(_)` warning in `src/bin/query_repl.rs`.
