Goal:
Fix remaining non-parallel partition_prune regression mismatches without editing expected output or regression filters.

Key decisions:
- Kept changes behavior/render scoped to pgrust, not expected output.
- Added EXPLAIN ANALYZE InitPlan capture/de-duplication for direct plan subplans and partition-prune filters.
- Preserved flattened nested partition child domains through projection translation so parent prune filters map to child physical keys.
- Collapsed one-visible-child runtime-pruned Append aliases to match PostgreSQL in part_abc_q1-style plans.
- Split timestamp/timestamptz pruning by volatility: timestamp <-> timestamptz casts stay out of static pruning, but are allowed for executor startup/runtime pruning.
- Normalized timestamp/timestamptz EXPLAIN rendering in the render layer: PostgreSQL-style PST/Postgres date style, no redundant key cast, canonical timestamp array literal elements, and collapsed same-type array casts.

Files touched:
- src/backend/commands/explain.rs
- src/backend/commands/tablecmds.rs
- src/backend/executor/exec_expr/subquery.rs
- src/backend/executor/nodes.rs
- src/backend/executor/tests.rs
- src/backend/optimizer/bestpath.rs
- src/backend/optimizer/partition_prune.rs
- src/backend/optimizer/path/allpaths.rs
- src/backend/optimizer/path/costsize.rs
- src/backend/optimizer/setrefs.rs
- src/backend/optimizer/tests.rs
- src/backend/parser/analyze/modify.rs
- src/include/nodes/execnodes.rs

Tests run:
- cargo fmt
- RUSTC_WRAPPER= CARGO_TARGET_DIR=/tmp/pgrust-target-freetown-v4-direct TMPDIR="/Volumes/OSCOO PSSD/pgrust/tmp/freetown-v4" cargo test --lib --quiet partition_prune
- RUSTC_WRAPPER= CARGO_TARGET_DIR=/tmp/pgrust-target-freetown-v4-direct TMPDIR="/Volumes/OSCOO PSSD/pgrust/tmp/freetown-v4" scripts/run_regression.sh --test partition_prune --port 55447
- git diff --check

Current regression artifacts:
- Latest: /Volumes/OSCOO PSSD/pgrust/tmp/freetown-v4/pgrust_regress_results.freetown-v4.a0s9kr
- Copied diff: /tmp/diffs/partition_prune.freetown-v4.current-live24.diff
- Result: partition_prune FAIL, 708/750 queries matched, 42 mismatches, 1020 diff lines.
- This pass improved from live19 703/750 and 1092 diff lines.

Remaining:
- Explicit parallel hunks remain acceptable: Gather, Parallel Append, Parallel Seq Scan, worker/parallel aggregate shapes.
- Non-parallel buckets still visible:
  - scoped filter-order rendering in rlp, mc2p/mc3p, and rp_prefix_test statements;
  - lateral aggregate/join orientation around mc2p/mc3p/asptab;
  - shared InitPlan numbering and nested InitPlan display in ma_test and runtime-pruned Append/MergeAppend;
  - UNION ALL/SubqueryScan/Projection wrapper flattening and InitPlan placement;
  - ab_a1 DML UPDATE join/materialize/index shape;
  - rangep nested MergeAppend alias numbering and Sort Key qualifier;
  - part_abc window UNION ALL nested partition structure and filter/index-condition shape.
