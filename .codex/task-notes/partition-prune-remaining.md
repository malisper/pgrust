Goal:
Fix remaining non-parallel partition_prune regression mismatches without editing expected output or filters.

Key decisions:
- Kept render-only bpchar/varchar EXPLAIN shims in executor rendering.
- Kept OR-arm range pruning compatibility that avoids re-proving contradictory range conjuncts inside OR arms.
- Kept constraint_exclusion changes that avoid using constraint_exclusion=partition for declarative partitions when enable_partition_pruning is off, while still pruning ordinary inherited children.
- Reverted the scalar-array typed coercion experiment because it did not reduce the mismatch count and regressed coercepart/varchar scalar-array output.
- Added ordered partitioned path generation for partitioned parents and window/order requirements. This improves rangep/part_abc from Sort/SeqScan toward child Index Scan/MergeAppend shapes, but runtime-pruning alias/display differences remain.
- Added a guarded ordered-path fallback in make_ordered_rel and base-pathkey lowering for partitioned ORDER BY planning. The SQL PREPARE ma_test case is still not cleanly fixed in the latest usable regression.
- Narrowed the generic SQL PREPARE no-index compatibility hack so it keeps index scans available when enable_sort=off. Latest regression with this change had a ma_test CREATE INDEX timeout, so its diff is not a clean comparison.
- Added UNION ALL filter rewrite support for SubPlan/array literals. Current UNION ALL parent mismatches still show visible Subquery Scan filters, so another blocker remains.

Files touched:
- src/backend/executor/nodes.rs
- src/backend/executor/tests.rs
- src/backend/optimizer/partition_prune.rs
- src/backend/optimizer/path/allpaths.rs
- src/backend/optimizer/plan/planner.rs
- src/backend/optimizer/setrefs.rs
- src/backend/commands/tablecmds.rs
- src/backend/commands/explain.rs
- src/pgrust/session.rs

Tests run:
- cargo fmt
- RUSTC_WRAPPER= CARGO_TARGET_DIR=/tmp/pgrust-target-freetown-v4-direct TMPDIR="/Volumes/OSCOO PSSD/pgrust/tmp/freetown-v4" cargo test --lib --quiet partition_prune
- RUSTC_WRAPPER= CARGO_TARGET_DIR=/tmp/pgrust-target-freetown-v4-direct TMPDIR="/Volumes/OSCOO PSSD/pgrust/tmp/freetown-v4" scripts/run_regression.sh --test partition_prune --port 54467
- Same focused cargo test after plan-shape edits: passed, 22 tests.
- Clean regression after ordered/window/UNION/Memoize edits: /Volumes/OSCOO PSSD/pgrust/tmp/freetown-v4/pgrust_regress_results.freetown-v4.Laesrv, 686/750 matched, 64 mismatches, 1731 diff lines. Copied to /tmp/diffs/partition_prune.freetown-v4.plan-shape-3.diff.
- Later regression after narrowing generic PREPARE index disabling: /Volumes/OSCOO PSSD/pgrust/tmp/freetown-v4/pgrust_regress_results.freetown-v4.O0lhW9, 685/750 matched, 65 mismatches, 1741 diff lines, but ma_test index creation timed out before the prepared-plan checks; copied to /tmp/diffs/partition_prune.freetown-v4.plan-shape-4.diff and should not be used as a clean mismatch baseline.

Remaining:
- Latest confirmed clean regression after retained plan-shape fixes: 64 mismatched queries, 1731 diff lines, from /Volumes/OSCOO PSSD/pgrust/tmp/freetown-v4/pgrust_regress_results.freetown-v4.Laesrv.
- Remaining non-parallel buckets: UNION ALL/SubqueryScan filter pushdown/flattening; ma_test SQL PREPARE ordered index scans; rangep/part_abc nested runtime-pruning EXPLAIN aliases/Subplans Removed/never executed; ab_a1 DML UPDATE materialize/SubqueryScan shape; lateral mc2p/mc3p aggregate orientation and Memoize/join-shape differences.
