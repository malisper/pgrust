Goal:
Fix non-parallel partition_prune regression mismatches without changing expected output.

Key decisions:
Do not globally fold contradictory Var equality conjuncts to false in constfold; leave contradiction detection to pruning/constraint-exclusion code.
Remove the relaxed OR-arm fallback in static partition pruning so each OR arm is analyzed exactly.
Accept typmod/text-family compatible partition-key casts for pruning.
Treat stable time expressions as startup-prune-evaluable.
Skip top-level Memoize around lateral Aggregate-over-partitioned-Append shapes.
Use constraint_exclusion=on, not constraint_exclusion=partition, for ordinary inheritance/direct-relation constraint pruning outside partitioned DML roots.
Normalize contextual scalar-array EXPLAIN rendering by omitting redundant outer array casts for dynamic ARRAY expressions.
Avoid linker-wrapper here-strings so local builds do not require temp space on the nearly-full system volume.

Files touched:
.codex/task-notes/partition-prune-diff.md
scripts/macos-rust-lld-linker.sh
src/backend/commands/tablecmds.rs
src/backend/executor/nodes.rs
src/backend/optimizer/constfold.rs
src/backend/optimizer/partition_prune.rs
src/backend/optimizer/path/allpaths.rs
src/backend/optimizer/setrefs.rs

Tests run:
cargo fmt
scripts/cargo_isolated.sh test --lib --quiet partition_prune (with TMPDIR on external volume): passed, 21 tests.
bash -n scripts/macos-rust-lld-linker.sh: passed.
git diff --check: passed.
scripts/run_regression.sh --test partition_prune --port 56544 was attempted but stopped during Cargo build after several minutes of build contention; no usable regression diff was produced.

Remaining:
Run the target regression when the local build queue is clear and inspect remaining non-parallel hunks.
Likely remaining larger items: InitPlan/startup-pruning EXPLAIN display, parameterized nested-loop/index paths for partitioned inners, MergeAppend/index path selection, and timestamp/range canonical rendering.
