Goal:
Finish PostgreSQL-style threaded parallel query support for `select_parallel` and keep `write_parallel` green. Vacuum parallelism remains out of scope.

Key decisions:
- Worker threads still build worker-local `ExecutorContext` values from a Send-safe seed; `PlanNode` and `TupleSlot` stay thread-local.
- Cross-thread rows use owned `WorkerTuple` values over bounded channels.
- Shared `ParallelRuntime` now coordinates parallel seq scans by `source_id` with an atomic block allocator instead of the prior per-worker stride.
- `GatherMerge` exists as a plan/state and currently materializes worker/leader rows, then sorts them with existing order-key comparison. Correctness-first v1; not yet PostgreSQL's one-row lookahead streaming merge.
- Catalog-driven `ParallelSafety::{Safe, Restricted, Unsafe}` is wired through planner safety checks using `pg_proc.proparallel`; unknown/user-defined without catalog proof defaults unsafe.
- Uncorrelated quantified `SubPlan` filters can be worker-safe when their test expression is safe; `ANY`/`NOT IN` subplan scans stay serial so hashed-subplan semantics do not spawn nested parallel workers.
- Restricted aggregate expressions are kept above `Gather`; worker-safe count/sum/avg/min/max can split into partial/final aggregates, including sorted group aggregates through `GatherMerge`.
- Join parallelization is conservative: only the outer side is made parallel-aware; inner side remains worker-local/serial to avoid duplicate or missing join pairs until real partial paths exist.
- Parallel index/index-only/bitmap scan metadata and executor partitioning exist, but path selection still often chooses disabled seq scans instead of PostgreSQL's index paths.
- `pg_stat_database.parallel_workers_to_launch/launched` now records launched Gather/GatherMerge workers.

Files touched:
- `src/backend/executor/parallel.rs`
- `src/backend/executor/nodes.rs`
- `src/backend/executor/startup.rs`
- `src/backend/executor/exec_expr/subquery.rs`
- `src/backend/optimizer/setrefs.rs`
- `src/backend/optimizer/plan/subselect.rs`
- `src/backend/commands/explain.rs`
- `src/backend/utils/activity/pgstat.rs`
- `src/backend/utils/cache/system_views.rs`
- `src/include/nodes/{execnodes,pathnodes,plannodes,primnodes}.rs`
- GUC/write-path plumbing in `src/pgrust/session.rs`, `src/pgrust/database/commands/execute.rs`, `src/pl/plpgsql/exec.rs`
- Focused test exhaustiveness/initializer updates.

Tests run:
- `cargo fmt`
- `scripts/cargo_isolated.sh check`
- `scripts/cargo_isolated.sh test --lib --quiet parallel`
- `scripts/run_regression.sh --test write_parallel --jobs 1 --port 55497 --results-dir /tmp/diffs/write_parallel-after-parallel-work` PASS, 22/22
- `scripts/run_regression.sh --test select_parallel --jobs 1 --port 55496 --results-dir /tmp/diffs/select_parallel-after-stats` FAIL, 224/265 matched, 41 mismatched, no timeout

Remaining:
- Replace v1 post-plan rewrite with real partial path planning and proper worker-count costing.
- Make append scheduling match PostgreSQL mixed partial/non-partial child behavior and worker-count parity.
- Make planner prefer PostgreSQL-like parallel btree index/index-only and bitmap heap paths under forced settings.
- Stream `GatherMerge` with per-stream lookahead instead of materializing all rows.
- Merge worker executor stats into EXPLAIN ANALYZE child rows/loops and worker sort details.
- Push safe projections into worker plans so verbose EXPLAIN output is not full-width above Gather/GatherMerge.
- Finish SubLink display parity: hashed `ANY`/`NOT IN` still renders as generic `SubPlan`.
- Address remaining select_parallel plan-shape diffs around InitPlan placement, right/left join orientation, SRF/window subquery rewrites, single-copy debug_parallel_query behavior, and worker error context in debug forced plans.
