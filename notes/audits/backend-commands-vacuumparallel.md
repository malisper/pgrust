# Audit: backend-commands-vacuumparallel

- **Unit:** `backend-commands-vacuumparallel` (C: `src/backend/commands/vacuumparallel.c`, PostgreSQL 18.3)
- **Branch:** `port/backend-commands-vacuumparallel`
- **Date:** 2026-06-16
- **Model:** Opus 4.8 (1M context) — `claude-opus-4-8[1m]`
- **Verdict:** **PASS** (one real divergence found + fixed during audit)

Independent function-by-function audit per `.claude/skills/audit-crate/SKILL.md`.
Re-derived from the C source and the Rust port; the port's own committed
self-review was treated as untrusted.

## 1. Function inventory

`vacuumparallel.c` defines exactly **14** functions (6 exported + 7 static +
the `parallel_vacuum_main` bgworker entry). Every function gets a row.

| # | C function (loc) | Rust port (loc) | Verdict |
|---|---|---|---|
| 1 | `parallel_vacuum_init` (242) | `parallel_vacuum_init` (lib.rs:352) | MATCH |
| 2 | `parallel_vacuum_end` (435) | `parallel_vacuum_end` (lib.rs:566) | **FIXED** (stats out-param) |
| 3 | `parallel_vacuum_get_dead_items` (466) | `parallel_vacuum_get_dead_items` (lib.rs:609) | MATCH |
| 4 | `parallel_vacuum_reset_dead_items` (474) | `parallel_vacuum_reset_dead_items` (lib.rs:618) | MATCH |
| 5 | `parallel_vacuum_bulkdel_all_indexes` (499) | `..._bulkdel_all_indexes` (lib.rs:649) | MATCH |
| 6 | `parallel_vacuum_cleanup_all_indexes` (518) | `..._cleanup_all_indexes` (lib.rs:670) | MATCH |
| 7 | `parallel_vacuum_compute_workers` (548) | `..._compute_workers` (lib.rs:721) | MATCH |
| 8 | `parallel_vacuum_process_all_indexes` (610) | `..._process_all_indexes` (lib.rs:792) | MATCH |
| 9 | `parallel_vacuum_process_safe_indexes` (773) | `..._process_safe_indexes` (lib.rs:967) | MATCH |
| 10 | `parallel_vacuum_process_unsafe_indexes` (827) | `..._process_unsafe_indexes` (lib.rs:1014) | MATCH |
| 11 | `parallel_vacuum_process_one_index` (864) | `..._process_one_index` (lib.rs:1047) | MATCH |
| 12 | `parallel_vacuum_index_is_parallel_safe` (950) | `..._index_is_parallel_safe` (lib.rs:1137) | MATCH |
| 13 | `parallel_vacuum_main` (988) | `parallel_vacuum_main` (lib.rs:1170) | MATCH |
| 14 | `parallel_vacuum_error_callback` (1118) | `..._error_callback` (lib.rs:1298) | MATCH |

## 2. Per-function notes

### 1. `parallel_vacuum_init` — MATCH
Asserts (nrequested>=0, nindexes>0) → `palloc0(will_parallel_vacuum)` →
`compute_workers` → NULL-return on `parallel_workers <= 0`. The DSM-sizing
sequence is byte-faithful: INDEX_STATS chunk (`sizeof(PVIndStats) * nindexes`)
+1 key, SHARED chunk (`sizeof(PVShared)`) +1 key, BUFFER_USAGE
(`sizeof(BufferUsage) * nworkers`) +1 key, WAL_USAGE (`sizeof(WalUsage) *
nworkers`) +1 key, then QUERY_TEXT (`querylen + 1`) +1 key only when
`debug_query_string` is set, all via the parallel-infra estimate seams in the
same order as the C `shm_toc_estimate_*` calls. `nindexes_mwm` accumulation +
the three `nindexes_parallel_*` counters guarded by `will_parallel_vacuum[i]`
match. Both `Assert`s on vacoptions (cleanup-mutual-exclusion +
`<= VACUUM_OPTION_MAX_VALID_VALUE`) are preserved as `debug_assert!`. Shared
fields set in C order; `maintenance_work_mem_worker = (nindexes_mwm>0) ? mwm /
Min(parallel_workers, nindexes_mwm) : mwm` and `max_bytes = vac_work_mem *
1024` exact. `relid = rel` is correct under the handle model
(`RelationGetRelid(rel) == rel`, the Oid). `ring_nbuffers =
GetAccessStrategyBufferCount(bstrategy)` uses the **real** seam
`vac::get_access_strategy_buffer_count` (returns the strategy's buffer count;
NOT a re-derived approximation). The three atomics init to 0. The shm_toc
inserts are mirrored as the in-process DSM side-table writes (see §3).

### 2. `parallel_vacuum_end` — FIXED
**Real divergence found.** C's signature is `void parallel_vacuum_end(pvs,
IndexBulkDeleteResult **istats)` where `istats[]` is an OUT-parameter: lines
441–452 copy `pvs->indstats[i].istat` into `istats[i]` (or NULL when
`!istat_updated`) **before** teardown. This is the *only* path that delivers
the parallel pass's per-index bulk-deletion stats back to the lazy-vacuum
driver (`vacrel->indstats[]`, consumed by `update_index_statistics`). The
original port took `_indstats: Vec<...>` *by value* and **discarded** it, so the
accumulated stats were stranded in the registry and dropped — index stats for
parallel-processed indexes would never reach `pg_class`. **Fix:** re-signed the
seam (`backend-access-heap-vacuumlazy-seams`) to
`fn(pvs) -> PgResult<Vec<Option<IndexBulkDeleteResult>>>`; the owner now builds
that vector from `state.indstats[i]` (`Some(istat)` when `istat_updated` else
`None`) and the consumer (`vacuumlazy::dead_items_cleanup`) assigns it into
`vacrel.indstats`. Teardown order unchanged and matches C: TidStoreDestroy →
DestroyParallelContext → ExitParallelMode → drop owned state.

### 3. `parallel_vacuum_get_dead_items` — MATCH
Returns `(dead_items, shared.dead_items_info)`. C returns the TidStore* and
sets `*dead_items_info_p = &pvs->shared->dead_items_info`; the repo returns the
info by value (the handle model — `VacDeadItemsInfo` is a small POD).

### 4. `parallel_vacuum_reset_dead_items` — MATCH
Destroy + recreate TidStore with the same `max_bytes` and
`LWTRANCHE_PARALLEL_VACUUM_DSA`, refresh both DSA handles, `num_items = 0`.
Extra `dsm_store_shared` keeps the worker snapshot consistent with the new
handle (in-process analogue of the DSM being live-shared). Faithful.

### 5–6. bulkdel/cleanup `_all_indexes` — MATCH
Set `reltuples`/`estimated_count` (bulkdel: `estimated_count = true`; cleanup:
caller-supplied) then dispatch `process_all_indexes(.., vacuum=true|false)`.
`IsParallelWorker()` assert preserved.

### 7. `parallel_vacuum_compute_workers` — MATCH
`!IsUnderPostmaster || max_parallel_maintenance_workers == 0 → 0`. Per-index
loop skips `VACUUM_OPTION_NO_PARALLEL` or `RelationGetNumberOfBlocks(indrel) <
min_parallel_index_scan_size`, else sets `will_parallel_vacuum[i]` and bumps
`nindexes_parallel_bulkdel` / `nindexes_parallel_cleanup` (cleanup counts
PARALLEL_CLEANUP **or** COND_CLEANUP). `nindexes_parallel = Max(bulkdel,
cleanup); nindexes_parallel--; if <= 0 return 0; parallel_workers = nrequested>0
? Min(nrequested, np) : np; Min(parallel_workers,
max_parallel_maintenance_workers)`. Byte-for-byte. The blocks compare uses `as
u32` matching `RelationGetNumberOfBlocks` (BlockNumber) vs the GUC.

### 8. `parallel_vacuum_process_all_indexes` — MATCH
`new_status`/`nworkers` selection (bulkdel → nindexes_parallel_bulkdel;
cleanup → nindexes_parallel_cleanup, +condcleanup when num_index_scans==0),
`nworkers--` (leader participates), `Min(nworkers, pcxt->nworkers)`. Per-index
status set + `parallel_workers_can_process = will_parallel_vacuum[i] &&
index_is_parallel_safe(...)` with the `status == INITIAL` assert. `idx`
atomic reset to 0. Worker-launch block: `ReinitializeParallelDSM` only when
`num_index_scans > 0`; cost_balance/active_nworkers written **before** launch;
`ReinitializeParallelWorkers(pcxt, nworkers)` (the new keystone seam — see §4);
`LaunchParallelWorkers`; on `nworkers_launched > 0` reset leader cost locals and
enable shared cost balance/active-nworkers; ngettext singular/plural by
`nworkers_launched` matches C exactly (singular iff `== 1`). Then unsafe-index
leader pass, safe-index join, wait+`InstrAccumParallelQuery` per launched
worker, the COMPLETED assertion loop (elog ERROR with relname when an index
isn't completed) + reset to INITIAL, and the shared-cost-balance carry-back +
disable. Order and conditions all faithful.

### 9–10. process_safe / process_unsafe — MATCH
Safe: optional active-nworkers++/--, `for(;;)` `fetch_add(idx,1)`, break on
`idx >= nindexes`, skip when `!parallel_workers_can_process`, else
`process_one_index`. Unsafe (leader only, with assert): linear scan skipping
indexes that ARE parallel-safe. Both bracket with the active-nworkers
increment/decrement under `VacuumActiveNWorkers` set. Atomic orderings are
Relaxed (acceptable: in-process model; C uses pg_atomic which is process-shared
but unordered here).

### 11. `parallel_vacuum_process_one_index` — MATCH
`istat = istat_updated ? &indstats->istat : NULL`. `IndexVacuumInfo` built with
all 8 fields (index, heaprel, analyze_only=false, report_progress=false,
message_level=DEBUG2, estimated_count, num_heap_tuples=reltuples, strategy).
Error-traceback `indname`/`status` set. Switch: NEED_BULKDELETE →
`vac_bulkdel_one_index(ivinfo, istat, dead_items, &dead_items_info)`;
NEED_CLEANUP → `vac_cleanup_one_index(ivinfo, istat)`; default → elog ERROR
with status+relname. First-cycle copy-out into `indstats->istat` +
`istat_updated = true` (the seam returns an owned value, so no local pfree —
correctly noted). `status = COMPLETED`, reset traceback, then
`pgstat_progress_parallel_incr_param(PROGRESS_VACUUM_INDEXES_PROCESSED, 1)`.

### 12. `parallel_vacuum_index_is_parallel_safe` — MATCH
vacuum → `(vacoptions & PARALLEL_BULKDEL) != 0`. Else false unless
CLEANUP|COND_CLEANUP set; then false when `num_index_scans > 0 &&
COND_CLEANUP`; else true. Exact.

### 13. `parallel_vacuum_main` — MATCH
PROC_IN_VACUUM assert, DEBUG1 start log, SHARED lookup, QUERY_TEXT lookup →
`debug_query_string` + `pgstat_report_activity(STATE_RUNNING, ...)`,
`pgstat_report_query_id(queryid, false)`, `table_open(relid,
ShareUpdateExclusiveLock)`, `vac_open_indexes(.., RowExclusiveLock)`, apply
`maintenance_work_mem_worker` when > 0, INDEX_STATS lookup, `TidStoreAttach`,
`VacuumUpdateCosts` + cost-balance setup pointing at the shared atomics, build
the worker `pvs` (relnamespace via `get_namespace_name(RelationGetNamespace)`,
relname via pstrdup), per-worker access strategy
`GetAccessStrategyWithSize(BAS_VACUUM, ring_nbuffers * (BLCKSZ/1024))`, push
error context, `InstrStartParallelQuery`, `process_safe_indexes`, end-query
instr at `ParallelWorkerNumber`, optional `PROGRESS_VACUUM_DELAY_TIME` report
under `track_cost_delay_timing`, `TidStoreDetach`, pop error context,
`vac_close_indexes` + `table_close` + `FreeAccessStrategy`. Full teardown order
preserved.

### 14. `parallel_vacuum_error_callback` — MATCH
NEED_BULKDELETE → `"while vacuuming index \"%s\" of relation \"%s.%s\""`;
NEED_CLEANUP → `"while cleaning up ..."`; INITIAL/COMPLETED → no context.
Format strings exact.

## 3. Handle / DSM model (inherited opacity — not introduced)

The Oid-relation / `TidStore` / `StrategyHandle` / `ParallelContextHandle` /
`ParallelVacuumStateHandle` handles are the **inherited** vacuumlazy/parallel
contract, not new opacity invented here. The owned
`ParallelVacuumState`/`PVShared`/`PVIndStats` live in a process-global
thread-local registry keyed by the handle id and never cross a seam by value.
The leader→worker DSM handoff (`PARALLEL_VACUUM_KEY_{SHARED,QUERY_TEXT,
BUFFER_USAGE,WAL_USAGE,INDEX_STATS}`) is mirrored as a crate-private in-process
side table (`DSM`) holding the typed snapshots the untyped `shm_toc_lookup`
helper cannot carry; the actual `shm_toc_estimate_*` sizing is still driven
through the real parallel-infra seams in C order. This is the same in-process
register/lookup model the seam machinery itself uses — a faithful rendering of
the DSM, not a divergence.

## 4. Seams

~40 outward seams into `backend-commands-vacuum-seams` (owner
`backend-commands-vacuum`, status `ported` — not complete, so these are
legitimately seam-and-panic until the owner lands; guard-exempt). The 6 inward
`parallel_vacuum_*` seams are declared in `backend-access-heap-vacuumlazy-seams`
and installed cross-crate by this crate's `init_seams()` (cleared by the
guard's `installed.contains` cross-crate-install rule). The one **new** seam
`reinitialize_parallel_workers` (declared in
`backend-access-transam-parallel-seams`, owner `merged`) is installed by its
real owner `backend-access-transam-parallel` (lib.rs:2169) with a body matching
C `ReinitializeParallelWorkers` (`pcxt->nworkers_to_launch = Min(pcxt->nworkers,
nworkers_to_launch)`). `GetAccessStrategyBufferCount` is the real seam, not a
re-derivation.

## 5. Gates

- `cargo check --workspace` — PASS (exit 0).
- `cargo test -p no-todo-guard` — PASS (no `todo!`/`unimplemented!`).
- `cargo test -p seams-init` — PASS (both recurrence guards;
  CONTRACT_RECONCILE parity green, no stale/missing entries).
- `cargo test -p backend-commands-vacuumparallel --no-run` — PASS.

## 6. Fixes applied during audit

1. **`parallel_vacuum_end` stats out-parameter** (3-crate contract reconcile):
   re-signed the seam to return `Vec<Option<IndexBulkDeleteResult>>`, owner
   builds it from the per-index `istat_updated`/`istat`, consumer
   (`vacuumlazy::dead_items_cleanup`) stores it into `vacrel.indstats`. Restores
   the C `istats[]` out-parameter semantics so parallel-pass index stats reach
   the driver. Resolved in-place (no CONTRACT_RECONCILE_PENDING entry).
