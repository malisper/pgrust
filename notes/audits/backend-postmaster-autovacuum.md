# Audit: backend-postmaster-autovacuum

- **Date:** 2026-06-13
- **Model:** Opus 4.8 (1M context) — `claude-opus-4-8[1m]`
- **Branch:** `port/backend-postmaster-autovacuum`
- **C source (`c_sources`):** `src/backend/postmaster/autovacuum.c` (PostgreSQL 18.3)
- **c2rust:** `../pgrust/c2rust-runs/backend-postmaster-autovacuum/src/autovacuum.rs`
- **Port:** `crates/backend-postmaster-autovacuum/src/{lib,core,shmem,cost_balance,schedule,launcher,worker}.rs`

## Top-line verdict: **PASS**

This is an independent, from-scratch re-audit. The whole unit is a faithful 1:1
port: the scheduling/scoring/threshold/balancing math is byte-for-byte, and the
five findings of the 2026-06-12 FAIL audit (M1, D1, D2, D3, D4) are all resolved
on this branch (fix commit `10c8ed5d`). Every function is `MATCH` or properly
`SEAMED`; the owned seam crate is fully installed; no §3b violations.

## Function inventory (31 function definitions in autovacuum.c, lines 368–3475)

| # | C function | C lines | Port location | Verdict | Notes |
|---|-----------|---------|---------------|---------|-------|
| 1 | `AutoVacLauncherMain` | 368–741 | launcher.rs | MATCH | Pre-loop lifecycle (InitProcess/BaseInit/sigsetjmp/SetConfigOption) is out-of-crate; scheduling-loop body ported 1:1. Emergency `do_start_worker()` routed via worker emergency-start. |
| 2 | `ProcessAutoVacLauncherInterrupts` | 747–786 | launcher.rs | MATCH | barrier/log-memctx/catchup interrupts folded into a thin foreign seam. |
| 3 | `AutoVacLauncherShutdown` | 792–799 | launcher.rs | MATCH | launcherpid=0, proc_exit(0). |
| 4 | `launcher_determine_sleep` | 809–877 | launcher.rs | MATCH | Recursion-once, MIN/MAX clamps, tail-element pick faithful. |
| 5 | `rebuild_database_list` | 893–1068 | launcher.rs | MATCH | Scoring/dedup/ordering match. `millis_increment` is now `i32`, truncated before the `<= MIN` compare and on the `*1.1` store, mirroring C's `int millis_increment` (D4 fixed). |
| 6 | `db_comparator` | 1072–1076 | launcher.rs | MATCH | `pg_cmp_s32(adl_score)` → `adl_score.cmp`. |
| 7 | `do_start_worker` | 1090–1288 | launcher.rs | MATCH | Wrap-priority loop, force limits (`wrapping_sub`), skip-recent, freelist pop, startingWorker set faithful. |
| 8 | `launch_worker` | 1302–1346 | launcher.rs | MATCH | Update-or-rebuild; `dlist_move_head` → `remove`+`insert(0,..)`. |
| 9 | `AutoVacWorkerFailed` | 1354–1357 | launcher.rs | MATCH | Sets `av_signal[AutoVacForkFailed]`. |
| 10 | `avl_sigusr2_handler` | 1361–1365 | launcher.rs | MATCH | Sets `got_SIGUSR2`; SetLatch via runtime signal layer. |
| 11 | `AutoVacWorkerMain` | 1376–1600 | worker.rs | MATCH | Pre-setup out-of-crate; slot-claim/running-list/startingWorker-clear/launcher-wake/pgstat/recentXid/recentMulti/`do_autovacuum` body faithful. |
| 12 | `FreeWorkerInfo` | 1606–1645 | worker.rs | MATCH | on_shmem_exit callback; field clears, freelist push, AutoVacRebalance set. |
| 13 | `VacuumUpdateCosts` | 1654–1712 | cost_balance.rs | MATCH | Branch order, failsafe assert, DEBUG2 lock-guarded log faithful. (`void` → `PgResult<()>` mirrors elog/lock surface.) |
| 14 | `AutoVacuumUpdateCostLimit` | 1723–1758 | cost_balance.rs | MATCH | storage-param vs GUC, dobalance early-return, `Max(limit/n,1)`, `elog(ERROR)` on n<=0. |
| 15 | `autovac_recalculate_workers_for_balance` | 1769–1794 | cost_balance.rs | MATCH | running-worker walk, wi_proc/dobalance skip, conditional write. |
| 16 | `get_database_list` | 1809–1882 | ext-seam | SEAMED | pg_database seqscan body is entirely foreign (table_open/heap_getnext/transaction). Thin delegate. |
| 17 | `do_autovacuum` | 1885–2599 | worker.rs `do_autovacuum` | MATCH | Two-pass pg_class collection, concurrent-worker skip, claim/recheck/cost/dobalance/VacuumUpdateCosts, work-item drain all faithful. **D1 fixed**: per-table error isolation reproduced — on Ok clears QueryCancelPending; on Err builds autovacuum's own `errcontext("automatic {vacuum,analyze} of table …")` in-crate (VACOPT_VACUUM branch), delegates the foreign PG_CATCH body (HOLD_INTERRUPTS/EmitErrorReport/AbortOutOfAnyTransaction/FlushErrorState/reset PortalContext/StartTransactionCommand/RESUME_INTERRUPTS) to one seam, then continues the loop (no `?` escape). `did_vacuum`/AutovacMemCxt switch match. **D2 fixed**: orphan-temp recheck/drop control flow ported in-crate over fine-grained leaf seams — recheck predicates (relkind/relpersistence, `temp_namespace_is_idle`), conditional locks, LOG decision, push/pop snapshot, commit/start, memctx switch all in-crate; only the foreign `performDeletion` leaf is a seam. **M1 fixed**: both call sites (2036/2116) use `core::extract_autovac_opts`. |
| 18 | `perform_work_item` | 2605–2705 | schedule.rs | MATCH | name-lookup-skip, report, portal reset, BRIN-summarize dispatch, unrecognized-type WARNING. brin_summarize_range foreign (seam). |
| 19 | `extract_autovac_opts` | 2719–2737 | core.rs `extract_autovac_opts` | MATCH | **M1 fixed.** Carriers now hand the raw `extractRelOptions` result as `Option<StdRdOptions>`; in-crate function does the relkind assert + `.autovacuum` projection (the `memcpy(av, &…->autovacuum)` C step). Called at all three C call sites. |
| 20 | `table_recheck_autovac` | 2749–2889 | schedule.rs | MATCH | reloptions fallback (toast→main) now via `extract_autovac_opts`, VACOPT bitset, freeze-age selection, dobalance, name fields faithful. |
| 21 | `recheck_relation_needs_vacanalyze` | 2900–2925 | schedule.rs | MATCH | pgstat fetch (seam), delegate, toast doanalyze=false. |
| 22 | `relation_needs_vacanalyze` | 2967–3163 | schedule.rs | MATCH | All threshold/scale/`>=-1`/Min-clamp/wraparound (`wrapping_sub`)/pcnt_unfrozen/stat-or-force decision/pg_statistic guard verified 1:1. |
| 23 | `autovacuum_do_vac_analyze` | 3173–3199 | ext-seam | SEAMED | Body is RangeVar/VacuumRelation node-build + `vacuum()` over the unported vacuum executor + memctx. Thin delegate. |
| 24 | `autovac_report_activity` | 3213–3241 | schedule.rs | MATCH | Command/options string, qualified name, wraparound suffix; suffix bounded via shared `snprintf_append` to `MAX_AUTOVAC_ACTIV_LEN`. |
| 25 | `autovac_report_workitem` | 3248–3280 | schedule.rs | MATCH | **D3 fixed**: ` nsp.rel blk` suffix bounded by `snprintf_append(.., MAX_AUTOVAC_ACTIV_LEN, ..)`, capping the total like C's `snprintf(activity+len, MAX-len, …)`. |
| 26 | `AutoVacuumingActive` | 3288–3293 | shmem.rs | MATCH | `start_daemon && track_counts`. |
| 27 | `AutoVacuumRequestWork` | 3300–3333 | shmem.rs | MATCH | Lock, first-unused slot fill, result. |
| 28 | `autovac_init` | 3342–3352 | shmem.rs | MATCH | start_daemon guard, track_counts WARNING+hint, else check_av_worker_gucs. |
| 29 | `AutoVacuumShmemSize` | 3359–3371 | shmem.rs | SEAMED | Byte layout (`sizeof`/MAXALIGN/add_size/mul_size) owned by substrate; port returns worker-slot count that drives it. |
| 30 | `AutoVacuumShmemInit` | 3378–3417 | shmem.rs → ext-seam | SEAMED | ShmemInitStruct + `!IsUnderPostmaster` free-list/atomic seeding run in the substrate against fresh shmem. |
| 31 | `check_autovacuum_work_mem` | 3423–3443 | shmem.rs | MATCH | -1 passthrough, clamp `<64 → 64`. |
| 32 | `av_worker_available` | 3449–3460 | shmem.rs | MATCH | `free_slots > Max(0, worker_slots - max_workers)`. |
| 33 | `check_av_worker_gucs` | 3466–3475 | shmem.rs | MATCH | `worker_slots < max_workers` → WARNING with ERRCODE_INVALID_PARAMETER_VALUE + errdetail; format strings verified. |

(The C file defines 31 function bodies at lines 368–3475; rows above enumerate
every one, with the two prototype-only forward declarations folded into their
bodies. No function is uncovered.)

## Re-derivation of the previously-failing findings (verified resolved)

- **M1 (`extract_autovac_opts` MISSING) → resolved.** `core::extract_autovac_opts(relkind,
  relopts: Option<StdRdOptions>) -> Option<AutoVacOpts>` performs the relkind assert
  and the `.autovacuum` projection in-crate. The scan/recheck carriers
  (`PgClassScanRow`, `OrphanClassRow`, recheck row) now carry the raw
  `extractRelOptions` result as `Option<StdRdOptions>` (foreign reloptions parse
  only), and the projection is in-crate at all three C call sites (do_autovacuum
  2036/2116, table_recheck_autovac 2772). Unit-tested.
- **D1 (per-table error isolation) → resolved.** `do_autovacuum` matches
  `autovacuum_do_vac_analyze` instead of `?`-propagating: Ok clears
  `QueryCancelPending`; Err adorns the in-flight error with autovacuum's own
  errcontext line (vacuum vs analyze on `VACOPT_VACUUM`) and hands it to a foreign
  emit-report-and-restart seam, then the loop continues. `did_vacuum = true` and the
  AutovacMemCxt switch are inside the `relname/nspname/datname all-present` block,
  matching C's `goto deleted` skip.
- **D2 (orphan recheck/drop dissolved into a seam) → resolved.** The orphan loop is
  ported in-crate: `CHECK_FOR_INTERRUPTS`, `ConditionalLockRelationOid`, re-fetch
  class row, the relkind/relpersistence + `TEMP_NAMESPACE_IDLE` recheck predicates,
  `ConditionalLockDatabaseObject`, the `LOG("autovacuum: dropping orphan temp
  table …")` decision, push/`performDeletion`/pop, per-table commit/start, memctx
  switch — all in-crate over fine-grained leaf seams; only `performDeletion` itself
  is foreign.
- **D3 (`autovac_report_workitem` truncation) → resolved.** Shared `snprintf_append`
  bounds the appended suffix to `MAX_AUTOVAC_ACTIV_LEN - 1` (char-boundary safe),
  exactly mirroring C's `snprintf(activity+len, MAX_AUTOVAC_ACTIV_LEN - len, …)`.
  Used by both report functions. Unit-tested (`snprintf_append_bounds_total_like_c`).
- **D4 (`rebuild_database_list` millis truncation) → resolved.** `millis_increment`
  is `i32`; `(1000.0 * naptime / nelems) as i32` truncates before the `<= MIN`
  compare, and `(MIN_AUTOVAC_SLEEPTIME * 1.1) as i32` = 110, matching C's
  `int millis_increment`.

## Seam audit (SKILL §3)

**Owned seam crates (by C-source coverage):** `crates/backend-postmaster-autovacuum-seams`
maps to `autovacuum.c`. It declares exactly the two child-launch entry points
(`auto_vac_launcher_main`, `auto_vac_worker_main`), both `-> !`. The crate's
`init_seams()` (lib.rs) installs **both** via `set()` and contains nothing else;
`seams-init::init_all()` calls it. No uninstalled owned-seam declarations, no
`set()` outside the owner. **Clean.**

`crates/backend-postmaster-autovacuum-ext-seams` is the unit's external-dependency
seam crate (autovacuum-owned shmem accessors + the genuinely-foreign
vacuum/pgstat/catalog/lock/xact surface). Its installers belong to the
(mostly unported) owners and panic loudly until then; the consolidation is banked
in DESIGN_DEBT.md. The fix added fine-grained leaf seams
(`conditional_lock_relation_oid_exclusive`, `unlock_relation_oid_exclusive`,
`orphan_recheck_fetch_class_row`, `temp_namespace_is_idle`,
`conditional_lock_namespace_object_share`, `perform_deletion_orphan_temp_table`,
`emit_report_and_restart_after_table_error`) — each a thin marshal+delegate, with
the decision logic now in-crate. Spot-checked: no seam path carries branching, node
construction, or computation. No logic-across-the-seam findings remain.

## Design conformance (SKILL §3b)

- Per-backend C file-statics (`MyWorkerInfo`, `recentXid`/`recentMulti`, GUC knobs,
  `av_storage_param_*`, `AutovacuumLauncherPid`, `default_*_age`) are `thread_local!`
  cells, not shared statics — conforms to "Backend-global state."
- Process-local `DatabaseList` / `orphan_oids` / `table_toast_map` are per-backend
  `Vec`s mirroring the C order; no registry-shaped side table.
- Shmem `WorkerInfoData`/`AutoVacuumWorkItem`/`AutoVacuumShmemStruct` are addressed
  by index through accessor seams (substrate owns layout); the crate never holds
  `&mut WorkerInfoData`. No invented opacity — the new `OrphanClassRow` carrier is a
  plain row projection of real pg_class fields, and `relopts: Option<StdRdOptions>`
  is the real reloptions type, not a stand-in.
- Allocating/erroring seams carry `PgResult`; lock acquire/release are explicit on
  both sides of each body and not held across `?`. No ambient-global seam abuse.
- The errcontext adornment uses `PgError::add_context_line` (real error type), not a
  side channel. No unledgered divergence markers introduced.

No §3b violations.

## Build / test gate

`cargo check -p backend-postmaster-autovacuum -p types-autovacuum` is green
(only pre-existing rustdoc warnings in a dependency). `cargo test -p
backend-postmaster-autovacuum` → 13 passed, 0 failed (covers extract_autovac_opts,
snprintf_append bound, and the relation_needs_vacanalyze decision math).

## Conclusion

**PASS.** Every function is `MATCH` or properly `SEAMED`; the owned seam crate is
fully installed; all five prior findings (M1/D1/D2/D3/D4) are independently
confirmed resolved; no §3b violations. CATALOG.tsv may mark this unit `audited`.
