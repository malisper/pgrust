# Audit: backend-replication-logical-slotsync

- **Top-line verdict:** **PASS**
- **Date:** 2026-06-13
- **Model:** Opus 4.8 (1M)
- **Branch:** port/backend-replication-logical-slotsync
- **Audited against:** `slotsync.c` (postgres-18.3) and the c2rust rendering,
  with the unit reconciled to current `refs/heads/main`'s authoritative owner
  seam contracts (main merged into the branch in this worktree).

## Scope

This re-audit follows the 2026-06-13 FAIL, which reconciled the shared owner
seam crates to main's contracts but left the 1542-line unit calling the old
seam vocabulary (it did not compile). This pass completed the call-site
reconciliation and re-derived every function from the C.

## Per-function table

All 27 function definitions in slotsync.c, cross-checked against the c2rust
output. Verdict legend per the audit skill.

| # | C function (slotsync.c:line) | Port (lib.rs) | Verdict | Notes |
|---|------------------------------|---------------|---------|-------|
| 1 | update_local_synced_slot (167) | update_local_synced_slot | MATCH | MyReplicationSlot site; persistency LOG/DEBUG1 ternary, precedes-return, SnapBuildSnapshotExists spinlock-direct vs LogicalSlotAdvanceAndCheckSnapState branch, confirmed_flush sanity ERROR, config-update spinlock block, mark-dirty/save, effective_catalog_xmin + compute calls — faithful via implicit accessors. Owner-hoisted `wal_segment_size`/`MyDatabaseId` args to logical_slot_advance threaded at the call site. |
| 2 | get_local_synced_slots (349) | get_local_synced_slots | MATCH | ControlLock SHARED array scan; in_use && synced; SlotIsLogical assert; by-handle; try_reserve OOM. |
| 3 | local_sync_slot_required (380) | local_sync_slot_required | MATCH | name compare, spin-guarded locally_invalidated predicate, break. By-handle. |
| 4 | drop_local_obsolete_slots (433) | drop_local_obsolete_slots | MATCH | LockSharedObject→guard, spin synced-recheck, conditional Acquire+DropAcquired, UnlockSharedObject→guard.release(), LOG. |
| 5 | reserve_wal_for_local_slot (490) | reserve_wal_for_local_slot | MATCH | MyReplicationSlot site; AllocationLock EXCLUSIVE, min_safe_lsn = min(redo, slotmin), Max(restart,min_safe) under mutex, compute_required_lsn, XLByteToSeg removed-segno ERROR. |
| 6 | update_and_persist_local_synced_slot (562) | update_and_persist_local_synced_slot | MATCH | precedes early-false, !found_consistent LOG+false, persist, sync-ready LOG. |
| 7 | synchronize_one_slot (626) | synchronize_one_slot | MATCH | latestFlushPtr ahead check (AmLogicalSlotSyncWorkerProcess ? LOG : ERROR+SQLSTATE); SearchNamedReplicationSlot by-handle path: synced ERROR, ReplicationSlotAcquire then `slot == MyReplicationSlot` so subsequent access via implicit accessors (invalidated copy under mutex, skip-invalidated release, RS_TEMPORARY persist-vs-sync, confirmed_flush sanity ERROR); else create branch (RS_TEMPORARY, MyDatabaseId, db+plugin mutex, reserve_wal, Control+ProcArray xmin_horizon block, compute_required_xmin(true), persist); final ReplicationSlotRelease. |
| 8 | synchronize_slots (810) | synchronize_slots | MATCH | slotRow OID array, started_tx, walrcv_exec, WALRCV_OK_TUPLES status check, MakeSingleTupleTableSlot→make_result_tupslot, tuplestore_gettupleslot(true,false)→result_gettupleslot loop, per-column ++col slot_getattr decode with null handling, RS_EPHEMERAL skip predicate, drop_local_obsolete_slots, per-slot lock/sync/unlock, walrcv_clear_result, commit. |
| 9 | validate_remote_info (956) | validate_remote_info | MATCH | quote_literal_cstr cmd, exec, status check, single-tuple fetch, remote_in_recovery (FEATURE_NOT_SUPPORTED), primary_slot_valid (INVALID_PARAMETER_VALUE), clear, commit. |
| 10 | CheckAndGetDbnameFromConninfo (1034) | CheckAndGetDbnameFromConninfo | MATCH | walrcv_get_dbname_from_conninfo; None → INVALID_PARAMETER_VALUE. |
| 11 | ValidateSlotSyncParams (1061) | ValidateSlotSyncParams | MATCH | wal_level<logical, primary_slot_name empty, hot_standby_feedback off, primary_conninfo empty — each report_at(elevel, INVALID_PARAMETER_VALUE)+false; report_at surfaces Err at elevel>=ERROR. |
| 12 | slotsync_reread_config (1128) | slotsync_reread_config | MATCH | old/new conninfo/slotname/sync_slots/hsf reads, SetConfigReloadPending(false), ProcessConfigFile, sync-slots-disabled LOG+proc_exit, param-change LOG + last_start_time=0 (lock-free) + proc_exit. |
| 13 | ProcessSlotSyncInterrupts (1177) | ProcessSlotSyncInterrupts | MATCH | CHECK_FOR_INTERRUPTS, spin stop_signaled read, promotion LOG+proc_exit, ConfigReloadPending→reread. |
| 14 | slotsync_worker_disconnect (1199) | slotsync_worker_disconnect | MATCH | DatumGetPointer(arg)→WalReceiverConn(as_usize), walrcv_disconnect. |
| 15 | slotsync_worker_onexit (1212) | slotsync_worker_onexit | MATCH | release if MyReplicationSlot set, cleanup(false), spin pid=InvalidPid, syncing_slots cleanup. |
| 16 | wait_for_slot_activity (1258) | wait_for_slot_activity | MATCH | sleep_ms doubling (cap MAX) / reset MIN, WaitLatch, WL_LATCH_SET reset. |
| 17 | check_and_set_sync_info (1293) | check_and_set_sync_info | MATCH | spin, asserts, stopSignaled ERROR, syncing ERROR (both OBJECT_NOT_IN_PREREQUISITE_STATE), set syncing+pid, syncing_slots=true. |
| 18 | reset_syncing_flag (1337) | reset_syncing_flag | MATCH | spin syncing=false, syncing_slots=false. |
| 19 | ReplSlotSyncWorkerMain (1353) | ReplSlotSyncWorkerMain + _inner | MATCH | (&StartupData)->! wrapper over PgResult inner (the repo worker-main idiom; C's sigsetjmp ERROR→proc_exit recovery maps to the daemon-boundary panic). Bootstrap order faithful: B_SLOTSYNC_WORKER, init_ps_display, InitProcess, BaseInit, assert ctx, signal setup, check_and_set_sync_info(MyProcPid), LOG, before_shmem_exit(onexit), InitializeTimeouts, load libpqwalreceiver, unblock signals, search_path empty, dbname, InitPostgres, NormalProcessing, app_name, walrcv_connect(replication=false), connect-fail CONNECTION_FAILURE, before_shmem_exit(disconnect), validate_remote_info, the for(;;) loop. |
| 20 | update_synced_slots_inactive_since (1535) | update_synced_slots_inactive_since | MATCH | !StandbyMode return, pid/syncing assert, ControlLock SHARED scan, in_use&&synced, SlotIsLogical+active_pid==0 asserts, now-once, ReplicationSlotSetInactiveSince(s,now,true). |
| 21 | ShutDownSlotSync (1585) | ShutDownSlotSync | MATCH | stopSignaled=true, !syncing early update+return, worker_pid, kill SIGUSR1, wait loop (WaitLatch 10ms, WL_LATCH_SET reset+CHECK, spin recheck syncing/break), update. |
| 22 | SlotSyncWorkerCanRestart (1657) | SlotSyncWorkerCanRestart | MATCH | time(NULL) seconds, (unsigned)(cur-last) < INTERVAL throttle, last_start_time update. |
| 23 | IsSyncingReplicationSlots (1677) | IsSyncingReplicationSlots | MATCH | returns syncing_slots. |
| 24 | SlotSyncShmemSize (1686) | SlotSyncShmemSize | MATCH | size_of SlotSyncCtxStruct. |
| 25 | SlotSyncShmemInit (1695) | SlotSyncShmemInit | MATCH | ShmemInitStruct, !found → zero + pid=InvalidPid + SpinLockInit. |
| 26 | slotsync_failure_callback (1715) | slotsync_failure_callback | MATCH | release if set, cleanup(true), reset_syncing_flag if syncing, walrcv_disconnect. |
| 27 | SyncReplicationSlots (1752) | SyncReplicationSlots | MATCH | PG_ENSURE_ERROR_CLEANUP: body (check_and_set_sync_info(InvalidPid), validate_remote_info, synchronize_slots, cleanup(true), reset_syncing_flag); on Err run failure_callback then re-raise. |

File-scope state (SlotSyncCtxStruct shmem struct, RemoteSlot, the transam xid
macros, lsn helpers, sleep_ms/syncing_slots/SlotSyncCtx/sync_replication_slots
file statics): MATCH. Per-backend file statics are `thread_local`; the
SlotSyncCtxStruct control area is a real `#[repr(C)]` struct backed by
`ShmemInitStruct` with an embedded `slock_t`-shaped `Spinlock`, owned here.

## Seam audit

**Owned seam crate:** `backend-replication-logical-slotsync-seams` (slotsync.c
is the unit's only C source). All 9 declarations (shut_down_slot_sync,
validate_slot_sync_params, slot_sync_worker_can_restart,
is_syncing_replication_slots, sync_replication_slots,
check_and_get_dbname_from_conninfo, repl_slot_sync_worker_main,
slot_sync_shmem_size, slot_sync_shmem_init) are installed by the unit's
`init_seams()`, which contains only `set()` calls; `seams-init::init_all()`
calls it. No uninstalled or externally-set seam.

**Outward seam calls** are all justified by real dependency cycles and are thin
marshal+delegate. The unit reaches owner-resident objects only through value
handles (ReplicationSlotHandle, WalReceiverConn, WalRcvExecResult,
WalRcvResultTupslot) — opacity inherited from the C, never invented.

**Owner surfaces added this pass and verified installed:**
- `backend-replication-slot` (slot.c owner): the by-`ReplicationSlotHandle`
  array surface (max_replication_slots, replication_slot, search_named,
  set_inactive_since, slot_spin_acquire/release, slot_in_use/is_logical/
  data_synced/data_name/data_database/active_pid/data_invalidated) and the
  added MyReplicationSlot accessors/mutators (slot_persistency, slot_failover,
  slot_set_invalidated/database/failover) are all installed in the owner's
  `init_seams()` (81/81 declarations installed; thin field read/writes, no
  logic in the seam path; the search-named seam marshals Option→handle only).
- `backend-replication-libpqwalreceiver-seams` (libpqwalreceiver.c owner,
  genuinely unported on main): walrcv_exec / res_status / res_err /
  make_result_tupslot / result_gettupleslot / getattr_text/lsn/xid/bool /
  exec_clear_tuple / walrcv_clear_result / walrcv_get_dbname_from_conninfo, and
  the full-prototype walrcv_connect — declared in the proper owner,
  panic-until-owner-lands (acceptable: panicking on an unported *callee*).
- `types-walreceiver`: WalRcvExecStatus / WalRcvExecResult / WalRcvResultTupslot.
- `backend-replication-walreceiver-seams`: hot_standby_feedback (walreceiver.c
  GUC). `xlog-seams`: xlog_get_replication_slot_minimum_lsn. `walsender-seams`:
  get_standby_flush_rec_ptr. `procarray-seams`:
  get_oldest_safe_decoding_transaction_id. `dbcommands-seams`: get_database_oid.
  `lmgr` LockGuard release idiom for the Unlock pairing. `latch-seams`:
  kill_sigusr1. `xlogrecovery-seams`: standby_mode. `miscinit`/`guc`
  worker-bootstrap group (proper-owner aliases, panic-until-owner).
  `types-pgstat`: WAIT_EVENT_REPLICATION_SLOTSYNC_MAIN/_SHUTDOWN
  (PG_WAIT_ACTIVITY+11/+12, verified against wait_event_names.txt ordering).

## Design conformance

- Opacity inherited, never introduced: handles map 1:1 to C pointers/indices;
  no invented stand-ins.
- Allocating GUC-string reads (primary_conninfo/primary_slot_name) take `Mcx`
  and return `PgResult`; the unit reads them through a transient
  `MemoryContext` (faithful to C's `pstrdup` into the current context).
- Per-backend globals are `thread_local`; the only shared state
  (SlotSyncCtxStruct) is real shared memory.
- Heavyweight locks held across `?` use the lmgr `LockGuard` (release/abort drop
  mirror C's explicit unlock / transaction-abort release). Manual
  lwlock/spinlock acquire/release mirror the C's explicit pairs.
- Merge-artifact de-dup: the branch's `int` WAL_LEVEL_* copies in `types-wal`
  (which collided with main's canonical `WalLevel`-typed xlog_consts and broke
  `backend-storage-ipc-standby`) were removed; duplicate `relowner` fields in
  two unrelated test crates (brin-tuple, copyfromparse), introduced by the
  additive merge, were removed. xact-seams confirmed free of the previously
  de-duplicated start/commit_transaction_command duplicates.

## Gate

- `cargo check --workspace`: clean (0 errors).
- `cargo test --workspace`: clean (0 failed).

## Verdict

PASS. Every function MATCHes the C; all owned seams are installed; no logic
lives in any seam path; design rules hold. CATALOG row set to `audited`.
