# Audit: backend-postmaster-checkpointer

- **C source**: `src/backend/postmaster/checkpointer.c` (PostgreSQL 18.3, 1412 LOC)
- **c2rust**: `c2rust-runs/backend-postmaster-checkpointer/src/checkpointer.rs`
- **Port**: `crates/backend-postmaster-checkpointer/src/lib.rs`
- **Audit verdict**: **PASS (self-audit)**

Re-derived from the C and the src-idiomatic reference; comments/self-review not
trusted. The src-idiomatic base was reconciled to the repo's per-owner seam
model (central `seams_ub_postmaster::checkpointer` replaced by direct calls to
ported crates + per-owner `-seams`).

## 1. Function inventory (every C definition)

15 function definitions in checkpointer.c, all present:

| # | C function (line) | Port | Verdict | Notes |
|---|---|---|---|---|
| 1 | `CheckpointerMain` (182) | `CheckpointerMain` + `checkpointer_main_loop` + `checkpointer_abort_cleanup` | MATCH | `MyBackendType=B_CHECKPOINTER` + `AuxiliaryProcessMainCommon` (seams); pqsignal/sigprocmask block delegated to aux bootstrap; checkpointer_pid store; last_*_time init; before_shmem_exit(pgstat_before_server_shutdown); UpdateSharedMemoryConfig; ProcGlobal->checkpointerProc=MyProcNumber (seam); sigsetjmp realized as outer `Err` catch→abort cleanup + pg_usleep(1s); shutdown-checkpoint path (ShutdownXLOG seam + stats + PMSIGNAL_XLOG_IS_SHUTDOWN); final wait-for-shutdown loop. |
| 2 | `ProcessCheckpointerInterrupts` (642) | `ProcessCheckpointerInterrupts` | MATCH | ProcSignalBarrier; ConfigReloadPending→ProcessConfigFile(SIGHUP)+UpdateSharedMemoryConfig; LogMemoryContext. |
| 3 | `CheckArchiveTimeout` (684) | `CheckArchiveTimeout` | MATCH | archive_timeout/recovery guards; stale quick-check; GetLastSegSwitchData; important-rec-ptr>last_switch→RequestXLogSwitch(true); XLogSegmentOffset!=0 DEBUG1; idle-state update. |
| 4 | `ImmediateCheckpointRequested` (745) | `ImmediateCheckpointRequested` | MATCH | `ckpt_flags & CHECKPOINT_IMMEDIATE` (lockless flag read). |
| 5 | `CheckpointWriteDelay` (772) | `CheckpointWriteDelay` | MATCH | non-checkpointer no-op; on-schedule branch (config reload, AbsorbSyncRequests, CheckArchiveTimeout, report, 100ms WaitLatch); else absorb_counter decrement; barrier check. |
| 6 | `IsCheckpointOnSchedule` (842) | `IsCheckpointOnSchedule` | MATCH | progress*completion_target; cached-elapsed shortcut; WAL-segment progress vs CheckPointSegments; time progress vs CheckPointTimeout; gettimeofday. |
| 7 | `ReqShutdownXLOG` (921) | `ReqShutdownXLOG` | MATCH | sets ShutdownXLOGPending; SetLatch is host signal-shim. |
| 8 | `CheckpointerShmemSize` (938) | `CheckpointerShmemSize` | MATCH | requests_offset + Min(NBuffers,MAX)*sizeof(req) via add_size/mul_size seams. |
| 9 | `CheckpointerShmemInit` (960) | `CheckpointerShmemInit` | MATCH | shmem_init_struct; on !found MemSet 0 + SpinLockInit + max_requests + ConditionVariableInit x2. |
| 10 | `RequestCheckpoint` (1003) | `RequestCheckpoint` | MATCH | standalone inline CreateCheckPoint(IMMEDIATE)+smgrdestroyall; spinlock flag set + counter snapshot; latch-retry loop (MAX_SIGNAL_TRIES, CHECK_FOR_INTERRUPTS, 0.1s); CHECKPOINT_WAIT: start_cv then done_cv modulo-counter sleeps; failed-counter check. |
| 11 | `ForwardSyncRequest` (1153) | `ForwardSyncRequest` | MATCH | under-postmaster guard; checkpointer-process error; CommLock; full→Compact else false; insert; half-full→SetLatch after release. |
| 12 | `CompactCheckpointerRequestQueue` (1220) | `CompactCheckpointerRequestQueue` | MATCH | held-by-me assert; crit-section bail; skip_slot + (type,ftag) HashMap = HASH_BLOBS equivalence; forward dedup; in-place compaction; DEBUG1. |
| 13 | `AbsorbSyncRequests` (1330) | `AbsorbSyncRequests` | MATCH | non-checkpointer no-op; CommLock; copy ring (try_reserve_exact = palloc); clear count (START_CRIT_SECTION); release; RememberSyncRequest loop. |
| 14 | `UpdateSharedMemoryConfig` (1377) | `UpdateSharedMemoryConfig` | MATCH | SyncRepUpdateSyncStandbysDefined + UpdateFullPageWrites. |
| 15 | `FirstCallSinceLastCheckpoint` (1396) | `FirstCallSinceLastCheckpoint` | MATCH | spinlock read ckpt_done vs process-local first_call_ckpt_done. |

## 2. Shared memory faithfulness

`CheckpointerShmemStruct` is `#[repr(C)]` field-for-field with checkpointer.c
(pid, slock_t ckpt_lck, 4 ckpt_* ints, start_cv/done_cv, num/max_requests, then
the flexible `requests[]`). Placed by `shmem_init_struct`; `requests_offset()` =
`offsetof(struct, requests)`. `ckpt_lck` is a real `Spinlock` driven by the
s-lock crate with a RAII `SpinGuard` (release on drop). CVs are real
`ConditionVariable`s; the comm queue is under the built-in `CheckpointerCommLock`
(#11). Layout asserted by unit tests.

## 3. Seam boundaries

- **Real owners called directly / via -seams installed by COMPLETE owners**:
  shmem (shmem_init_struct/add_size/mul_size), latch, procsignal, pmsignal
  (NEW send_postmaster_signal_xlog_is_shutdown), lwlock, condition-variable,
  proc (NEW checkpointer_proc/set_checkpointer_proc_to_self), aio, bufmgr, fd,
  smgr, sync (NEW remember_sync_request), auxprocess, init-small, miscinit (NEW
  am_checkpointer_process/in_critical_section/set_my_backend_type_checkpointer),
  guc, mcxt, resowner, dynahash, waitevent, pgstat-wal, pgstat, port-pgsleep,
  activity-small (pending checkpointer stats + report). The 9 NEW seams on
  complete owners are all installed in their init_seams (seams-init recurrence
  guard green).
- **Seam-and-panic (owner unported)**: xlog-seams — create_checkpoint,
  create_restartpoint, shutdown_xlog, get_last_seg_switch_data,
  get_last_important_rec_ptr, request_xlog_switch, update_full_page_writes,
  xlog_archive_timeout, check_point_segments, checkpoint_stats_set
  (xlog is needs-decomp; the WAL-write driver + CheckpointState machine + these
  GUC/global readers are not yet reachable, so these panic-until-landed —
  correct per mirror-PG-and-panic). syncrep-seams sync_rep_update_sync_standbys_defined
  (syncrep owner unported).
- **Inward seams installed (7)**: checkpointer_main, absorb_sync_requests,
  forward_sync_request, request_checkpoint, checkpoint_stats_set,
  checkpointer_shmem_size, checkpointer_shmem_init. Consumers
  (xlog/sync/launch-backend/ipci/bgwriter) now resolve.

## 4. Divergences / debt

- OS clock reads (`(pg_time_t)time(NULL)`, `gettimeofday`) use `std::time`
  directly, not a seam (matching pgarch's `now_seconds`; not a subsystem
  boundary).
- pending checkpointer stats bumped through `with_pending_checkpointer_stats`
  (activity-small's PGDLLIMPORT-global access path) — the faithful equivalent of
  C's direct `PendingCheckpointerStats.field++`.
- `checkpoint_stats_set` inward seam (consumed by sync.c) delegates to the xlog
  owner since `CheckpointStats` is an xlog.c global, not checkpointer-owned.
- No `todo!`/`unimplemented!`/own-logic stubs (no-todo-guard green).

## 5. Gate

`cargo check` (new crate + all touched seam/owner crates + seams-init) green;
`cargo test -p backend-postmaster-checkpointer` (5 tests) green; no-todo-guard
green; seams-init both guards green.
