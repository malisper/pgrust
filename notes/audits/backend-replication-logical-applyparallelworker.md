# Audit: backend-replication-logical-applyparallelworker

- Date: 2026-06-12
- Model: Opus (Opus 4.8 / claude-opus-4-8[1m])
- Branch: port/backend-replication-logical-applyparallelworker
- C source: `postgres-18.3/src/backend/replication/logical/applyparallelworker.c` (1647 lines)
- c2rust: `c2rust-runs/backend-replication-logical-applyparallelworker/src/applyparallelworker.rs`
- Port: `crates/backend-replication-logical-applyparallelworker/src/lib.rs`
- Owned seam crate: `crates/backend-replication-logical-applyparallelworker-seams`
- Carried types: `crates/types-applyparallel`
- Auditor: independent re-derivation from C + headers; the full function
  inventory was re-derived from the C file (not trusted from the prior fix).

## Top-line verdict: **PASS**

All 36 C functions are present in-crate with faithful logic. The single
merge-blocking finding from the prior audit — the file-static `subxactlist`
(C line 255) and its list-management logic having been seamed out to
`backend-replication-logical-worker-seams` — has been resolved: `subxactlist`
is now an in-crate `Vec<TransactionId>` field of the per-backend `Globals`
(`thread_local!`), and the three functions that touch it (`pa_start_subtrans`,
`pa_reset_subtrans`, `pa_stream_abort`) manage it directly with inlined
list operations. The six `subxact_*` worker seams have been removed from
`backend-replication-logical-worker-seams`; no crate references them.

## Function inventory (re-derived from C)

36 C function definitions were enumerated directly from
applyparallelworker.c (line ranges below). Every one maps to a present
in-crate function. No function body is replaced by a "call somewhere else"
seam to a non-cycle owner.

| # | C function (loc) | Port loc | Verdict | Notes |
|---|---|---|---|---|
| 1 | pa_can_start (264-316) | lib.rs 321 | MATCH | Four early-return predicates in order; maybe_reread_subscription before checks; skiplsn via XLogRecPtrIsInvalid; AllTablesyncsReady via tablesync seam -> PgResult<bool>. |
| 2 | pa_setup_dsm (326-397) | lib.rs 376 | MATCH | Header built in-crate (xact_state=UNKNOWN, pending=0, last_commit_end=Invalid, fileset_state=FS_EMPTY); DSM/shm_toc/shm_mq build delegated to worker setup_dsm seam (unported subsystems); token carries in-crate Arc; None on dsm_create failure. |
| 3 | pa_launch_parallel_worker (403-459) | lib.rs 415 | MATCH | Pool scan for !in_use; palloc0 -> slot; setup_dsm failure frees slot; launch seam; lappend on success / pa_free_worker_info on failure. |
| 4 | pa_allocate_worker (469-512) | lib.rs 497 | MATCH | can_start + launch gates; lazy hash init; HASH_ENTER+found => "hash table corrupted"; spinlock write xact_state=UNKNOWN+xid; in_use=true, serialize_changes=false. |
| 5 | pa_find_worker (517-543) | lib.rs 546 | MATCH | TransactionIdIsValid + NULL-hash guards; stream_apply_worker cache short-circuit; HASH_FIND; Assert(in_use) -> debug_assert. |
| 6 | pa_free_worker (555-588) | lib.rs 578 | MATCH | Asserts; HASH_REMOVE + "hash table corrupted"; threshold `> max/2`; serialize_changes OR threshold => stop+free; gen/slot read under mutex; else in_use=false,serialize_changes=false. |
| 7 | pa_free_worker_info (594-616) | lib.rs 655 | MATCH | mq/error_mq detach guarded by nonzero handle; serialize_changes => stream_cleanup_files(subid,xid); dsm_detach; list_delete_ptr -> slot=None. |
| 8 | pa_detach_all_error_mq (621-636) | lib.rs 689 | MATCH | Iterates pool, detaches+NULLs nonzero error_mq_handle (snapshot-then-call). |
| 9 | pa_has_spooled_message_pending (641-649) | lib.rs 721 | MATCH | fileset_state != FS_EMPTY. |
| 10 | pa_process_spooled_messages_if_required (657-706) | lib.rs 731 | MATCH | FS_EMPTY->false; FS_SERIALIZE_IN_PROGRESS lock/unlock stream then re-read; DONE->READY; READY->apply_spooled_messages+EMPTY. apply_spooled_messages seam takes only lsn (fileset/xid re-read from in-crate MyParallelShared) — thin marshalling. |
| 11 | ProcessParallelApplyInterrupts (711-730) | lib.rs 770 | MATCH | CHECK_FOR_INTERRUPTS; ShutdownRequestPending->LOG+proc_exit(0); ConfigReloadPending->clear+ProcessConfigFile(PGC_SIGHUP). |
| 12 | LogicalParallelApplyLoop (733-833) | lib.rs 801 | MATCH | SUCCESS: len==0 elog; first byte 'w'; cursor=1+SIZE_STATS_MESSAGE; apply_dispatch. WOULD_BLOCK: spooled-or-wait(1000ms MAIN). DETACHED: ereport "apply worker". Error-context push/pop preserved. |
| 13 | pa_shutdown (843-851) | lib.rs 877 | MATCH | SendProcSignal(leader_pid, PROCSIG_PARALLEL_APPLY_MESSAGE, INVALID_PROC_NUMBER); dsm_detach(seg). |
| 14 | ParallelApplyWorkerMain (856-986) | lib.rs 903 | MATCH | Attach phases delegated to worker_attach_dsm; MyParallelShared bound from token; spinlock write generation/slot_no; loop; Assert(false) trailer. |
| 15 | HandleParallelApplyMessageInterrupt (995-1001) | lib.rs 936 | MATCH | InterruptPending=true; pending=true; SetLatch. Installed by init_seams (the one owned seam). |
| 16 | ProcessParallelApplyMessage (1007-1064) | lib.rs 949 | MATCH | 'E': pq_parse_errornotice(after type byte), context psprintf/pstrdup, restore apply_error_context_stack, ereport ERROR ONIPS + errcontext. 'N'/'A': no-op. default: elog "unrecognized message type ... %c (message length %d bytes)". |
| 17 | ProcessParallelApplyMessages (1069-1143) | lib.rs 1002 | MATCH | HOLD_INTERRUPTS; private hpam_context; pending=false; per-worker: skip NULL handle; WOULD_BLOCK continue; SUCCESS->ProcessParallelApplyMessage; DETACHED->ereport "parallel apply worker"; RESUME_INTERRUPTS. |
| 18 | pa_send_data (1152-1209) | lib.rs 1066 | MATCH | Asserts; immediate-mode->false; loop SUCCESS->true / DETACHED->ereport / WOULD_BLOCK wait(1000ms SEND_DATA)+reset+CFI; startTime=0 first iter else timeout 9000ms->false. |
| 19 | pa_switch_to_partial_serialize (1217-1244) | lib.rs 1124 | MATCH | LOG with xid; serialize_changes=true; stream_start_internal(xid,true); conditional pa_lock_stream(AccessExclusive); pa_set_fileset_state(FS_SERIALIZE_IN_PROGRESS). |
| 20 | pa_wait_for_xact_state (1250-1275) | lib.rs 1163 | MATCH | break on >= xact_state (Ord); WaitLatch(10ms STATE_CHANGE); ResetLatch; CHECK_FOR_INTERRUPTS. |
| 21 | pa_wait_for_xact_finish (1280-1308) | lib.rs 1194 | MATCH | wait_for_xact_state(STARTED); lock/unlock transaction AccessShare; state!=FINISHED -> ereport "parallel apply worker". |
| 22 | pa_set_xact_state (1313-1320) | lib.rs 1228 | MATCH | Mutex write (slock_t). |
| 23 | pa_get_xact_state (1325-1335) | lib.rs 1243 | MATCH | Mutex read. |
| 24 | pa_set_stream_apply_worker (1340-1344) | lib.rs 1252 | MATCH | stream_apply_worker = winfo (Option<handle>). |
| 25 | pa_savepoint_name (1354-1358) | lib.rs 1264 | MATCH | snprintf "pg_sp_%u_%u" with szsp-1+NUL truncation; format!+truncate; NAMEDATALEN=64. |
| 26 | pa_start_subtrans (1368-1405) | lib.rs 1289 | MATCH | **Fixed.** `current_xid != top_xid && !list_member_xid(subxactlist, current_xid)` -> `!g.subxactlist.contains(&current_xid)`; transaction-block setup + DefineSavepoint + CommitTransactionCommand preserved; `lappend_xid(subxactlist, current_xid)` -> `g.subxactlist.push(current_xid)`. The TopTransactionContext MemoryContextSwitchTo wrapping is a no-op for a plain Vec (memory freed at txn end is modeled by `pa_reset_subtrans`). |
| 27 | pa_reset_subtrans (1408-1416) | lib.rs 1328 | MATCH | **Fixed.** `subxactlist = NIL` -> `g.subxactlist.clear()`. In-crate; no seam. |
| 28 | pa_stream_abort (1422-1497) | lib.rs 1351 | MATCH | **Fixed.** Origin LSN/timestamp updates; toplevel-abort path (subxid==xid): set state FINISHED, unlock xact AccessExclusive, AbortCurrentTransaction, EndTransactionBlock(false)+CommitTransactionCommand if in block, pa_reset_subtrans, report idle. Subxact path: savepoint name; reverse scan `for i in list_length-1..=0` over in-crate `g.subxactlist`, on match RollbackToSavepoint + CommitTransactionCommand + `list_truncate(subxactlist, i)` -> `g.subxactlist.truncate(i)` (keeps first i), break. All list ops in-crate; no subxact seam. |
| 29 | pa_set_fileset_state (1504-1519) | lib.rs 1425 | MATCH | Mutex write fileset_state; FS_SERIALIZE_DONE asserts leader + nonnull stream_fileset, copies fileset. stream_fileset read hoisted before mutex (value stable; mutex guards destination). |
| 30 | pa_get_fileset_state (1524-1536) | lib.rs 1486 | MATCH | Assert(am_parallel_apply_worker); Mutex read of MyParallelShared->fileset_state. |
| 31 | pa_lock_stream (1546-1551) | lib.rs 1497 | MATCH | LockApplyTransactionForSession(subid, xid, STREAM=0, lockmode). |
| 32 | pa_unlock_stream (1553-1558) | lib.rs 1507 | MATCH | UnlockApplyTransactionForSession(..., STREAM=0, ...). |
| 33 | pa_lock_transaction (1579-1584) | lib.rs 1517 | MATCH | LockApplyTransactionForSession(..., XACT=1, ...). |
| 34 | pa_unlock_transaction (1586-1591) | lib.rs 1527 | MATCH | UnlockApplyTransactionForSession(..., XACT=1, ...). |
| 35 | pa_decr_and_wait_stream_block (1597-1619) | lib.rs 1541 | MATCH | Assert; pending==0 -> spooled? return : elog "invalid pending streaming chunk 0"; fetch_sub-1==0 -> lock/unlock stream AccessShare. |
| 36 | pa_xact_finish (1624-1646) | lib.rs 1573 | MATCH | Assert(am_leader_apply_worker); pa_unlock_stream(AccessExclusive); pa_wait_for_xact_finish; remote_lsn valid -> store_flush_position(remote_lsn, last_commit_end); pa_free_worker. |

## Constants / headers verified

- PG_LOGICAL_APPLY_SHM_MAGIC = 0x787ca067 (C 175) — match.
- PARALLEL_APPLY_KEY_{SHARED=1,MQ=2,ERROR_QUEUE=3} (C 182-184) — match.
- DSM_QUEUE_SIZE=16*1024*1024, DSM_ERROR_QUEUE_SIZE=16*1024 (C 187/195) — match.
- SIZE_STATS_MESSAGE = 2*sizeof(XLogRecPtr)+sizeof(TimestampTz) (C 203) — match.
- PARALLEL_APPLY_LOCK_{STREAM=0,XACT=1} (C 209-210) — match.
- SHM_SEND_RETRY_INTERVAL_MS=1000, SHM_SEND_TIMEOUT_MS=9000 (C 1175-1176) — match.
- WL_LATCH_SET=1<<0, WL_TIMEOUT=1<<3, WL_EXIT_ON_PM_DEATH=1<<5 — match.
- NAMEDATALEN=64 (pg_config_manual.h 29) — match.
- ParallelTransState order UNKNOWN=0,STARTED=1,FINISHED=2 — match; Ord enables `>=`.
- PartialFileSetState order FS_EMPTY=0,FS_SERIALIZE_IN_PROGRESS=1,FS_SERIALIZE_DONE=2,FS_READY=3 — match.
- ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE on all four exit-due-to-error ereports; "apply worker" vs "parallel apply worker" wording distinguished per call site — match.

## Seam audit

Owned seam crate `backend-replication-logical-applyparallelworker-seams`
declares exactly one seam (`handle_parallel_apply_message_interrupt`),
installed by this crate's `init_seams()`, which contains only that `set()`
call. PASS for the owned seam.

Outward seams (justified by unported neighbors / DSM-shm subsystems): worker,
tablesync, libpq-pqmq, xact, lmgr, latch, origin, activity-status, timestamp,
tcop, ipc, guc-file, init-small, procsignal — thin marshal+delegate to
genuinely unported owners across real dependency cycles.

The six `subxact_*` seams previously declared in
`backend-replication-logical-worker-seams` (`subxact_member`,
`subxact_append`, `subxact_reset`, `subxact_length`, `subxact_nth`,
`subxact_truncate`) have been removed. A repo-wide grep confirms no remaining
reference to any of them. `subxactlist` is purely local backend state (a list
of TransactionIds owned by this C file with no callback into worker.c), so it
belongs in-crate; the prior seaming was unjustified and is now corrected.

## Design conformance

- mirror-PG-and-panic: all 36 bodies present; panics only on uninstalled seams
  to genuinely unported owners.
- Shared vocab: consumes existing types-* and seam crates; no reshaping.
- Per-backend C globals modeled as `thread_local!` `Globals`; `subxactlist`
  added there alongside the other file-statics.
- `cargo check --workspace` green; `cargo test` green for this crate and its
  seam crate. (A pre-existing, unrelated test compile error in
  `backend-postmaster-launch-backend` — `BackendType::SlotSyncWorker` naming —
  exists on the base commit and is outside this unit's scope.)

## Resolution

Verdict PASS. Catalog row set to `audited`.

---

## Re-audit after merge of current main (2026-06-13)

- Date: 2026-06-13
- Model: Opus (Opus 4.8 / claude-opus-4-8[1m])
- Branch: port/backend-replication-logical-applyparallelworker (merged current
  refs/heads/main)
- Trigger: this unit was developed against a different seam vocabulary than
  current main. Shared seam crates had add/add contract collisions; main's owner
  contracts are authoritative (seam-signatures-mirror-c-failure-surface).

### Reconciled contract collisions (logic re-verified against C unchanged)

- `worker-seams::am_leader_apply_worker`: branch `-> bool` reconciled to main's
  owner contract `-> PgResult<bool>`. The two C `Assert(am_leader_apply_worker())`
  sites (`pa_set_fileset_state_handle`, `pa_xact_finish`) and the
  `pa_can_start` early-return now thread the `PgResult` (`?`). Logic identical.
- `latch-seams::wait_latch_my_latch`: branch `(i32,..)->i32` reconciled to main's
  `(u32, i64, u32) -> PgResult<u32>`; local `WL_*` consts now `u32`; the three
  `WaitLatch(MyLatch,..)` sites (`LogicalParallelApplyLoop`, `pa_send_data`,
  `pa_wait_for_xact_state`) propagate `?`.
- Launch/stop path re-homed to the **launcher** owner (launcher.c owns both):
  - `pa_launch_parallel_worker` now calls
    `launcher-seams::logicalrep_worker_launch(WORKERTYPE_PARALLEL_APPLY, dbid,
    oid, name, userid, InvalidOid, dsm_segment_handle(dsm_seg))` — exactly the C
    call (C 438-444). The branch's re-forked
    `worker-seams::logicalrep_worker_launch_parallel_apply` was removed.
  - `pa_free_worker` now builds the launcher-facing
    `types_replication_applyparallel::ParallelApplyWorkerInfo` value snapshot and
    calls `launcher-seams::logicalrep_pa_worker_stop(&mut winfo)` (C 580). The
    branch's re-forked `worker-seams::logicalrep_pa_worker_stop(generation,
    slot_no)` was removed. The spinlock read of generation/slot_no and the
    error-mq detach now happen inside the launcher's stop path, which reaches
    back into this crate's owned seams (below).
- Disjoint owner-set merges (no logic change): origin/xact/activity-status/
  wait_event seam crates take main's owner declarations plus this unit's genuine
  new declarations; `set_replorigin_session_origin_timestamp` deduped onto main's.

### Owned seam crate now fully installed (was the merge-blocking gap)

`backend-replication-logical-applyparallelworker-seams` (owned by this unit; the
merged launcher consumer depends on it) declares 5 seams. The branch installed
only 1; `init_seams()` now installs all 5:

- `handle_parallel_apply_message_interrupt` -> `HandleParallelApplyMessageInterrupt`.
- `pa_detach_all_error_mq` -> the in-crate `pa_detach_all_error_mq` (C 621-636).
- `pa_read_winfo_slot` -> returns `(generation, slot_no)` from the snapshot's
  shared identity (the C `SpinLockAcquire` read; snapshot taken under the in-crate
  mutex when built).
- `pa_winfo_has_error_mq` -> `winfo.has_error_mq_handle`.
- `pa_winfo_detach_error_mq` -> resolves the owning pool slot by the shared
  (generation, slot_no, xid) identity, `shm_mq_detach`es the real error queue and
  NULLs the handle in the pool slot + snapshot (launcher.c 661-665).

Thin marshal+delegate over this crate's owned pool/header; the only branching is
the slot lookup the owned-pool model requires (the C `winfo` pointer identity is
the pool index, recovered here from the value snapshot). The seam crate's
`init_seams()` is `set()`-only and invoked by `seams-init::init_all()`.

### Gate

`cargo check --workspace` clean; `cargo test --workspace` clean (1073 test
suites ok, 0 failed), including this crate (14) and the launcher (5). All 36 C
functions remain MATCH (logic unchanged by the reconciliation). Verdict **PASS**;
catalog row kept `audited`.
