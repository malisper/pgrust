# Audit: backend-commands-async (`commands/async.c`)

Auditor: Claude Opus 4.8 (1M). Independent re-derivation from
`../pgrust/postgres-18.3/src/backend/commands/async.c` (2521 LOC, 46 functions)
and `../pgrust/c2rust-runs/backend-commands-async/src/async.rs`.

## Architecture

C keeps `AsyncQueueControl` (a `ShmemInitStruct` segment: fixed header +
`QueueBackendStatus backend[FLEXIBLE_ARRAY_MEMBER]`) and a file-static
`SlruCtlData NotifyCtlData`. Both are owned in-crate as `thread_local!` slots
following the landed multixact (`MultiXactStateData` + per-backend arrays) and
clog (`XactCtlData`) precedent: the header scalars + an owned
`Vec<QueueBackendStatus>` for the per-backend array, and a `SlruCtlData` built
via `SimpleLruInit`. Guarded by the fixed `NotifyQueueLock` (lwlocklist offset
27) / `NotifyQueueTailLock` (47) main-array LWLocks and the NotifyCtl SLRU bank
locks, exactly as in C. The src-idiomatic "AsyncQueueControl shmem unowned /
NotifyCtl SLRU unbuilt" blockers were stale against this repo's
multixact/clog model.

Backend-local state (`listenChannels`, `pendingActions`, `pendingNotifies`,
the dedup hashtab, `notifyInterruptPending`, the GUC knobs, the exit-registered
/ registered-listener / try-advance-tail flags) is `thread_local!`, matching C's
per-backend globals.

## Per-function table

| C function (async.c) | Port (lib.rs) | Verdict | Notes |
|---|---|---|---|
| asyncQueuePageDiff | asyncQueuePageDiff | MATCH | `p - q` |
| asyncQueuePagePrecedes | asyncQueuePagePrecedes | MATCH | `p < q`; also set as NotifyCtl->PagePrecedes |
| QUEUE_POS_* macros | QUEUE_POS_{PAGE,OFFSET,EQUAL,IS_ZERO,MIN,MAX}, SET_QUEUE_POS | MATCH | MIN/MAX ternary branch-for-branch |
| QUEUE_* accessors | QUEUE_HEAD/TAIL/STOP_PAGE/FIRST_LISTENER/LAST_FILL_WARN/BACKEND_{PID,DBOID,POS}/NEXT_LISTENER (+set_) | MATCH | over the owned AsyncQueueControlData |
| AsyncShmemSize | AsyncShmemSize | MATCH | mul/add_size overflow → ERRCODE_PROGRAM_LIMIT_EXCEEDED; SimpleLruShmemSize(notify_buffers,0) |
| AsyncShmemInit | AsyncShmemInit | MATCH | first-time zero-init of header + every backend[] entry; SimpleLruInit("notify",…,LWTRANCHE_NOTIFY_{BUFFER,SLRU},SYNC_HANDLER_NONE,true); PagePrecedes set; SlruScanDirectory+DeleteAll on first init |
| pg_notify | pg_notify_core | SEAMED (fmgr value layer) | own logic: PreventCommandDuringRecovery("NOTIFY") seam + Async_Notify; the `PG_ARGISNULL`→"" decode + `PG_RETURN_VOID` Datum marshaling is the project-wide fmgr deferral |
| Async_Notify | Async_Notify | MATCH | nest level; parallel-worker reject (errmsg_internal); empty/too-long channel + too-long payload → ERRCODE_INVALID_PARAMETER_VALUE; packed data build via try_reserve; need-new vs dedup vs append |
| queue_listen | queue_listen | MATCH | no dedup; need-new vs append |
| Async_Listen | Async_Listen | MATCH | trace; LISTEN_LISTEN |
| Async_Unlisten | Async_Unlisten | MATCH | early-out when no pending & !exitRegistered; LISTEN_UNLISTEN |
| Async_UnlistenAll | Async_UnlistenAll | MATCH | same early-out; LISTEN_UNLISTEN_ALL with "" channel |
| pg_listening_channels | pg_listening_channels_rows | SEAMED (fmgr SRF) | own logic: `list_nth(listenChannels, i)` walk = listenChannels clone; SRF call-counter/CStringGetTextDatum is the funcapi value-layer deferral |
| Async_UnlistenOnExit | Async_UnlistenOnExit | MATCH | Exec_UnlistenAllCommit + asyncQueueUnregister; `(code,Datum)` callback shape carries PgResult (before_shmem_exit seam) |
| AtPrepare_Notify | AtPrepare_Notify | MATCH | ERRCODE_FEATURE_NOT_SUPPORTED if any pending |
| PreCommit_Notify | PreCommit_Notify | MATCH | preflight Exec_ListenPreCommit only for LISTEN; GetCurrentTransactionId; LockSharedObject(DatabaseRelationId,Invalid,0,AccessExclusiveLock); loop: lock excl, fillWarning, isFull→ERRCODE_PROGRAM_LIMIT_EXCEEDED (release lock first), addEntries; lock released each iteration; pendingNotifies not cleared |
| AtCommit_Notify | AtCommit_Notify | MATCH | Exec_{Listen,Unlisten,UnlistenAll}Commit per action; unregister if registered+empty; SignalBackends iff notifies; tryAdvanceTail; Clear |
| Exec_ListenPreCommit | Exec_ListenPreCommit | MATCH | already-registered early out; before_shmem_exit register; lock excl; max over same-db listeners; prev-listener insertion ordered by ProcNumber; readAll if max≠head |
| Exec_ListenCommit | Exec_ListenCommit | MATCH | IsListeningOn guard; push to listenChannels (try_reserve) |
| Exec_UnlistenCommit | Exec_UnlistenCommit | MATCH | strcmp-remove first match; no complaint if absent |
| Exec_UnlistenAllCommit | Exec_UnlistenAllCommit | MATCH | clear listenChannels |
| IsListeningOn | IsListeningOn | MATCH | any strcmp==0 |
| asyncQueueUnregister | asyncQueueUnregister | MATCH | assert empty; lock excl; invalidate pid/dboid; unlink from list; NEXT_LISTENER=INVALID |
| asyncQueueIsFull | asyncQueueIsFull | MATCH | head_page-tail_page ≥ max_notify_queue_pages |
| asyncQueueAdvance | asyncQueueAdvance | MATCH | offset+=len; page jump if no empty-entry room |
| asyncQueueNotificationToEntry | asyncQueueNotificationToEntry | MATCH | QUEUEALIGN(EmptySize+payload+channel); dboid/xid/srcPid; memcpy channel+payload+2 |
| asyncQueueAddEntries | asyncQueueAddEntries | MATCH | local QUEUE_HEAD; zero/read first page; mark dirty; per-event: build entry, fit-on-page else dummy-fill; write to slot buffer; advance; on page jump rotate bank lock, zero next page, set tryAdvanceTail on QUEUE_CLEANUP_DELAY boundary, break; write back QUEUE_HEAD; release bank lock |
| pg_notification_queue_usage | pg_notification_queue_usage_core | SEAMED (fmgr value layer) | own logic: advanceTail + lock shared + asyncQueueUsage + release; PG_RETURN_FLOAT8 is fmgr deferral |
| asyncQueueUsage | asyncQueueUsage | MATCH | fast 0.0 path; occupied/max |
| asyncQueueFillWarning | asyncQueueFillWarning | MATCH | <0.5 early out; TimestampDifferenceExceeds(lastWarn,t,5000ms); min-pos/min-pid scan; WARNING + detail/hint when min_pid valid; update lastWarn |
| SignalBackends | SignalBackends | MATCH | preallocate pids/procnos (try_reserve); lock excl; same-db skip-if-caught-up, other-db skip-unless-far-behind; self→set flag; else SendProcSignal(PROCSIG_NOTIFY_INTERRUPT)<0→DEBUG3 |
| AtAbort_Notify | AtAbort_Notify | MATCH | unregister if registered+empty; Clear |
| AtSubCommit_Notify | AtSubCommit_Notify | MATCH | reparent actions (give-to-parent vs concat); reparent notifies (give vs merge-dedup) |
| AtSubAbort_Notify | AtSubAbort_Notify | MATCH | pop action+notify stacks ≥ my_level; void (infallible) |
| HandleNotifyInterrupt | HandleNotifyInterrupt | MATCH | set pending flag + SetLatch(MyLatch); void |
| ProcessNotifyInterrupt | ProcessNotifyInterrupt | MATCH | not-idle early out; loop ProcessIncomingNotify while pending |
| asyncQueueReadAllNotifications | asyncQueueReadAllNotifications | MATCH | lock shared read pos/head; equal→return; RegisterSnapshot(GetLatestSnapshot); ExitOnAnyError save/set/restore around the do-while + pos write-back; UnregisterSnapshot |
| asyncQueueProcessPageEntries | asyncQueueProcessPageEntries | MATCH | local page-sized buf; SimpleLruReadPage_ReadOnly (snapshot copy of page); per-entry parse length/dboid/xid; advance; other-db skip; XidInMVCCSnapshot→back-up+stop; not-listening skip; TransactionIdDidCommit→copy to local_buf; release bank lock; deliver matches via NotifyMyFrontEnd; reached_stop at stop |
| asyncQueueAdvanceTail | asyncQueueAdvanceTail | MATCH | TailLock excl, QueueLock excl; min over listeners; set tail; release queue; segment-boundary truncate via SimpleLruTruncate + set stopPage; release tail |
| AsyncNotifyFreezeXids | AsyncNotifyFreezeXids | MATCH | TailLock shared, QueueLock shared (read pos/head), release queue; per-entry: re-bank on page change (release prev, mark dirty); read xid@off+8; if normal & precedes newFrozen: commit→Frozen else→Invalid, dirty; advance; final release |
| ProcessIncomingNotify | ProcessIncomingNotify | MATCH | reset flag; not-listening early out; set_ps_display("notify interrupt"); StartTransactionCommand; readAll; CommitTransactionCommand; flush→pq_flush; set_ps_display("idle") |
| NotifyMyFrontEnd | NotifyMyFrontEnd | MATCH | whereToSendOutput==DestRemote → pq_beginmessage(PqMsg_NotificationResponse='A')/sendint32(srcPid)/sendstring(channel)/sendstring(payload)/endmessage via pqformat (transient ctx; no pq_flush); else elog(INFO) |
| AsyncExistsPendingNotify | AsyncExistsPendingNotify | MATCH | hashtab.find else linear byte-compare scan |
| AddEventToPendingNotifies | AddEventToPendingNotifies | MATCH | build hashtab at MIN_HASHABLE_NOTIFIES (16); push event; enter into hashtab |
| notification_hash | notification_hash | MATCH | hash_bytes over channel+payload+1 (excl trailing null) |
| notification_match | notification_match | MATCH | 0 equal / 1 not; byte-identical over channel+payload+2 |
| ClearPendingActionsAndNotifies | ClearPendingActionsAndNotifies | MATCH | drop both lists |
| check_notify_buffers | check_notify_buffers | MATCH | delegates check_slru_buffers("notify_buffers", newval) |

## Constants verified against headers

- NOTIFY_PAYLOAD_MAX_LENGTH = BLCKSZ - NAMEDATALEN - 128 (types-async)
- AsyncQueueEntryEmptySize = offsetof(data)=16 + 2 = 18 (4×4-byte fixed fields)
- QUEUE_CLEANUP_DELAY = 4; QUEUE_FULL_WARN_INTERVAL = 5000 ms; MIN_HASHABLE_NOTIFIES = 16
- DatabaseRelationId = 1262; AccessExclusiveLock = 8
- NotifyQueueLock offset 27, NotifyQueueTailLock offset 47 (lwlocklist.h PG_LWLOCK)
- LWTRANCHE_NOTIFY_BUFFER/SLRU (types-storage); PqMsg_NotificationResponse = 'A'
- PROCSIG_NOTIFY_INTERRUPT = 1
- FirstNormalTransactionId=3, FrozenTransactionId=2, InvalidTransactionId=0

## Seam audit

Inward (owned `backend-commands-async-seams`, all 10 installed by init_seams,
wired in seams-init): handle_notify_interrupt, pre_commit_notify,
at_commit_notify, at_abort_notify, at_subcommit_notify, at_subabort_notify,
at_prepare_notify, async_shmem_size, async_shmem_init, async_unlisten_all.

Direct deps (no cycle): SLRU (owns NotifyCtl in-crate), lwlock (fixed + bank
locks), pqformat (NotificationResponse), common-hashfn, init-small globals
(MyProcNumber/MyDatabaseId/MyProcPid), mcx (transient ctx). All thin/real.

Outward seams (cycle partners / unported owners — thin marshal+delegate):
- xact-seams: get_current_transaction_{nest_level,id}, start/commit_transaction_command, is_transaction_or_transaction_block (xact↔async cycle: xact deps async-seams).
- transam-seams: transaction_id_did_commit (threads transaction_xmin from snapmgr-pc-seams, per the seam contract).
- parallel-seams: is_parallel_worker.
- lmgr-seams: lock_shared_object.
- procsignal-seams: send_proc_signal (procsignal↔async cycle).
- latch-seams: set_latch_my_latch.
- ipc dsm-core-seams: before_shmem_exit (callback fn(i32,Datum)->PgResult).
- snapmgr-seams: register_snapshot, get_latest_snapshot, unregister_snapshot, xid_in_mvcc_snapshot; snapmgr-pc-seams: transaction_xmin.
- init-small-seams: max_backends, exit_on_any_error, set_exit_on_any_error.
- timestamp-seams: get_current_timestamp, timestamp_difference_exceeds (unported owner).
- ps-status-seams: set_ps_display.
- pqcomm-seams: pq_flush.
- tcop-postgres-seams: where_to_send_output (NEW decl on this owner).
- tcop-utility-seams: prevent_command_during_recovery (NEW decl on this owner).

No logic in any seam closure; init_seams() is set()-only. New decls
(where_to_send_output, prevent_command_during_recovery) land on their true
C-owner crates (postgres.c global; utility.c function), uninstalled until those
owners land (guard-exempt; both owners not yet complete).

## Design conformance

- No invented opacity: AsyncQueueControl/QueueBackendStatus/QueuePosition are the
  real types in types-async; shared state owned in-crate (multixact pattern), not
  a registry. No Oid/usize stand-ins, no &[u8] blobs in signatures.
- Allocations (Async_Notify packed data, SignalBackends pids/procnos,
  AsyncShmemInit backend[]) use try_reserve → mcx OOM (PgResult); overflow →
  ERRCODE_PROGRAM_LIMIT_EXCEEDED.
- Per-backend globals are thread_local; backend identity read off init-small.
- Locks: every LWLockAcquire/Release pairs explicitly; the PreCommit_Notify and
  the asyncQueueAddEntries error paths release before propagating Err (mirroring
  the C ereport longjmp's LWLockReleaseAll). Bank locks rotated/released in
  asyncQueueAddEntries / freeze / read path matching C.
- No ambient-global zero-arg foreign seams: transaction_xmin is read via the
  snapmgr-pc-seams contract (consumer threads it into transaction_id_did_commit);
  ExitOnAnyError save/restore via init-small-seams.
- No todo!/unimplemented!. No unledgered divergence markers.

## Verdict: PASS

All 46 functions MATCH or SEAMED (the 3 fmgr-wrapped SQL entry points carry
their own logic as `*_core`/`*_rows`; only the fmgr/funcapi Datum value layer is
the sanctioned project-wide deferral). Zero seam findings. 14 in-crate unit tests
pass. Workspace check, no-todo-guard, seams-init guards green.
