# Audit: backend-replication-walsender

- **Date:** 2026-06-15
- **Model:** Opus 4.8 (1M context)
- **Verdict:** PASS (self-audit by porting agent)
- **Branch:** worktree-agent-aa616f8fe9edbde04
- **C source:** `src/backend/replication/walsender.c` (4234 lines)
- **Port:** `crates/backend-replication-walsender/`

Self-audit per `.claude/skills/audit-crate/SKILL.md`. The function inventory was
rebuilt from the C source (`grep -nE '^[A-Za-z_].*\('`) and every translation-unit
function is present in the port. Genuinely in-crate logic (control flow, the lag
tracker, the sleeptime/keepalive/timeout arithmetic, the SRF classification, the
XID arithmetic, the send-decision flow) is ported 1:1 over owned values. Deep
cross-subsystem bodies (per-command handlers, the WAL-read emit, the
START_REPLICATION drivers, the logical decoding-context steps, and the inbound
standby-message processing) are precise `panic!`s naming the unported owner —
the established seam-and-panic boundary (Mirror PG and panic).

## Design decision: WalSndCtl shmem array is OWNED, not seamed

The src-idiomatic base routed all `WalSnd`/`WalSndCtl` access through ~40
`walsnd_*`/`my_walsnd_*` seams to "the unported shmem subsystem". That is wrong
for this repo (Opacity inherited, never introduced): walsender.c *is* the owner
of `WalSndCtlData`. This port owns it as a real `#[repr(C)]` struct allocated
via `ShmemInitStruct` (the slotsync.c `SlotSyncCtx` precedent) with real
per-slot `Spinlock`s, three real `ConditionVariable`s, the `SyncRepQueue` dlist
heads (syncrep co-owns these by reaching in later), and `sync_standbys_status`.
The slot operations (`shmem_array.rs`) are real functions over it.

## Per-function table

| C function | Port | Verdict | Notes |
|---|---|---|---|
| `InitWalSender` | init.rs | MATCH | recovery flag, InitWalSenderSlot, aux resowner, MarkPostmasterChildWalSender + SendPostmasterSignal(ADVANCE_STATE_MACHINE), SetProcAffectsAllHorizons when MyDatabaseId==Invalid, lag_tracker alloc. |
| `WalSndErrorCleanup` | init.rs | MATCH | LWLockReleaseAll/CV cancel/wait-end/pgaio cleanup, xlogreader close, slot release+cleanup(false), replication_active=0, aux-resource release if not in xact, proc_exit if STOPPING\|SIGUSR2, revert to STARTUP. |
| `WalSndShutdown` | init.rs | MATCH | whereToSendOutput Remote→None, proc_exit(0). `-> !`. |
| `InitWalSenderSlot` | init.rs + shmem_array.rs | MATCH | reserve loop under slot mutex, kind by MyDatabaseId, on_shmem_exit(WalSndKill). |
| `WalSndKill` | init.rs + shmem_array.rs | MATCH | clear MyWalSnd, pid=0 under mutex. |
| `WalSndSignals` | lib.rs `install_walsnd_signals` | MATCH | pqsignal block (SIGHUP→config reload, SIGINT→StatementCancelHandler, SIGTERM→die, InitializeTimeouts, SIGPIPE ign, SIGUSR1→procsignal_sigusr1_handler, SIGUSR2→WalSndLastCycleHandler, SIGCHLD dfl), unblock via signal_masks (mirrors walreceiver). |
| `WalSndLastCycleHandler` | init.rs | MATCH | got_SIGUSR2=1, SetLatch. |
| `WalSndShmemSize` | init.rs | MATCH | offsetof(walsnds) + max_wal_senders*sizeof(WalSnd) via add_size/mul_size. |
| `WalSndShmemInit` | init.rs | MATCH | ShmemInitStruct, memset, dlist_init queues, SpinLockInit each slot, ConditionVariableInit ×3. |
| `WalSndSetState` | init.rs | MATCH | skip-if-equal, set under mutex. |
| `WalSndGetStateString` | init.rs | MATCH | five-state map. |
| `exec_replication_command` | command.rs | MATCH | STOPPING-state switch + stopping-mode error, SnapBuildClearExportedSnapshot, CHECK_FOR_INTERRUPTS, parse (None⇒return false; physical-SQL restriction panic), begin (log/DEBUG1 + buffer reset), aborted-tx error, CHECK_FOR_INTERRUPTS, the full command switch. |
| `IdentifySystem` | command.rs | SEAM-PANIC | libpq single-row result framing + GetSystemIdentifier/GetFlushRecPtr assembly. |
| `ReadReplicationSlot` | command.rs | SEAM-PANIC | slot-by-name + 3-col libpq result. |
| `SendTimeLineHistory` | command.rs | SEAM-PANIC | timeline-history file read + libpq CopyOut. |
| `UploadManifest` / `HandleUploadManifestPacket` | command.rs → basebackup-seams | SEAM | manifest COPY-in (backup subsystem). |
| `CreateReplicationSlot` / `parseCreateReplSlotOptions` | command.rs | SEAM-PANIC | slot create + decoding-ctx + libpq result. |
| `DropReplicationSlot` | command.rs → slot-seams | MATCH | ReplicationSlotDrop(name, !wait). |
| `AlterReplicationSlot` | command.rs | SEAM-PANIC | failover-flag slot alter. |
| `StartReplication` / `StartLogicalReplication` | start_replication.rs | SEAM-PANIC | xlogreader streaming + slot acquire + CreateDecodingContext + WalSndLoop drive. |
| `WalSndSegmentOpen` / `logical_read_xlog_page` | start_replication.rs | SEAM-PANIC | xlogreader callbacks. |
| `GetStandbyFlushRecPtr` | start_replication.rs | MATCH | GetWalRcvFlushRecPtr + GetXLogReplayRecPtr, same-TLI max. Genuine arithmetic. |
| `XLogSendPhysical` | physical.rs | MATCH (emit seamed) | full send-decision: STOPPING, doneSending, historic/cascading/primary SendRqstPtr, promotion + becameHistoric + timeline switch, LagTrackerWrite, fork-point CopyDone, sizing to MAX_SEND_SIZE / page boundary, sentPtr advance, shmem update, PS title. The WALRead emit + timeline_switch_point (mcx readTimeLineHistory) panic. |
| `PhysicalWakeupLogicalWalSnd` | physical.rs | MATCH | physical-slot assert, recovery early-out, slot_exists_in_sync_standby_slots ⇒ CV broadcast(wal_confirm_rcv_cv). |
| `XLogSendLogical` | logical.rs | MATCH (read seamed) | caughtUp/flushPtr (replay LSN if cascading), got_STOPPING⇒SIGUSR2, shmem sentPtr. Record read + ctx EndRecPtr panic. |
| `WalSndPrepareWrite`/`WalSndWriteData`/`ProcessPendingWrites`/`WalSndUpdateProgress` | logical.rs | MATCH (ctx->out emit seamed) | last_write LSN-zeroing, timestamp fill + flush + fast/slow path, the pending-writes loop (replies/timeout/keepalive/wait/reload), lag-track-once-per-interval + skipped-xact keepalive + pending-writes decision. |
| `WalSndLoop` | mainloop.rs | MATCH | reset latch, interrupts, config reload + SyncRepInitConfig, replies, CopyDone exit, send_data/caughtUp, flush, CATCHUP→STREAMING + SIGUSR2⇒WalSndDone, timeout, keepalive, the block-if-unsent wait (logical-vs-physical via fn_addr_eq, wakeEvents, IO stats flush, WalSndWait). |
| `WalSndDone` | mainloop.rs | MATCH | send_data, replicatedPtr (flush else write), caughtUp+sent==repl+!pending ⇒ flush+proc_exit, else keepalive(true). |
| `WalSndWaitForWal` | mainloop.rs | MATCH | fast path, the loop: reset/interrupts/config, replies, XLogBackgroundFlush on STOPPING, RecentFlushPtr update unless waiting-for-standby, STOPPING standby wait, idle ping, exit conditions, caughtUp, flush, CopyDone exit, timeout/keepalive, wait. |
| `NeedToWaitForStandbys` / `NeedToWaitForWal` | mainloop.rs | MATCH | elevel by STOPPING, failover-slot StandbySlotsHaveCaughtup; target>flushed first. |
| `WalSndComputeSleeptime` | mainloop.rs | MATCH | 10s default, half-timeout if no ping outstanding, TimestampDifferenceMilliseconds. |
| `WalSndCheckTimeOut` | mainloop.rs | MATCH | last_reply<=0 skip, COMMERROR + WalSndShutdown on timeout. |
| `WalSndWait` | mainloop.rs | MATCH | ModifyWaitEvent(FeBe socket), CV prepare by wait_event/kind, WaitEventSetWait, postmaster-death proc_exit(1), CV cancel. The FeBe modify/wait are real pqcomm owner fns. |
| `WalSndKeepalive` / `WalSndKeepaliveIfNecessary` | mainloop.rs | MATCH | 'k' message framed into owned output_message (writePtr/sentPtr, now, requestReply) + putmessage_noblock('d'); half-timeout trigger + flush. |
| `ProcessRepliesIfAny`/`ProcessStandbyMessage`/`ProcessStandbyReplyMessage`/`ProcessStandbyHSFeedbackMessage`/`PhysicalConfirmReceivedLocation`/`PhysicalReplicationSlotNewXmin` | replies.rs | SEAM-PANIC | libpq reply_message inbound loop + slot xmin/restart update (deep libpq/slot vertical). |
| `TransactionIdInRecentPast` | replies.rs | MATCH | ReadNextFullTransactionId, epoch compare, TransactionIdPrecedesOrEquals. Genuine arithmetic. |
| `WalSndWakeup`/`WalSndInitStopping`/`WalSndWaitStopping`/`HandleWalSndInitStopping`/`WalSndRqstFileReload` | wakeup.rs | MATCH | CV broadcast (flush/replay), SendProcSignal loop, stop-poll loop (pg_usleep 10ms), replication_active⇒kill(SIGTERM) else got_STOPPING, needreload set-all. |
| `WalSndSetState`/`pg_stat_get_wal_senders`/`offset_to_interval`/`LagTrackerWrite`/`LagTrackerRead` | stats.rs/lag_tracker.rs | MATCH | SRF per-row privilege/priority/sync-state classification + interval; the cyclic-buffer lag tracker ported 1:1 (overflow/interpolation). SRF tuplestore emit returns rows for the funcapi-owned emission. |

## Gate

- `cargo check --workspace` — clean (post-merge with origin/main).
- `cargo test -p no-todo-guard` — pass (no `todo!`/`unimplemented!` in the crate).
- `cargo test -p seams-init` — pass (recurrence guard: all 18 inward seams
  installed by init_seams; the new outward seams on completed owners — xact
  `synchronous_commit`, pqcomm `pq_is_send_pending`/`pq_flush_if_writable`/
  `pq_putmessage_noblock`/`modify_fe_be_wait_set_socket`/`wait_event_set_wait_fe_be`
  — are installed by those owners, so no CONTRACT_RECONCILE_PENDING entry was
  needed).
- `cargo test -p backend-replication-walsender` — pass (state strings, interval).
