//! Seam declarations for the `backend-access-transam-xlogrecovery` unit
//! (`access/transam/xlogrecovery.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::{TimeLineID, TimestampTz, XLogRecPtr};

// ---------------------------------------------------------------------------
// WAL-recovery orchestrator entry seams (xlogrecovery.c). The recovery owner
// installs these from its `init_seams()`; `StartupXLOG` (xlog.c, the unported
// driver) seam-and-calls them around the redo loop. They operate on the owner's
// process-lifetime backend-local recovery state (C's file-static globals).
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `void InitWalRecovery(ControlFileData *ControlFile, bool *wasShutdown_ptr,`
    /// `bool *haveBackupLabel_ptr, bool *haveTblspcMap_ptr)` (xlogrecovery.c:519)
    /// — set up recovery: read signal/backup-label files, allocate the reader,
    /// read the starting checkpoint, validate the timeline, and update the
    /// in-memory `ControlFile`. Returns the three C out-params.
    pub fn init_wal_recovery<'mcx>(
        control_file: &mut types_control::ControlFileData,
        mcx: mcx::Mcx<'mcx>,
    ) -> types_error::PgResult<types_wal::xlogrecovery_carriers::InitWalRecoveryResult>
);

seam_core::seam!(
    /// `EndOfWalRecoveryInfo *FinishWalRecovery(void)` (xlogrecovery.c:1476) —
    /// determine where to start writing WAL next; produce the end-of-recovery
    /// info the caller uses to seed the WAL writer.
    pub fn finish_wal_recovery<'mcx>(
        mcx: mcx::Mcx<'mcx>,
    ) -> types_error::PgResult<types_wal::xlogrecovery_carriers::EndOfWalRecoveryInfo>
);

seam_core::seam!(
    /// `void ShutdownWalRecovery(void)` (xlogrecovery.c:1626) — clean up the WAL
    /// reader and leftovers from restoring WAL from archive.
    pub fn shutdown_wal_recovery() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `GetXLogReceiptTime(*rtime, *fromStream)` — the last WAL receipt time
    /// and whether it arrived via streaming replication.
    pub fn get_xlog_receipt_time() -> (TimestampTz, bool)
);

seam_core::seam!(
    /// `GetXLogReplayRecPtr(*replayTLI)` (xlogrecovery.c) — the last replayed
    /// WAL record's end LSN and the timeline being replayed. Returns
    /// `(lsn, tli)`.
    pub fn get_xlog_replay_rec_ptr() -> (XLogRecPtr, TimeLineID)
);

seam_core::seam!(
    /// `WakeupRecovery()` (xlogrecovery.c) — set the recovery-wakeup latch so
    /// the redo loop notices new state. Safe in signal-handler context.
    pub fn wakeup_recovery()
);

seam_core::seam!(
    /// `TimestampTz GetCurrentChunkReplayStartTime(void)` (xlogrecovery.c) —
    /// the timestamp of the WAL chunk currently being replayed, or 0 when
    /// unavailable. Consumed by `GetReplicationApplyDelay`.
    pub fn get_current_chunk_replay_start_time() -> TimestampTz
);

seam_core::seam!(
    /// Read the `PrimaryConnInfo` GUC string (xlogrecovery.c), copied into
    /// `mcx` (the C call sites `pstrdup` it in the current context; never
    /// NULL in C, the boot value is `""`). `Err` is the copy's OOM.
    pub fn primary_conninfo<'mcx>(
        mcx: mcx::Mcx<'mcx>,
    ) -> types_error::PgResult<mcx::PgString<'mcx>>
);

seam_core::seam!(
    /// Read the `PrimarySlotName` GUC string (xlogrecovery.c), copied into
    /// `mcx` (the C call sites `pstrdup` it in the current context). `Err`
    /// is the copy's OOM.
    pub fn primary_slot_name<'mcx>(
        mcx: mcx::Mcx<'mcx>,
    ) -> types_error::PgResult<mcx::PgString<'mcx>>
);

seam_core::seam!(
    /// Read the `wal_receiver_create_temp_slot` GUC bool (xlogrecovery.c).
    pub fn wal_receiver_create_temp_slot() -> bool
);

seam_core::seam!(
    /// `StartupRequestWalReceiverRestart()` (xlogrecovery.c) — flag that the
    /// walreceiver must be restarted because a critical option changed.
    pub fn startup_request_wal_receiver_restart()
);

seam_core::seam!(
    /// `XLogRequestWalReceiverReply()` (xlogrecovery.c:4528) — schedule a
    /// walreceiver wakeup in the main recovery loop (sets the backend-local
    /// `doRequestWalReceiverReply` flag the redo loop consumes). Consumed by
    /// xact redo for `remote_apply` feedback during replay.
    pub fn xlog_request_wal_receiver_reply()
);

seam_core::seam!(
    /// `reachedConsistency` (xlogrecovery.c global) — true once recovery has
    /// reached a consistent state. Pure read of backend-local/shared state.
    pub fn reached_consistency() -> bool
);

seam_core::seam!(
    /// `InRecovery` (`access/xlogutils.h`, owned by xlogrecovery.c) — true
    /// while this process is performing WAL replay (the startup process's
    /// local flag, distinct from the shared `RecoveryInProgress()`). Pure
    /// read of the owner's per-backend flag at the point of use; the
    /// zero-arg-getter shape is recorded in DESIGN_DEBT.md.
    pub fn in_recovery() -> bool
);

seam_core::seam!(
    /// `GetXLogReplayRecPtr(NULL)` (xlogrecovery.c) — the last WAL position
    /// replayed by the startup process.
    pub fn get_xlog_replay_recptr() -> XLogRecPtr
);

seam_core::seam!(
    /// `HotStandbyActive()` (xlogrecovery.c) — true once hot standby is
    /// accepting connections.
    pub fn hot_standby_active() -> bool
);

seam_core::seam!(
    /// `GetXLogReplayRecPtr(&replayTLI)` (xlogrecovery.c) — the position up to
    /// which WAL has been replayed by the startup process, with the replay
    /// timeline. Returns `(read_upto, replayTLI)`. Distinct from the
    /// NULL-`replayTLI` variant some callers use.
    pub fn get_xlog_replay_rec_ptr_tli() -> (XLogRecPtr, types_core::TimeLineID)
);

seam_core::seam!(
    /// `StandbyMode` (xlogrecovery.c global bool): true while the server is in
    /// standby mode (continuous recovery awaiting more WAL). Pure read.
    pub fn standby_mode() -> bool
);

seam_core::seam!(
    /// `XLogRecoveryShmemSize()` (ipci.c `CalculateShmemSize` accumulator) — shared-memory
    /// bytes this subsystem needs. `Err` carries the `add_size`/`mul_size`
    /// overflow `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn xlog_recovery_shmem_size() -> types_error::PgResult<types_core::Size>
);

seam_core::seam!(
    /// `XLogRecoveryShmemInit()` (ipci.c `CreateOrAttachShmemStructs`) — allocate-or-attach
    /// this subsystem's shared-memory structures. `Err` carries the C
    /// out-of-shared-memory `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn xlog_recovery_shmem_init() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ArchiveRecoveryRequested` (xlogrecovery.c global bool): true when a
    /// `recovery.signal`/`standby.signal` was present at startup, i.e. we are
    /// performing archive recovery (not crash recovery). Pure read.
    pub fn archive_recovery_requested() -> bool
);

seam_core::seam!(
    /// `recoveryTargetTLI` (xlogrecovery.c global `TimeLineID`): the timeline the
    /// startup process is recovering toward (the latest active timeline per
    /// `pg_control`, or the configured recovery-target timeline). Read by
    /// `StartupXLOG` (xlog.c) for `restoreTimeLineHistoryFiles` /
    /// `findNewestTimeLine` / `writeTimeLineHistory`. Pure read of the owner's
    /// per-backend recovery state.
    pub fn recovery_target_tli() -> TimeLineID
);

seam_core::seam!(
    /// `recoveryRestoreCommand` (xlogrecovery.c global) — the configured
    /// `restore_command`, or `None`/empty when unset (standby mode may omit it).
    /// Returned charged to `mcx` (the `pstrdup` analog).
    pub fn recovery_restore_command<'mcx>(
        mcx: mcx::Mcx<'mcx>,
    ) -> Option<mcx::PgString<'mcx>>
);

seam_core::seam!(
    /// `GetOldestRestartPoint(*lastCheckPointRecPtr, *lastCheckPointTLI)`
    /// (xlogrecovery.c) — the redo pointer and timeline of the oldest restart
    /// point still needed. Reads `ControlFile` under a spinlock.
    pub fn get_oldest_restart_point() -> types_error::PgResult<(XLogRecPtr, TimeLineID)>
);

seam_core::seam!(
    /// `GetRecoveryState()` (xlogrecovery.c) — the server's current recovery
    /// state (`Crash`/`Archive`/`Done`). Reads `XLogRecoveryCtl` under a
    /// spinlock.
    pub fn get_recovery_state() -> types_error::PgResult<types_wal::RecoveryState>
);

// ---------------------------------------------------------------------------
// Recovery pause / promotion controls (xlogrecovery.c) consumed by xlogfuncs.c
// (pg_wal_replay_pause/resume, pg_is_wal_replay_paused,
// pg_get_wal_replay_pause_state, pg_last_xact_replay_timestamp). Owned by
// xlogrecovery.c; declared here so xlogfuncs.c can call them and panic loudly
// until xlogrecovery.c installs them.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `SetRecoveryPause(recoveryPause)` (xlogrecovery.c) — request/clear a
    /// recovery pause. Writes `XLogRecoveryCtl->recoveryPauseState` under a
    /// spinlock.
    pub fn set_recovery_pause(recovery_pause: bool)
);

seam_core::seam!(
    /// `GetRecoveryPauseState()` (xlogrecovery.c) — the current recovery pause
    /// state (`NotPaused`/`PauseRequested`/`Paused`). Reads
    /// `XLogRecoveryCtl->recoveryPauseState` under a spinlock.
    pub fn get_recovery_pause_state() -> types_wal::RecoveryPauseState
);

seam_core::seam!(
    /// `PromoteIsTriggered()` (xlogrecovery.c) — whether standby promotion has
    /// been triggered. Reads `LocalPromoteIsTriggered` (refreshed from shmem).
    pub fn promote_is_triggered() -> bool
);

seam_core::seam!(
    /// `GetLatestXTime()` (xlogrecovery.c) — the timestamp of the latest
    /// processed commit/abort record during recovery, or 0 when not in/after
    /// recovery. Reads `XLogRecoveryCtl->recoveryLastXTime` under a spinlock.
    pub fn get_latest_x_time() -> TimestampTz
);
