//! Seam declarations for the `backend-access-transam-xlogrecovery` unit
//! (`access/transam/xlogrecovery.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::{TimestampTz, XLogRecPtr};

seam_core::seam!(
    /// `GetXLogReceiptTime(*rtime, *fromStream)` — the last WAL receipt time
    /// and whether it arrived via streaming replication.
    pub fn get_xlog_receipt_time() -> (TimestampTz, bool)
);

seam_core::seam!(
    /// `WakeupRecovery()` (xlogrecovery.c) — set the recovery-wakeup latch so
    /// the redo loop notices new state. Safe in signal-handler context.
    pub fn wakeup_recovery()
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
