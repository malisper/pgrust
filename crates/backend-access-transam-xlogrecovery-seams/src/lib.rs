//! Seam declarations for the `backend-access-transam-xlogrecovery` unit
//! (`access/transam/xlogrecovery.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::{TimeLineID, TimestampTz, XLogRecPtr};

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
