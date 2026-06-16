//! Seam declarations for the `backend-replication-walreceiver` unit
//! (`src/backend/replication/walreceiver.c`). The owning unit installs these from its `init_seams()`;
//! until then a call panics loudly.

seam_core::seam!(
    /// `WalReceiverMain(startup_data, startup_data_len)` (`src/backend/replication/walreceiver.c`): child entry
    /// point invoked by `postmaster_child_launch`; never returns.
    pub fn wal_receiver_main(startup_data: &types_startup::StartupData) -> !
);

seam_core::seam!(
    /// `wal_receiver_timeout` (walreceiver.c GUC, milliseconds): maximum time
    /// to wait for WAL receipt. A backend-local config knob the GUC machinery
    /// assigns; the launcher reads it to time out workers that fail to attach.
    pub fn wal_receiver_timeout() -> i32
);

seam_core::seam!(
    /// `wal_retrieve_retry_interval` (walreceiver.c GUC, milliseconds): how
    /// long to wait before retrying WAL retrieval; the launcher uses it to pace
    /// apply-worker restarts. Backend-local GUC state.
    pub fn wal_retrieve_retry_interval() -> i32
);

seam_core::seam!(
    /// `GetWalRcvFlushRecPtr(*latestChunkStart, *receiveTLI)` (walreceiver.c)
    /// — the last WAL byte + 1 received and flushed to disk by the
    /// walreceiver, and the timeline it was received on. Returns
    /// `(lsn, tli)`.
    pub fn get_wal_rcv_flush_rec_ptr() -> (types_core::XLogRecPtr, types_core::TimeLineID)
);

seam_core::seam!(
    /// `hot_standby_feedback` (walreceiver.c GUC bool): whether the standby
    /// sends its xmin/catalog_xmin back to the primary. Slot synchronization
    /// requires it enabled. Backend-local GUC state.
    pub fn hot_standby_feedback() -> bool
);

// `WalRcvShmemSize`/`WalRcvShmemInit` live in `walreceiverfuncs.c`, not
// `walreceiver.c`; their seams are homed in
// `backend-replication-walreceiverfuncs-seams` (the real owner unit). The ipci
// `CalculateShmemSize`/`CreateOrAttachShmemStructs` accumulators reach them
// there. No mis-homed decl lives here.

seam_core::seam!(
    /// `WalRcvForceReply(void)` (walreceiver.c) — set the walreceiver's
    /// `force_reply` flag and wake it so it sends an immediate reply to the
    /// primary (used by the redo loop after replaying a record that requested
    /// a reply). Async-signal-safe / infallible in C.
    pub fn wal_rcv_force_reply()
);
