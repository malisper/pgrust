//! Seam declarations for the `backend-replication-walreceiver` unit
//! (`src/backend/replication/walreceiver.c`). The owning unit installs these from its `init_seams()`;
//! until then a call panics loudly.

seam_core::seam!(
    /// `WalReceiverMain(startup_data, startup_data_len)` (`src/backend/replication/walreceiver.c`): child entry
    /// point invoked by `postmaster_child_launch`; never returns.
    pub fn wal_receiver_main(startup_data: &types_startup::StartupData) -> !
);

seam_core::seam!(
    /// `GetWalRcvFlushRecPtr(*latestChunkStart, *receiveTLI)` (walreceiver.c)
    /// — the last WAL byte + 1 received and flushed to disk by the
    /// walreceiver, and the timeline it was received on. Returns
    /// `(lsn, tli)`.
    pub fn get_wal_rcv_flush_rec_ptr() -> (types_core::XLogRecPtr, types_core::TimeLineID)
);
