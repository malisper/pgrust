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
