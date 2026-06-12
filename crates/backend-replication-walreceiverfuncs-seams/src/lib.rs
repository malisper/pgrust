//! Seam declarations for the `backend-replication-walreceiverfuncs` unit
//! (`replication/walreceiverfuncs.c`). The owning unit installs these from
//! its `init_seams()` when it lands; until then a call panics loudly.

seam_core::seam!(
    /// `XLogRequestWalReceiverReply()` — ask walreceiver to send a reply
    /// immediately (remote_apply feedback during redo).
    pub fn xlog_request_wal_receiver_reply()
);
