//! Seam declarations for walreceiverfuncs.c (owner: the walreceiver unit,
//! unported). xlogrecovery guards every call on is_installed(): uninstalled
//! behaves as "no walreceiver process exists", which is C's state whenever
//! the walreceiver was never launched.

use types_core::{TimeLineID, XLogRecPtr};
use types_error::PgResult;

seam_core::seam!(
    // WalRcvStreaming() (walreceiverfuncs.c).
    pub fn wal_rcv_streaming() -> bool
);

seam_core::seam!(
    // WalRcvRunning() (walreceiverfuncs.c).
    pub fn wal_rcv_running() -> bool
);

seam_core::seam!(
    // ShutdownWalRcv() (walreceiverfuncs.c).
    pub fn shutdown_wal_rcv()
);

seam_core::seam!(
    // RequestXLogStreaming(tli, recptr, conninfo, slotname, create_temp_slot)
    // (walreceiverfuncs.c).
    pub fn request_xlog_streaming(
        tli: TimeLineID,
        recptr: XLogRecPtr,
        conninfo: &str,
        slotname: &str,
        create_temp_slot: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    // GetWalRcvFlushRecPtr(&latestChunkStart, &receiveTLI) (walreceiverfuncs.c):
    // (flushedUpto, latestChunkStart, receiveTLI).
    pub fn get_wal_rcv_flush_rec_ptr() -> (XLogRecPtr, XLogRecPtr, TimeLineID)
);

seam_core::seam!(
    // WalRcvForceReply() (walreceiver.c).
    pub fn wal_rcv_force_reply()
);

/// Marshal shape for pg_stat_get_wal_receiver (walreceiver.c).
pub struct WalRcvStatSnapshot {
    pub pid: i32,
    pub state: &'static str,
    pub receive_start_lsn: XLogRecPtr,
    pub receive_start_tli: TimeLineID,
    pub written_lsn: XLogRecPtr,
    pub flushed_lsn: XLogRecPtr,
    pub received_tli: TimeLineID,
    pub last_send_time: i64,
    pub last_receipt_time: i64,
    pub latest_end_lsn: XLogRecPtr,
    pub latest_end_time: i64,
    pub slotname: String,
    pub sender_host: String,
    pub sender_port: i32,
    pub conninfo: String,
}

seam_core::seam!(
    // pg_stat_get_wal_receiver's WalRcv read; None == C's PG_RETURN_NULL arm.
    pub fn pg_stat_wal_receiver_snapshot() -> Option<WalRcvStatSnapshot>
);
