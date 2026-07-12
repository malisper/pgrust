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
