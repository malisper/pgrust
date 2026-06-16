//! START_REPLICATION (physical + logical) and the standby flush-position helper.
//!
//! `GetStandbyFlushRecPtr` is genuine in-crate LSN arithmetic and is ported
//! here.  `StartReplication` / `StartLogicalReplication` set up the xlogreader,
//! acquire/validate the replication slot, install the decoding context, and
//! drive `WalSndLoop`; their bodies are dominated by unported subsystems
//! (xlogreader, slots, logical decoding, timeline history, libpq) and panic
//! precisely until those land.

#![allow(non_snake_case)]

use crate::core::{proc_get, StartReplicationCmd, TimeLineID, XLogRecPtr};
use crate::{slotsync, walrcvfuncs, xlogrecovery};

/// `static XLogRecPtr GetStandbyFlushRecPtr(TimeLineID *tli)`.
pub fn GetStandbyFlushRecPtr(tli: &mut TimeLineID) -> XLogRecPtr {
    debug_assert!(
        proc_get(|p| p.am_cascading_walsender)
            || slotsync::is_syncing_replication_slots::call()
    );

    // We can safely send what's already been replayed.  If walreceiver streams
    // WAL from the same timeline, we can also send what it has streamed but not
    // yet replayed.
    let (_written, receivePtr, receiveTLI) = walrcvfuncs::get_wal_rcv_flush_rec_ptr_full::call();
    let (replayPtr, replayTLI) = xlogrecovery::get_xlog_replay_rec_ptr_tli::call();

    *tli = replayTLI;

    let mut result = replayPtr;
    if receiveTLI == replayTLI && receivePtr > replayPtr {
        result = receivePtr;
    }
    result
}

/// `void WaitForStandbyConfirmation(XLogRecPtr moveto)` — wait for the standby
/// slots to confirm `moveto` (the logical walsender's failover-slot wait).
pub fn WaitForStandbyConfirmation(moveto: XLogRecPtr) -> types_error::PgResult<()> {
    crate::slot::wait_for_standby_confirmation::call(moveto)
}

/// `static void StartReplication(StartReplicationCmd *cmd)`.
pub fn StartReplication(_cmd: &StartReplicationCmd) {
    panic!(
        "StartReplication: depends on unported xlogreader segment streaming + \
         physical slot acquire + libpq CopyBoth setup + WalSndLoop(XLogSendPhysical)"
    );
}

/// `static void StartLogicalReplication(StartReplicationCmd *cmd)`.
pub fn StartLogicalReplication(_cmd: &StartReplicationCmd) {
    panic!(
        "StartLogicalReplication: depends on unported logical decoding context \
         creation (CreateDecodingContext) + output plugin + WalSndLoop(XLogSendLogical)"
    );
}

/// `static void WalSndSegmentOpen(...)` — xlogreader segment-open callback.
pub fn WalSndSegmentOpen() {
    panic!("WalSndSegmentOpen: depends on unported xlogreader segment open");
}

/// `static int logical_read_xlog_page(...)` — xlogreader page-read callback.
pub fn logical_read_xlog_page() -> i32 {
    panic!("logical_read_xlog_page: depends on unported xlogreader page read");
}

/// `if (xlogreader) XLogReaderFree(xlogreader)` in WalSndErrorCleanup — close the
/// walsender's open xlogreader if any.  The xlogreader is owned elsewhere.
pub fn xlogreader_close_if_open() {
    // The walsender's `XLogReaderState *xlogreader` is owned by the xlogreader
    // subsystem; there is nothing for this crate to free in the owned model
    // until that vertical lands.  (No-op, matching the C `if (xlogreader ==
    // NULL) return;` early-out in the common error path.)
}
