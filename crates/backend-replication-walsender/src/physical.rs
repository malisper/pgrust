//! Physical (streaming) replication: the physical send-data callback.
//!
//! 1:1 port of `XLogSendPhysical` and `PhysicalWakeupLogicalWalSnd`.  The
//! send-decision control flow (how far we may send, message sizing, caught-up
//! bookkeeping, lag-tracker recording, sent-pointer advance) is ported here.
//! The WAL read itself (`WALReadFromBuffers` / `WALRead`), the timeline-history
//! lookup, the `output_message` framing, and the xlogreader/segment state are
//! owned by the unported xlog/xlogreader subsystems; the emit step is a precise
//! panic until that vertical lands.

#![allow(non_snake_case)]

use crate::core::{
    proc_get, with_proc, TimeLineID, WalSndState, XLogRecPtr, MAX_SEND_SIZE, XLOG_BLCKSZ,
};
use crate::{ps_status, timestamp, xlog};

/// `static void XLogSendPhysical(void)`.
pub fn XLogSendPhysical() {
    // If requested switch the WAL sender to the stopping state.
    if proc_get(|p| p.got_STOPPING) != 0 {
        crate::init::WalSndSetState(WalSndState::WALSNDSTATE_STOPPING);
    }

    if proc_get(|p| p.streamingDoneSending) {
        with_proc(|p| p.WalSndCaughtUp = true);
        return;
    }

    // Figure out how far we can safely send the WAL.
    let SendRqstPtr: XLogRecPtr = if proc_get(|p| p.sendTimeLineIsHistoric) {
        // Streaming an old timeline that is in this server's history but is not
        // the one we're currently inserting or replaying: stream up to the
        // switch-off point.
        proc_get(|p| p.sendTimeLineValidUpto)
    } else if proc_get(|p| p.am_cascading_walsender) {
        // Streaming the latest timeline on a standby.  Detect promotion /
        // timeline switch after computing FlushPtr to avoid a race.
        let mut SendRqstTLI: TimeLineID = 0;
        let mut becameHistoric = false;
        let mut send_rqst_ptr = crate::start_replication::GetStandbyFlushRecPtr(&mut SendRqstTLI);

        if !xlog::recovery_in_progress::call() {
            // We have been promoted.
            SendRqstTLI = xlog::get_wal_insertion_timeline_if_set::call();
            with_proc(|p| p.am_cascading_walsender = false);
            becameHistoric = true;
        } else if proc_get(|p| p.sendTimeLine) != SendRqstTLI {
            // Still a cascading standby, but the timeline we're sending is no
            // longer the one recovery is recovering from.
            becameHistoric = true;
        }

        if becameHistoric {
            // The timeline we were sending has become historic.  Read the new
            // timeline's history to find where we forked off.
            let send_tl = proc_get(|p| p.sendTimeLine);
            let (valid_upto, next_tli) = timeline_switch_point(send_tl, SendRqstTLI);
            with_proc(|p| {
                p.sendTimeLineValidUpto = valid_upto;
                p.sendTimeLineNextTLI = next_tli;
            });
            debug_assert!(send_tl < next_tli);
            with_proc(|p| p.sendTimeLineIsHistoric = true);
            send_rqst_ptr = valid_upto;
        }

        send_rqst_ptr
    } else {
        // Streaming the current timeline on a primary: send all data written
        // out and fsync'd to disk.
        xlog::get_flush_rec_ptr::call().0
    };

    // Record the current time as an approximation of when this WAL was written.
    crate::lag_tracker::LagTrackerWrite(SendRqstPtr, timestamp::get_current_timestamp::call());

    // If this is a historic timeline and we've reached the fork point, stop.
    let (is_historic, valid_upto, sent) =
        proc_get(|p| (p.sendTimeLineIsHistoric, p.sendTimeLineValidUpto, p.sentPtr));
    if is_historic && valid_upto <= sent {
        // Close the current file and send CopyDone.
        crate::start_replication::xlogreader_close_if_open();
        crate::pq_putmessage_noblock_copydone();
        with_proc(|p| {
            p.streamingDoneSending = true;
            p.WalSndCaughtUp = true;
        });
        return;
    }

    // Do we have any work to do?
    debug_assert!(sent <= SendRqstPtr);
    if SendRqstPtr <= sent {
        with_proc(|p| p.WalSndCaughtUp = true);
        return;
    }

    // Figure out how much to send in one message.
    let startptr: XLogRecPtr = sent;
    let mut endptr: XLogRecPtr = startptr + MAX_SEND_SIZE as u64;

    if SendRqstPtr <= endptr {
        endptr = SendRqstPtr;
        with_proc(|p| p.WalSndCaughtUp = !is_historic);
    } else {
        // Round down to page boundary.
        endptr -= endptr % XLOG_BLCKSZ as u64;
        with_proc(|p| p.WalSndCaughtUp = false);
    }

    let nbytes = (endptr - startptr) as usize;
    debug_assert!(nbytes <= MAX_SEND_SIZE);

    // The WAL read into the output buffer, the message header/terminator, the
    // timestamp fill-in, the put-message, and the CheckXLogRemoved /
    // cascading-reload retry loop are owned by the xlog/xlogreader subsystem.
    xlog_send_physical_emit(startptr, endptr, SendRqstPtr);

    let new_sent = endptr;
    with_proc(|p| p.sentPtr = new_sent);

    // Update shared memory status.
    crate::shmem_array::my_set_sentptr(new_sent);

    // Report progress of XLOG streaming in PS display.
    if ps_status::update_process_title::call() {
        let title = alloc::format!("streaming {:X}/{:X}", (new_sent >> 32) as u32, new_sent as u32);
        ps_status::set_ps_display::call(title);
    }
}

/// The WAL read + framing + put-message of `XLogSendPhysical` (the
/// `WALReadFromBuffers`/`WALRead` loop, the `'w'` header, the CheckXLogRemoved
/// + cascading needreload retry).  Owned by the xlog/xlogreader subsystem.
fn xlog_send_physical_emit(_startptr: XLogRecPtr, _endptr: XLogRecPtr, _send_rqst_ptr: XLogRecPtr) {
    panic!(
        "XLogSendPhysical emit: depends on unported WALRead / xlogreader segment \
         read + CheckXLogRemoved + libpq 'w' message framing"
    );
}

/// `readTimeLineHistory(rqstTLI)` + `tliSwitchPoint(sendTLI, history)`.  The
/// timeline-history read allocates in a memory context owned by the xlog
/// subsystem; reached there until the mcx-threaded vertical lands.
fn timeline_switch_point(_send_tli: TimeLineID, _rqst_tli: TimeLineID) -> (XLogRecPtr, TimeLineID) {
    panic!(
        "XLogSendPhysical timeline switch: depends on unported mcx-threaded \
         readTimeLineHistory + tliSwitchPoint"
    );
}

/// `void PhysicalWakeupLogicalWalSnd(void)`.
pub fn PhysicalWakeupLogicalWalSnd() {
    debug_assert!(crate::slot::slot_is_physical::call());

    // On a standby there are no walsenders waiting for standbys to catch up.
    if xlog::recovery_in_progress::call() {
        return;
    }

    if crate::slot::slot_exists_in_sync_standby_slots::call(&crate::slot::slot_name::call()) {
        crate::condvar::condition_variable_broadcast::call(
            &crate::core::wal_snd_ctl().wal_confirm_rcv_cv,
        );
    }
}
