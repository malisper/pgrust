//! Logical replication: the logical send-data callback and the decoding-output
//! write/progress callbacks.
//!
//! 1:1 port of `XLogSendLogical`, `WalSndPrepareWrite`, `WalSndWriteData`,
//! `ProcessPendingWrites`, `WalSndUpdateProgress`.  The caught-up /
//! pending-write / progress control flow is ported here; the decoding context,
//! the output-plugin StringInfo, the `XLogReadRecord` step, and the message
//! framing are owned by the unported logical-decoding / xlogreader / libpq
//! subsystems and reached through their seams (or a precise panic where the
//! whole step is that subsystem's logic).

#![allow(non_snake_case)]

use core::ffi::c_long;

use crate::core::{
    proc_get, with_proc, InvalidXLogRecPtr, TimestampTz, TransactionId, XLogRecPtr,
    SYNC_STANDBY_DEFINED, WALSND_LOGICAL_LAG_TRACK_INTERVAL_MS,
};
use crate::{timestamp, xlog, xlogrecovery};

// Socket wakeup bits + wait event.
const WL_SOCKET_READABLE: u32 = 1 << 1;
const WL_SOCKET_WRITEABLE: u32 = 1 << 2;
const WAIT_EVENT_WAL_SENDER_WRITE_DATA: u32 = 0x06000000 | 8;

#[inline]
fn timestamptz_plus_ms(tz: TimestampTz, ms: i64) -> TimestampTz {
    tz + ms * 1000
}

/// `static void XLogSendLogical(void)`.
pub fn XLogSendLogical() {
    // Don't know whether we've caught up yet.
    with_proc(|p| p.WalSndCaughtUp = false);

    // Read the next record.  The decoding read step (LogicalDecodingProcessRecord
    // over XLogReadRecord) is owned by the logical-decoding/xlogreader vertical.
    let have_record = xlog_read_record_logical_and_process();

    if have_record {
        let end = logical_decoding_ctx_end_rec_ptr();
        with_proc(|p| p.sentPtr = end);
    }

    let end = logical_decoding_ctx_end_rec_ptr();

    // Initialize flushPtr first time through; otherwise update only if EndRecPtr
    // is past it.
    let flush_ptr = proc_get(|p| p.XLogSendLogical_flushPtr);
    if flush_ptr == InvalidXLogRecPtr || end >= flush_ptr {
        // Cascading logical walsenders use the replay LSN.
        let new_flush = if proc_get(|p| p.am_cascading_walsender) {
            xlogrecovery::get_xlog_replay_rec_ptr::call().0
        } else {
            xlog::get_flush_rec_ptr::call().0
        };
        with_proc(|p| p.XLogSendLogical_flushPtr = new_flush);
    }

    let flush_ptr = proc_get(|p| p.XLogSendLogical_flushPtr);
    if end >= flush_ptr {
        with_proc(|p| p.WalSndCaughtUp = true);
    }

    // If caught up and asked to stop, terminate after writing pending data.
    if proc_get(|p| p.WalSndCaughtUp && p.got_STOPPING != 0) {
        with_proc(|p| p.got_SIGUSR2 = 1);
    }

    // Update shared memory status.
    let sent = proc_get(|p| p.sentPtr);
    crate::shmem_array::my_set_sentptr(sent);
}

/// `static void WalSndPrepareWrite(LogicalDecodingContext *ctx, XLogRecPtr lsn,
/// TransactionId xid, bool last_write)`.
pub fn WalSndPrepareWrite(mut lsn: XLogRecPtr, _xid: TransactionId, last_write: bool) {
    // Can't have sync rep confused by sending the same LSN several times.
    if !last_write {
        lsn = InvalidXLogRecPtr;
    }

    // Build the `'w'` header into ctx->out: dataStart=lsn, walEnd=lsn, and a
    // reserved sendtime int64 (filled later, as in XLogSendPhysical).  The
    // output-plugin StringInfo `ctx->out` is owned by the logical-decoding
    // vertical.
    walsnd_prepare_write_emit(lsn);
}

/// `static void WalSndWriteData(LogicalDecodingContext *ctx, ...)`.
pub fn WalSndWriteData(_lsn: XLogRecPtr, _xid: TransactionId, _last_write: bool) {
    // Fill the send timestamp last, then put the gathered data out in a CopyData
    // packet.  The gathered data lives in `ctx->out` (logical-decoding owned).
    let now: TimestampTz = timestamp::get_current_timestamp::call();
    walsnd_write_data_emit(now);

    crate::check_for_interrupts();

    if crate::pq_flush_if_writable() != 0 {
        crate::init::WalSndShutdown();
    }

    // Try the fast path unless we get too close to the walsender timeout.
    let (timeout, last_reply) = proc_get(|p| (p.wal_sender_timeout, p.last_reply_timestamp));
    if now < timestamptz_plus_ms(last_reply, (timeout / 2) as i64) && !crate::pq_is_send_pending() {
        return;
    }

    ProcessPendingWrites();
}

/// `static void ProcessPendingWrites(void)`.
pub fn ProcessPendingWrites() {
    loop {
        crate::replies::ProcessRepliesIfAny();
        crate::mainloop::WalSndCheckTimeOut();
        crate::mainloop::WalSndKeepaliveIfNecessary();

        if !crate::pq_is_send_pending() {
            break;
        }

        let sleeptime: c_long =
            crate::mainloop::WalSndComputeSleeptime(timestamp::get_current_timestamp::call());

        crate::mainloop::WalSndWait(
            WL_SOCKET_WRITEABLE | WL_SOCKET_READABLE,
            sleeptime,
            WAIT_EVENT_WAL_SENDER_WRITE_DATA,
        );

        crate::reset_latch_my_latch_loop();
        crate::check_for_interrupts();

        if crate::config_reload_pending() {
            crate::clear_config_reload_pending();
            crate::process_config_file_sighup();
            crate::sync_rep_init_config();
        }

        if crate::pq_flush_if_writable() != 0 {
            crate::init::WalSndShutdown();
        }
    }

    // Reactivate latch so WalSndLoop knows to continue.
    crate::set_latch_my_latch();
}

/// `static void WalSndUpdateProgress(LogicalDecodingContext *ctx, ...)`.
pub fn WalSndUpdateProgress(lsn: XLogRecPtr, _xid: TransactionId, skipped_xact: bool) {
    let now: TimestampTz = timestamp::get_current_timestamp::call();
    let mut pending_writes = false;
    let end_xact = decoding_ctx_end_xact();

    // Track lag at most once per interval (we only get acks for end-of-tx LSNs).
    let send_time = proc_get(|p| p.WalSndUpdateProgress_sendTime);
    if end_xact
        && timestamp::timestamp_difference_exceeds::call(
            send_time,
            now,
            WALSND_LOGICAL_LAG_TRACK_INTERVAL_MS,
        )
    {
        crate::lag_tracker::LagTrackerWrite(lsn, now);
        with_proc(|p| p.WalSndUpdateProgress_sendTime = now);
    }

    // When skipping empty transactions in synchronous replication, send a
    // keepalive to avoid delaying such transactions.
    if skipped_xact
        && crate::sync_rep_requested()
        && (crate::shmem_array::ctl_sync_standbys_status() & SYNC_STANDBY_DEFINED) != 0
    {
        crate::mainloop::WalSndKeepalive(false, lsn);

        if crate::pq_flush_if_writable() != 0 {
            crate::init::WalSndShutdown();
        }

        if crate::pq_is_send_pending() {
            pending_writes = true;
        }
    }

    // Process pending writes if any, or try a keepalive if required.
    let (timeout, last_reply) = proc_get(|p| (p.wal_sender_timeout, p.last_reply_timestamp));
    if pending_writes
        || (!end_xact && now >= timestamptz_plus_ms(last_reply, (timeout / 2) as i64))
    {
        ProcessPendingWrites();
    }
}

// ---------------------------------------------------------------------------
// The logical-decoding-context-touching steps (owned by the logical decoding /
// xlogreader vertical).
// ---------------------------------------------------------------------------

/// `record = XLogReadRecord(ctx->reader, &errm); if (record) LogicalDecodingProcessRecord(...)`.
fn xlog_read_record_logical_and_process() -> bool {
    panic!(
        "XLogSendLogical record read: depends on unported XLogReadRecord + \
         LogicalDecodingProcessRecord"
    );
}

/// `ctx->reader->EndRecPtr`.
fn logical_decoding_ctx_end_rec_ptr() -> XLogRecPtr {
    panic!("XLogSendLogical: depends on unported LogicalDecodingContext->reader->EndRecPtr");
}

/// `ctx->end_xact` flag set during the output-plugin callback.
fn decoding_ctx_end_xact() -> bool {
    panic!("WalSndUpdateProgress: depends on unported LogicalDecodingContext->end_xact");
}

/// Build the `'w'` header into `ctx->out` (the output-plugin StringInfo).
fn walsnd_prepare_write_emit(_lsn: XLogRecPtr) {
    panic!("WalSndPrepareWrite: depends on unported LogicalDecodingContext->out framing");
}

/// Fill the send timestamp into `ctx->out` and `pq_putmessage_noblock('d', ...)`.
fn walsnd_write_data_emit(_now: TimestampTz) {
    panic!("WalSndWriteData: depends on unported LogicalDecodingContext->out CopyData send");
}
