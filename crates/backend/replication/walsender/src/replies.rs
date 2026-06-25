//! Inbound standby messages (replies, status updates, hot-standby feedback).
//!
//! The genuinely in-crate computation — `TransactionIdInRecentPast` and the
//! transam XID arithmetic — is ported here.  The whole message-processing
//! functions read the `reply_message` libpq buffer and update the replication
//! slot's xmin/restart fields; those buffers and the slot are owned by the
//! (unported) libpq / slot subsystems, so each message-processing function is a
//! precise panic documenting that dependency until those owners land.

#![allow(non_snake_case)]

use crate::core::{
    proc_get, uint32, with_proc, InvalidTransactionId, InvalidXLogRecPtr, TimestampTz,
    TransactionId, XLogRecPtr, SYNC_REP_WAIT_APPLY, SYNC_REP_WAIT_FLUSH, SYNC_REP_WAIT_WRITE,
};
use crate::{logical_seam, slot, syncrep, timestamp, varsup};

/// `FirstNormalTransactionId` (`access/transam.h`): `((TransactionId) 3)`.
const FirstNormalTransactionId: TransactionId = 3;

// libpq message type bytes (protocol.h).
const PQMSG_COPY_DATA: u8 = b'd';
const PQMSG_COPY_DONE: u8 = b'c';
const PQMSG_TERMINATE: u8 = b'X';

// pq_getmessage maximum body lengths (pqcomm.h).
const PQ_LARGE_MESSAGE_LIMIT: i32 = 0x3fffffff;
const PQ_SMALL_MESSAGE_LIMIT: i32 = 10000;

#[inline]
fn TransactionIdIsNormal(xid: TransactionId) -> bool {
    xid >= FirstNormalTransactionId
}

/// `TransactionIdPrecedes(id1, id2)` (transam.c).
#[inline]
fn TransactionIdPrecedes(id1: TransactionId, id2: TransactionId) -> bool {
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 < id2;
    }
    let diff = id1.wrapping_sub(id2) as i32;
    diff < 0
}

/// A forward cursor over a received message body, mirroring the `pq_getmsg*`
/// readers (`pqformat.c`) used to decode standby replies (big-endian).
struct MsgReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> MsgReader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        MsgReader { buf, pos: 0 }
    }
    /// `pq_getmsgbyte(buf)`.
    fn get_byte(&mut self) -> u8 {
        let b = self.buf[self.pos];
        self.pos += 1;
        b
    }
    /// `pq_getmsgint64(buf)`.
    fn get_int64(&mut self) -> i64 {
        let mut a = [0u8; 8];
        a.copy_from_slice(&self.buf[self.pos..self.pos + 8]);
        self.pos += 8;
        i64::from_be_bytes(a)
    }
    /// `pq_getmsgint(buf, 4)`.
    fn get_int32(&mut self) -> u32 {
        let mut a = [0u8; 4];
        a.copy_from_slice(&self.buf[self.pos..self.pos + 4]);
        self.pos += 4;
        u32::from_be_bytes(a)
    }
}

#[inline]
fn XidFromFullTransactionId(value: u64) -> TransactionId {
    value as u32
}

#[inline]
fn EpochFromFullTransactionId(value: u64) -> uint32 {
    (value >> 32) as u32
}

#[inline]
fn TransactionIdPrecedesOrEquals(id1: TransactionId, id2: TransactionId) -> bool {
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 <= id2;
    }
    let diff = id1.wrapping_sub(id2) as i32;
    diff <= 0
}

/// `static void ProcessRepliesIfAny(void)` — process incoming messages while
/// streaming, and check whether the remote end closed the connection.
pub fn ProcessRepliesIfAny() {
    let mut received = false;

    // last_processing = GetCurrentTimestamp();
    let last_processing = timestamp::get_current_timestamp::call();
    with_proc(|p| p.last_processing = last_processing);

    // If we already received a CopyDone from the frontend, any subsequent
    // message is the beginning of a new command, and should be processed in the
    // main processing loop.
    while !proc_get(|p| p.streamingDoneReceiving) {
        // pq_startmsgread();
        crate::pq::pq_startmsgread::call().expect("pq_startmsgread");

        // r = pq_getbyte_if_available(&firstchar);
        let firstchar = match crate::pq::pq_getbyte_if_available::call() {
            Err(_) => {
                // unexpected error or EOF — COMMERROR already logged by the
                // owner; proc_exit(0).
                crate::ereport_commerror_eof_on_standby();
                crate::proc_exit(0);
            }
            Ok(None) => {
                // no data available without blocking
                crate::pq::pq_endmsgread::call();
                break;
            }
            Ok(Some(c)) => c,
        };

        // Validate message type and set packet size limit.
        let maxmsglen = match firstchar {
            PQMSG_COPY_DATA => PQ_LARGE_MESSAGE_LIMIT,
            PQMSG_COPY_DONE | PQMSG_TERMINATE => PQ_SMALL_MESSAGE_LIMIT,
            _ => {
                crate::ereport_fatal_invalid_standby_message(firstchar);
            }
        };

        // Read the message contents.
        //   resetStringInfo(&reply_message);
        //   if (pq_getmessage(&reply_message, maxmsglen)) { COMMERROR; proc_exit(0); }
        // The C `reply_message` is a file-static StringInfo; in the owned model
        // the message body is read into a short-lived context that lasts for
        // this one message's processing.
        let ctx = mcx::MemoryContext::new("reply_message");
        let body = match crate::pq::pq_getmessage::call(ctx.mcx(), maxmsglen) {
            Ok(Some(b)) => b,
            Ok(None) | Err(_) => {
                crate::ereport_commerror_eof_on_standby();
                crate::proc_exit(0);
            }
        };

        // ... and process it.
        match firstchar {
            // 'd' means a standby reply wrapped in a CopyData packet.
            PQMSG_COPY_DATA => {
                ProcessStandbyMessage(&body);
                received = true;
            }
            // CopyDone means the standby requested to finish streaming. Reply
            // with CopyDone, if we had not sent that already.
            PQMSG_COPY_DONE => {
                if !proc_get(|p| p.streamingDoneSending) {
                    crate::pq_putmessage_noblock_copydone();
                    with_proc(|p| p.streamingDoneSending = true);
                }
                with_proc(|p| p.streamingDoneReceiving = true);
                received = true;
            }
            // 'X' means that the standby is closing down the socket.
            PQMSG_TERMINATE => crate::proc_exit(0),
            _ => debug_assert!(false), // NOT REACHED
        }
    }

    // Save the last reply timestamp if we've received at least one reply.
    if received {
        with_proc(|p| {
            p.last_reply_timestamp = last_processing;
            p.waiting_for_ping_response = false;
        });
    }
}

/// `static void ProcessStandbyMessage(void)` — dispatch a standby status update.
/// `reply_message` is passed as `body` (after `pq_getmessage`).
pub fn ProcessStandbyMessage(body: &[u8]) {
    let mut r = MsgReader::new(body);

    // msgtype = pq_getmsgbyte(&reply_message);
    let msgtype = r.get_byte();

    match msgtype {
        b'r' => ProcessStandbyReplyMessage(&mut r),
        b'h' => ProcessStandbyHSFeedbackMessage(&mut r),
        _ => {
            crate::ereport_commerror_unexpected_message_type(msgtype);
            crate::proc_exit(0);
        }
    }
}

/// `static void PhysicalConfirmReceivedLocation(XLogRecPtr lsn)`.
pub fn PhysicalConfirmReceivedLocation(lsn: XLogRecPtr) {
    debug_assert!(lsn != InvalidXLogRecPtr);

    // SpinLockAcquire(&slot->mutex);
    // if (slot->data.restart_lsn != lsn) { changed = true; slot->data.restart_lsn = lsn; }
    // SpinLockRelease(&slot->mutex);
    slot::slot_mutex_acquire::call();
    let changed = slot::slot_restart_lsn::call() != lsn;
    if changed {
        slot::slot_set_restart_lsn::call(lsn);
    }
    slot::slot_mutex_release::call();

    if changed {
        // ReplicationSlotMarkDirty();
        slot::replication_slot_mark_dirty::call();
        // ReplicationSlotsComputeRequiredLSN();
        slot::replication_slots_compute_required_lsn::call()
            .expect("ReplicationSlotsComputeRequiredLSN");
        // PhysicalWakeupLogicalWalSnd();
        crate::physical::PhysicalWakeupLogicalWalSnd();
    }

    // One could argue that the slot should be saved to disk now, but that'd be
    // energy wasted — see the C comment.
}

/// `static void ProcessStandbyReplyMessage(void)`.  Reads from the `reply_message`
/// cursor (the caller already consumed the msgtype byte).
fn ProcessStandbyReplyMessage(r: &mut MsgReader<'_>) {
    // writePtr = pq_getmsgint64(&reply_message);
    let writePtr = r.get_int64() as XLogRecPtr;
    let flushPtr = r.get_int64() as XLogRecPtr;
    let applyPtr = r.get_int64() as XLogRecPtr;
    let replyTime: TimestampTz = r.get_int64();
    let replyRequested = r.get_byte() != 0;

    // See if we can compute the round-trip lag for these positions.
    let now = timestamp::get_current_timestamp::call();
    let writeLag = crate::lag_tracker::LagTrackerRead(SYNC_REP_WAIT_WRITE, writePtr, now);
    let flushLag = crate::lag_tracker::LagTrackerRead(SYNC_REP_WAIT_FLUSH, flushPtr, now);
    let applyLag = crate::lag_tracker::LagTrackerRead(SYNC_REP_WAIT_APPLY, applyPtr, now);

    // If the standby reports it has fully replayed the WAL in two consecutive
    // reply messages, the second results from wal_receiver_status_interval; a
    // convenient time to forget stale lag times.
    let sentPtr = proc_get(|p| p.sentPtr);
    let mut clearLagTimes = false;
    if applyPtr == sentPtr {
        if proc_get(|p| p.ProcessStandbyReplyMessage_fullyAppliedLastTime) {
            clearLagTimes = true;
        }
        with_proc(|p| p.ProcessStandbyReplyMessage_fullyAppliedLastTime = true);
    } else {
        with_proc(|p| p.ProcessStandbyReplyMessage_fullyAppliedLastTime = false);
    }

    // Send a reply if the standby requested one.
    if replyRequested {
        crate::mainloop::WalSndKeepalive(false, InvalidXLogRecPtr);
    }

    // Update shared state for this WalSender process based on reply data.
    crate::shmem_array::my_set_reply(
        writePtr, flushPtr, applyPtr, writeLag, flushLag, applyLag, clearLagTimes, replyTime,
    );

    if !proc_get(|p| p.am_cascading_walsender) {
        syncrep::sync_rep_release_waiters::call().expect("SyncRepReleaseWaiters");
    }

    // Advance our local xmin horizon when the client confirmed a flush
    // (walsender.c ProcessStandbyReplyMessage):
    //   if (SlotIsLogical(MyReplicationSlot))
    //       LogicalConfirmReceivedLocation(flushPtr);
    //   else
    //       PhysicalConfirmReceivedLocation(flushPtr);
    if slot::my_replication_slot_is_set::call() && flushPtr != InvalidXLogRecPtr {
        if !slot::slot_is_physical::call() {
            // Logical slot: persist the client's confirmed flush so a later
            // START_REPLICATION (or restart) resumes after the acknowledged
            // changes instead of re-decoding them (recovery TAP 006 subtest 8).
            logical_seam::logical_confirm_received_location::call(flushPtr)
                .expect("LogicalConfirmReceivedLocation");
        } else {
            PhysicalConfirmReceivedLocation(flushPtr);
        }
    }
}

/// `static void ProcessStandbyHSFeedbackMessage(void)`.
fn ProcessStandbyHSFeedbackMessage(r: &mut MsgReader<'_>) {
    // Decipher the reply message (caller already consumed the msgtype byte).
    let _replyTime: TimestampTz = r.get_int64();
    let feedbackXmin: TransactionId = r.get_int32();
    let feedbackEpoch: uint32 = r.get_int32();
    let feedbackCatalogXmin: TransactionId = r.get_int32();
    let feedbackCatalogEpoch: uint32 = r.get_int32();

    // Update shared state for this WalSender: only the reply timestamp.
    crate::shmem_array::my_set_reply_time(_replyTime);

    // Unset WalSender's xmins if the feedback message values are invalid (the
    // downstream turned hot_standby_feedback off).
    if !TransactionIdIsNormal(feedbackXmin) && !TransactionIdIsNormal(feedbackCatalogXmin) {
        crate::proc_seams::set_my_proc_xmin::call(InvalidTransactionId);
        if slot::my_replication_slot_is_set::call() {
            PhysicalReplicationSlotNewXmin(feedbackXmin, feedbackCatalogXmin);
        }
        return;
    }

    // Check that the provided xmin/epoch are sane; ignore if not.
    if TransactionIdIsNormal(feedbackXmin)
        && !TransactionIdInRecentPast(feedbackXmin, feedbackEpoch)
    {
        return;
    }
    if TransactionIdIsNormal(feedbackCatalogXmin)
        && !TransactionIdInRecentPast(feedbackCatalogXmin, feedbackCatalogEpoch)
    {
        return;
    }

    // Set the WalSender's xmin to the standby's requested xmin (held back via
    // the slot if one is in use, otherwise via the walsender's PGPROC entry).
    if slot::my_replication_slot_is_set::call() {
        PhysicalReplicationSlotNewXmin(feedbackXmin, feedbackCatalogXmin);
    } else if TransactionIdIsNormal(feedbackCatalogXmin)
        && TransactionIdPrecedes(feedbackCatalogXmin, feedbackXmin)
    {
        crate::proc_seams::set_my_proc_xmin::call(feedbackCatalogXmin);
    } else {
        crate::proc_seams::set_my_proc_xmin::call(feedbackXmin);
    }
}

/// `void PhysicalReplicationSlotNewXmin(TransactionId feedbackXmin,
/// TransactionId feedbackCatalogXmin)`.
pub fn PhysicalReplicationSlotNewXmin(
    feedback_xmin: TransactionId,
    feedback_catalog_xmin: TransactionId,
) {
    // SpinLockAcquire(&slot->mutex);
    slot::slot_mutex_acquire::call();
    // MyProc->xmin = InvalidTransactionId;
    crate::proc_seams::set_my_proc_xmin::call(InvalidTransactionId);

    let mut changed = false;

    // For physical replication we don't need the xmin/effective_xmin interlock,
    // so set both at once.
    let slot_xmin = slot::slot_xmin::call();
    if !TransactionIdIsNormal(slot_xmin)
        || !TransactionIdIsNormal(feedback_xmin)
        || TransactionIdPrecedes(slot_xmin, feedback_xmin)
    {
        changed = true;
        slot::slot_set_xmin::call(feedback_xmin);
        slot::slot_set_effective_xmin::call(feedback_xmin);
    }
    let slot_catalog_xmin = slot::slot_catalog_xmin::call();
    if !TransactionIdIsNormal(slot_catalog_xmin)
        || !TransactionIdIsNormal(feedback_catalog_xmin)
        || TransactionIdPrecedes(slot_catalog_xmin, feedback_catalog_xmin)
    {
        changed = true;
        slot::slot_set_catalog_xmin::call(feedback_catalog_xmin);
        slot::slot_set_effective_catalog_xmin::call(feedback_catalog_xmin);
    }
    // SpinLockRelease(&slot->mutex);
    slot::slot_mutex_release::call();

    if changed {
        // ReplicationSlotMarkDirty();
        slot::replication_slot_mark_dirty::call();
        // ReplicationSlotsComputeRequiredXmin(false);
        slot::replication_slots_compute_required_xmin::call(false)
            .expect("ReplicationSlotsComputeRequiredXmin");
    }
}

/// `static bool TransactionIdInRecentPast(TransactionId xid, uint32 epoch)`.
pub fn TransactionIdInRecentPast(xid: TransactionId, epoch: uint32) -> bool {
    let nextFullXid: u64 = varsup::read_next_full_transaction_id::call().value;
    let nextXid: TransactionId = XidFromFullTransactionId(nextFullXid);
    let nextEpoch: uint32 = EpochFromFullTransactionId(nextFullXid);

    if xid <= nextXid {
        if epoch != nextEpoch {
            return false;
        }
    } else if epoch.wrapping_add(1) != nextEpoch {
        return false;
    }

    if !TransactionIdPrecedesOrEquals(xid, nextXid) {
        return false; // epoch OK, but it's wrapped around
    }

    true
}
