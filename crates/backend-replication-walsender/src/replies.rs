//! Inbound standby messages (replies, status updates, hot-standby feedback).
//!
//! The genuinely in-crate computation — `TransactionIdInRecentPast` and the
//! transam XID arithmetic — is ported here.  The whole message-processing
//! functions read the `reply_message` libpq buffer and update the replication
//! slot's xmin/restart fields; those buffers and the slot are owned by the
//! (unported) libpq / slot subsystems, so each message-processing function is a
//! precise panic documenting that dependency until those owners land.

#![allow(non_snake_case)]

use crate::core::{uint32, TransactionId, XLogRecPtr};
use crate::varsup;

/// `FirstNormalTransactionId` (`access/transam.h`): `((TransactionId) 3)`.
const FirstNormalTransactionId: TransactionId = 3;

#[inline]
fn TransactionIdIsNormal(xid: TransactionId) -> bool {
    xid >= FirstNormalTransactionId
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
    panic!(
        "ProcessRepliesIfAny: depends on unported libpq inbound message loop \
         (pq_getbyte_if_available / pq_getmessage)"
    );
}

/// `static void ProcessStandbyMessage(void)` — dispatch a standby status update.
pub fn ProcessStandbyMessage() {
    panic!("ProcessStandbyMessage: depends on unported libpq reply_message read");
}

/// `static void PhysicalConfirmReceivedLocation(XLogRecPtr lsn)`.
pub fn PhysicalConfirmReceivedLocation(_lsn: XLogRecPtr) {
    panic!(
        "PhysicalConfirmReceivedLocation: depends on unported slot restart-LSN \
         advance + ReplicationSlotsComputeRequiredLSN"
    );
}

/// `static void ProcessStandbyReplyMessage(void)`.
pub fn ProcessStandbyReplyMessage() {
    panic!(
        "ProcessStandbyReplyMessage: depends on unported libpq reply_message \
         read + slot confirm + LagTrackerRead-driven shmem update"
    );
}

/// `static void ProcessStandbyHSFeedbackMessage(void)`.
pub fn ProcessStandbyHSFeedbackMessage() {
    panic!(
        "ProcessStandbyHSFeedbackMessage: depends on unported libpq \
         reply_message read + slot effective-xmin/catalog-xmin update"
    );
}

/// `void PhysicalReplicationSlotNewXmin(TransactionId feedbackXmin,
/// TransactionId feedbackCatalogXmin)`.
pub fn PhysicalReplicationSlotNewXmin(
    _feedback_xmin: TransactionId,
    _feedback_catalog_xmin: TransactionId,
) {
    panic!(
        "PhysicalReplicationSlotNewXmin: depends on unported slot effective-xmin \
         update + ReplicationSlotsComputeRequiredXmin"
    );
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
