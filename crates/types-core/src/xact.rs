//! Transaction-system scalar vocabulary (`c.h`).
//!
//! Populated incrementally from ../pgrust/src-idiomatic/crates/types/src/xact.rs
//! as ports need items; only the items currently consumed are present.

use crate::primitive::{TransactionId, XLogRecPtr};

/// `CommandId` (`c.h`) — a `uint32`.
pub type CommandId = u32;

/// `XidStatus` (`access/clog.h`) — transaction status in pg_xact, an `int`.
pub type XidStatus = i32;

/// `InvalidTransactionId` (`access/transam.h`).
pub const InvalidTransactionId: crate::primitive::TransactionId = 0;
pub const BootstrapTransactionId: TransactionId = 1;
pub const FrozenTransactionId: TransactionId = 2;
pub const FirstNormalTransactionId: TransactionId = 3;

/// `InvalidXLogRecPtr` (`access/xlogdefs.h`).
pub const InvalidXLogRecPtr: XLogRecPtr = 0;

pub const TRANSACTION_STATUS_IN_PROGRESS: XidStatus = 0x00;
pub const TRANSACTION_STATUS_COMMITTED: XidStatus = 0x01;
pub const TRANSACTION_STATUS_ABORTED: XidStatus = 0x02;
pub const TRANSACTION_STATUS_SUB_COMMITTED: XidStatus = 0x03;

/// `TransactionIdIsValid(xid)` (`access/transam.h`).
#[inline]
pub const fn TransactionIdIsValid(xid: crate::primitive::TransactionId) -> bool {
    xid != InvalidTransactionId
}

/// One created/dropped pgstat item carried on commit/abort/prepare WAL
/// records, matching C's `xl_xact_stats_item` (`access/xact.h`:
/// `{ int kind; Oid dboid; uint32 objid_lo; uint32 objid_hi; }`). The split
/// `objid_lo`/`objid_hi` words (alignment-friendly WAL layout) are carried as
/// the single `u64` they encode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct XlXactStatsItem {
    pub kind: i32,
    pub dboid: crate::primitive::Oid,
    pub objid: u64,
}
