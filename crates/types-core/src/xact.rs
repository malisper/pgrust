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


/// `MaxTransactionId` (access/transam.h) — the largest 32-bit `TransactionId`.
pub const MaxTransactionId: TransactionId = 0xFFFF_FFFF;

/// `TransactionIdIsNormal(xid)` (access/transam.h).
#[inline]
pub const fn TransactionIdIsNormal(xid: TransactionId) -> bool {
    xid >= FirstNormalTransactionId
}

/// `FullTransactionId` (`access/transam.h`) — a 64-bit transaction id
/// (epoch in the high 32 bits, xid in the low 32).
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FullTransactionId {
    pub value: u64,
}

impl FullTransactionId {
    pub const fn from_u64(value: u64) -> Self {
        Self { value }
    }

    /// `FullTransactionIdFromEpochAndXid` (access/transam.h).
    pub const fn from_epoch_and_xid(epoch: u32, xid: TransactionId) -> Self {
        Self {
            value: ((epoch as u64) << 32) | xid as u64,
        }
    }

    /// `EpochFromFullTransactionId(x)` — `(uint32) ((x).value >> 32)`.
    pub const fn epoch(self) -> u32 {
        (self.value >> 32) as u32
    }

    /// `XidFromFullTransactionId(x)` — `(uint32) (x).value`.
    pub const fn xid(self) -> crate::primitive::TransactionId {
        self.value as u32
    }
}
