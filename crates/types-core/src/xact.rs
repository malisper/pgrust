//! Transaction-system scalar vocabulary (`c.h`).
//!
//! Populated incrementally from ../pgrust/src-idiomatic/crates/types/src/xact.rs
//! as ports need items; only the items currently consumed are present.

/// `CommandId` (`c.h`) — a `uint32`.
pub type CommandId = u32;

/// `InvalidTransactionId` (`access/transam.h`).
pub const InvalidTransactionId: crate::primitive::TransactionId = 0;

/// `FullTransactionId` (`access/transam.h`) — a 64-bit transaction id carrying
/// the epoch in the high 32 bits and the `TransactionId` in the low 32 bits.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FullTransactionId {
    pub value: u64,
}

impl FullTransactionId {
    /// `FullTransactionIdFromEpochAndXid(epoch, xid)` (`access/transam.h`).
    pub const fn from_epoch_and_xid(epoch: u32, xid: crate::primitive::TransactionId) -> Self {
        Self {
            value: ((epoch as u64) << 32) | xid as u64,
        }
    }

    /// `XidFromFullTransactionId(fxid)` (`access/transam.h`).
    pub const fn xid(self) -> crate::primitive::TransactionId {
        self.value as crate::primitive::TransactionId
    }

    /// `FullTransactionIdIsValid(fxid)` (`access/transam.h`).
    pub const fn is_valid(self) -> bool {
        self.xid() != InvalidTransactionId
    }
}

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
