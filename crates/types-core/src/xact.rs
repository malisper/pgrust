//! Transaction-system scalar vocabulary (`c.h`).
//!
//! Populated incrementally from ../pgrust/src-idiomatic/crates/types/src/xact.rs
//! as ports need items; only the items currently consumed are present.

/// `CommandId` (`c.h`) — a `uint32`.
pub type CommandId = u32;

use crate::primitive::TransactionId;

/// `InvalidTransactionId` (access/transam.h).
pub const InvalidTransactionId: TransactionId = 0;
/// `BootstrapTransactionId` (access/transam.h).
pub const BootstrapTransactionId: TransactionId = 1;
/// `FrozenTransactionId` (access/transam.h).
pub const FrozenTransactionId: TransactionId = 2;
/// `FirstNormalTransactionId` (access/transam.h).
pub const FirstNormalTransactionId: TransactionId = 3;
/// `MaxTransactionId` (access/transam.h) — the largest 32-bit `TransactionId`.
pub const MaxTransactionId: TransactionId = 0xFFFF_FFFF;

/// `TransactionIdIsValid(xid)` (access/transam.h).
#[inline]
pub const fn TransactionIdIsValid(xid: TransactionId) -> bool {
    xid != InvalidTransactionId
}

/// `TransactionIdIsNormal(xid)` (access/transam.h).
#[inline]
pub const fn TransactionIdIsNormal(xid: TransactionId) -> bool {
    xid >= FirstNormalTransactionId
}

/// `FullTransactionId` (access/transam.h) — a 64-bit epoch-qualified xid.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FullTransactionId {
    pub value: u64,
}

impl FullTransactionId {
    /// `FullTransactionIdFromEpochAndXid` (access/transam.h).
    pub const fn from_epoch_and_xid(epoch: u32, xid: TransactionId) -> Self {
        Self {
            value: ((epoch as u64) << 32) | xid as u64,
        }
    }

    /// `XidFromFullTransactionId` (access/transam.h) — the low 32 bits.
    pub const fn xid(self) -> TransactionId {
        self.value as TransactionId
    }

    /// `EpochFromFullTransactionId` (access/transam.h) — the high 32 bits.
    pub const fn epoch(self) -> u32 {
        (self.value >> 32) as u32
    }
}
