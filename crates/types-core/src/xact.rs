//! Transaction-system scalar vocabulary (`c.h`).
//!
//! Populated incrementally from ../pgrust/src-idiomatic/crates/types/src/xact.rs
//! as ports need items; only the items currently consumed are present.

/// `CommandId` (`c.h`) — a `uint32`.
pub type CommandId = u32;

/// `FullTransactionId` (`access/transam.h`) — a 64-bit transaction id
/// (epoch in the high 32 bits, xid in the low 32).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct FullTransactionId {
    pub value: u64,
}

impl FullTransactionId {
    pub const fn from_u64(value: u64) -> Self {
        Self { value }
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
