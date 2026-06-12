//! Transaction-system scalar vocabulary (`c.h`).
//!
//! Populated incrementally from ../pgrust/src-idiomatic/crates/types/src/xact.rs
//! as ports need items; only the items currently consumed are present.

/// `CommandId` (`c.h`) — a `uint32`.
pub type CommandId = u32;

/// `InvalidTransactionId` (`access/transam.h`).
pub const InvalidTransactionId: crate::primitive::TransactionId = 0;

/// `TransactionIdIsValid(xid)` (`access/transam.h`).
#[inline]
pub const fn TransactionIdIsValid(xid: crate::primitive::TransactionId) -> bool {
    xid != InvalidTransactionId
}
