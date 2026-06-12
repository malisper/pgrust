//! Transaction-system scalar vocabulary (`c.h`).
//!
//! Populated incrementally from ../pgrust/src-idiomatic/crates/types/src/xact.rs
//! as ports need items; only the items currently consumed are present.

/// `CommandId` (`c.h`) — a `uint32`.
pub type CommandId = u32;

/// `SubTransactionId` (`c.h`) — a `uint32`.
pub type SubTransactionId = u32;

/// `InvalidSubTransactionId` (`c.h`).
pub const InvalidSubTransactionId: SubTransactionId = 0;
