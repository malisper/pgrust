//! Transaction-system scalar vocabulary (`c.h`).
//!
//! Populated incrementally from ../pgrust/src-idiomatic/crates/types/src/xact.rs
//! as ports need items; only the items currently consumed are present.

use crate::primitive::{TransactionId, XLogRecPtr};

/// `CommandId` (`c.h`) — a `uint32`.
pub type CommandId = u32;

/// `XidStatus` (`access/clog.h`) — transaction status in pg_xact, an `int`.
pub type XidStatus = i32;

pub const InvalidTransactionId: TransactionId = 0;
pub const BootstrapTransactionId: TransactionId = 1;
pub const FrozenTransactionId: TransactionId = 2;
pub const FirstNormalTransactionId: TransactionId = 3;

/// `InvalidXLogRecPtr` (`access/xlogdefs.h`).
pub const InvalidXLogRecPtr: XLogRecPtr = 0;

pub const TRANSACTION_STATUS_IN_PROGRESS: XidStatus = 0x00;
pub const TRANSACTION_STATUS_COMMITTED: XidStatus = 0x01;
pub const TRANSACTION_STATUS_ABORTED: XidStatus = 0x02;
pub const TRANSACTION_STATUS_SUB_COMMITTED: XidStatus = 0x03;
