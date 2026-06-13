//! Reorder-buffer vocabulary (`replication/reorderbuffer.h`), trimmed to the
//! fields the logical-replication protocol writers consume.

use mcx::PgVec;
use types_core::{TimestampTz, TransactionId, XLogRecPtr};

/// `RBTXN_IS_PREPARED` (`replication/reorderbuffer.h`): this transaction is a
/// two-phase transaction whose PREPARE has been decoded.
pub const RBTXN_IS_PREPARED: u32 = 0x0040;

/// `ReorderBufferTXN` (`replication/reorderbuffer.h`), trimmed.
#[derive(Debug)]
pub struct ReorderBufferTXN<'mcx> {
    /// `uint32 txn_flags` — `RBTXN_*` bits.
    pub txn_flags: u32,
    /// `TransactionId xid` — the toplevel transaction's XID.
    pub xid: TransactionId,
    /// `char *gid` — the global transaction id; only set for two-phase
    /// transactions (the C pointer is NULL otherwise).
    pub gid: Option<PgVec<'mcx, u8>>,
    /// `XLogRecPtr final_lsn` — LSN of the record that lead to this xact's
    /// commit/abort.
    pub final_lsn: XLogRecPtr,
    /// `XLogRecPtr end_lsn` — LSN pointing to the end of the commit record +
    /// 1.
    pub end_lsn: XLogRecPtr,
    /// `union { TimestampTz commit_time; TimestampTz prepare_time;
    /// TimestampTz abort_time; } xact_time` — all union members are the same
    /// `TimestampTz` storage; the single field carries whichever the
    /// transaction's state makes meaningful.
    pub xact_time: TimestampTz,
}

impl ReorderBufferTXN<'_> {
    /// `rbtxn_is_prepared(txn)`: `(txn->txn_flags & RBTXN_IS_PREPARED) != 0`.
    pub fn is_prepared(&self) -> bool {
        self.txn_flags & RBTXN_IS_PREPARED != 0
    }
}
