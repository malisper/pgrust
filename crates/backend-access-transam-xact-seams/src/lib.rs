//! Seam declarations for the `backend-access-transam-xact` unit
//! (`access/transam/xact.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_error::PgResult;

seam_core::seam!(
    /// `CommandCounterIncrement()` (xact.c): bump the command counter so
    /// in-progress catalog changes become visible. Can `ereport(ERROR)`
    /// (e.g. `cannot have more than 2^32-2 commands in a transaction`),
    /// carried on `Err`.
    pub fn command_counter_increment() -> PgResult<()>
);

seam_core::seam!(
    /// `GetCurrentTransactionNestLevel()` (xact.c): the current
    /// (sub)transaction nesting depth (1 = top level). Pure read of
    /// backend-local transaction state; cannot `ereport`.
    pub fn get_current_transaction_nest_level() -> i32
);

seam_core::seam!(
    /// `TransactionIdIsCurrentTransactionId(xid)` (xact.c): true iff `xid` is
    /// the current top transaction's xid or one of its in-progress
    /// subtransactions'. Pure lookup over backend-local transaction state;
    /// cannot `ereport`.
    pub fn transaction_id_is_current_transaction_id(xid: types_core::TransactionId) -> bool
);
