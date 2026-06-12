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
    /// `TransactionIdIsCurrentTransactionId(xid)` (xact.c): true iff `xid` is
    /// the current top transaction's xid or one of its in-progress
    /// subtransactions'. Pure lookup over backend-local transaction state;
    /// cannot `ereport`.
    pub fn transaction_id_is_current_transaction_id(xid: types_core::TransactionId) -> bool
);

seam_core::seam!(
    /// `GetCurrentSubTransactionId()` (xact.c): the current subtransaction's
    /// id. Pure read of backend-local transaction state.
    pub fn get_current_sub_transaction_id() -> types_core::SubTransactionId
);

seam_core::seam!(
    /// `MyXactFlags |= XACT_FLAGS_ACCESSEDTEMPNAMESPACE` (xact.h flag on
    /// xact.c's `MyXactFlags`). Plain global-flag write.
    pub fn set_xact_accessed_temp_namespace()
);

seam_core::seam!(
    /// `StartTransactionCommand()` (xact.c). Can `ereport(ERROR)`, carried
    /// on `Err`.
    pub fn start_transaction_command() -> PgResult<()>
);

seam_core::seam!(
    /// `CommitTransactionCommand()` (xact.c). Can `ereport(ERROR)`, carried
    /// on `Err`.
    pub fn commit_transaction_command() -> PgResult<()>
);

seam_core::seam!(
    /// `AbortOutOfAnyTransaction()` (xact.c): abort the current transaction
    /// (at any nesting level) and return to default state.
    pub fn abort_out_of_any_transaction() -> PgResult<()>
);
