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
    /// `GetCurrentTransactionId()` — assigns an xid if none yet; assignment
    /// can `ereport(ERROR)`.
    pub fn get_current_transaction_id() -> PgResult<types_core::TransactionId>
);

seam_core::seam!(
    /// `MyXactFlags |= XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK` (xact.c
    /// backend-global).
    pub fn set_my_xact_flags_acquired_access_exclusive_lock()
);
