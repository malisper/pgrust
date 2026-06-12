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

seam_core::seam!(
    /// `IsTransactionState()` (xact.c): true when in a live transaction
    /// (`TRANS_INPROGRESS`). Pure read of backend-local transaction state.
    pub fn is_transaction_state() -> bool
);

seam_core::seam!(
    /// `GetCurrentCommandId(used)` (xact.c): the current command id; with
    /// `used` true the caller intends to use it to mark inserted/updated/
    /// deleted tuples, which is forbidden in parallel mode — that check
    /// `elog(ERROR)`s, carried on `Err`.
    pub fn get_current_command_id(used: bool) -> PgResult<types_core::xact::CommandId>
);

seam_core::seam!(
    /// `CheckXidAlive` (xact.c global): the xid being checked during logical
    /// decoding via the historic snapshot (`InvalidTransactionId` outside
    /// that path). Pure read of backend-local state.
    pub fn check_xid_alive() -> types_core::TransactionId
);

seam_core::seam!(
    /// `bsysscan` (xact.c global): true while inside a catalog scan started
    /// with a valid `CheckXidAlive`. Pure read of backend-local state.
    pub fn bsysscan() -> bool
);
