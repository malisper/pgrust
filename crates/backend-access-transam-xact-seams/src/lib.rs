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
    /// `IsolationUsesXactSnapshot()` (xact.h): true when the current
    /// transaction isolation level is REPEATABLE READ or higher
    /// (`XactIsoLevel >= XACT_REPEATABLE_READ`). Pure read of the backend-local
    /// `XactIsoLevel`.
    pub fn isolation_uses_xact_snapshot() -> bool
);

seam_core::seam!(
    /// `IsInParallelMode()` (xact.c): true when the current transaction (or
    /// subtransaction) has entered parallel mode
    /// (`CurrentTransactionState->parallelModeLevel != 0`). Pure read of
    /// backend-local transaction state; cannot `ereport`.
    pub fn is_in_parallel_mode() -> bool
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

seam_core::seam!(
    /// `xact_redo(record)` (xact.c) — WAL redo for RM_XACT_ID records
    /// (`rm_redo` slot of `RmgrTable`). Can `ereport(ERROR)`, carried on
    /// `Err`.
    pub fn xact_redo(record: &mut types_wal::rmgr::XLogReaderState<'_>) -> PgResult<()>
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

seam_core::seam!(
    /// `XactLogCommitRecord(...)` (xact.c): assemble and `XLogInsert` the
    /// transaction commit record (incl. the 2PC variant when `twophase_xid` is
    /// set) and return its end LSN. The WAL insert can `ereport(ERROR)`,
    /// carried on `Err`.
    pub fn xact_log_commit_record(
        args: &types_wal::xact_records::XactLogCommitRecordArgs,
    ) -> PgResult<types_core::XLogRecPtr>
);

seam_core::seam!(
    /// `XactLogAbortRecord(...)` (xact.c): assemble and `XLogInsert` the
    /// transaction abort record (incl. the 2PC variant) and return its end LSN.
    /// The WAL insert can `ereport(ERROR)`, carried on `Err`.
    pub fn xact_log_abort_record(
        args: &types_wal::xact_records::XactLogAbortRecordArgs,
    ) -> PgResult<types_core::XLogRecPtr>
);

seam_core::seam!(
    /// `XactLastRecEnd` (xact.c global): the end LSN of the last record this
    /// backend inserted; read after the commit/abort emit for
    /// `replorigin_session_advance`. Pure read of backend-local state.
    pub fn xact_last_rec_end() -> types_core::XLogRecPtr
);

seam_core::seam!(
    /// `RequireTransactionBlock(isTopLevel, stmtType)` (xact.c) — `ereport`s if
    /// the statement is not running inside a transaction block (so it would
    /// have no user-visible effect). The C arg is `const char *stmtType`.
    pub fn require_transaction_block(is_top_level: bool, stmt_type: &str) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `IsTransactionOrTransactionBlock()` (xact.c): true when in a
    /// transaction or transaction block. Pure read of backend-local state.
    pub fn is_transaction_or_transaction_block() -> bool
);

seam_core::seam!(
    /// `GetTopTransactionIdIfAny()` (xact.c): the top transaction's xid, or
    /// `InvalidTransactionId` if none assigned. Pure read of backend-local
    /// state.
    pub fn get_top_transaction_id_if_any() -> types_core::TransactionId
);

seam_core::seam!(
    /// Set the `CheckXidAlive` global (xact.c) — `ResetLogicalStreamingState`.
    pub fn set_check_xid_alive(xid: types_core::TransactionId)
);

seam_core::seam!(
    /// Set the `bsysscan` global (xact.c) — `ResetLogicalStreamingState`.
    pub fn set_bsysscan(value: bool)
);
