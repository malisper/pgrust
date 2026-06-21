//! Seam declarations for the `backend-access-transam-xact` unit
//! (`access/transam/xact.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_error::PgResult;

seam_core::seam!(
    /// `GetStableLatestTransactionId()` (xact.c:607): the transaction's XID if
    /// it has one, else the next-to-be-assigned XID, latched for the rest of
    /// the transaction. The reference point `age(xid)` (`xid_age`) measures
    /// against. Reads backend-local + shared transaction state; can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn get_stable_latest_transaction_id() -> PgResult<types_core::TransactionId>
);

seam_core::seam!(
    /// `GetCurrentStatementStartTimestamp()` (xact.c): the timestamp the
    /// current statement (transaction command) started. Pure read of
    /// backend-local transaction state.
    pub fn get_current_statement_start_timestamp() -> types_core::TimestampTz
);

seam_core::seam!(
    /// `GetCurrentTransactionStartTimestamp()` (xact.c:870): the timestamp the
    /// current transaction started. Pure read of backend-local transaction
    /// state.
    pub fn get_current_transaction_start_timestamp() -> types_core::TimestampTz
);

seam_core::seam!(
    /// `GetCurrentTransactionStopTimestamp()` (xact.c:891): the timestamp the
    /// current transaction stopped, setting it to the current time on first
    /// call if still unset. Read by `pgstat_relation_flush_cb` to stamp the
    /// per-table `lastscan`. Backend-local transaction state.
    pub fn get_current_transaction_stop_timestamp() -> types_core::TimestampTz
);

seam_core::seam!(
    /// `CommandCounterIncrement()` (xact.c): bump the command counter so
    /// in-progress catalog changes become visible. Can `ereport(ERROR)`
    /// (e.g. `cannot have more than 2^32-2 commands in a transaction`),
    /// carried on `Err`.
    pub fn command_counter_increment() -> PgResult<()>
);

seam_core::seam!(
    /// `StartTransactionCommand()` (xact.c): begin a transaction command,
    /// starting a transaction if none is active. Can `ereport(ERROR)`.
    pub fn start_transaction_command() -> PgResult<()>
);

seam_core::seam!(
    /// `CommitTransactionCommand()` (xact.c): commit the current transaction
    /// command. Can `ereport(ERROR)`.
    pub fn commit_transaction_command() -> PgResult<()>
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
    /// `IsAbortedTransactionBlockState()` (xact.c): true when the current
    /// transaction block is in the failed/aborted state
    /// (`TBLOCK_ABORT` / `TBLOCK_SUBABORT`), so only `ROLLBACK`-class commands
    /// may run. Pure read of backend-local transaction state.
    pub fn is_aborted_transaction_block_state() -> bool
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
    /// `MyXactFlags` (xact.c backend-global) — the current transaction's
    /// accumulated flags. Read by `FinishPreparedTransaction` (twophase.c) to
    /// OR into the 2nd-phase commit/abort record's `xinfo`.
    pub fn my_xact_flags() -> i32
);

seam_core::seam!(
    /// `GetCurrentSubTransactionId()` (xact.c): the current subtransaction's
    /// id. Pure read of backend-local transaction state.
    pub fn get_current_sub_transaction_id() -> types_core::SubTransactionId
);

seam_core::seam!(
    /// `IsSubTransaction()` (xact.c): true when the current transaction state
    /// is a subtransaction (`TBLOCK_SUBINPROGRESS` family). Pure read of
    /// backend-local transaction state; consumed by SPI's
    /// `SPI_inside_nonatomic_context`.
    pub fn is_sub_transaction() -> bool
);

seam_core::seam!(
    /// `MyXactFlags |= XACT_FLAGS_ACCESSEDTEMPNAMESPACE` (xact.h flag on
    /// xact.c's `MyXactFlags`). Plain global-flag write.
    pub fn set_xact_accessed_temp_namespace()
);

seam_core::seam!(
    /// `AbortOutOfAnyTransaction()` (xact.c): abort the current transaction
    /// (at any nesting level) and return to default state.
    pub fn abort_out_of_any_transaction() -> PgResult<()>
);

seam_core::seam!(
    /// `AbortCurrentTransaction()` (xact.c): abort the current transaction
    /// command. Can `ereport(ERROR)`, carried on `Err`.
    pub fn abort_current_transaction() -> PgResult<()>
);

seam_core::seam!(
    /// `BeginInternalSubTransaction(name)` (xact.c): start an internal
    /// subtransaction (the `name` is the C `const char *name`, `NULL` modeled as
    /// `None`). Used by reorder-buffer invalidation replay to isolate catalog
    /// cache invalidations. Can `ereport(ERROR)`, carried on `Err`.
    pub fn begin_internal_sub_transaction(name: Option<&str>) -> PgResult<()>
);

seam_core::seam!(
    /// `RollbackAndReleaseCurrentSubTransaction()` (xact.c): roll back and
    /// release the current (internal) subtransaction. Can `ereport(ERROR)`,
    /// carried on `Err`.
    pub fn rollback_and_release_current_sub_transaction() -> PgResult<()>
);

seam_core::seam!(
    /// `BeginTransactionBlock()` (xact.c): begin a transaction block (the
    /// `BEGIN`/`START TRANSACTION` driver). Can `ereport(ERROR)`/`WARNING`,
    /// carried on `Err`.
    pub fn begin_transaction_block() -> PgResult<()>
);

seam_core::seam!(
    /// `EndTransactionBlock(chain)` (xact.c): end a transaction block
    /// (`COMMIT`/`END`); `chain` requests `AND CHAIN`. Returns whether the
    /// commit should be fully performed now. Can `ereport`, carried on `Err`.
    pub fn end_transaction_block(chain: bool) -> PgResult<bool>
);

seam_core::seam!(
    /// `DefineSavepoint(name)` (xact.c): define a savepoint with `name`. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn define_savepoint(name: &str) -> PgResult<()>
);

seam_core::seam!(
    /// `RollbackToSavepoint(name)` (xact.c): roll back to the named savepoint.
    /// Can `ereport(ERROR)`, carried on `Err`.
    pub fn rollback_to_savepoint(name: &str) -> PgResult<()>
);

seam_core::seam!(
    /// `IsTransactionBlock()` (xact.c): true when inside an explicit
    /// transaction block (`BEGIN`...). Pure read of backend-local state.
    pub fn is_transaction_block() -> bool
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
    /// `GetTopTransactionId()` (xact.c): the top-level transaction's xid,
    /// assigning one if none has been assigned yet. Assignment can
    /// `ereport(ERROR)` (e.g. xid exhaustion / in a parallel worker).
    pub fn get_top_transaction_id() -> PgResult<types_core::TransactionId>
);

seam_core::seam!(
    /// `GetTopFullTransactionId()` (xact.c): the top-level transaction's
    /// `FullTransactionId`, assigning one if none has been assigned yet.
    /// Assignment can `ereport(ERROR)` (e.g. xid exhaustion / parallel worker).
    pub fn get_top_full_transaction_id() -> PgResult<types_core::FullTransactionId>
);

seam_core::seam!(
    /// `GetTopFullTransactionIdIfAny()` (xact.c): the top transaction's
    /// `FullTransactionId`, or `InvalidFullTransactionId` if none assigned.
    /// Pure read of backend-local state.
    pub fn get_top_full_transaction_id_if_any() -> types_core::FullTransactionId
);

seam_core::seam!(
    /// `GetCurrentTransactionIdIfAny()` (xact.c): the current (sub)transaction's
    /// xid, or `InvalidTransactionId` if none has been assigned. Pure read of
    /// backend-local state. Read by `XLogRecordAssemble` to set `xl_xid`.
    pub fn get_current_transaction_id_if_any() -> types_core::TransactionId
);

seam_core::seam!(
    /// `IsSubxactTopXidLogPending()` (xact.c): whether the top-level xid still
    /// needs to be logged in a record for the benefit of logical decoding (a
    /// subtransaction performed its first WAL write without the top xid yet
    /// having been included in any record). Pure read of backend-local state.
    pub fn is_subxact_top_xid_log_pending() -> bool
);

seam_core::seam!(
    /// Set the `CheckXidAlive` global (xact.c) — `ResetLogicalStreamingState`.
    pub fn set_check_xid_alive(xid: types_core::TransactionId)
);

seam_core::seam!(
    /// Set the `bsysscan` global (xact.c) — `ResetLogicalStreamingState`.
    pub fn set_bsysscan(value: bool)
);

seam_core::seam!(
    /// `PreventInTransactionBlock(isTopLevel, stmtType)` (xact.c).
    pub fn prevent_in_transaction_block(is_top_level: bool, stmt_type: &str) -> PgResult<()>
);

seam_core::seam!(
    /// `IsInTransactionBlock(isTopLevel)` (xact.c).
    pub fn is_in_transaction_block(is_top_level: bool) -> PgResult<bool>
);

// --- backend-utils-init-postinit consumers (xact.c) ---

seam_core::seam!(
    /// `SetCurrentStatementStartTimestamp()` (xact.c): set the statement-start
    /// timestamp (required for timeouts to work).
    pub fn set_current_statement_start_timestamp()
);

seam_core::seam!(
    /// `XactIsoLevel = XACT_READ_COMMITTED` (xact.c global): lower the
    /// just-started transaction's isolation level to read committed.
    pub fn set_xact_iso_level_read_committed()
);

seam_core::seam!(
    /// `XactIsoLevel = XACT_REPEATABLE_READ` (xact.c global): raise the
    /// just-started transaction's isolation level. snapbuild.c's
    /// SnapBuildExportSnapshot sets this before building the exported snapshot.
    pub fn set_xact_iso_level_repeatable_read()
);

seam_core::seam!(
    /// `XactReadOnly = value` (xact.c global): set the current transaction's
    /// read-only flag. snapbuild.c's SnapBuildExportSnapshot sets it true.
    pub fn set_xact_read_only(value: bool)
);

seam_core::seam!(
    /// `PreventCommandIfReadOnly(cmdname)` (utility.c): raise
    /// `ERRCODE_READ_ONLY_SQL_TRANSACTION` "cannot execute %s in a read-only
    /// transaction" if `XactReadOnly` is set (and the command is not allowed in
    /// recovery). `Ok(())` when the command may proceed.
    pub fn prevent_command_if_read_only(cmdname: &str) -> PgResult<()>
);

seam_core::seam!(
    /// `PreventCommandIfParallelMode(cmdname)` (utility.c): raise
    /// `ERRCODE_INVALID_TRANSACTION_STATE` "cannot execute %s during a parallel
    /// operation" if `IsInParallelMode()`. `Ok(())` otherwise.
    pub fn prevent_command_if_parallel_mode(cmdname: &str) -> PgResult<()>
);

seam_core::seam!(
    /// `bool XactReadOnly` (xact.c global): the current transaction's read-only
    /// flag. variable.c's `check_transaction_read_only` reads it.
    pub fn xact_read_only() -> bool
);

seam_core::seam!(
    /// `bool XactDeferrable` (xact.c global): the current transaction's
    /// deferrable flag. predicate.c's `GetSerializableTransactionSnapshot`
    /// reads it (`XactReadOnly && XactDeferrable` routes to `GetSafeSnapshot`).
    pub fn xact_deferrable() -> bool
);

seam_core::seam!(
    /// `int XactIsoLevel` (xact.c global): the current transaction's isolation
    /// level. variable.c's `check_transaction_isolation` reads it.
    pub fn xact_iso_level() -> i32
);

seam_core::seam!(
    /// `bool DefaultXactReadOnly` (xact.c GUC) — backing variable for the
    /// `default_transaction_read_only` GUC (guc_tables.c). The default
    /// read-only status applied to new transactions at StartTransaction.
    pub fn default_transaction_read_only() -> bool
);

seam_core::seam!(
    /// `bool DefaultXactDeferrable` (xact.c GUC) — backing variable for the
    /// `default_transaction_deferrable` GUC (guc_tables.c). The default
    /// deferrable status applied to new transactions at StartTransaction.
    pub fn default_transaction_deferrable() -> bool
);

seam_core::seam!(
    /// `int DefaultXactIsoLevel` (xact.c GUC) — backing variable for the
    /// `default_transaction_isolation` GUC (guc_tables.c). The default
    /// isolation level applied to new transactions at StartTransaction.
    pub fn default_transaction_isolation() -> i32
);

seam_core::seam!(
    /// `int synchronous_commit` (xact.c GUC) — the current
    /// `synchronous_commit` level; `SyncRepRequested()` compares it against
    /// `SYNCHRONOUS_COMMIT_LOCAL_FLUSH`.
    pub fn synchronous_commit() -> i32
);

seam_core::seam!(
    /// `TransactionBlockStatusCode()` (xact.c:5003): the one-character
    /// transaction-block status indicator the protocol `ReadyForQuery` ('Z')
    /// message carries — `'I'` idle (not in a block), `'T'` in a transaction,
    /// `'E'` in a failed transaction. Pure read of backend-local block state.
    pub fn transaction_block_status_code() -> char
);
