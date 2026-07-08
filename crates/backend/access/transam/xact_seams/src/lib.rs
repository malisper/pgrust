seam_core::seam!(
    pub fn transaction_block_status_code() -> u8
);

seam_core::seam!(
    // GetCurrentSubTransactionId() (xact.c); consumed by fd.c's AllocateDesc.
    pub fn get_current_sub_transaction_id() -> types_core::SubTransactionId
);

seam_core::seam!(
    pub fn set_xact_accessed_temp_namespace()
);

seam_core::seam!(
    // GetCurrentCommandId(used) (xact.c); ereports in parallel mode when used.
    pub fn get_current_command_id(used: bool) -> types_error::PgResult<types_core::CommandId>
);

seam_core::seam!(
    pub fn get_current_transaction_nest_level() -> i32
);

seam_core::seam!(
    pub fn get_current_transaction_stop_timestamp() -> types_core::TimestampTz
);

seam_core::seam!(
    pub fn get_current_transaction_start_timestamp() -> types_core::TimestampTz
);

seam_core::seam!(
    pub fn get_top_transaction_id_if_any() -> types_core::TransactionId
);

seam_core::seam!(
    // xid_age() (xid.c) needs the age reference point.
    pub fn get_stable_latest_transaction_id() -> types_error::PgResult<types_core::TransactionId>
);

seam_core::seam!(
    pub fn is_transaction_or_transaction_block() -> bool
);

seam_core::seam!(
    // StartTransactionCommand() (xact.c) — can ereport.
    pub fn start_transaction_command() -> types_error::PgResult<()>
);

seam_core::seam!(
    pub fn commit_transaction_command() -> types_error::PgResult<()>
);

seam_core::seam!(
    pub fn is_in_parallel_mode() -> bool
);

seam_core::seam!(
    // IsolationUsesXactSnapshot() (xact.h): XactIsoLevel >= REPEATABLE READ.
    pub fn isolation_uses_xact_snapshot() -> bool
);

seam_core::seam!(
    pub fn isolation_is_serializable() -> bool
);

seam_core::seam!(
    // bool XactReadOnly (xact.c global).
    pub fn xact_read_only() -> bool
);

seam_core::seam!(
    // bool XactDeferrable (xact.c global).
    pub fn xact_deferrable() -> bool
);

seam_core::seam!(
    pub fn is_sub_transaction() -> bool
);

seam_core::seam!(
    // XactIsoLevel's numeric value (xact.h).
    pub fn get_xact_iso_level() -> i32
);

seam_core::seam!(
    // xactGetCommittedChildren: committed-subxact XIDs of the top xact.
    pub fn xact_get_committed_children(
    ) -> types_error::PgResult<Vec<types_core::TransactionId>>
);

seam_core::seam!(
    pub fn transaction_id_is_current_transaction_id(xid: types_core::TransactionId) -> bool
);

seam_core::seam!(
    pub fn is_transaction_state() -> bool
);

seam_core::seam!(
    // MarkCurrentTransactionIdLoggedIfAny() (xact.c).
    pub fn mark_current_transaction_id_logged_if_any()
);

seam_core::seam!(
    // MarkSubxactTopXidLogged() (xact.c).
    pub fn mark_subxact_top_xid_logged()
);

seam_core::seam!(
    // GetCurrentTransactionId (xact.c): assigns an XID if none yet.
    pub fn get_current_transaction_id() -> types_error::PgResult<types_core::TransactionId>
);

seam_core::seam!(
    // `MyXactFlags |= flags` (xact.h); C callers OR the global directly.
    pub fn or_my_xact_flags(flags: i32)
);
