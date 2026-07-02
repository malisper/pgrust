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
