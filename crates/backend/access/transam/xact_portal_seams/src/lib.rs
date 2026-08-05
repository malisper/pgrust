// Portal-facing xact.c slice, kept out of xact_seams while the xact port is
// claimed by a concurrent session (interleaved writes corrupt both).
seam_core::seam!(
    // GetCurrentStatementStartTimestamp() (xact.c stmtStartTimestamp).
    pub fn get_current_statement_start_timestamp() -> types_core::TimestampTz
);
