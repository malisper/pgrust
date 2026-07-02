use types_error::{ErrorLevel, PgError, PgResult, SqlState};

seam_core::seam!(
    pub fn ereport(err: PgError) -> PgResult<()>
);

seam_core::seam!(
    pub fn sqlstate_for_file_access(errno: i32) -> SqlState
);

seam_core::seam!(
    pub fn ereport_msg(elevel: ErrorLevel, msg: String, detail: Option<String>) -> PgResult<()>
);
