//! Seam declarations for the `backend-utils-misc-guc` unit
//! (`utils/misc/guc.c` / `guc-file.l`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// `ProcessConfigFile(GucContext context)` (guc-file.l): re-read the
    /// configuration file(s) and apply changed settings. Allocates (OOM) and
    /// parse/apply paths can `ereport(ERROR)`.
    pub fn process_config_file(context: types_guc::GucContext) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `GUC_check_errdetail(fmt, ...)` (guc.h): record errdetail for the
    /// in-progress GUC check-hook failure (`GUC_check_errdetail_string`).
    /// Plain backend-local state write.
    pub fn guc_check_errdetail(detail: String)
);
