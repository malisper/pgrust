//! Seam declarations for the `backend-utils-misc-guc-file` unit
//! (`utils/misc/guc-file.l`).
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
    /// `NewGUCNestLevel()` (guc.c): open a new GUC nesting level and return
    /// it (`++GUCNestLevel`). Infallible.
    pub fn new_guc_nest_level() -> i32
);

seam_core::seam!(
    /// `AtEOXact_GUC(isCommit, nestLevel)` (guc.c): pop GUC stack entries at
    /// transaction / subtransaction / nest-level end, restoring or
    /// propagating values. Restore paths allocate and can `ereport`.
    pub fn at_eoxact_guc(is_commit: bool, nest_level: i32) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `GUC_check_errdetail(fmt, ...)` (guc.h): record errdetail for the
    /// in-progress GUC check-hook failure (`GUC_check_errdetail_string`).
    /// Plain backend-local state write.
    pub fn guc_check_errdetail(detail: String)
);
