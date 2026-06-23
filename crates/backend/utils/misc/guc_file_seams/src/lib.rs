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

// NOTE: NewGUCNestLevel / AtEOXact_GUC / GUC_check_errdetail / GUC_check_errhint
// / set_config_with_handle were mis-homed here (guc-file.l consumers reached
// them first) but are guc.c functions. Their decls have been RE-HOMED onto
// guc.c's own `-seams` crate (`backend-utils-misc-guc-seams`), where the merged
// owner installs them (CONTRACT_RECONCILE_PENDING retired); consumers call them
// there now. Only `process_config_file` (guc-file.l's own) stays declared here.

seam_core::seam!(
    /// `AtStart_GUC()` (guc.c) — sanity-reset GUC nesting at transaction
    /// start.
    pub fn at_start_guc()
);

seam_core::seam!(
    /// Read the `log_transaction_sample_rate` GUC
    /// (`double log_xact_sample_rate`, guc_tables.c).
    pub fn log_xact_sample_rate() -> f64
);
