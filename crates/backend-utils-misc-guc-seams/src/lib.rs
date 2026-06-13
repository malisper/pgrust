//! Seam declarations for the `backend-utils-misc-guc` unit (`utils/misc/guc.c`):
//! the GUC-machinery operations slotsync invokes.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `cluster_name` GUC (guc_tables.c) — the configured cluster name (`""`
    /// when unset), included in the worker's application name.
    pub fn cluster_name() -> String
);

seam_core::seam!(
    /// `SetConfigOption("search_path", "", PGC_SUSET, PGC_S_OVERRIDE)` (guc.c)
    /// — set an always-secure empty search path for the worker. `ereport(ERROR)`
    /// on assign failure, carried on `Err`.
    pub fn set_config_option_search_path_empty() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ProcessConfigFile(PGC_SIGHUP)` (guc.c / guc-file.l) — re-read and apply
    /// the configuration files on a pending SIGHUP. Allocates (OOM) and
    /// parse/apply paths can `ereport(ERROR)`.
    pub fn process_config_file_sighup() -> types_error::PgResult<()>
);
