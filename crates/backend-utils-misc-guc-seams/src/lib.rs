//! Seam declarations for the GUC subsystem (`utils/misc/guc.c`): the numeric
//! value parsers (`parse_int`/`parse_real`), the bootstrap-mode entry points,
//! and the transactional-SET calls `ri_triggers.c` makes to bump
//! `work_mem`/`hash_mem_multiplier` for its bulk validation queries and to
//! unwind them at transaction end.
//!
//! The owning unit (`backend-utils-misc-guc`) installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.


seam_core::seam!(
    /// `parse_int(value, &result, 0, NULL)` (guc.c): parse an integer GUC
    /// value (with optional unit suffix). `None` is the C `false` return.
    pub fn parse_int(value: String) -> Option<i32>
);

seam_core::seam!(
    /// `parse_real(value, &result, 0, NULL)` (guc.c): parse a floating-point
    /// GUC value (with optional unit suffix). `None` is the C `false` return.
    pub fn parse_real(value: String) -> Option<f64>
);

seam_core::seam!(
    /// `SetConfigOption(name, value, PGC_INTERNAL, PGC_S_DYNAMIC_DEFAULT)`
    /// (guc.c) — used by `SetOuterUserId` to keep the `is_superuser` GUC in
    /// sync. Can `ereport(ERROR)` on an invalid value.
    pub fn set_config_option_internal_dynamic_default(
        name: &str,
        value: &str,
    ) -> types_error::PgResult<()>
);

// ---- bootstrap-mode GUC entry points (guc.c) ----

seam_core::seam!(
    /// `InitializeGUCOptions()` (guc.c): build the GUC variable table and set
    /// all variables to their compiled-in defaults. The C path can
    /// `ereport(FATAL)` on a bad built-in default.
    pub fn initialize_guc_options() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `SetConfigOption(name, value, context, source)` (guc.c): set a GUC
    /// variable, raising `ereport(ERROR)` on an invalid name/value/permission.
    pub fn set_config_option(
        name: &str,
        value: &str,
        context: types_guc::guc::GucContext,
        source: types_guc::guc::GucSource,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `SetConfigOption(name, value, PGC_BACKEND, PGC_S_OVERRIDE)` (guc.c) —
    /// used by `InitializeSessionUserId` to set `session_authorization`. Can
    /// `ereport(ERROR)`.
    pub fn set_config_option_backend_override(
        name: &str,
        value: &str,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ParseLongOption(string, &name, &value)` (guc.c): split a `name=value`
    /// (or bare `name`) long option, allocating the parsed pieces in `mcx`
    /// (C: `guc_strdup`/`pstrdup`). `value` is `None` for a bare `name`.
    /// `Err` carries the OOM surface of the copies.
    pub fn parse_long_option<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        string: &str,
    ) -> types_error::PgResult<(mcx::PgString<'mcx>, Option<mcx::PgString<'mcx>>)>
);

seam_core::seam!(
    /// `SelectConfigFiles(userDoption, progname)` (guc.c): locate and read
    /// `postgresql.conf` etc. Returns `false` for the C `return false` config
    /// problems; `Err` carries the `ereport(ERROR)` failures.
    pub fn select_config_files(
        user_doption: Option<&str>,
        progname: &str,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `allowSystemTableMods` (guc_tables.c / guc.c): the GUC controlling
    /// whether DDL may modify system catalogs. relmapper's
    /// `perform_relmap_update` reads it to decide `add_okay` when merging map
    /// updates. Pure backend-local GUC read.
    pub fn allow_system_table_mods() -> bool
);

seam_core::seam!(
    /// `NewGUCNestLevel()` (guc.c): begin a new GUC nesting level for
    /// transactional/function SET, returning the save-nestlevel to pass to
    /// `AtEOXact_GUC`.
    pub fn new_guc_nest_level() -> i32
);

seam_core::seam!(
    /// `AtEOXact_GUC(isCommit, nestLevel)` (guc.c): roll back the GUC settings
    /// made above `nestLevel`. Can `ereport(ERROR)` on a bad assign hook,
    /// carried on `Err`.
    pub fn at_eoxact_guc(is_commit: bool, nest_level: i32) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `maintenance_work_mem` (guc.c global, KB): the value `ri_triggers.c`
    /// installs as `work_mem` for its validation query.
    pub fn maintenance_work_mem() -> i32
);

seam_core::seam!(
    /// `ProcessConfigFile(PGC_SIGHUP)` (guc.c): re-read postgresql.conf on a
    /// SIGHUP. `Err` carries a parse/apply `ereport(ERROR)`.
    pub fn process_config_file_sighup() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `set_config_option("search_path", "", PGC_SUSET, PGC_S_OVERRIDE, ...)`
    /// (slotsync.c): force an empty search_path for the worker's catalog
    /// access. `Err` carries the option-set `ereport(ERROR)`.
    pub fn set_config_option_search_path_empty() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `cluster_name` (guc_tables.c GUC string): the configured cluster name,
    /// `""` when unset. Backend-local GUC state.
    pub fn cluster_name() -> String
);
