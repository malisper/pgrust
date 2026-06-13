//! Seam declarations for the GUC numeric value parsers
//! (`utils/misc/guc.c`): `parse_int` / `parse_real`.
//!
//! The reloptions parser calls `parse_int(value, &result, 0, NULL)` and
//! `parse_real(value, &result, 0, NULL)` (flags `0`, no hint message). Both
//! return a C `bool` success and never `ereport` — they are infallible and
//! pure, so the seams return `Option<_>` (`Some` on success, `None` on a parse
//! failure) and take no `Mcx`.
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
