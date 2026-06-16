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
    /// `check_GUC_name_for_parameter_acl(name)` (guc.c:1410): throw unless a
    /// `pg_parameter_acl` entry may be created for `name` — i.e. the GUC exists
    /// (`find_option`) or `name` is a valid custom GUC name
    /// (`assignable_custom_variable_name`). `Ok(())` means allowed; `Err` is the
    /// C `ereport(ERROR)`. Consumed by pg_parameter_acl.c's `ParameterAclCreate`.
    pub fn check_guc_name_for_parameter_acl(name: &str) -> types_error::PgResult<()>
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
    /// `GUC_check_errcode(sqlerrcode)` (guc.h): record the SQLSTATE for the
    /// in-progress GUC check-hook failure (`GUC_check_errcode_value`).
    /// Plain backend-local state write.
    pub fn guc_check_errcode(sqlstate: types_error::SqlState)
);

seam_core::seam!(
    /// `GUC_check_errdetail(fmt, ...)` (guc.h): record errdetail for the
    /// in-progress GUC check-hook failure (`GUC_check_errdetail_string`).
    /// Plain backend-local state write.
    pub fn guc_check_errdetail(detail: String)
);

seam_core::seam!(
    /// `GUC_check_errhint(fmt, ...)` (guc.h): record errhint for the
    /// in-progress GUC check-hook failure (`GUC_check_errhint_string`).
    /// Plain backend-local state write.
    pub fn guc_check_errhint(hint: String)
);

seam_core::seam!(
    /// `set_config_with_handle(name, get_config_handle(name), value, context,
    /// PGC_S_SESSION, srole, GUC_ACTION_SAVE, true, 0, false)` as called by
    /// `fmgr_security_definer` for each of a function's `proconfig` SET items.
    /// The handle lookup (`get_config_handle`) is folded in (owner-side). C
    /// varies only `context` (`PGC_SUSET` when the current user is superuser,
    /// else `PGC_USERSET`) and `srole` (`GetUserId()`); source / action /
    /// changeVal / elevel / is_reload are fixed for this caller. Apply paths
    /// allocate and can `ereport(ERROR)`, carried on `Err`.
    pub fn set_config_with_handle(
        name: &str,
        value: &str,
        context: types_guc::GucContext,
        srole: types_core::Oid,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `maintenance_work_mem` (guc.c global, KB): the value `ri_triggers.c`
    /// installs as `work_mem` for its validation query.
    pub fn maintenance_work_mem() -> i32
);

seam_core::seam!(
    /// `work_mem` (guc.c global, KB): the per-operation working-memory limit.
    /// Read by `ginInsertCleanup` (for a regular-insert cleanup) and other
    /// memory-bounded operators.
    pub fn work_mem() -> i32
);

seam_core::seam!(
    /// `autovacuum_work_mem` (guc.c global, KB; `-1` means "use
    /// maintenance_work_mem"): the memory limit an autovacuum worker uses.
    /// Read by `ginInsertCleanup` when invoked from an autovacuum worker.
    pub fn autovacuum_work_mem() -> i32
);

seam_core::seam!(
    /// `ProcessConfigFile(PGC_SIGHUP)` (guc.c): re-read postgresql.conf on a
    /// SIGHUP. `Err` carries a parse/apply `ereport(ERROR)`.
    pub fn process_config_file_sighup() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ProcessConfigFileInternal(context, applySettings, elevel)` (guc.c):
    /// the parse-then-apply core of `ProcessConfigFile` — read the active
    /// `ConfigFileName` (and `postgresql.auto.conf`), parse it into a
    /// `ConfigVariable` list, then apply each setting to the live GUC
    /// registry at `PGC_S_FILE`. The list it returns feeds `pg_file_settings`;
    /// `ProcessConfigFile` (guc-file.l) discards it, so the seam returns `()`.
    /// Parse/apply errors `ereport(ERROR)` (carried on `Err`) during
    /// postmaster startup.
    pub fn process_config_file_internal(
        context: types_guc::GucContext,
        apply_settings: bool,
        elevel: types_error::ErrorLevel,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `set_config_option("search_path", "", PGC_SUSET, PGC_S_OVERRIDE, ...)`
    /// (slotsync.c): force an empty search_path for the worker's catalog
    /// access. `Err` carries the option-set `ereport(ERROR)`.
    pub fn set_config_option_search_path_empty() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `set_config_option("search_path", value, PGC_USERSET, PGC_S_SESSION,
    /// GUC_ACTION_SAVE, true, 0, false)` (schemacmds.c `CreateSchemaCommand`):
    /// prepend the new schema to the session search_path for exactly the
    /// duration of the CREATE SCHEMA (guc.c rolls it back via `AtEOXact_GUC`).
    /// The fixed call-site arguments (USERSET/SESSION/ACTION_SAVE, changeVal,
    /// elevel 0, is_reload false) are baked into the seam. `Err` carries the
    /// option-set `ereport(ERROR)`.
    pub fn set_search_path_save(value: &str) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `cluster_name` (guc_tables.c GUC string): the configured cluster name,
    /// `""` when unset. Backend-local GUC state.
    pub fn cluster_name() -> String
);

seam_core::seam!(
    /// `GetConfigOption(name, missing_ok, restrict_privileged)` (guc.c) — read
    /// the current string value of a GUC. ipci.c
    /// `CreateSharedMemoryAndSemaphores` asserts `huge_pages_status !=
    /// "unknown"`. `None` is the C `NULL`/missing return. Owner unported;
    /// scaffolded slot.
    pub fn get_config_option(
        name: String,
        missing_ok: bool,
        restrict_privileged: bool,
    ) -> Option<String>
);

seam_core::seam!(
    /// `SetPGVariable("session_authorization", NIL, false)` (guc.c) — the one
    /// `DISCARD ALL` call site. With a `NIL` args list this resets
    /// `session_authorization` to its session default; `is_local` is `false`.
    /// Encapsulated at the GUC owner because the generic
    /// `SetPGVariable(name, args, is_local)` marshals a `List *` of `A_Const`
    /// the owner has yet to model here. `Err` carries its `ereport(ERROR)`.
    pub fn set_pg_variable_session_authorization_reset() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ResetAllOptions()` (guc.c) — reset all GUCs to their session-start /
    /// reset values (`DISCARD ALL` and `RESET ALL`). May `ereport(ERROR)` from
    /// per-variable assign hooks.
    pub fn reset_all_options() -> types_error::PgResult<()>
);

// ---- proconfig / pg_db_role_setting option-array helpers (guc.c) ----
// The `text[]` proconfig array is carried as an owned `Vec<String>` of
// `"name=value"` entries (the consumers' value-model). Re-homed here from
// `backend-commands-functioncmds-seams` (first consumer); the real owner
// `backend-utils-misc-guc` installs them.

seam_core::seam!(
    /// `GUCArrayAdd(array, name, value)` (utils/misc/guc.c) — append/replace the
    /// `name=value` entry in the proconfig `text[]`.
    pub fn guc_array_add(
        a: Option<Vec<String>>,
        name: String,
        value: String,
    ) -> types_error::PgResult<Vec<String>>
);

seam_core::seam!(
    /// `GUCArrayDelete(array, name)` (utils/misc/guc.c) — drop the `name=...`
    /// entry from the proconfig `text[]` (`None` if the array becomes empty).
    pub fn guc_array_delete(
        a: Option<Vec<String>>,
        name: String,
    ) -> types_error::PgResult<Option<Vec<String>>>
);

seam_core::seam!(
    /// `GUCArrayReset(array)` (utils/misc/guc.c) — for a superuser, reset
    /// (drop) all GUC entries, returning `None`; for a non-superuser, drop only
    /// the entries that user may reset, returning the surviving `text[]`
    /// (`None` if it becomes empty). `Err` carries the permission/validation
    /// error surface.
    pub fn guc_array_reset(a: Vec<String>) -> types_error::PgResult<Option<Vec<String>>>
);

seam_core::seam!(
    /// `ProcessGUCArray(array, PGC_SUSET, source, GUC_ACTION_SET)`
    /// (utils/misc/guc.c) — apply each `"name=value"` entry of a
    /// proconfig/setconfig `text[]` to the current session via
    /// `set_config_option`. `pg_db_role_setting`'s `ApplySetting` processes all
    /// options at `PGC_SUSET` with `GUC_ACTION_SET`, so those two are baked into
    /// the seam contract; the caller supplies the array and its [`GucSource`].
    /// `Err` carries the per-entry value-parse / permission error surface.
    pub fn process_guc_array(
        a: Vec<String>,
        source: types_guc::guc::GucSource,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `RestrictSearchPath()` (guc.c:2246): outside bootstrap mode, set
    /// `search_path` to the safe value `"pg_catalog, pg_temp"` via
    /// `set_config_option(..., PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SAVE,
    /// changeVal=true, elevel=0, is_reload=false)`. Used to harden
    /// security-restricted maintenance operations (REINDEX / CLUSTER / matview
    /// refresh / index build). Re-homed here from
    /// `backend-catalog-namespace-seams` (RestrictSearchPath is guc.c's
    /// function, not namespace.c's); installed by `backend-utils-misc-guc`.
    pub fn restrict_search_path() -> types_error::PgResult<()>
);
