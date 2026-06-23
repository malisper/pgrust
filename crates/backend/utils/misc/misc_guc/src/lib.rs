#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(clippy::result_large_err)]

//! `backend-utils-misc-guc` — idiomatic Rust port of the **core** of
//! PostgreSQL 18.3 `src/backend/utils/misc/guc.c`: the GUC variable machinery.
//!
//! # What is ported (the core)
//!
//! * The **GUC name vocabulary** ([`name`]): `guc_name_compare`,
//!   `guc_name_hash`, `guc_name_match`, the `map_old_guc_names[]` table, and
//!   `convert_GUC_name_for_parameter_acl`.
//! * The **units machinery** ([`units`]): the memory/time conversion tables,
//!   `convert_to_base_unit`, `convert_int/real_from_base_unit`,
//!   `get_config_unit_name`, and the `parse_int` / `parse_real` value parsers
//!   (with faithful C `strtol`/`strtod` semantics in [`cnum`]).
//! * The **enum lookups** ([`enum_lookup`]).
//! * The **define / find / set / reset / SHOW** operations and the
//!   `GucContext`/`GucSource` **access-permission rules** ([`registry`]).
//! * The **live unified GUC store** ([`live`]) seeded by
//!   `initialize_guc_options` from the resolved [`guc_tables`]
//!   metadata, read through `config_*->variable` and written by
//!   `set_config_option_global`.
//! * The **`ProcessConfigFileInternal` apply core** ([`process_config`]).
//! * The **GUC_REPORT transmission** ([`report`]).
//! * The **GUC check-error reporting protocol** (this module).
//!
//! # Seamed / deferred (honest partial)
//!
//! The per-subsystem assign/check/show hooks are *called* through the typed
//! `guc_tables` slots each `config_*` record references; their bodies live in
//! the owning subsystems and are installed there. A handful of predicates the
//! permission switch and the `GUC_IS_NAME` branch reach (`IsUnderPostmaster`,
//! `IsInParallelMode`, `InLocalUserIdChange`, `InSecurityRestrictedOperation`,
//! `pg_parameter_aclcheck`, `truncate_identifier`) cross per-owner seams in
//! [`seam`].
//!
//! The larger sub-features of guc.c that are their **own units** are *not* this
//! core's content and are not stubbed-behind-a-pretend-success: the
//! transactional **GUC stack** (`NewGUCNestLevel`/`AtEOXact_GUC`/guc_stack.c),
//! `postgresql.conf` **parsing** (`ParseLongOption`/`SelectConfigFiles`/the
//! `ProcessConfigFile` orchestration in config_file.c / guc-file.l), and
//! `SetPGVariable`'s `List *A_Const` marshaling. Their Tier-A seams are
//! installed here (this is guc.c's home) with a loud panic into the unported
//! sub-unit — the sanctioned mirror-and-panic, never a silent stub.

pub mod cnum;
pub mod custom;
pub mod enum_lookup;
pub mod guc_array;
pub mod live;
pub mod model;
pub mod name;
pub mod process_config;
pub mod registry;
pub mod report;
pub mod seam;
pub mod serialize;
pub mod units;

#[cfg(test)]
mod tests;

use utils_error::ereport;
use types_error::{
    PgError, PgResult, SqlState, ERRCODE_INVALID_PARAMETER_VALUE, ERROR, WARNING,
};

pub use enum_lookup::{
    config_enum_get_options, config_enum_lookup_by_name, config_enum_lookup_by_value,
};
pub use live::{
    get_bool, get_enum, get_int, get_real, get_reset_string, get_string, initialize_guc_options,
    is_initialized as guc_options_initialized, pg_reload_time, reset_all_options_global,
    set_config_option_global, set_pg_reload_time, try_initialize_guc_options,
};
pub use name::{
    convert_guc_name_for_parameter_acl, guc_name_compare, guc_name_eq, guc_name_hash,
    guc_name_match, MAP_OLD_GUC_NAMES,
};
pub use process_config::{apply_config_variables, show_all_file_settings_items, ConfigItem};
pub use registry::{
    get_config_option_by_name, get_config_option_flags, parse_and_validate_value, reset_all_options,
    reset_value_string, set_config_option, show_guc_option, GucAction, GucRegistry, GucVariable,
};
pub use report::{begin_reporting_guc_options, report_changed_guc_options};
pub use units::{
    convert_int_from_base_unit, convert_real_from_base_unit, convert_to_base_unit, fmt_e, fmt_g,
    fmt_g_prec, get_config_unit_name, parse_int as parse_int_units, parse_real as parse_real_units,
    ParseNum, MAX_UNIT_LEN, MEMORY_UNITS_HINT, TIME_UNITS_HINT,
};

/// Map an allocation-failure (`TryReserveError`) into the project's OOM
/// `PgError`.
pub(crate) fn alloc_err(_e: alloc::collections::TryReserveError) -> PgError {
    ereport(ERROR)
        .errcode(types_error::ERRCODE_OUT_OF_MEMORY)
        .errmsg("out of memory")
        .into_error()
}

extern crate alloc;

/// `GUC_ACTION_SET` (`utils/guc.h`): set value for the session.
pub const GUC_ACTION_SET: u32 = 0;
/// `GUC_ACTION_LOCAL`: set value for the current transaction.
pub const GUC_ACTION_LOCAL: u32 = 1;
/// `GUC_ACTION_SAVE`: set for the duration of a function call.
pub const GUC_ACTION_SAVE: u32 = 2;

// ---------------------------------------------------------------------------
// GUC check-error reporting protocol (guc.c: GUC_check_errcode/msg/detail/hint
// + the GUC_check_error* globals consulted by call_*_check_hook). A GUC check
// hook signals failure by returning false after setting these; the caller
// translates them into the ereport. The per-call state lives in a process-global
// `Mutex` — the safe analog of the C file static; the backend mutates it
// sequentially on the SET path, and the broad test harness reads it safely from
// any thread.
// ---------------------------------------------------------------------------

/// The portions of an error report a check hook may supply
/// (`GUC_check_err*_string` / `GUC_check_errcode_value` in guc.c).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GucCheckError {
    pub sqlstate: SqlState,
    pub message: Option<String>,
    pub detail: Option<String>,
    pub hint: Option<String>,
}

impl Default for GucCheckError {
    fn default() -> Self {
        Self {
            sqlstate: ERRCODE_INVALID_PARAMETER_VALUE,
            message: None,
            detail: None,
            hint: None,
        }
    }
}

static GUC_CHECK_ERROR: std::sync::Mutex<Option<GucCheckError>> = std::sync::Mutex::new(None);

fn with_check_error<R>(f: impl FnOnce(&mut GucCheckError) -> R) -> R {
    let mut guard = GUC_CHECK_ERROR.lock().unwrap();
    if guard.is_none() {
        *guard = Some(GucCheckError::default());
    }
    f(guard.as_mut().unwrap())
}

/// Reset the check-error state to defaults (C resets the globals before each
/// `check_hook` call).
pub fn reset_guc_check_error() {
    *GUC_CHECK_ERROR.lock().unwrap() = Some(GucCheckError::default());
}

/// Snapshot the current check-error state.
pub fn guc_check_error() -> GucCheckError {
    with_check_error(|s| s.clone())
}

/// `GUC_check_errcode(sqlerrcode)` (guc.c:6796).
pub fn GUC_check_errcode(sqlstate: SqlState) {
    with_check_error(|s| s.sqlstate = sqlstate);
}

/// `GUC_check_errmsg(...)`.
pub fn GUC_check_errmsg(message: impl Into<String>) {
    with_check_error(|s| s.message = Some(message.into()));
}

/// `GUC_check_errdetail(...)`.
pub fn GUC_check_errdetail(detail: impl Into<String>) {
    with_check_error(|s| s.detail = Some(detail.into()));
}

/// `GUC_check_errhint(...)`.
pub fn GUC_check_errhint(hint: impl Into<String>) {
    with_check_error(|s| s.hint = Some(hint.into()));
}

/// Drain the check-error state, returning it and resetting to defaults.
pub fn take_guc_check_error() -> GucCheckError {
    with_check_error(core::mem::take)
}

/// Build a `PgError` from the current check-error state, using `fallback` when
/// the hook supplied no message. Returns `None` when the hook supplied nothing
/// at all and left the default errcode (caller builds a value-specific message).
pub(crate) fn guc_check_error_to_pg_error_or(fallback: String) -> Option<PgError> {
    let check = take_guc_check_error();
    if check.message.is_none() && check.detail.is_none() && check.hint.is_none() {
        if check.sqlstate == ERRCODE_INVALID_PARAMETER_VALUE {
            return None;
        }
    }
    Some(build_pg_error(check, fallback))
}

/// Build a `PgError` from a `GucCheckError`, using `fallback_message` for the
/// message when the hook supplied none.
pub fn guc_check_error_to_pg_error(fallback_message: impl Into<String>) -> PgError {
    let check = guc_check_error();
    build_pg_error(check, fallback_message)
}

fn build_pg_error(check: GucCheckError, fallback_message: impl Into<String>) -> PgError {
    let mut builder = ereport(ERROR)
        .errcode(check.sqlstate)
        .errmsg(check.message.unwrap_or_else(|| fallback_message.into()));
    if let Some(detail) = check.detail {
        builder = builder.errdetail_internal(detail);
    }
    if let Some(hint) = check.hint {
        builder = builder.errhint(hint);
    }
    builder.into_error()
}

/// Convenience: turn the current check-error state into an `Err(PgError)`.
pub fn finish_guc_check_error(fallback_message: impl Into<String>) -> PgResult<()> {
    Err(guc_check_error_to_pg_error(fallback_message))
}

// ---------------------------------------------------------------------------
// GUC nesting level (guc.c: `static int GUCNestLevel`). The integer half of the
// transactional GUC stack (guc_stack.c): `NewGUCNestLevel` opens a new nesting
// level and returns it, `AtEOXact_GUC(isCommit, nestLevel)` pops back to it. The
// nest-level counter itself is owned here (it is a guc.c file static, not a
// guc_stack.c entity); only the stack-entry rollback (`AtEOXact_GUC`) still
// belongs to the unported guc_stack.c. An `AtomicI32` is the safe process-global
// analog of the C `int` file static.
// ---------------------------------------------------------------------------

/// `static int GUCNestLevel = 0;` (guc.c:231) — 1 when in the main transaction,
/// bumped for each open subtransaction / function SET scope.
static GUC_NEST_LEVEL: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(0);

/// `NewGUCNestLevel(void)` (guc.c:2235) — `return ++GUCNestLevel;`. Begin a new
/// GUC nesting level, returning the save-nestlevel to later pass to
/// `AtEOXact_GUC`.
pub fn NewGUCNestLevel() -> i32 {
    GUC_NEST_LEVEL.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1
}

/// Read `GUCNestLevel` (guc.c file static). Used by `push_old_value`
/// (registry.rs) to decide whether a save level is open and at what level.
pub(crate) fn guc_nest_level() -> i32 {
    GUC_NEST_LEVEL.load(std::sync::atomic::Ordering::Relaxed)
}

/// `GUCNestLevel = level` — set the nest-level counter. Used by `AtStart_GUC`
/// (set to 1) and `AtEOXact_GUC` (set to `nestLevel - 1`).
pub(crate) fn set_guc_nest_level(level: i32) {
    GUC_NEST_LEVEL.store(level, std::sync::atomic::Ordering::Relaxed);
}

/// `AtStart_GUC(void)` (guc.c:2215): GUC processing at main transaction start.
/// The nest level should be 0 between transactions; if not, warn (somebody
/// missed an `AtEOXact_GUC`) and reset to 1.
pub fn at_start_guc() {
    if guc_nest_level() != 0 {
        let e = ereport(WARNING)
            .errmsg(format!(
                "GUC nest level = {} at transaction start",
                guc_nest_level()
            ))
            .into_error();
        utils_error::emit_error_report_for(&e);
    }
    set_guc_nest_level(1);
}

/// `AtEOXact_GUC(isCommit, nestLevel)` (guc.c:2262): GUC processing at
/// transaction/subtransaction commit or abort, or when exiting a function with
/// proconfig settings, or undoing a transient assignment. Discards/restores all
/// GUC settings applied at nesting levels >= `nest_level`, then updates the
/// nesting level to `nest_level - 1`. Owned here (guc_stack.c is part of the GUC
/// unit); the per-variable rollback walk lives in [`registry`].
pub fn at_eoxact_guc(is_commit: bool, nest_level: i32) {
    // Collect each restored variable's assign hook to fire AFTER the store
    // borrow is released. A hook (e.g. `role`'s `assign_role`) may recursively
    // re-enter `SetConfigOption` and re-lock the process-global GUC store, which
    // would deadlock if fired while the `with_store_mut` guard is held. This
    // mirrors `set_config_option_global`'s deferral.
    let mut deferred_hooks: Vec<registry::DeferredAssignHook> = Vec::new();
    live::with_store_mut(|reg| {
        registry::at_eoxact_guc(reg, is_commit, nest_level, &mut deferred_hooks);
    });
    // Store borrow is now released: fire the assign hook(s), which may
    // recursively re-enter the GUC store, in restore order.
    for hook in deferred_hooks {
        hook();
    }
    // GUCNestLevel = nestLevel - 1 (guc.c:2536).
    set_guc_nest_level(nest_level - 1);
}

/// `ParseLongOption(string, &name, &value)` (guc.c:6367) — a little "long
/// argument" simulation. Takes `"some-option=some value"` and returns
/// `name = "some_option"` and `value = Some("some value")` in `mcx`-allocated
/// storage; `'-'` in the option name is converted to `'_'`. If there is no `'='`
/// in the input, `value` is `None`.
pub fn ParseLongOption<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    string: &str,
) -> types_error::PgResult<(mcx::PgString<'mcx>, Option<mcx::PgString<'mcx>>)> {
    // equal_pos = strcspn(string, "="): byte offset of the first '=', or the
    // whole length if none.
    let bytes = string.as_bytes();
    let equal_pos = bytes.iter().position(|&b| b == b'=');

    let (name_src, value): (&str, Option<mcx::PgString<'mcx>>) = match equal_pos {
        Some(pos) => {
            // *name = strlcpy of string[..pos]; *value = pstrdup(string[pos+1..]).
            let value = mcx::PgString::from_str_in(&string[pos + 1..], mcx)?;
            (&string[..pos], Some(value))
        }
        // no equal sign in string: *name = pstrdup(string); *value = NULL.
        None => (string, None),
    };

    // *name with '-' converted to '_'.
    let mut name = mcx::PgString::new_in(mcx);
    for c in name_src.chars() {
        name.try_push(if c == '-' { '_' } else { c })?;
    }

    Ok((name, value))
}

// ---------------------------------------------------------------------------
// Tier-A seam install (this crate is guc.c's home).
// ---------------------------------------------------------------------------

use types_core::BOOTSTRAP_SUPERUSERID;
use types_guc::{
    GucContext, GucSource, PGC_BACKEND, PGC_INTERNAL, PGC_POSTMASTER, PGC_SIGHUP, PGC_SUSET,
    PGC_S_DYNAMIC_DEFAULT, PGC_S_OVERRIDE,
};

/// `SetConfigOption(name, value, context, source)` over the global store, with
/// the C `set_config_option_ext(..., changeVal=true, GUC_ACTION_SET, elevel=ERROR)`
/// arguments `SetConfigOption` fixes.
fn set_config_option_seam(
    name: &str,
    value: &str,
    context: GucContext,
    source: GucSource,
) -> PgResult<()> {
    set_config_option_global(
        name,
        Some(value),
        context,
        source,
        BOOTSTRAP_SUPERUSERID,
        GUC_ACTION_SET,
        true,
        ERROR,
        false,
    )
    .map(|_| ())
}

/// `CONFIG_FILENAME` (guc.h): default postgresql.conf basename.
const CONFIG_FILENAME: &str = "postgresql.conf";
/// `HBA_FILENAME` (guc.h): default pg_hba.conf basename.
const HBA_FILENAME: &str = "pg_hba.conf";
/// `IDENT_FILENAME` (guc.h): default pg_ident.conf basename.
const IDENT_FILENAME: &str = "pg_ident.conf";

/// `pg_timezone_abbrev_initialize()` (guc.c:1991): if no `timezone_abbreviations`
/// setting was found, select the `"Default"` value (a no-op if a non-default is
/// already installed, per the GUC source-precedence rules).
fn pg_timezone_abbrev_initialize() -> PgResult<()> {
    set_config_option_seam(
        "timezone_abbreviations",
        "Default",
        PGC_POSTMASTER,
        PGC_S_DYNAMIC_DEFAULT,
    )
}

/// `SelectConfigFiles(userDoption, progname)` (guc.c:1784): locate and read
/// `postgresql.conf`, establishing `config_file`/`data_directory`/`hba_file`/
/// `ident_file` and the `DataDir`. Returns `Ok(false)` for the C `return false`
/// configuration problems (the caller `proc_exit(1)`s); `Err` carries an
/// `ereport(ERROR)`.
///
/// `ConfigFileName`/`HbaFileName`/`IdentFileName` are themselves GUC string
/// variables in this crate's registry; they are read with [`live::get_string`].
/// `SetConfigOption(..., PGC_S_OVERRIDE)` pins each path so it cannot be
/// overridden later. `make_absolute_path`/`SetDataDir` are reached through the
/// miscinit-seams re-exports, and `ProcessConfigFile(PGC_POSTMASTER)` through the
/// guc-file owner.
pub fn SelectConfigFiles(user_doption: Option<&str>, progname: &str) -> PgResult<bool> {
    use miscinit_seams as misc;

    // configdir is -D option, or $PGDATA if no -D.
    let configdir: Option<String> = match user_doption {
        Some(d) => Some(misc::make_absolute_path::call(d)?),
        None => match std::env::var("PGDATA") {
            Ok(v) => Some(misc::make_absolute_path::call(&v)?),
            Err(_) => None,
        },
    };

    if let Some(dir) = &configdir {
        if std::fs::metadata(dir).is_err() {
            eprintln!("{progname}: could not access directory \"{dir}\"");
            eprintln!(
                "Run initdb or pg_basebackup to initialize a PostgreSQL data directory."
            );
            return Ok(false);
        }
    }

    // Find the configuration file: config_file GUC if set, else
    // configdir/postgresql.conf. Make the result absolute.
    let config_file_guc = live::get_string("config_file").flatten();
    let fname: String = if let Some(cf) = config_file_guc.as_deref().filter(|s| !s.is_empty()) {
        misc::make_absolute_path::call(cf)?
    } else if let Some(dir) = &configdir {
        format!("{dir}/{CONFIG_FILENAME}")
    } else {
        eprintln!(
            "{progname} does not know where to find the server configuration file.\n\
             You must specify the --config-file or -D invocation option or set the PGDATA \
             environment variable."
        );
        return Ok(false);
    };

    // Pin config_file to its final value.
    set_config_option_seam("config_file", &fname, PGC_POSTMASTER, PGC_S_OVERRIDE)?;

    // Read the config file for the first time (only data_directory is picked up
    // this pass, to find the data directory so the autoconf file can be read).
    let config_file_name = live::get_string("config_file").flatten().unwrap_or_default();
    if std::fs::metadata(&config_file_name).is_err() {
        eprintln!(
            "{progname}: could not access the server configuration file \"{config_file_name}\""
        );
        return Ok(false);
    }
    guc_file_seams::process_config_file::call(PGC_POSTMASTER)?;

    // If data_directory has been set, use that as DataDir; else configdir; else
    // punt.
    let data_directory = live::get_string("data_directory").flatten().unwrap_or_default();
    if !data_directory.is_empty() {
        misc::set_data_dir::call(&data_directory)?;
    } else if let Some(dir) = &configdir {
        misc::set_data_dir::call(dir)?;
    } else {
        eprintln!(
            "{progname} does not know where to find the database system data.\n\
             This can be specified as \"data_directory\" in \"{config_file_name}\", or by the \
             -D invocation option, or by the PGDATA environment variable."
        );
        return Ok(false);
    }

    // Reflect the final DataDir back into the data_directory GUC var.
    let data_dir = init_small_seams::data_dir::call().unwrap_or_default();
    set_config_option_seam("data_directory", &data_dir, PGC_POSTMASTER, PGC_S_OVERRIDE)?;

    // Read the config file a second time, allowing autoconf settings to apply.
    guc_file_seams::process_config_file::call(PGC_POSTMASTER)?;

    // If timezone_abbreviations wasn't set in the file, install the default.
    pg_timezone_abbrev_initialize()?;

    // Figure out where pg_hba.conf is, make absolute, pin it.
    let hba_guc = live::get_string("hba_file").flatten();
    let fname = if let Some(h) = hba_guc.as_deref().filter(|s| !s.is_empty()) {
        misc::make_absolute_path::call(h)?
    } else if let Some(dir) = &configdir {
        format!("{dir}/{HBA_FILENAME}")
    } else {
        eprintln!(
            "{progname} does not know where to find the \"hba\" configuration file.\n\
             This can be specified as \"hba_file\" in \"{config_file_name}\", or by the -D \
             invocation option, or by the PGDATA environment variable."
        );
        return Ok(false);
    };
    set_config_option_seam("hba_file", &fname, PGC_POSTMASTER, PGC_S_OVERRIDE)?;

    // Likewise for pg_ident.conf.
    let ident_guc = live::get_string("ident_file").flatten();
    let fname = if let Some(i) = ident_guc.as_deref().filter(|s| !s.is_empty()) {
        misc::make_absolute_path::call(i)?
    } else if let Some(dir) = &configdir {
        format!("{dir}/{IDENT_FILENAME}")
    } else {
        eprintln!(
            "{progname} does not know where to find the \"ident\" configuration file.\n\
             This can be specified as \"ident_file\" in \"{config_file_name}\", or by the -D \
             invocation option, or by the PGDATA environment variable."
        );
        return Ok(false);
    };
    set_config_option_seam("ident_file", &fname, PGC_POSTMASTER, PGC_S_OVERRIDE)?;

    Ok(true)
}

/// `GUC_SAFE_SEARCH_PATH` (guc.c:74): the locked-down `search_path` value
/// `RestrictSearchPath` installs for security-restricted maintenance.
const GUC_SAFE_SEARCH_PATH: &str = "pg_catalog, pg_temp";

/// `RestrictSearchPath()` (guc.c:2246). Outside bootstrap processing mode, set
/// `search_path` to the safe value via `set_config_option("search_path",
/// GUC_SAFE_SEARCH_PATH, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SAVE, true, 0,
/// false)`. The C ignores the return; elevel 0 reports nothing.
fn restrict_search_path() -> PgResult<()> {
    if miscinit_seams::is_bootstrap_processing_mode::call() {
        return Ok(());
    }
    // C `set_config_option` (8-arg, guc.c:3342) derives srole from source:
    // PGC_S_SESSION >= PGC_S_INTERACTIVE, so srole = GetUserId().
    let srole = miscinit_seams::get_user_id::call();
    set_config_option_global(
        "search_path",
        Some(GUC_SAFE_SEARCH_PATH),
        types_guc::PGC_USERSET,
        types_guc::PGC_S_SESSION,
        srole,
        GUC_ACTION_SAVE,
        true,
        types_error::ErrorLevel(0),
        false,
    )
    .map(|_| ())
}

/// `set_config_option("search_path", value, PGC_USERSET, PGC_S_SESSION,
/// GUC_ACTION_SAVE, changeVal=true, elevel=0, is_reload=false)`
/// (schemacmds.c `CreateSchemaCommand`). The C `set_config_option` (8-arg,
/// guc.c:3342) derives `srole` from the source: `PGC_S_SESSION >=
/// PGC_S_INTERACTIVE`, so `srole = GetUserId()`.
fn set_search_path_save(value: &str) -> PgResult<()> {
    let srole = miscinit_seams::get_user_id::call();
    set_config_option_global(
        "search_path",
        Some(value),
        types_guc::PGC_USERSET,
        types_guc::PGC_S_SESSION,
        srole,
        GUC_ACTION_SAVE,
        true,
        types_error::ErrorLevel(0),
        false,
    )
    .map(|_| ())
}

/// Install every seam declared in `backend-utils-misc-guc-seams`.
pub fn init_seams() {
    use guc_seams as s;

    // --- Numeric value parsers (guc.c parse_int / parse_real). ---
    s::parse_int::set(|value| match units::parse_int(&value, 0) {
        ParseNum::Ok(v) => Some(v),
        ParseNum::Err { .. } => None,
    });
    s::parse_real::set(|value| match units::parse_real(&value, 0) {
        ParseNum::Ok(v) => Some(v),
        ParseNum::Err { .. } => None,
    });

    // vacuum.c's ExecVacuum parses BUFFER_USAGE_LIMIT via
    // `parse_int(vac_buffer_size, &result, GUC_UNIT_KB, &hintmsg)`; guc.c owns
    // parse_int. The seam returns `(ok, result, hintmsg)`.
    vacuum_seams::parse_int_kb::set(|value| {
        match units::parse_int(&value, types_guc::GUC_UNIT_KB) {
            ParseNum::Ok(v) => Ok((true, v, None)),
            ParseNum::Err { hint } => Ok((
                false,
                0,
                hint.map(alloc::string::ToString::to_string),
            )),
        }
    });

    // --- check_GUC_name_for_parameter_acl (guc.c:1410), consumed by
    //     pg_parameter_acl.c's ParameterAclCreate. ---
    s::check_guc_name_for_parameter_acl::set(check_guc_name_for_parameter_acl);

    // --- Boot init. ---
    s::initialize_guc_options::set(try_initialize_guc_options);

    // --- SetConfigOption + the fixed-argument variants. ---
    s::set_config_option::set(set_config_option_seam);
    s::set_config_option_internal_dynamic_default::set(|name, value| {
        set_config_option_seam(name, value, PGC_INTERNAL, PGC_S_DYNAMIC_DEFAULT)
    });
    s::set_config_option_backend_override::set(|name, value| {
        set_config_option_seam(name, value, PGC_BACKEND, PGC_S_OVERRIDE)
    });
    s::set_config_option_search_path_empty::set(|| {
        set_config_option_seam("search_path", "", PGC_SUSET, PGC_S_OVERRIDE)
    });

    // --- Backend-local GUC reads (read through the live store). ---
    s::allow_system_table_mods::set(|| get_bool("allow_system_table_mods").unwrap_or(false));
    tablespace_globals_seams::allowSystemTableMods::set(|| {
        Ok(get_bool("allow_system_table_mods").unwrap_or(false))
    });
    s::maintenance_work_mem::set(|| get_int("maintenance_work_mem").unwrap_or(0));
    s::work_mem::set(|| get_int("work_mem").unwrap_or(0));
    s::standard_conforming_strings::set(|| {
        get_bool("standard_conforming_strings").unwrap_or(true)
    });
    s::autovacuum_work_mem::set(|| get_int("autovacuum_work_mem").unwrap_or(-1));
    s::cluster_name::set(|| get_string("cluster_name").flatten().unwrap_or_default());

    // --- GetConfigOption(name, missing_ok, restrict_privileged). ---
    s::get_config_option::set(|name, missing_ok, _restrict_privileged| {
        // The restrict-privileged ACL gate is the caller's; the value lookup is
        // the store's. `Ok(None)` (missing & missing_ok) and a found value both
        // map to the C `char *`/`NULL` return; a hard error would only arise for
        // `!missing_ok` on an unknown name, which this read-only seam returns as
        // `None`.
        with_store_lookup(&name, missing_ok)
    });

    // --- ResetAllOptions (RESET ALL / DISCARD ALL). ---
    s::reset_all_options::set(|| {
        reset_all_options_global();
        Ok(())
    });

    // --- proconfig / pg_db_role_setting option-array helpers (guc.c). ---
    s::guc_array_add::set(|a, name, value| guc_array::GUCArrayAdd(a, &name, &value));
    s::guc_array_delete::set(|a, name| guc_array::GUCArrayDelete(a, &name));
    s::guc_array_reset::set(guc_array::GUCArrayReset);
    // `ApplySetting` processes all options at PGC_SUSET (the right to insert was
    // checked at insert time) with GUC_ACTION_SET — both baked into the seam.
    s::process_guc_array::set(|a, source| {
        guc_array::ProcessGUCArray(a, PGC_SUSET, source, GUC_ACTION_SET)
    });

    // --- RestrictSearchPath (guc.c:2246; mis-homed seam re-homed here). ---
    s::restrict_search_path::set(restrict_search_path);
    s::set_search_path_save::set(set_search_path_save);

    // --- Sub-features owned by separate, not-yet-ported units. Installed here
    //     (guc.c is their home) but each loud-panics into the unported sub-unit
    //     rather than silently stubbing (mirror-and-panic). ---

    // Parallel-worker GUC-state transfer (guc.c EstimateGUCStateSpace /
    // SerializeGUCState / RestoreGUCState). These are guc.c's own bodies; the
    // `parallel-rt` seam crate declares them (consumed by parallel.c's
    // InitializeParallelDSM / ParallelWorkerMain) and guc.c is their owner, so
    // they are installed here. The `space: usize` carried by the seams is the
    // raw start address inside the DSM segment shm_toc_allocate handed back;
    // bridging it to a byte slice is the audited DSM-pointer primitive (same as
    // the sibling combocid/snapshot serializers).
    install_guc_state_transfer_seams();

    // GUC nesting level + transactional stack (guc.c / guc_stack.c, both part of
    // the GUC unit and owned here). NewGUCNestLevel is `++GUCNestLevel`;
    // AtEOXact_GUC pops the per-variable save stack and restores/discards prior
    // values; AtStart_GUC sanity-resets the nest level at xact start.
    s::new_guc_nest_level::set(NewGUCNestLevel);
    s::at_eoxact_guc::set(|is_commit, nest_level| {
        at_eoxact_guc(is_commit, nest_level);
        Ok(())
    });
    // `AtStart_GUC()` is declared in the guc-file-seams sibling (guc-file.l's
    // historical home) but is genuinely guc_stack.c's; install it here, its real
    // owner. Consumed by xact (engine.rs StartTransaction).
    guc_file_seams::at_start_guc::set(at_start_guc);

    // --- guc.c bodies whose seam decls previously lived (mis-homed) in the
    //     sibling `backend-utils-misc-guc-file-seams` crate; they are now
    //     RE-HOMED onto guc.c's own `-seams` crate (`s` =
    //     backend-utils-misc-guc-seams) so the install is dir-owner-attributable
    //     and the guard re-asserts the contract (retires
    //     CONTRACT_RECONCILE_PENDING). The `process_config_file` decl stays in
    //     guc-file-seams — it is genuinely guc-file.l's (the lexer/reader unit). ---

    // GUC_check_errcode / GUC_check_errdetail / GUC_check_errhint (guc.c):
    // record check-hook failure code/detail/hint into the backend-local
    // check-error state.
    s::guc_check_errcode::set(GUC_check_errcode);
    s::guc_check_errdetail::set(GUC_check_errdetail);
    s::guc_check_errhint::set(GUC_check_errhint);

    // set_config_with_handle(name, handle, value, context, PGC_S_SESSION,
    // srole, GUC_ACTION_SAVE, true, 0, false) as called by
    // fmgr_security_definer (fmgr.c:723) for each proconfig SET item. The
    // get_config_handle lookup is folded into set_config_option_global's
    // by-name dispatch; C varies only `context` and `srole` (passed through),
    // with source/action/changeVal/elevel/is_reload fixed for this caller.
    s::set_config_with_handle::set(|name, value, context, srole| {
        set_config_option_global(
            name,
            Some(value),
            context,
            types_guc::PGC_S_SESSION,
            srole,
            GUC_ACTION_SAVE,
            true,
            types_error::ErrorLevel(0),
            false,
        )
        .map(|_| ())
    });

    // ParseLongOption (guc.c) — split a `name=value` long option. guc.c's own
    // body, owned here.
    s::parse_long_option::set(ParseLongOption);
    // SelectConfigFiles (guc.c) — locate/read postgresql.conf and pin the
    // config-file/data-directory GUCs. guc.c's own body, owned here.
    s::select_config_files::set(SelectConfigFiles);
    // ProcessConfigFile(PGC_SIGHUP) (guc-file.l) — the SIGHUP reload. The
    // memory-context wrapper lives in guc-file.l (its own unit); drive it
    // through its seam so a SIGHUP reload re-reads and applies the file.
    s::process_config_file_sighup::set(|| {
        guc_file_seams::process_config_file::call(PGC_SIGHUP)
    });
    // ProcessConfigFileInternal (guc.c) — the parse-then-apply core. Parsing
    // lives in guc-file.l (called directly here, mirroring guc.c → guc-file.l);
    // the apply phase is `apply_config_variables` in this crate.
    s::process_config_file_internal::set(|context, apply_settings, elevel| {
        process_config::process_config_file_internal(context, apply_settings, elevel).map(|_| ())
    });

    // SetPGVariable's List *A_Const marshaling (the DISCARD ALL session_auth
    // reset) is installed by the seam's owner, guc_funcs.c
    // (backend-utils-misc-guc-funcs), which owns `SetPGVariable` and depends on
    // this crate's seam crate (acyclic). See its `init_seams`.

    // --- lazy-vacuum driver GUC reads (vacuumlazy.c reads these process-global
    //     GUCs directly; they home in vacuumlazy-seams, guc.c is their owner) ---
    use vacuumlazy_seams as vx;
    vx::maintenance_work_mem::set(|| Ok(get_int("maintenance_work_mem").unwrap_or(0)));
    vx::autovacuum_work_mem::set(|| Ok(get_int("autovacuum_work_mem").unwrap_or(-1)));
    vx::track_io_timing::set(|| Ok(get_bool("track_io_timing").unwrap_or(false)));
    vx::track_cost_delay_timing::set(|| Ok(get_bool("track_cost_delay_timing").unwrap_or(false)));
    // matview.c reaches NewGUCNestLevel / AtEOXact_GUC (both guc.c) through its
    // outward frontier seam crate; guc owns the bodies. Both can ereport via
    // GUC assign hooks, so the seams carry PgResult.
    {
        use matview_deps_seams as m;
        m::new_guc_nest_level::set(|| Ok(NewGUCNestLevel()));
        m::at_eoxact_guc::set(|is_commit, nest_level| {
            at_eoxact_guc(is_commit, nest_level);
            Ok(())
        });
    }

    // pg_proc.c (ProcedureCreate) runs the language validator wrapped in the
    // per-function GUC nesting level (`pg_proc.c:700-726`). The fmgr dispatch
    // (`OidFunctionCall1(languageValidator, ObjectIdGetDatum(retval))`) crosses
    // through the fmgr seam and the GUC nest is owned here, so guc.c installs
    // the combined seam. `set_items` (proconfig) only opens a nest when
    // `check_function_bodies` is on.
    // pg_proc.c reads `check_function_bodies` (guc.c) to gate the validator
    // body parse + per-function GUC nest; guc.c is its owner.
    pg_proc_seams::check_function_bodies::set(|| {
        Ok(get_bool("check_function_bodies").unwrap_or(true))
    });
    pg_proc_seams::run_language_validator::set(
        |language_validator, retval, proconfig| {
            let mut save_nestlevel: Option<i32> = None;
            if get_bool("check_function_bodies").unwrap_or(true) {
                if let Some(set_items) = proconfig {
                    if !set_items.is_empty() {
                        let nest = NewGUCNestLevel();
                        save_nestlevel = Some(nest);
                        let scratch = mcx::MemoryContext::new("run_language_validator superuser");
                        let context = if miscinit_seams::superuser::call(
                            scratch.mcx(),
                        )? {
                            types_guc::PGC_SUSET
                        } else {
                            types_guc::PGC_USERSET
                        };
                        guc_array::ProcessGUCArray(
                            set_items,
                            context,
                            types_guc::PGC_S_SESSION,
                            GUC_ACTION_SAVE,
                        )?;
                    }
                }
            }

            // OidFunctionCall1(languageValidator, ObjectIdGetDatum(retval)).
            let call_ctx = mcx::MemoryContext::new("run_language_validator call");
            let mcx = call_ctx.mcx();
            let result = fmgr_seams::function_call_invoke_datum::call(
                mcx,
                language_validator,
                types_core::primitive::InvalidOid,
                &[types_tuple::heaptuple::Datum::from_oid(retval)],
                &[],
                None,
            )
            .map(|_| ());

            if let Some(nest) = save_nestlevel {
                at_eoxact_guc(true, nest);
            }
            result
        },
    );
}

/// Install the parallel-worker GUC-state transfer seams declared in
/// `backend-access-transam-parallel-rt-seams` (guc.c owns the bodies).
///
/// `EstimateGUCStateSpace` / `SerializeGUCState` / `RestoreGUCState` operate on
/// the process-global live GUC store; the `space: usize` argument is the raw DSM
/// start address `shm_toc_allocate` returned, bridged here into a byte slice.
/// The serialize side receives the planned length (`len`, equal to the prior
/// estimate); the restore side reads the payload length from the first
/// `size_of::<usize>()` bytes of the stream (mirroring C
/// `RestoreGUCState(void *gucstate)`), so it forms the slice in two steps.
fn install_guc_state_transfer_seams() {
    use parallel_rt_seams as rt;

    rt::estimate_guc_state_space::set(|| {
        live::with_store(serialize::estimate_guc_state_space)
            .ok_or_else(guc_store_uninitialized)
    });

    rt::serialize_guc_state::set(|len, space| {
        // SAFETY: `space` is the start of a `len`-byte chunk shm_toc_allocate
        // reserved for the GUC state (EstimateGUCStateSpace sized it); the
        // leader writes the whole chunk here. This is the audited DSM-pointer
        // primitive.
        let buf = unsafe { core::slice::from_raw_parts_mut(space as *mut u8, len) };
        live::with_store(|reg| serialize::serialize_guc_state(reg, buf))
            .ok_or_else(guc_store_uninitialized)?
    });

    rt::restore_guc_state::set(|space| {
        // The first machine-word of the stream is the payload length; read it,
        // then form the full `size_of::<usize>() + len` slice. SAFETY: `space`
        // points at the GUC-state chunk the leader serialized; the length
        // prefix bounds the readable extent.
        let prefix = core::mem::size_of::<usize>();
        let len = unsafe {
            let head = core::slice::from_raw_parts(space as *const u8, prefix);
            usize::from_ne_bytes(head.try_into().expect("usize-sized prefix"))
        };
        let total = prefix + len;
        let buf = unsafe { core::slice::from_raw_parts(space as *const u8, total) };
        // Collect assign hooks to fire AFTER releasing the store borrow, so a
        // recursively re-entrant SetConfigOption (e.g. via session
        // authorization) does not re-lock the store / alias the live `&mut reg`.
        let mut deferred_hooks: Vec<crate::registry::DeferredAssignHook> = Vec::new();
        let res = live::with_store_mut(|reg| serialize::restore_guc_state(reg, buf, &mut deferred_hooks))
            .ok_or_else(guc_store_uninitialized)?;
        for hook in deferred_hooks {
            hook();
        }
        res
    });
}

/// `assignable_custom_variable_name(name, skip_errors, elevel)` (guc.c:1121):
/// decide whether `name` is acceptable as a (yet-to-be-defined) custom GUC. A
/// custom name is `class.subname` (a `GUC_QUALIFIER_SEPARATOR` `.` separator),
/// must be syntactically valid (`valid_custom_variable_name`), and must not
/// collide with a previously-reserved class prefix. A single-part unknown name
/// is rejected. `skip_errors == false` turns each rejection into the C
/// `ereport(elevel, ...)`; here `elevel == ERROR`, so a rejection is `Err`.
///
/// `reserved_class_prefix` (populated only by `MarkGUCPrefixReserved`, which is
/// not yet ported) is empty in this build, so the reserved-prefix loop has no
/// iterations — matching a backend that has not loaded any prefix-reserving
/// extension.
pub(crate) fn assignable_custom_variable_name(name: &str, skip_errors: bool) -> PgResult<bool> {
    // const char *sep = strchr(name, GUC_QUALIFIER_SEPARATOR);
    const GUC_QUALIFIER_SEPARATOR: char = '.';
    if name.contains(GUC_QUALIFIER_SEPARATOR) {
        // The name must be syntactically acceptable ...
        if !process_config::valid_custom_variable_name(name) {
            if !skip_errors {
                return Err(ereport(ERROR)
                    .errcode(types_error::ERRCODE_INVALID_NAME)
                    .errmsg(format!("invalid configuration parameter name \"{name}\""))
                    .errdetail(
                        "Custom parameter names must be two or more simple identifiers separated by dots.",
                    )
                    .into_error());
            }
            return Ok(false);
        }
        // ... and it must not match any previously-reserved prefix. C iterates
        // `reserved_class_prefix` comparing `className` against the name's class
        // (the substring up to the `.`); `MarkGUCPrefixReserved` populates that
        // list (see [`custom::mark_guc_prefix_reserved`]).
        let class = &name[..name.find(GUC_QUALIFIER_SEPARATOR).unwrap()];
        if custom::is_reserved_class_prefix(class) {
            if !skip_errors {
                return Err(ereport(ERROR)
                    .errcode(types_error::ERRCODE_INVALID_NAME)
                    .errmsg(format!("invalid configuration parameter name \"{name}\""))
                    .errdetail(format!("\"{class}\" is a reserved prefix."))
                    .into_error());
            }
            return Ok(false);
        }

        // OK to create it.
        return Ok(true);
    }

    // Unrecognized single-part name.
    if !skip_errors {
        return Err(ereport(ERROR)
            .errcode(types_error::ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("unrecognized configuration parameter \"{name}\""))
            .into_error());
    }
    Ok(false)
}

/// `check_GUC_name_for_parameter_acl(name)` (guc.c:1410): allow creating a
/// `pg_parameter_acl` entry for `name` only if the GUC exists or `name` is a
/// valid custom GUC name; otherwise throw. (May be applied before or after
/// canonicalization.)
fn check_guc_name_for_parameter_acl(name: &str) -> PgResult<()> {
    // OK if the GUC exists.  C: find_option(name, false, true, DEBUG5) — a pure
    // lookup with placeholder-creation disabled and errors skipped.
    let found = live::with_store(|reg| reg.find_option(name).is_some()).unwrap_or(false);
    if found {
        return Ok(());
    }
    // Otherwise, it'd better be a valid custom GUC name.
    assignable_custom_variable_name(name, false)?;
    Ok(())
}

/// The `!resetall` validation block of `AlterSystemSetConfigFile` (guc.c:4676-
/// 4742): look the target variable up (without creating a placeholder), reject
/// parameters that can't live in `postgresql.auto.conf`, validate the proposed
/// `value` if one is given, and (for an unknown name) check that the custom GUC
/// would be assignable. Finally reject a value containing a newline. `value ==
/// None` is the `VAR_SET_DEFAULT` / `VAR_RESET` case (a removal), which still
/// validates the target. This is purely the GUC-engine half; the caller owns
/// the superuser/ACL check, the file read/merge/write and the post-alter hook.
pub fn validate_auto_config_value(name: &str, value: Option<&str>) -> PgResult<()> {
    use types_guc::{config_type, PGC_S_FILE, GUC_DISALLOW_IN_AUTO_FILE, GUC_DISALLOW_IN_FILE};

    // record = find_option(name, false, true, DEBUG5);  -- no placeholder.
    // The lookup, the flag/context check and the parse_and_validate_value call
    // all need the live record, so run them inside one store borrow.
    let found = live::with_store(|reg| {
        let Some(record) = reg.find_option(name) else {
            return Ok::<bool, PgError>(false);
        };
        let gen = record.gen();

        // Don't allow parameters that can't be set in configuration files to be
        // set in PG_AUTOCONF_FILENAME file.
        if gen.context == PGC_INTERNAL
            || (gen.flags & GUC_DISALLOW_IN_FILE) != 0
            || (gen.flags & GUC_DISALLOW_IN_AUTO_FILE) != 0
        {
            return Err(ereport(ERROR)
                .errcode(types_error::ERRCODE_CANT_CHANGE_RUNTIME_PARAM)
                .errmsg(format!("parameter \"{name}\" cannot be changed"))
                .into_error());
        }

        // If a value is specified, verify that it's sane.
        if let Some(value) = value {
            // parse_and_validate_value(record, value, PGC_S_FILE, ERROR, ...).
            // The C wraps a false return in "invalid value for parameter"; the
            // ported parse_and_validate_value already returns that error form
            // (and the check-hook errors verbatim), so propagate it directly.
            // The converted value / extra are discarded (guc_free in C).
            let _ = registry::parse_and_validate_value(record, value, PGC_S_FILE)?;
            // (record->vartype == PGC_STRING) guc_free path is a no-op here:
            // the converted value is dropped when `_` falls out of scope.
            let _ = config_type::PGC_STRING;
        }
        Ok(true)
    })
    .ok_or_else(guc_store_uninitialized)??;

    if !found {
        // Variable not known; check we'd be allowed to create it.  As an
        // exception, skip this check for a RESET of an unknown custom GUC (so a
        // reserved-prefix setting remains removable).
        if value.is_some() || !process_config::valid_custom_variable_name(name) {
            assignable_custom_variable_name(name, false)?;
        }
    }

    // Reject values containing newlines: the config-file grammar has no embedded
    // newline in string literals.
    if let Some(value) = value {
        if value.contains('\n') {
            return Err(ereport(ERROR)
                .errcode(types_error::ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("parameter value for ALTER SYSTEM must not contain a newline")
                .into_error());
        }
    }

    Ok(())
}

/// `escape_single_quotes_ascii(src)` (port/quotes.c): escape (by doubling) any
/// single quote or backslash, as `postgresql.conf` string literals require
/// (backslashes are escapes there). `write_auto_conf_file` quotes each value
/// with this before emitting `name = 'value'`.
pub fn escape_single_quotes_ascii(src: &str) -> String {
    let mut out = String::with_capacity(src.len());
    for ch in src.chars() {
        // SQL_STR_DOUBLE(c, escape_backslash=true): a single quote or backslash
        // is doubled.
        if ch == '\'' || ch == '\\' {
            out.push(ch);
        }
        out.push(ch);
    }
    out
}

/// The live GUC store has not been built yet (`initialize_guc_options` not run).
/// A parallel transfer cannot proceed without it — surface the project's error.
pub(crate) fn guc_store_uninitialized() -> PgError {
    ereport(ERROR)
        .errcode(types_error::ERRCODE_INTERNAL_ERROR)
        .errmsg("GUC state transfer attempted before the GUC store was initialized")
        .into_error()
}

/// `GetConfigOption` value lookup over the live store.
fn with_store_lookup(name: &str, missing_ok: bool) -> Option<String> {
    live::with_store(|reg| match get_config_option_by_name(reg, name, missing_ok) {
        Ok(v) => v,
        Err(_) => None,
    })
    .flatten()
}
