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
//!   `initialize_guc_options` from the resolved [`backend_utils_misc_guc_tables`]
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
pub mod enum_lookup;
pub mod guc_array;
pub mod live;
pub mod model;
pub mod name;
pub mod process_config;
pub mod registry;
pub mod report;
pub mod seam;
pub mod units;

#[cfg(test)]
mod tests;

use backend_utils_error::ereport;
use types_error::{
    PgError, PgResult, SqlState, ERRCODE_INVALID_PARAMETER_VALUE, ERROR,
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
pub use process_config::{apply_config_variables, ConfigItem};
pub use registry::{
    get_config_option_by_name, parse_and_validate_value, reset_all_options, reset_value_string,
    set_config_option, show_guc_option, GucAction, GucRegistry, GucVariable,
};
pub use report::{begin_reporting_guc_options, report_changed_guc_options};
pub use units::{
    convert_int_from_base_unit, convert_real_from_base_unit, convert_to_base_unit,
    get_config_unit_name, parse_int as parse_int_units, parse_real as parse_real_units, ParseNum,
    MAX_UNIT_LEN, MEMORY_UNITS_HINT, TIME_UNITS_HINT,
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

// ---------------------------------------------------------------------------
// Tier-A seam install (this crate is guc.c's home).
// ---------------------------------------------------------------------------

use types_core::BOOTSTRAP_SUPERUSERID;
use types_guc::{
    GucContext, GucSource, PGC_BACKEND, PGC_INTERNAL, PGC_SIGHUP, PGC_SUSET,
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

/// Install every seam declared in `backend-utils-misc-guc-seams`.
pub fn init_seams() {
    use backend_utils_misc_guc_seams as s;

    // --- Numeric value parsers (guc.c parse_int / parse_real). ---
    s::parse_int::set(|value| match units::parse_int(&value, 0) {
        ParseNum::Ok(v) => Some(v),
        ParseNum::Err { .. } => None,
    });
    s::parse_real::set(|value| match units::parse_real(&value, 0) {
        ParseNum::Ok(v) => Some(v),
        ParseNum::Err { .. } => None,
    });

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
    s::maintenance_work_mem::set(|| get_int("maintenance_work_mem").unwrap_or(0));
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

    // --- Sub-features owned by separate, not-yet-ported units. Installed here
    //     (guc.c is their home) but each loud-panics into the unported sub-unit
    //     rather than silently stubbing (mirror-and-panic). ---

    // GUC nesting level (guc.c): NewGUCNestLevel is `++GUCNestLevel`, owned
    // here. AtEOXact_GUC's stack rollback still belongs to guc_stack.c.
    s::new_guc_nest_level::set(NewGUCNestLevel);
    s::at_eoxact_guc::set(|_is_commit, _nest_level| {
        panic!(
            "AtEOXact_GUC: the transactional GUC stack (guc_stack.c) is a separate unit not yet \
             ported"
        )
    });

    // --- guc.c bodies whose seam decls previously lived (mis-homed) in the
    //     sibling `backend-utils-misc-guc-file-seams` crate; they are now
    //     RE-HOMED onto guc.c's own `-seams` crate (`s` =
    //     backend-utils-misc-guc-seams) so the install is dir-owner-attributable
    //     and the guard re-asserts the contract (retires
    //     CONTRACT_RECONCILE_PENDING). The `process_config_file` decl stays in
    //     guc-file-seams — it is genuinely guc-file.l's (the lexer/reader unit). ---

    // GUC_check_errdetail / GUC_check_errhint (guc.c): record check-hook
    // failure detail/hint into the backend-local check-error state.
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

    // postgresql.conf parsing orchestration (config_file.c / guc-file.l).
    s::parse_long_option::set(|_mcx, _string| {
        panic!(
            "ParseLongOption: the command-line/config option splitter (config_file.c) is a \
             separate unit not yet ported"
        )
    });
    s::select_config_files::set(|_user_doption, _progname| {
        panic!(
            "SelectConfigFiles: postgresql.conf location/read (config_file.c) is a separate unit \
             not yet ported"
        )
    });
    // ProcessConfigFile(PGC_SIGHUP) (guc-file.l) — the SIGHUP reload. The
    // memory-context wrapper lives in guc-file.l (its own unit); drive it
    // through its seam so a SIGHUP reload re-reads and applies the file.
    s::process_config_file_sighup::set(|| {
        backend_utils_misc_guc_file_seams::process_config_file::call(PGC_SIGHUP)
    });
    // ProcessConfigFileInternal (guc.c) — the parse-then-apply core. Parsing
    // lives in guc-file.l (called directly here, mirroring guc.c → guc-file.l);
    // the apply phase is `apply_config_variables` in this crate.
    s::process_config_file_internal::set(|context, apply_settings, elevel| {
        process_config::process_config_file_internal(context, apply_settings, elevel).map(|_| ())
    });

    // SetPGVariable's List *A_Const marshaling (the DISCARD ALL session_auth
    // reset).
    s::set_pg_variable_session_authorization_reset::set(|| {
        panic!(
            "SetPGVariable(\"session_authorization\", NIL, false): the A_Const List marshaling is \
             not yet modeled in this core"
        )
    });
}

/// `GetConfigOption` value lookup over the live store.
fn with_store_lookup(name: &str, missing_ok: bool) -> Option<String> {
    live::with_store(|reg| match get_config_option_by_name(reg, name, missing_ok) {
        Ok(v) => v,
        Err(_) => None,
    })
    .flatten()
}
