#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

// guc.c runtime. GUC storage is C guc_malloc (raw malloc, never palloc), so
// the store uses std String/Vec on mimalloc — the same cost shape.

pub mod array;
pub mod autotune;
pub mod cnum;
pub mod enum_lookup;
pub mod layers;
pub mod model;
pub mod name;
pub mod process_config;
pub mod registry;
pub mod report;
pub mod select;
pub mod store;
pub mod units;

#[cfg(test)]
mod tests;

use std::cell::{Cell, RefCell};

use elog::ereport;
use types_core::{Oid, BOOTSTRAP_SUPERUSERID};
use types_error::{
    PgError, PgResult, SqlState, ERRCODE_INVALID_PARAMETER_VALUE, ERROR, WARNING,
};
use types_guc::{GucContext, GucSource, PGC_INTERNAL, PGC_S_CLIENT, PGC_S_DYNAMIC_DEFAULT, PGC_S_INTERACTIVE, PGC_S_SESSION};

pub use enum_lookup::{
    config_enum_get_options, config_enum_lookup_by_name, config_enum_lookup_by_value,
};
pub use name::{
    convert_guc_name_for_parameter_acl, guc_name_compare, guc_name_eq, guc_name_hash,
    MAP_OLD_GUC_NAMES,
};
pub use registry::{
    get_config_option_by_name, get_config_option_flags, parse_and_validate_value,
    reset_value_string, show_guc_option, GucAction, GucRegistry, GucVariable,
};
pub use report::{begin_reporting_guc_options, report_changed_guc_options};
pub use select::SelectConfigFiles;
pub use store::{
    get_bool, get_enum, get_int, get_real, get_string, initialize_guc_options, is_initialized,
    pg_reload_time, set_config_option_global, set_pg_reload_time, with_store, with_store_mut,
};
pub use units::{
    convert_int_from_base_unit, convert_real_from_base_unit, convert_to_base_unit, fmt_e, fmt_g,
    fmt_g_prec, get_config_unit_name, parse_int, parse_real, ParseNum, MAX_UNIT_LEN,
    MEMORY_UNITS_HINT, TIME_UNITS_HINT,
};

// GucAction (utils/guc.h).
pub const GUC_ACTION_SET: u32 = 0;
pub const GUC_ACTION_LOCAL: u32 = 1;
pub const GUC_ACTION_SAVE: u32 = 2;

// GUC_check_errcode/errmsg/errdetail/errhint protocol (guc.c:6796): a check
// hook signals failure by returning false after filling these.
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

thread_local! {
    static GUC_CHECK_ERROR: RefCell<GucCheckError> = RefCell::new(GucCheckError::default());
    // static int GUCNestLevel = 0 (guc.c:231).
    static GUC_NEST_LEVEL: Cell<i32> = const { Cell::new(0) };
    // static List *reserved_class_prefix (guc.c:78); per-backend in C, so
    // session-scoped TLS here.
    static RESERVED_CLASS_PREFIX: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

pub fn reset_guc_check_error() {
    GUC_CHECK_ERROR.with(|s| *s.borrow_mut() = GucCheckError::default());
}

pub fn take_guc_check_error() -> GucCheckError {
    GUC_CHECK_ERROR.with(|s| core::mem::take(&mut *s.borrow_mut()))
}

pub fn GUC_check_errcode(sqlstate: SqlState) {
    GUC_CHECK_ERROR.with(|s| s.borrow_mut().sqlstate = sqlstate);
}

pub fn GUC_check_errmsg(message: impl Into<String>) {
    GUC_CHECK_ERROR.with(|s| s.borrow_mut().message = Some(message.into()));
}

pub fn GUC_check_errdetail(detail: impl Into<String>) {
    GUC_CHECK_ERROR.with(|s| s.borrow_mut().detail = Some(detail.into()));
}

pub fn GUC_check_errhint(hint: impl Into<String>) {
    GUC_CHECK_ERROR.with(|s| s.borrow_mut().hint = Some(hint.into()));
}

pub fn guc_nest_level() -> i32 {
    GUC_NEST_LEVEL.get()
}

// AtStart_GUC (guc.c:2215).
pub fn AtStart_GUC() {
    if GUC_NEST_LEVEL.get() != 0 {
        let e = ereport(WARNING)
            .errmsg(format!("GUC nest level = {} at transaction start", GUC_NEST_LEVEL.get()))
            .into_error();
        elog::emit_error_report_for(&e);
    }
    GUC_NEST_LEVEL.set(1);
}

// NewGUCNestLevel (guc.c:2235): return ++GUCNestLevel.
#[inline]
pub fn NewGUCNestLevel() -> i32 {
    let level = GUC_NEST_LEVEL.get() + 1;
    GUC_NEST_LEVEL.set(level);
    level
}

// AtEOXact_GUC (guc.c:2262). Per-commit hot: a transaction that changed no
// GUCs sees an empty guc_stack_list and exits after the nest-level store.
pub fn AtEOXact_GUC(is_commit: bool, nest_level: i32) {
    debug_assert!(
        nest_level > 0
            && (nest_level <= GUC_NEST_LEVEL.get()
                || (nest_level == GUC_NEST_LEVEL.get() + 1 && !is_commit))
    );
    // A statement that set no GUC pays one Cell load, as C pays one bare
    // slist_is_empty(&guc_stack_list); the store borrow lives in the cold path.
    if store::has_stacked_hint() {
        at_eoxact_guc_haswork(is_commit, nest_level);
    }
    GUC_NEST_LEVEL.set(nest_level - 1);
}

#[cold]
#[inline(never)]
fn at_eoxact_guc_haswork(is_commit: bool, nest_level: i32) {
    let has_work = store::with_store(|reg| reg.has_stacked()).unwrap_or(false);
    if has_work {
        let mut deferred_hooks: Vec<registry::DeferredAssignHook> = Vec::new();
        store::with_store_mut(|reg| {
            registry::at_eoxact_guc(reg, is_commit, nest_level, &mut deferred_hooks);
        });
        for hook in deferred_hooks {
            hook();
        }
    }
    // Re-arm the hint from the real list (entries can survive to outer levels).
    store::set_has_stacked_hint(
        store::with_store(|reg| reg.has_stacked()).unwrap_or(false),
    );
}

// set_config_option (guc.c:3342): srole from the source class.
#[allow(clippy::too_many_arguments)]
pub fn set_config_option(
    name: &str,
    value: Option<&str>,
    context: GucContext,
    source: GucSource,
    action: GucAction,
    change_val: bool,
    elevel: types_error::ErrorLevel,
    is_reload: bool,
) -> PgResult<i32> {
    let srole = if source >= PGC_S_INTERACTIVE || source == PGC_S_CLIENT {
        miscinit::GetUserId()
    } else {
        BOOTSTRAP_SUPERUSERID
    };
    set_config_option_global(name, value, context, source, srole, action, change_val, elevel, is_reload)
}

// set_config_option_ext (guc.c:3382).
#[allow(clippy::too_many_arguments)]
pub fn set_config_option_ext(
    name: &str,
    value: Option<&str>,
    context: GucContext,
    source: GucSource,
    srole: Oid,
    action: GucAction,
    change_val: bool,
    elevel: types_error::ErrorLevel,
    is_reload: bool,
) -> PgResult<i32> {
    set_config_option_global(name, value, context, source, srole, action, change_val, elevel, is_reload)
}

// SetConfigOption (guc.c:4332).
pub fn SetConfigOption(
    name: &str,
    value: Option<&str>,
    context: GucContext,
    source: GucSource,
) -> PgResult<()> {
    set_config_option(
        name,
        value,
        context,
        source,
        GUC_ACTION_SET,
        true,
        types_error::ErrorLevel(0),
        false,
    )
    .map(|_| ())
}

// GetConfigOption (guc.c:4355).
pub fn GetConfigOption(
    name: &str,
    missing_ok: bool,
    restrict_privileged: bool,
) -> PgResult<Option<String>> {
    store::with_store(|reg| {
        let Some(record) = reg.find_option(name) else {
            if missing_ok {
                return Ok(None);
            }
            return Err(Box::new(unrecognized(name)));
        };
        if restrict_privileged && record.gen().flags & types_guc::GUC_SUPERUSER_ONLY != 0 {
            // has_privs_of_role(GetUserId(), ROLE_PG_READ_ALL_SETTINGS) is
            // acl.c's, unported; loud panic, never a silent allow.
            panic!("GetConfigOption({name:?}): GUC_SUPERUSER_ONLY privilege check not yet ported");
        }
        Ok(Some(show_guc_option(record, false)))
    })
    .expect("GUC store not initialized")
}

// GetConfigOptionFlags (guc.c:4438).
pub fn GetConfigOptionFlags(name: &str, missing_ok: bool) -> PgResult<i32> {
    store::with_store(|reg| get_config_option_flags(reg, name, missing_ok))
        .expect("GUC store not initialized")
}

// GetConfigOptionResetString (guc.c:4405), minus the same privilege gate.
pub fn GetConfigOptionResetString(name: &str) -> Option<String> {
    store::with_store(|reg| reg.find_option(name).and_then(reset_value_string))
        .expect("GUC store not initialized")
}

pub fn ResetAllOptions() {
    store::reset_all_options();
}

#[cold]
fn unrecognized(name: &str) -> PgError {
    ereport(ERROR)
        .errcode(types_error::ERRCODE_UNDEFINED_OBJECT)
        .errmsg(format!("unrecognized configuration parameter \"{name}\""))
        .into_error()
}

const GUC_QUALIFIER_SEPARATOR: char = '.';

// valid_custom_variable_name (guc.c:1076).
pub use array::{
    GUCArrayAdd, GUCArrayDelete, GUCArrayReset, ProcessGUCArray, TransformGUCArray,
    validate_option_array_item,
};

pub fn valid_custom_variable_name(name: &str) -> bool {
    let mut saw_sep = false;
    let mut name_start = true;
    for &b in name.as_bytes() {
        if b == b'.' {
            if name_start {
                return false;
            }
            saw_sep = true;
            name_start = true;
        } else if b.is_ascii_alphabetic() || b == b'_' || b & 0x80 != 0 {
            name_start = false;
        } else if !name_start && (b.is_ascii_digit() || b == b'$') {
        } else {
            return false;
        }
    }
    !name_start && saw_sep
}

// assignable_custom_variable_name (guc.c:1121).
pub fn assignable_custom_variable_name(name: &str, skip_errors: bool) -> PgResult<bool> {
    if let Some(class_len) = name.find(GUC_QUALIFIER_SEPARATOR) {
        if !valid_custom_variable_name(name) {
            if !skip_errors {
                return Err(ereport(ERROR)
                    .errcode(types_error::ERRCODE_INVALID_NAME)
                    .errmsg(format!("invalid configuration parameter name \"{name}\""))
                    .errdetail(
                        "Custom parameter names must be two or more simple identifiers separated by dots.",
                    )
                    .into_error()
                    .into());
            }
            return Ok(false);
        }
        let reserved = RESERVED_CLASS_PREFIX.with(|s| {
            s.borrow().iter().find(|p| p.len() == class_len && name.starts_with(p.as_str())).cloned()
        });
        if let Some(rcprefix) = reserved {
            if !skip_errors {
                return Err(ereport(ERROR)
                    .errcode(types_error::ERRCODE_INVALID_NAME)
                    .errmsg(format!("invalid configuration parameter name \"{name}\""))
                    .errdetail(format!("\"{rcprefix}\" is a reserved prefix."))
                    .into_error()
                    .into());
            }
            return Ok(false);
        }
        return Ok(true);
    }

    if !skip_errors {
        return Err(unrecognized(name).into());
    }
    Ok(false)
}

// MarkGUCPrefixReserved (guc.c:5285): purge existing placeholders under the
// prefix (WARNING each), then reserve the prefix against future placeholders.
pub fn MarkGUCPrefixReserved(class_name: &str) {
    let removed =
        store::with_store_mut(|reg| reg.remove_reserved_placeholders(class_name)).unwrap_or_default();
    for name in removed {
        let e = ereport(WARNING)
            .errcode(types_error::ERRCODE_INVALID_NAME)
            .errmsg(format!("invalid configuration parameter name \"{name}\", removing it"))
            .errdetail(format!("\"{class_name}\" is now a reserved prefix."))
            .into_error();
        elog::emit_error_report_for(&e);
    }
    RESERVED_CLASS_PREFIX.with(|s| {
        let mut prefixes = s.borrow_mut();
        if !prefixes.iter().any(|p| p == class_name) {
            prefixes.push(class_name.to_string());
        }
    });
}

// check_GUC_name_for_parameter_acl (guc.c:1410).
pub fn check_GUC_name_for_parameter_acl(name: &str) -> PgResult<()> {
    let found = store::with_store(|reg| reg.find_option(name).is_some()).unwrap_or(false);
    if found {
        return Ok(());
    }
    assignable_custom_variable_name(name, false)?;
    Ok(())
}

// GUC_SAFE_SEARCH_PATH (guc.c:74) + RestrictSearchPath (guc.c:2246).
const GUC_SAFE_SEARCH_PATH: &str = "pg_catalog, pg_temp";

pub fn RestrictSearchPath() -> PgResult<()> {
    if miscinit::IsBootstrapProcessingMode() {
        return Ok(());
    }
    set_config_option(
        "search_path",
        Some(GUC_SAFE_SEARCH_PATH),
        types_guc::PGC_USERSET,
        PGC_S_SESSION,
        GUC_ACTION_SAVE,
        true,
        types_error::ErrorLevel(0),
        false,
    )
    .map(|_| ())
}

// ParseLongOption (guc.c:6368): "some-option=some value" -> ("some_option",
// Some("some value")); '-' becomes '_'.
pub fn ParseLongOption(string: &str) -> (String, Option<String>) {
    match string.split_once('=') {
        Some((name, value)) => (name.replace('-', "_"), Some(value.to_string())),
        None => (string.replace('-', "_"), None),
    }
}

pub fn init_seams() {
    use guc_seams as s;

    s::new_guc_nest_level::set(NewGUCNestLevel);
    s::get_config_option_missing_ok::set(|name| GetConfigOption(name, true, false));
    s::guc_check_errdetail::set(|detail| GUC_check_errdetail(detail));
    s::at_eoxact_guc::set(|is_commit, nest_level| {
        AtEOXact_GUC(is_commit, nest_level);
        Ok(())
    });
    s::set_config_option_internal_dynamic_default::set(|name, value| {
        SetConfigOption(name, Some(value), PGC_INTERNAL, PGC_S_DYNAMIC_DEFAULT)
    });
    s::set_config_option::set(SetConfigOption);
    s::process_guc_array_secdef::set(|array| {
        // fmgr.c:744: the secdef wrapper already switched to the owner, so
        // superuser() reflects the function owner here.
        let context = if superuser_seams::superuser::call()? {
            GucContext::PGC_SUSET
        } else {
            GucContext::PGC_USERSET
        };
        ProcessGUCArray(array, context, PGC_S_SESSION, GUC_ACTION_SAVE)
    });
    s::process_config_file_internal::set(|context, apply_settings, elevel| {
        process_config::process_config_file_internal(context, apply_settings, elevel).map(|_| ())
    });
    s::select_config_files::set(SelectConfigFiles);
    s::initialize_guc_options::set(initialize_guc_options);
}
