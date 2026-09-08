#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

// guc.c runtime. GUC storage is C guc_malloc (raw malloc, never palloc), so
// the store uses std String/Vec on mimalloc — the same cost shape.

pub mod array;
pub mod autotune;
pub mod cnum;
pub mod enum_lookup;
pub mod help_config;
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
    // static List *reserved_class_prefix (guc.c:78): a process static, so
    // session-scoped TLS here, seeded from the postmaster's list at child
    // launch the way fork inherits it (shared_preload_libraries _PG_init
    // reservations hold in every backend).
    static RESERVED_CLASS_PREFIX: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

pub fn reserved_class_prefixes() -> Vec<String> {
    RESERVED_CLASS_PREFIX.with(|s| s.borrow().clone())
}

pub fn inherit_reserved_class_prefixes(prefixes: &[String]) {
    RESERVED_CLASS_PREFIX.with(|s| *s.borrow_mut() = prefixes.to_vec());
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
        // C: ConfigOptionIsVisible (guc_funcs.c) — GUC_SUPERUSER_ONLY reads
        // need has_privs_of_role(GetUserId(), ROLE_PG_READ_ALL_SETTINGS).
        if restrict_privileged
            && record.gen().flags & types_guc::GUC_SUPERUSER_ONLY != 0
            && !acl_seams::has_privs_of_role::call(
                miscinit::GetUserId(),
                ROLE_PG_READ_ALL_SETTINGS,
            )?
        {
            return Err(ereport(ERROR)
                .errcode(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg(format!("permission denied to examine \"{name}\""))
                .errdetail(
                    "Only roles with privileges of the \"pg_read_all_settings\" role may examine this parameter.",
                )
                .into_error()
                .into());
        }
        Ok(Some(registry::raw_config_value(record)))
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

// ROLE_PG_READ_ALL_SETTINGS (pg_authid.dat).
const ROLE_PG_READ_ALL_SETTINGS: Oid = 3374;

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
                    .with_funcname("assignable_custom_variable_name")
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
                    .with_funcname("assignable_custom_variable_name")
                    .into());
            }
            return Ok(false);
        }
        return Ok(true);
    }

    if !skip_errors {
        return Err(unrecognized(name).with_funcname("assignable_custom_variable_name").into());
    }
    Ok(false)
}

// DefineCustomStringVariable (guc.c:5224) over define_custom_variable
// (guc.c:4937): an extension's string GUC, defined when its library loads.
// The value lives in the registry record (as a placeholder's does): there is
// no valueAddr, readers use GetConfigOption. A placeholder of the same name
// (a SET that preceded the load) is replaced and its reset, current and
// stacked values re-applied through set_config_option_ext at WARNING, in
// their original order (reapply_stacked_values, guc.c:5041); a
// non-placeholder of that name is "attempt to redefine parameter".
pub fn DefineCustomStringVariable(
    name: &'static str,
    short_desc: Option<&'static str>,
    long_desc: Option<&'static str>,
    boot_val: Option<&str>,
    context: GucContext,
    flags: i32,
) -> PgResult<()> {
    use types_guc::{config_group, config_type};
    // init_custom_variable (guc.c:4875): a PGC_POSTMASTER custom variable
    // after startup and GUC_LIST_QUOTE are FATAL there; no in-tree caller
    // asks for either, so they are refused as errors here.
    if context == types_guc::PGC_POSTMASTER {
        return Err(Box::new(PgError::error(
            "cannot create PGC_POSTMASTER variables after startup",
        )));
    }
    if flags & types_guc::GUC_LIST_QUOTE != 0 {
        return Err(Box::new(PgError::error(
            "extensions cannot define GUC_LIST_QUOTE variables",
        )));
    }
    let gen = model::config_generic::boot(
        name,
        context,
        config_group::CUSTOM_OPTIONS,
        short_desc,
        long_desc,
        flags,
        config_type::PGC_STRING,
    );
    let mut var = GucVariable::String(model::config_string {
        gen,
        variable: &guc_tables::vars::GucPlaceholderVariable,
        value: None,
        boot_val: boot_val.map(str::to_owned),
        check_hook: None,
        assign_hook: None,
        show_hook: None,
        reset_val: None,
        reset_extra: None,
    });
    // InitializeOneGUCOption: the default value first, even when a
    // placeholder value is about to be applied (it may be invalid).
    registry::initialize_one_guc_option_hooks(&mut var, false)?;
    if let GucVariable::String(conf) = &mut var {
        conf.reset_val = conf.boot_val.clone();
    }
    let holder = store::with_store_mut(|reg| reg.define_custom_variable(var))
        .unwrap_or_else(|| Err(Box::new(PgError::error("GUC store is not initialized"))))?;
    let Some(holder) = holder else {
        return Ok(());
    };
    // First, apply the reset value if any.
    if let Some(reset_val) = holder.reset_val.as_deref() {
        let _ = set_config_option_ext(
            name,
            Some(reset_val),
            holder.gen.reset_scontext,
            holder.gen.reset_source,
            holder.gen.reset_srole,
            GUC_ACTION_SET,
            true,
            WARNING,
            false,
        );
    }
    // Now, apply current and stacked values, in the order they were stacked.
    let curvalue = holder.value.clone().flatten();
    reapply_stacked_values(
        name,
        &holder,
        holder.gen.stack.as_deref(),
        curvalue.as_deref(),
        holder.gen.scontext,
        holder.gen.source,
        holder.gen.srole,
    );
    // Also copy over any saved source-location information.
    if let Some(file) = holder.gen.sourcefile.as_deref() {
        process_config::set_config_sourcefile(name, file, holder.gen.sourceline);
    }
    Ok(())
}

// reapply_stacked_values (guc.c:5041): recurse so the values are applied
// bottom to top; at each level apply the passed-in value the way its stack
// entry implies.
fn reapply_stacked_values(
    name: &str,
    holder: &model::config_string,
    stack: Option<&model::GucStack>,
    curvalue: Option<&str>,
    curscontext: GucContext,
    cursource: GucSource,
    cursrole: Oid,
) {
    let apply = |value: Option<&str>, scontext: GucContext, source: GucSource, srole: Oid, action: u32| {
        let _ = set_config_option_ext(name, value, scontext, source, srole, action, true, WARNING, false);
    };
    let stack_string = |v: &model::config_var_value| -> Option<String> {
        match &v.val {
            Some(model::config_var_val::Stringval(s)) => s.clone(),
            _ => None,
        }
    };
    if let Some(entry) = stack {
        let prior = stack_string(&entry.prior);
        reapply_stacked_values(
            name,
            holder,
            entry.prev.as_deref(),
            prior.as_deref(),
            entry.scontext,
            entry.source,
            entry.srole,
        );
        let depth_before = store::with_store(|reg| reg.stack_depth(name)).unwrap_or(0);
        match entry.state {
            model::GUC_SAVE => apply(curvalue, curscontext, cursource, cursrole, GUC_ACTION_SAVE),
            model::GUC_SET => apply(curvalue, curscontext, cursource, cursrole, GUC_ACTION_SET),
            model::GUC_LOCAL => apply(curvalue, curscontext, cursource, cursrole, GUC_ACTION_LOCAL),
            model::GUC_SET_LOCAL => {
                // First, apply the masked value as SET, then the current
                // value as LOCAL.
                let masked = stack_string(&entry.masked);
                apply(
                    masked.as_deref(),
                    entry.masked_scontext,
                    PGC_S_SESSION,
                    entry.masked_srole,
                    GUC_ACTION_SET,
                );
                apply(curvalue, curscontext, cursource, cursrole, GUC_ACTION_LOCAL);
            }
            _ => {}
        }
        // If we successfully made a stack entry, adjust its nest level.
        store::with_store_mut(|reg| {
            if reg.stack_depth(name) > depth_before {
                if let Some(level) = reg.stack_top_nest_level(name) {
                    *level = entry.nest_level;
                }
            }
        });
    } else if curvalue != holder.reset_val.as_deref()
        || curscontext != holder.gen.reset_scontext
        || cursource != holder.gen.reset_source
        || cursrole != holder.gen.reset_srole
    {
        // End of the stack: a previously committed session value. Apply it,
        // then drop the stack entry set_config_option pushed under the
        // impression that this is a transactional assignment.
        apply(curvalue, curscontext, cursource, cursrole, GUC_ACTION_SET);
        store::with_store_mut(|reg| reg.drop_stack(name));
    }
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
    s::guc_check_errcode::set(GUC_check_errcode);
    s::guc_check_errhint::set(|hint| GUC_check_errhint(hint));
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
