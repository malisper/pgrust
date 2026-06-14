//! The in-memory GUC variable registry and the core operations that act on it:
//! `find_option`, `set_config_option` (the access-permission rules + value
//! application), `GetConfigOptionByName`, `ShowGUCOption`, and
//! `ResetAllOptions`.
//!
//! In C these operate on `guc_hashtab` (a dynahash of `struct config_generic
//! *`). The idiomatic registry is a `Vec<GucVariable>` keyed by name through
//! [`crate::name::guc_name_compare`]; `GucVariable` is the safe equivalent of
//! the C "downcast by `gen.vartype`" pattern (a tagged union over the five typed
//! `config_*` records that all embed a `config_generic`).
//!
//! The per-variable check/assign/show hooks live in the `guc_tables` typed
//! *slots* the record references (the analog of the C record carrying the hook
//! pointers); a hook installed by its owning subsystem is called through the
//! slot, a hook the table leaves `None` is skipped, exactly as C does.

use backend_utils_error::{ereport, message_level_is_interesting};
use types_error::{
    ErrorLevel, PgError, PgResult, SqlState, DEBUG3, ERRCODE_CANT_CHANGE_RUNTIME_PARAM,
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_INVALID_TRANSACTION_STATE, ERRCODE_UNDEFINED_OBJECT, ERROR, LOG, WARNING,
};
use types_acl::{AclResult, ACL_SET};
use types_core::Oid;
use types_guc::{
    GucContext, GucSource, GUC_ALLOW_IN_PARALLEL, GUC_IS_NAME, GUC_NO_RESET, GUC_UNIT, PGC_BACKEND,
    PGC_INTERNAL, PGC_POSTMASTER, PGC_SIGHUP, PGC_SUSET, PGC_SU_BACKEND, PGC_S_CLIENT,
    PGC_S_DATABASE, PGC_S_DATABASE_USER, PGC_S_DEFAULT, PGC_S_FILE, PGC_S_GLOBAL, PGC_S_OVERRIDE,
    PGC_S_USER, PGC_USERSET,
};

use backend_utils_misc_guc_tables::GucHookExtra;

use crate::enum_lookup::{
    config_enum_get_options, config_enum_lookup_by_name, config_enum_lookup_by_value,
};
use crate::model::{
    config_bool, config_enum, config_generic, config_int, config_real, config_string,
    config_var_val,
};
use crate::name::{guc_name_eq, MAP_OLD_GUC_NAMES};
use crate::units::{
    convert_int_from_base_unit, convert_real_from_base_unit, get_config_unit_name, parse_int,
    parse_real, ParseNum,
};
use crate::GUC_ACTION_SAVE;

/// `GucAction` (`utils/guc.h`): GUC_ACTION_SET/LOCAL/SAVE (see crate root).
pub type GucAction = u32;

/// The five typed GUC variable shapes, all of which embed a `config_generic`.
/// Equivalent to the C `struct config_generic *` downcast by `gen.vartype`.
#[derive(Debug)]
pub enum GucVariable {
    Bool(config_bool),
    Int(config_int),
    Real(config_real),
    String(config_string),
    Enum(config_enum),
}

impl GucVariable {
    /// The embedded `config_generic` (C: `&conf->gen`).
    pub fn gen(&self) -> &config_generic {
        match self {
            GucVariable::Bool(c) => &c.gen,
            GucVariable::Int(c) => &c.gen,
            GucVariable::Real(c) => &c.gen,
            GucVariable::String(c) => &c.gen,
            GucVariable::Enum(c) => &c.gen,
        }
    }

    pub fn gen_mut(&mut self) -> &mut config_generic {
        match self {
            GucVariable::Bool(c) => &mut c.gen,
            GucVariable::Int(c) => &mut c.gen,
            GucVariable::Real(c) => &mut c.gen,
            GucVariable::String(c) => &mut c.gen,
            GucVariable::Enum(c) => &mut c.gen,
        }
    }

    fn name(&self) -> &str {
        self.gen().name
    }

    /// The variable's name (C: `conf->gen.name`).
    pub fn name_pub(&self) -> &str {
        self.name()
    }
}

/// The GUC variable table (C: `guc_hashtab` + the slist/dlist link lists). The
/// spine is a process-lifetime `Vec` (the C `guc_hashtab`-in-TopMemoryContext
/// analog — never freed); `define` grows it fallibly so a boot-time OOM is the
/// project's `PgError`, not an abort.
#[derive(Debug, Default)]
pub struct GucRegistry {
    vars: Vec<GucVariable>,
}

impl GucRegistry {
    pub fn new() -> Self {
        Self { vars: Vec::new() }
    }

    /// `add_guc_variable(var)` (guc.c:1047), minus the hashtab/placeholder
    /// bookkeeping: register a variable. Fallible: a spine growth surfaces OOM
    /// as the project's out-of-memory `PgError` (the C `guc_malloc` FATAL).
    pub fn define(&mut self, var: GucVariable) -> PgResult<()> {
        self.vars.try_reserve(1).map_err(crate::alloc_err)?;
        self.vars.push(var);
        Ok(())
    }

    /// `find_option(name, create_placeholders=false, skip_errors=true, ...)`
    /// (guc.c:1235), returning an immutable reference. The placeholder-creation
    /// path (custom variables) is not part of the core; callers that need it use
    /// the dedicated custom-variable machinery (deferred). Applies
    /// `map_old_guc_names`.
    pub fn find_option(&self, name: &str) -> Option<&GucVariable> {
        self.find_index(name).map(|idx| &self.vars[idx])
    }

    /// Mutable variant of [`find_option`].
    pub fn find_option_mut(&mut self, name: &str) -> Option<&mut GucVariable> {
        match self.find_index(name) {
            Some(idx) => Some(&mut self.vars[idx]),
            None => None,
        }
    }

    fn find_index(&self, name: &str) -> Option<usize> {
        // Direct match.
        if let Some(idx) = self.vars.iter().position(|v| guc_name_eq(v.name(), name)) {
            return Some(idx);
        }
        // Obsolete-name brute-force mapping (guc.c:1256).
        for (old, new) in MAP_OLD_GUC_NAMES {
            if guc_name_eq(name, old) {
                return self.vars.iter().position(|v| guc_name_eq(v.name(), new));
            }
        }
        None
    }

    /// Iterate the registered variables (sorted-name iteration is the caller's
    /// responsibility; C iterates the hashtab in arbitrary order then sorts).
    pub fn iter(&self) -> impl Iterator<Item = &GucVariable> {
        self.vars.iter()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut GucVariable> {
        self.vars.iter_mut()
    }

    pub fn len(&self) -> usize {
        self.vars.len()
    }

    pub fn is_empty(&self) -> bool {
        self.vars.is_empty()
    }

    /// Public index lookup used by `set_config_option`.
    pub fn find_index_pub(&self, name: &str) -> Option<usize> {
        self.find_index(name)
    }
}

impl core::ops::Index<usize> for GucRegistry {
    type Output = GucVariable;
    fn index(&self, i: usize) -> &GucVariable {
        &self.vars[i]
    }
}

impl core::ops::IndexMut<usize> for GucRegistry {
    fn index_mut(&mut self, i: usize) -> &mut GucVariable {
        &mut self.vars[i]
    }
}

/// Choose the report elevel from a 0 `elevel` argument, per the source
/// (`set_config_with_handle`, guc.c:3417).
fn resolve_elevel(elevel: ErrorLevel, source: GucSource) -> ErrorLevel {
    if elevel != ErrorLevel(0) {
        return elevel;
    }
    if source == PGC_S_DEFAULT || source == PGC_S_FILE {
        if crate::seam::is_under_postmaster::call() {
            DEBUG3
        } else {
            LOG
        }
    } else if source == PGC_S_GLOBAL
        || source == PGC_S_DATABASE
        || source == PGC_S_USER
        || source == PGC_S_DATABASE_USER
    {
        WARNING
    } else {
        ERROR
    }
}

/// Build a `PgError` for a given errcode/message (used when elevel >= ERROR).
fn err(sqlstate: SqlState, message: String) -> PgError {
    ereport(ERROR).errcode(sqlstate).errmsg(message).into_error()
}

/// Result of the access-permission check stage of `set_config_option`.
enum AccessCheck {
    Ok,
    Skip,
    Reject(PgError),
}

/// `pg_parameter_aclcheck(name, role, ACL_SET) == ACLCHECK_OK` (aclchk.c) via
/// the catalog seam. `Err` carries the seam's `ereport(ERROR)`.
fn parameter_acl_set_ok(name: &str, role: Oid) -> PgResult<bool> {
    Ok(crate::seam::pg_parameter_aclcheck::call(name, role, ACL_SET)? == AclResult::AclcheckOk)
}

/// The `GucContext`/`GucSource` access-permission rules of
/// `set_config_with_handle` (guc.c:3457..3671), factored out as a pure check
/// over a variable. `changeVal`/`is_reload` mirror the C parameters.
#[allow(clippy::too_many_arguments)]
fn check_can_set(
    record: &config_generic,
    value_is_null: bool,
    context: GucContext,
    source: GucSource,
    srole: Oid,
    action: GucAction,
    change_val: bool,
    is_reload: bool,
) -> PgResult<AccessCheck> {
    let name = record.name;

    // Parallel-operation restriction (guc.c:3457).
    if crate::seam::is_in_parallel_mode::call()
        && change_val
        && action != GUC_ACTION_SAVE
        && (record.flags & GUC_ALLOW_IN_PARALLEL) == 0
    {
        return Ok(AccessCheck::Reject(err(
            ERRCODE_INVALID_TRANSACTION_STATE,
            format!("parameter \"{name}\" cannot be set during a parallel operation"),
        )));
    }

    // Context switch (guc.c:3471).
    match record.context {
        PGC_INTERNAL => {
            if context != PGC_INTERNAL {
                return Ok(AccessCheck::Reject(err(
                    ERRCODE_CANT_CHANGE_RUNTIME_PARAM,
                    format!("parameter \"{name}\" cannot be changed"),
                )));
            }
        }
        PGC_POSTMASTER => {
            if context == PGC_SIGHUP {
                // prohibitValueChange handled by the caller after canonicalizing.
            } else if context != PGC_POSTMASTER {
                return Ok(AccessCheck::Reject(err(
                    ERRCODE_CANT_CHANGE_RUNTIME_PARAM,
                    format!(
                        "parameter \"{name}\" cannot be changed without restarting the server"
                    ),
                )));
            }
        }
        PGC_SIGHUP => {
            if context != PGC_SIGHUP && context != PGC_POSTMASTER {
                return Ok(AccessCheck::Reject(err(
                    ERRCODE_CANT_CHANGE_RUNTIME_PARAM,
                    format!("parameter \"{name}\" cannot be changed now"),
                )));
            }
        }
        PGC_SU_BACKEND => {
            if context == PGC_BACKEND && !parameter_acl_set_ok(name, srole)? {
                return Ok(AccessCheck::Reject(err(
                    ERRCODE_INSUFFICIENT_PRIVILEGE,
                    format!("permission denied to set parameter \"{name}\""),
                )));
            }
            // FALLTHROUGH to PGC_BACKEND handling.
            if let Some(r) = backend_context_rules(name, context, source, change_val, is_reload) {
                return Ok(r);
            }
        }
        PGC_BACKEND => {
            if let Some(r) = backend_context_rules(name, context, source, change_val, is_reload) {
                return Ok(r);
            }
        }
        PGC_SUSET => {
            if (context == PGC_USERSET || context == PGC_BACKEND)
                && !parameter_acl_set_ok(name, srole)?
            {
                return Ok(AccessCheck::Reject(err(
                    ERRCODE_INSUFFICIENT_PRIVILEGE,
                    format!("permission denied to set parameter \"{name}\""),
                )));
            }
        }
        PGC_USERSET => {
            // always okay
        }
    }

    // GUC_NOT_WHILE_SEC_REST (guc.c:3629).
    if record.flags & types_guc::GUC_NOT_WHILE_SEC_REST != 0 {
        if crate::seam::in_local_user_id_change::call() {
            return Ok(AccessCheck::Reject(err(
                ERRCODE_INSUFFICIENT_PRIVILEGE,
                format!("cannot set parameter \"{name}\" within security-definer function"),
            )));
        }
        if crate::seam::in_security_restricted_operation::call() {
            return Ok(AccessCheck::Reject(err(
                ERRCODE_INSUFFICIENT_PRIVILEGE,
                format!("cannot set parameter \"{name}\" within security-restricted operation"),
            )));
        }
    }

    // GUC_NO_RESET (guc.c:3653).
    if record.flags & GUC_NO_RESET != 0 {
        if value_is_null {
            return Ok(AccessCheck::Reject(err(
                ERRCODE_FEATURE_NOT_SUPPORTED,
                format!("parameter \"{name}\" cannot be reset"),
            )));
        }
        if action == GUC_ACTION_SAVE {
            return Ok(AccessCheck::Reject(err(
                ERRCODE_FEATURE_NOT_SUPPORTED,
                format!("parameter \"{name}\" cannot be set locally in functions"),
            )));
        }
    }

    Ok(AccessCheck::Ok)
}

/// The shared PGC_BACKEND / PGC_SU_BACKEND tail (guc.c:3545). Returns `Some` if
/// it determines a final outcome, `None` to continue.
fn backend_context_rules(
    name: &str,
    context: GucContext,
    source: GucSource,
    change_val: bool,
    is_reload: bool,
) -> Option<AccessCheck> {
    if context == PGC_SIGHUP {
        // Ignore in existing backends unless reloading.
        if crate::seam::is_under_postmaster::call() && change_val && !is_reload {
            return Some(AccessCheck::Skip);
        }
        None
    } else if context != PGC_POSTMASTER
        && context != PGC_BACKEND
        && context != PGC_SU_BACKEND
        && source != PGC_S_CLIENT
    {
        Some(AccessCheck::Reject(err(
            ERRCODE_CANT_CHANGE_RUNTIME_PARAM,
            format!("parameter \"{name}\" cannot be set after connection start"),
        )))
    } else {
        None
    }
}

/// `parse_and_validate_value(record, value, source, elevel, &newval, &newextra)`
/// (guc.c:3128): built-in checks (Boolean/int/real parsing, range limits, enum
/// lookup, GUC_IS_NAME truncation) plus the variable's check hook. Returns the
/// converted `config_var_val` plus the hook's `extra` payload on success, or a
/// `PgError` (the C ereport at `elevel`). The check hooks are called through the
/// typed `guc_tables` slots stored in the record; failures use the GUC
/// check-error protocol (crate root facade).
pub fn parse_and_validate_value(
    record: &GucVariable,
    value: &str,
    source: GucSource,
) -> PgResult<(config_var_val, Option<GucHookExtra>)> {
    let gen = record.gen();
    let name = gen.name;

    match record {
        GucVariable::Bool(conf) => {
            let mut newval = match backend_utils_adt_scalar_seams::parse_bool::call(value) {
                Some(b) => b,
                None => {
                    return Err(err(
                        ERRCODE_INVALID_PARAMETER_VALUE,
                        format!("parameter \"{name}\" requires a Boolean value"),
                    ))
                }
            };
            let extra = call_bool_check_hook(conf, &mut newval, source)?;
            Ok((config_var_val::Boolval(newval), extra))
        }
        GucVariable::Int(conf) => {
            let mut newval = match parse_int(value, gen.flags) {
                ParseNum::Ok(v) => v,
                ParseNum::Err { hint } => {
                    return Err(invalid_value_error(name, value, hint));
                }
            };
            if newval < conf.min || newval > conf.max {
                return Err(range_error_int(gen.flags, name, newval, conf.min, conf.max));
            }
            let extra = call_int_check_hook(conf, &mut newval, source)?;
            Ok((config_var_val::Intval(newval), extra))
        }
        GucVariable::Real(conf) => {
            let mut newval = match parse_real(value, gen.flags) {
                ParseNum::Ok(v) => v,
                ParseNum::Err { hint } => {
                    return Err(invalid_value_error(name, value, hint));
                }
            };
            if newval < conf.min || newval > conf.max {
                return Err(range_error_real(gen.flags, name, newval, conf.min, conf.max));
            }
            let extra = call_real_check_hook(conf, &mut newval, source)?;
            Ok((config_var_val::Realval(newval), extra))
        }
        GucVariable::String(conf) => {
            // strdup + GUC_IS_NAME truncation.
            let mut newval = value.to_string();
            if gen.flags & GUC_IS_NAME != 0 {
                newval = truncate_name(&newval)?;
            }
            let mut opt = Some(newval);
            let extra = call_string_check_hook(conf, &mut opt, source)?;
            Ok((config_var_val::Stringval(opt), extra))
        }
        GucVariable::Enum(conf) => {
            let mut newval = match config_enum_lookup_by_name(conf, value) {
                Some(v) => v,
                None => {
                    let hint = config_enum_get_options(conf, "Available values: ", ".", ", ");
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                        .errmsg(format!("invalid value for parameter \"{name}\": \"{value}\""))
                        .errhint(hint)
                        .into_error());
                }
            };
            let extra = call_enum_check_hook(conf, &mut newval, source)?;
            Ok((config_var_val::Enumval(newval), extra))
        }
    }
}

/// `truncate_identifier(str, strlen(str), true)` via the scansup seam (clip a
/// `GUC_IS_NAME` value to NAMEDATALEN-1 on a char boundary, emitting a NOTICE).
fn truncate_name(s: &str) -> PgResult<String> {
    // The seam allocates the truncated bytes in the supplied context; use a
    // short-lived scratch context and copy the result into a process-lifetime
    // owned String (the value is stored into the GUC record).
    let ctx = mcx::MemoryContext::new("guc_truncate_identifier");
    let out = crate::seam::truncate_identifier::call(ctx.mcx(), s.as_bytes(), true)?;
    Ok(String::from_utf8_lossy(out.as_slice()).into_owned())
}

fn invalid_value_error(name: &str, value: &str, hint: Option<&str>) -> PgError {
    let mut b = ereport(ERROR)
        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
        .errmsg(format!("invalid value for parameter \"{name}\": \"{value}\""));
    if let Some(h) = hint {
        b = b.errhint(h.to_string());
    }
    b.into_error()
}

fn range_error_int(flags: i32, name: &str, val: i32, min: i32, max: i32) -> PgError {
    let (unit, sp) = unit_and_space(flags);
    err(
        ERRCODE_INVALID_PARAMETER_VALUE,
        format!(
            "{val}{sp}{unit} is outside the valid range for parameter \"{name}\" ({min}{sp}{unit} .. {max}{sp}{unit})"
        ),
    )
}

fn range_error_real(flags: i32, name: &str, val: f64, min: f64, max: f64) -> PgError {
    let (unit, sp) = unit_and_space(flags);
    err(
        ERRCODE_INVALID_PARAMETER_VALUE,
        format!(
            "{val}{sp}{unit} is outside the valid range for parameter \"{name}\" ({min}{sp}{unit} .. {max}{sp}{unit})"
        ),
    )
}

fn unit_and_space(flags: i32) -> (&'static str, &'static str) {
    match get_config_unit_name(flags) {
        Some(u) => (u, " "),
        None => ("", ""),
    }
}

// ---------------------------------------------------------------------------
// Check-hook callers (guc.c:6809..). The hooks live in the `guc_tables` typed
// slots the record references. On failure they communicate via the GUC
// check-error protocol (crate-root `GUC_check_err*`), which these wrappers
// translate into the C ereport. A hook that `ereport(ERROR)`s returns its
// `Err` directly.
// ---------------------------------------------------------------------------

fn check_hook_error(name: &str, fallback: String) -> PgError {
    crate::guc_check_error_to_pg_error_or(fallback)
        .unwrap_or_else(|| err(ERRCODE_INVALID_PARAMETER_VALUE, format!("invalid value for parameter \"{name}\"")))
}

fn call_bool_check_hook(
    conf: &config_bool,
    newval: &mut bool,
    source: GucSource,
) -> PgResult<Option<GucHookExtra>> {
    let Some(slot) = conf.check_hook else {
        return Ok(None);
    };
    crate::reset_guc_check_error();
    let mut extra: Option<GucHookExtra> = None;
    if (slot.get())(newval, &mut extra, source)? {
        Ok(extra)
    } else {
        let name = conf.gen.name;
        Err(check_hook_error(
            name,
            format!("invalid value for parameter \"{name}\": {}", *newval as i32),
        ))
    }
}

fn call_int_check_hook(
    conf: &config_int,
    newval: &mut i32,
    source: GucSource,
) -> PgResult<Option<GucHookExtra>> {
    let Some(slot) = conf.check_hook else {
        return Ok(None);
    };
    crate::reset_guc_check_error();
    let mut extra: Option<GucHookExtra> = None;
    if (slot.get())(newval, &mut extra, source)? {
        Ok(extra)
    } else {
        let name = conf.gen.name;
        Err(check_hook_error(
            name,
            format!("invalid value for parameter \"{name}\": {}", *newval),
        ))
    }
}

fn call_real_check_hook(
    conf: &config_real,
    newval: &mut f64,
    source: GucSource,
) -> PgResult<Option<GucHookExtra>> {
    let Some(slot) = conf.check_hook else {
        return Ok(None);
    };
    crate::reset_guc_check_error();
    let mut extra: Option<GucHookExtra> = None;
    if (slot.get())(newval, &mut extra, source)? {
        Ok(extra)
    } else {
        let name = conf.gen.name;
        Err(check_hook_error(
            name,
            format!("invalid value for parameter \"{name}\": {}", *newval),
        ))
    }
}

fn call_string_check_hook(
    conf: &config_string,
    newval: &mut Option<String>,
    source: GucSource,
) -> PgResult<Option<GucHookExtra>> {
    let Some(slot) = conf.check_hook else {
        return Ok(None);
    };
    crate::reset_guc_check_error();
    let mut extra: Option<GucHookExtra> = None;
    if (slot.get())(newval, &mut extra, source)? {
        Ok(extra)
    } else {
        let name = conf.gen.name;
        // C fallback message: `newval ? newval : ""`.
        let shown = newval.clone().unwrap_or_default();
        Err(check_hook_error(
            name,
            format!("invalid value for parameter \"{name}\": \"{shown}\""),
        ))
    }
}

fn call_enum_check_hook(
    conf: &config_enum,
    newval: &mut i32,
    source: GucSource,
) -> PgResult<Option<GucHookExtra>> {
    let Some(slot) = conf.check_hook else {
        return Ok(None);
    };
    crate::reset_guc_check_error();
    let mut extra: Option<GucHookExtra> = None;
    if (slot.get())(newval, &mut extra, source)? {
        Ok(extra)
    } else {
        let name = conf.gen.name;
        let valname = config_enum_lookup_by_value(conf, *newval).unwrap_or("?");
        Err(check_hook_error(
            name,
            format!("invalid value for parameter \"{name}\": \"{valname}\""),
        ))
    }
}

// ---------------------------------------------------------------------------
// SHOW rendering.
// ---------------------------------------------------------------------------

/// `ShowGUCOption(record, use_units)` (guc.c:5471): the textual value of a GUC
/// as `SHOW` / `current_setting()` would render it, including unit suffixing.
/// `show_hook`s (a per-variable callback installed via the slot) are honored
/// when present.
pub fn show_guc_option(record: &GucVariable, use_units: bool) -> String {
    match record {
        GucVariable::Bool(conf) => {
            if let Some(slot) = conf.show_hook {
                return (slot.get())();
            }
            let v = conf.value.unwrap_or(conf.reset_val);
            if v { "on".to_string() } else { "off".to_string() }
        }
        GucVariable::Int(conf) => {
            if let Some(slot) = conf.show_hook {
                return (slot.get())();
            }
            let mut result: i64 = conf.value.unwrap_or(conf.reset_val) as i64;
            let mut unit = "";
            if use_units && result > 0 && (conf.gen.flags & GUC_UNIT) != 0 {
                let (v, u) = convert_int_from_base_unit(result, conf.gen.flags & GUC_UNIT);
                result = v;
                unit = u;
            }
            format!("{result}{unit}")
        }
        GucVariable::Real(conf) => {
            if let Some(slot) = conf.show_hook {
                return (slot.get())();
            }
            let mut result: f64 = conf.value.unwrap_or(conf.reset_val);
            let mut unit = "";
            if use_units && result > 0.0 && (conf.gen.flags & GUC_UNIT) != 0 {
                let (v, u) = convert_real_from_base_unit(result, conf.gen.flags & GUC_UNIT);
                result = v;
                unit = u;
            }
            format!("{}{unit}", fmt_g(result))
        }
        GucVariable::String(conf) => {
            if let Some(slot) = conf.show_hook {
                return (slot.get())();
            }
            match conf.value.as_ref() {
                Some(Some(s)) if !s.is_empty() => s.clone(),
                _ => String::new(),
            }
        }
        GucVariable::Enum(conf) => {
            if let Some(slot) = conf.show_hook {
                return (slot.get())();
            }
            let v = conf.value.unwrap_or(conf.reset_val);
            config_enum_lookup_by_value(conf, v).unwrap_or("?").to_string()
        }
    }
}

/// Render a double the way C `snprintf("%g")` does.
fn fmt_g(v: f64) -> String {
    if v == v.trunc() && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        let s = format!("{v:.6}");
        let trimmed = s.trim_end_matches('0').trim_end_matches('.');
        trimmed.to_string()
    }
}

/// `GetConfigOptionResetString`'s per-record core (guc.c): render a variable's
/// RESET value as a string. `None` when there is no renderable reset value.
pub fn reset_value_string(record: &GucVariable) -> Option<String> {
    Some(match record {
        GucVariable::Bool(c) => {
            if c.reset_val { "on".to_string() } else { "off".to_string() }
        }
        GucVariable::Int(c) => format!("{}", c.reset_val),
        GucVariable::Real(c) => fmt_g(c.reset_val),
        GucVariable::String(c) => c.reset_val.clone()?,
        GucVariable::Enum(c) => config_enum_lookup_by_value(c, c.reset_val)?.to_string(),
    })
}

/// `GetConfigOptionByName(name, &varname, missing_ok)` (guc.c:5438), reduced to
/// the value lookup: find the variable and render its current value with units.
pub fn get_config_option_by_name(
    reg: &GucRegistry,
    name: &str,
    missing_ok: bool,
) -> PgResult<Option<String>> {
    match reg.find_option(name) {
        Some(record) => Ok(Some(show_guc_option(record, true))),
        None => {
            if missing_ok {
                Ok(None)
            } else {
                Err(err(
                    ERRCODE_UNDEFINED_OBJECT,
                    format!("unrecognized configuration parameter \"{name}\""),
                ))
            }
        }
    }
}

/// `set_config_option`/`set_config_with_handle` core (guc.c:3341/3405).
///
/// Returns `Ok(1)` applied, `Ok(0)` rejected-below-ERROR, `Ok(-1)` skipped, and
/// `Err` when the resolved elevel is ERROR and the set is rejected (or a hook
/// `ereport(ERROR)`s).
#[allow(clippy::too_many_arguments)]
pub fn set_config_option(
    reg: &mut GucRegistry,
    name: &str,
    value: Option<&str>,
    context: GucContext,
    source: GucSource,
    srole: Oid,
    action: GucAction,
    change_val: bool,
    elevel: ErrorLevel,
    is_reload: bool,
) -> PgResult<i32> {
    let elevel = resolve_elevel(elevel, source);

    let Some(idx) = reg.find_index_pub(name) else {
        let e = err(
            ERRCODE_UNDEFINED_OBJECT,
            format!("unrecognized configuration parameter \"{name}\""),
        );
        return reject(elevel, e);
    };

    // Access-permission rules (immutable borrow first).
    let access = {
        let record = &reg.vars[idx];
        check_can_set(
            record.gen(),
            value.is_none(),
            context,
            source,
            srole,
            action,
            change_val,
            is_reload,
        )?
    };
    match access {
        AccessCheck::Ok => {}
        AccessCheck::Skip => return Ok(-1),
        AccessCheck::Reject(e) => return reject(elevel, e),
    }

    // makeDefault (guc.c:3673).
    let make_default =
        change_val && (source <= PGC_S_OVERRIDE) && (value.is_some() || source == PGC_S_DEFAULT);

    // Ignore attempted set if overridden by previously processed setting
    // (guc.c:3682).
    let mut change_val = change_val;
    if reg.vars[idx].gen().source > source {
        if change_val && !make_default {
            return Ok(-1);
        }
        change_val = false;
    }

    // Evaluate value.
    let mut source = source;
    let mut context = context;
    let mut srole = srole;
    let (newval, newextra) = match value {
        Some(v) => {
            let record = &reg.vars[idx];
            match parse_and_validate_value(record, v, source) {
                Ok(nv) => nv,
                Err(e) => return reject(elevel, e),
            }
        }
        None if source == PGC_S_DEFAULT => {
            let record = &reg.vars[idx];
            match boot_default_value(record, source) {
                Ok(nv) => nv,
                Err(e) => return reject(elevel, e),
            }
        }
        None => {
            // RESET: newval = conf->reset_val (no re-validation), adopting the
            // reset provenance. The reset_extra is not re-derived (C reuses
            // reset_extra; here a None hook-extra is supplied to the assign).
            let record = &reg.vars[idx];
            let gen = record.gen();
            source = gen.reset_source;
            context = gen.reset_scontext;
            srole = gen.reset_srole;
            (reset_value(record), None)
        }
    };

    if change_val {
        let record = &mut reg.vars[idx];
        apply_value(record, newval.clone(), newextra, source, context, srole);
    }
    if make_default {
        let record = &mut reg.vars[idx];
        make_default_bookkeeping(record, &newval, source, context, srole);
    }

    Ok(if change_val { 1 } else { -1 })
}

/// The NULL-value, `source == PGC_S_DEFAULT` arm: `newval = conf->boot_val` plus
/// the type's check hook.
fn boot_default_value(
    record: &GucVariable,
    source: GucSource,
) -> PgResult<(config_var_val, Option<GucHookExtra>)> {
    match record {
        GucVariable::Bool(c) => {
            let mut v = c.boot_val;
            let extra = call_bool_check_hook(c, &mut v, source)?;
            Ok((config_var_val::Boolval(v), extra))
        }
        GucVariable::Int(c) => {
            let mut v = c.boot_val;
            let extra = call_int_check_hook(c, &mut v, source)?;
            Ok((config_var_val::Intval(v), extra))
        }
        GucVariable::Real(c) => {
            let mut v = c.boot_val;
            let extra = call_real_check_hook(c, &mut v, source)?;
            Ok((config_var_val::Realval(v), extra))
        }
        GucVariable::String(c) => {
            let mut v = c.boot_val.clone();
            let extra = call_string_check_hook(c, &mut v, source)?;
            Ok((config_var_val::Stringval(v), extra))
        }
        GucVariable::Enum(c) => {
            let mut v = c.boot_val;
            let extra = call_enum_check_hook(c, &mut v, source)?;
            Ok((config_var_val::Enumval(v), extra))
        }
    }
}

/// The NULL-value RESET arm: `newval = conf->reset_val`.
fn reset_value(record: &GucVariable) -> config_var_val {
    match record {
        GucVariable::Bool(c) => config_var_val::Boolval(c.reset_val),
        GucVariable::Int(c) => config_var_val::Intval(c.reset_val),
        GucVariable::Real(c) => config_var_val::Realval(c.reset_val),
        GucVariable::String(c) => config_var_val::Stringval(c.reset_val.clone()),
        GucVariable::Enum(c) => config_var_val::Enumval(c.reset_val),
    }
}

/// Apply an evaluated value to a variable's live storage and update its
/// source/scontext/srole (the `if (changeVal)` body of each per-type case),
/// then run the variable's assign hook and push the value to the owner's
/// storage slot (`*conf->variable = newval`).
fn apply_value(
    record: &mut GucVariable,
    newval: config_var_val,
    extra: Option<GucHookExtra>,
    source: GucSource,
    context: GucContext,
    srole: Oid,
) {
    match (&mut *record, newval) {
        (GucVariable::Bool(c), config_var_val::Boolval(b)) => {
            c.value = Some(b);
            if let Some(slot) = c.assign_hook {
                (slot.get())(b, extra.as_ref());
            }
            if c.variable.installed() {
                c.variable.write(b);
            }
        }
        (GucVariable::Int(c), config_var_val::Intval(v)) => {
            c.value = Some(v);
            if let Some(slot) = c.assign_hook {
                (slot.get())(v, extra.as_ref());
            }
            if c.variable.installed() {
                c.variable.write(v);
            }
        }
        (GucVariable::Real(c), config_var_val::Realval(v)) => {
            c.value = Some(v);
            if let Some(slot) = c.assign_hook {
                (slot.get())(v, extra.as_ref());
            }
            if c.variable.installed() {
                c.variable.write(v);
            }
        }
        (GucVariable::String(c), config_var_val::Stringval(s)) => {
            if let Some(slot) = c.assign_hook {
                (slot.get())(s.as_deref(), extra.as_ref());
            }
            if c.variable.installed() {
                c.variable.write(s.clone());
            }
            c.value = Some(s);
        }
        (GucVariable::Enum(c), config_var_val::Enumval(v)) => {
            c.value = Some(v);
            if let Some(slot) = c.assign_hook {
                (slot.get())(v, extra.as_ref());
            }
            if c.variable.installed() {
                c.variable.write(v);
            }
        }
        (_record, _) => {}
    }

    let gen = record.gen_mut();
    gen.source = source;
    gen.scontext = context;
    gen.srole = srole;
}

/// The `if (makeDefault)` body of each per-type case (guc.c:3768 for bool;
/// identical shape for the others).
fn make_default_bookkeeping(
    record: &mut GucVariable,
    newval: &config_var_val,
    source: GucSource,
    context: GucContext,
    srole: Oid,
) {
    if record.gen().reset_source <= source {
        match (&mut *record, newval) {
            (GucVariable::Bool(c), config_var_val::Boolval(v)) => c.reset_val = *v,
            (GucVariable::Int(c), config_var_val::Intval(v)) => c.reset_val = *v,
            (GucVariable::Real(c), config_var_val::Realval(v)) => c.reset_val = *v,
            (GucVariable::String(c), config_var_val::Stringval(v)) => c.reset_val = v.clone(),
            (GucVariable::Enum(c), config_var_val::Enumval(v)) => c.reset_val = *v,
            (_record, _) => return,
        }
        let gen = record.gen_mut();
        gen.reset_source = source;
        gen.reset_scontext = context;
        gen.reset_srole = srole;
    }
    // for (stack = conf->gen.stack; stack; stack = stack->prev) ...
    let mut stack = record.gen_mut().stack.as_deref_mut();
    while let Some(s) = stack {
        if s.source <= source {
            s.prior.val = Some(newval.clone());
            s.source = source;
            s.scontext = context;
            s.srole = srole;
        }
        stack = s.prev.as_deref_mut();
    }
}

/// Turn a rejection into the C return convention: throw if elevel >= ERROR, else
/// return 0.
fn reject(elevel: ErrorLevel, e: PgError) -> PgResult<i32> {
    if elevel >= ERROR {
        Err(e)
    } else {
        // Below ERROR the C ereport(elevel) is emitted (if interesting), and the
        // set returns 0.
        if message_level_is_interesting(elevel) {
            backend_utils_error::emit_error_report_for(&e);
        }
        Ok(0)
    }
}

/// `ResetAllOptions()` (guc.c:2003), reduced to the core effect: reset every
/// USERSET/SUSET variable (that lacks GUC_NO_RESET_ALL) to its reset value and
/// reset_source. The stack/parallel/report bookkeeping is part of the stack
/// subsystem (deferred).
pub fn reset_all_options(reg: &mut GucRegistry) {
    use types_guc::GUC_NO_RESET_ALL;
    for var in reg.iter_mut() {
        let flags = var.gen().flags;
        let ctx = var.gen().context;
        if flags & GUC_NO_RESET_ALL != 0 {
            continue;
        }
        if ctx != PGC_SUSET && ctx != PGC_USERSET {
            continue;
        }
        reset_one(var);
    }
}

fn reset_one(var: &mut GucVariable) {
    match var {
        GucVariable::Bool(c) => {
            c.value = Some(c.reset_val);
            if let Some(slot) = c.assign_hook {
                (slot.get())(c.reset_val, None);
            }
            if c.variable.installed() {
                c.variable.write(c.reset_val);
            }
        }
        GucVariable::Int(c) => {
            c.value = Some(c.reset_val);
            if let Some(slot) = c.assign_hook {
                (slot.get())(c.reset_val, None);
            }
            if c.variable.installed() {
                c.variable.write(c.reset_val);
            }
        }
        GucVariable::Real(c) => {
            c.value = Some(c.reset_val);
            if let Some(slot) = c.assign_hook {
                (slot.get())(c.reset_val, None);
            }
            if c.variable.installed() {
                c.variable.write(c.reset_val);
            }
        }
        GucVariable::String(c) => {
            if let Some(slot) = c.assign_hook {
                (slot.get())(c.reset_val.as_deref(), None);
            }
            if c.variable.installed() {
                c.variable.write(c.reset_val.clone());
            }
            c.value = Some(c.reset_val.clone());
        }
        GucVariable::Enum(c) => {
            c.value = Some(c.reset_val);
            if let Some(slot) = c.assign_hook {
                (slot.get())(c.reset_val, None);
            }
            if c.variable.installed() {
                c.variable.write(c.reset_val);
            }
        }
    }
    let reset_source = var.gen().reset_source;
    let reset_scontext = var.gen().reset_scontext;
    let reset_srole = var.gen().reset_srole;
    let gen = var.gen_mut();
    gen.source = reset_source;
    gen.scontext = reset_scontext;
    gen.srole = reset_srole;
}
