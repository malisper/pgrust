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

use ::utils_error::{ereport, message_level_is_interesting};
use ::types_error::{
    ErrorLevel, PgError, PgResult, SqlState, DEBUG3, ERRCODE_CANT_CHANGE_RUNTIME_PARAM,
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_INVALID_TRANSACTION_STATE, ERRCODE_UNDEFINED_OBJECT, ERROR, LOG, WARNING,
};
use ::types_acl::{AclResult, ACL_SET};
use ::types_core::Oid;
use ::types_guc::{
    GucContext, GucSource, GUC_ALLOW_IN_PARALLEL, GUC_IS_NAME, GUC_NO_RESET, GUC_UNIT, PGC_BACKEND,
    PGC_INTERNAL, PGC_POSTMASTER, PGC_SIGHUP, PGC_SUSET, PGC_SU_BACKEND, PGC_S_CLIENT,
    PGC_S_DATABASE, PGC_S_DATABASE_USER, PGC_S_DEFAULT, PGC_S_DYNAMIC_DEFAULT, PGC_S_FILE,
    PGC_S_GLOBAL, PGC_S_OVERRIDE, PGC_S_SESSION, PGC_S_USER, PGC_USERSET,
};

use ::guc_tables::GucHookExtra;

use crate::enum_lookup::{
    config_enum_get_options, config_enum_lookup_by_name, config_enum_lookup_by_value,
};
use crate::model::{
    config_bool, config_enum, config_generic, config_int, config_real, config_string,
    config_var_val, config_var_value, GucStack, SharedExtra, GUC_NEEDS_REPORT, GUC_PENDING_RESTART,
    GUC_LOCAL, GUC_SAVE, GUC_SET, GUC_SET_LOCAL,
};
use ::types_guc::GUC_REPORT;
use crate::name::{guc_name_eq, MAP_OLD_GUC_NAMES};
use crate::units::{
    convert_int_from_base_unit, convert_real_from_base_unit, get_config_unit_name, parse_int,
    parse_real, ParseNum,
};
use crate::{GUC_ACTION_LOCAL, GUC_ACTION_SAVE, GUC_ACTION_SET};

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

    /// `add_placeholder_variable(name, elevel)` (guc.c:1177): create and register
    /// a `config_string` placeholder for a custom variable name. C allocates the
    /// `char *` at the end of the struct because the placeholder has no static
    /// storage; here the placeholder's current/boot/reset values live in the
    /// record's own `value`/`boot_val`/`reset_val` fields and the `variable` slot
    /// is the never-installed [`GucPlaceholderVariable`], so all access stays in
    /// the record (mirroring C's self-contained storage). Returns the index of
    /// the newly added variable. Fallible: a spine growth surfaces the C
    /// `guc_malloc` failure as the project's OOM `PgError`.
    pub fn add_placeholder_variable(&mut self, name: &str) -> PgResult<usize> {
        use crate::model::{config_generic, config_string};
        use ::types_guc::{config_group, config_type, GucContext};

        // C sets gen->context = PGC_USERSET, group = CUSTOM_OPTIONS,
        // short_desc = "GUC placeholder variable",
        // flags = GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_CUSTOM_PLACEHOLDER,
        // vartype = PGC_STRING. The current/boot/reset values "start out NULL".
        // The placeholder's name must outlive the record; the registry holds
        // process-lifetime entries, so leak the owned copy into a `'static` str
        // (the C `guc_strdup` into TopMemoryContext analog — never freed).
        let gen = config_generic::boot(
            Box::leak(name.to_string().into_boxed_str()),
            GucContext::PGC_USERSET,
            config_group::CUSTOM_OPTIONS,
            Some("GUC placeholder variable"),
            None,
            ::types_guc::GUC_NO_SHOW_ALL
                | ::types_guc::GUC_NOT_IN_SAMPLE
                | ::types_guc::GUC_CUSTOM_PLACEHOLDER,
            config_type::PGC_STRING,
        );
        let var = GucVariable::String(config_string {
            gen,
            variable: &::guc_tables::vars::GucPlaceholderVariable,
            value: None,
            boot_val: None,
            check_hook: None,
            assign_hook: None,
            show_hook: None,
            reset_val: None,
            reset_extra: None,
        });
        self.vars.try_reserve(1).map_err(crate::alloc_err)?;
        self.vars.push(var);
        Ok(self.vars.len() - 1)
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

    /// `hash_search(guc_hashtab, &name, HASH_REMOVE, NULL)` (guc.c): remove a
    /// variable by exact name. Used by `MarkGUCPrefixReserved` to drop invalid
    /// placeholders under a newly-reserved class prefix. No-op if absent.
    pub fn remove_by_name(&mut self, name: &str) {
        if let Some(idx) = self.vars.iter().position(|v| v.name() == name) {
            self.vars.remove(idx);
        }
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
    if record.flags & ::types_guc::GUC_NOT_WHILE_SEC_REST != 0 {
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
) -> PgResult<(config_var_val, Option<SharedExtra>)> {
    let gen = record.gen();
    let name = gen.name;

    match record {
        GucVariable::Bool(conf) => {
            let mut newval = match scalar_seams::parse_bool::call(value) {
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
    let _ = name;
    // When the hook supplied no message and left the default errcode,
    // `guc_check_error_to_pg_error_or` returns None; C then falls back to the
    // value-specific `invalid value for parameter "name": "value"` message,
    // which the caller already built into `fallback`.
    let fallback2 = fallback.clone();
    crate::guc_check_error_to_pg_error_or(fallback)
        .unwrap_or_else(|| err(ERRCODE_INVALID_PARAMETER_VALUE, fallback2))
}

fn call_bool_check_hook(
    conf: &config_bool,
    newval: &mut bool,
    source: GucSource,
) -> PgResult<Option<SharedExtra>> {
    let Some(slot) = conf.check_hook else {
        return Ok(None);
    };
    // An uninstalled check-hook slot means the owning unit has not been ported
    // yet (the GUC table names the C `check_*` symbol, but no Rust body is
    // wired). C always has the check hook linked in; here a missing body must
    // behave like the C `check_hook == NULL` case — no extra validation. The
    // compile-time PG boot defaults are valid by construction, so skipping the
    // unported hook is sound (and mirrors the assign-hook `installed()` guard in
    // initialize_one_guc_option_hooks).
    if !slot.installed() {
        return Ok(None);
    }
    crate::reset_guc_check_error();
    let mut extra: Option<GucHookExtra> = None;
    if (slot.get())(newval, &mut extra, source)? {
        Ok(extra.map(SharedExtra::new))
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
) -> PgResult<Option<SharedExtra>> {
    let Some(slot) = conf.check_hook else {
        return Ok(None);
    };
    // An uninstalled check-hook slot means the owning unit has not been ported
    // yet (the GUC table names the C `check_*` symbol, but no Rust body is
    // wired). C always has the check hook linked in; here a missing body must
    // behave like the C `check_hook == NULL` case — no extra validation. The
    // compile-time PG boot defaults are valid by construction, so skipping the
    // unported hook is sound (and mirrors the assign-hook `installed()` guard in
    // initialize_one_guc_option_hooks).
    if !slot.installed() {
        return Ok(None);
    }
    crate::reset_guc_check_error();
    let mut extra: Option<GucHookExtra> = None;
    if (slot.get())(newval, &mut extra, source)? {
        Ok(extra.map(SharedExtra::new))
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
) -> PgResult<Option<SharedExtra>> {
    let Some(slot) = conf.check_hook else {
        return Ok(None);
    };
    // An uninstalled check-hook slot means the owning unit has not been ported
    // yet (the GUC table names the C `check_*` symbol, but no Rust body is
    // wired). C always has the check hook linked in; here a missing body must
    // behave like the C `check_hook == NULL` case — no extra validation. The
    // compile-time PG boot defaults are valid by construction, so skipping the
    // unported hook is sound (and mirrors the assign-hook `installed()` guard in
    // initialize_one_guc_option_hooks).
    if !slot.installed() {
        return Ok(None);
    }
    crate::reset_guc_check_error();
    let mut extra: Option<GucHookExtra> = None;
    if (slot.get())(newval, &mut extra, source)? {
        Ok(extra.map(SharedExtra::new))
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
) -> PgResult<Option<SharedExtra>> {
    let Some(slot) = conf.check_hook else {
        return Ok(None);
    };
    // An uninstalled check-hook slot means the owning unit has not been ported
    // yet (the GUC table names the C `check_*` symbol, but no Rust body is
    // wired). C always has the check hook linked in; here a missing body must
    // behave like the C `check_hook == NULL` case — no extra validation. The
    // compile-time PG boot defaults are valid by construction, so skipping the
    // unported hook is sound (and mirrors the assign-hook `installed()` guard in
    // initialize_one_guc_option_hooks).
    if !slot.installed() {
        return Ok(None);
    }
    crate::reset_guc_check_error();
    let mut extra: Option<GucHookExtra> = None;
    if (slot.get())(newval, &mut extra, source)? {
        Ok(extra.map(SharedExtra::new))
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
) -> PgResult<Option<SharedExtra>> {
    let Some(slot) = conf.check_hook else {
        return Ok(None);
    };
    // An uninstalled check-hook slot means the owning unit has not been ported
    // yet (the GUC table names the C `check_*` symbol, but no Rust body is
    // wired). C always has the check hook linked in; here a missing body must
    // behave like the C `check_hook == NULL` case — no extra validation. The
    // compile-time PG boot defaults are valid by construction, so skipping the
    // unported hook is sound (and mirrors the assign-hook `installed()` guard in
    // initialize_one_guc_option_hooks).
    if !slot.installed() {
        return Ok(None);
    }
    crate::reset_guc_check_error();
    let mut extra: Option<GucHookExtra> = None;
    if (slot.get())(newval, &mut extra, source)? {
        Ok(extra.map(SharedExtra::new))
    } else {
        let name = conf.gen.name;
        let valname = config_enum_lookup_by_value(conf, *newval).unwrap_or("?");
        Err(check_hook_error(
            name,
            format!("invalid value for parameter \"{name}\": \"{valname}\""),
        ))
    }
}

/// `InitializeOneGUCOption`'s hook-firing step (guc.c:1644), for the boot value.
///
/// C, for every variable, sets `newval = boot_val`, then:
///
/// ```c
/// if (!call_<type>_check_hook(conf, &newval, &extra, PGC_S_DEFAULT, LOG))
///     elog(FATAL, "failed to initialize %s ...");
/// if (conf->assign_hook)
///     conf->assign_hook(newval, extra);
/// ```
///
/// i.e. the boot `extra` is **not** `NULL` — it is exactly the payload the
/// variable's own check hook produces from the boot value. (The earlier port
/// fired the assign hook with `extra = None`, which is why every extra-consuming
/// assign hook — `assign_datestyle`, `assign_timezone`, `assign_log_timezone`,
/// `assign_client_encoding`, `assign_role`, `assign_random_seed`,
/// `assign_log_destination` — panicked at boot trying to downcast a missing
/// payload.)
///
/// Returns the check hook's `extra` so the caller can stash it as the variable's
/// `reset_extra` (C's `conf->gen.extra = conf->reset_extra = extra`). On a check
/// hook failure C does `elog(FATAL)`; here that surfaces as the `PgError` the
/// caller turns into a fatal boot error.
pub fn initialize_one_guc_option_hooks(var: &mut GucVariable) -> PgResult<Option<SharedExtra>> {
    // C: `conf->gen.extra = conf->reset_extra = extra` (guc.c:1674). The boot
    // extra is the payload the variable's own check hook produced from the boot
    // value; stashing it in both `gen.extra` and `reset_extra` is what lets a
    // later GUC-stack restore or RESET re-run the assign hook with the matching
    // extra instead of `None`.
    match var {
        GucVariable::Bool(conf) => {
            let mut newval = conf.boot_val;
            let extra = call_bool_check_hook(conf, &mut newval, PGC_S_DEFAULT)?;
            // `*conf->variable = conf->reset_val = boot_val` (guc.c): seed the
            // owner-installed storage with the boot value, so a variable that
            // has no assign hook still has its bound global initialized.
            if conf.variable.installed() {
                conf.variable.write(newval);
            }
            if let Some(slot) = conf.assign_hook {
                if slot.installed() {
                    (slot.get())(newval, extra.as_deref());
                }
            }
            conf.gen.extra = extra.clone();
            conf.reset_extra = extra.clone();
            Ok(extra)
        }
        GucVariable::Int(conf) => {
            let mut newval = conf.boot_val;
            let extra = call_int_check_hook(conf, &mut newval, PGC_S_DEFAULT)?;
            if conf.variable.installed() {
                conf.variable.write(newval);
            }
            if let Some(slot) = conf.assign_hook {
                if slot.installed() {
                    (slot.get())(newval, extra.as_deref());
                }
            }
            conf.gen.extra = extra.clone();
            conf.reset_extra = extra.clone();
            Ok(extra)
        }
        GucVariable::Real(conf) => {
            let mut newval = conf.boot_val;
            let extra = call_real_check_hook(conf, &mut newval, PGC_S_DEFAULT)?;
            if conf.variable.installed() {
                conf.variable.write(newval);
            }
            if let Some(slot) = conf.assign_hook {
                if slot.installed() {
                    (slot.get())(newval, extra.as_deref());
                }
            }
            conf.gen.extra = extra.clone();
            conf.reset_extra = extra.clone();
            Ok(extra)
        }
        GucVariable::String(conf) => {
            // `newval = guc_strdup(boot_val)` (NULL boot_val stays NULL).
            let mut newval = conf.boot_val.clone();
            let extra = call_string_check_hook(conf, &mut newval, PGC_S_DEFAULT)?;
            if conf.variable.installed() {
                conf.variable.write(newval.clone());
            }
            if let Some(slot) = conf.assign_hook {
                if slot.installed() {
                    (slot.get())(newval.as_deref(), extra.as_deref());
                }
            }
            conf.gen.extra = extra.clone();
            conf.reset_extra = extra.clone();
            Ok(extra)
        }
        GucVariable::Enum(conf) => {
            let mut newval = conf.boot_val;
            let extra = call_enum_check_hook(conf, &mut newval, PGC_S_DEFAULT)?;
            if conf.variable.installed() {
                conf.variable.write(newval);
            }
            if let Some(slot) = conf.assign_hook {
                if slot.installed() {
                    (slot.get())(newval, extra.as_deref());
                }
            }
            conf.gen.extra = extra.clone();
            conf.reset_extra = extra.clone();
            Ok(extra)
        }
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
            // C `ShowGUCOption` reads `*conf->variable` live; when an owner
            // installed the storage accessor, read through it so a direct write
            // to the bound variable (e.g. xact.c's `XactReadOnly =
            // s->prevXactReadOnly` at subxact end, which bypasses the GUC
            // machinery) is reflected — without this SHOW returned the stale
            // cached `conf.value`.
            let v = if conf.variable.installed() {
                conf.variable.read()
            } else {
                conf.value.unwrap_or(conf.reset_val)
            };
            if v { "on".to_string() } else { "off".to_string() }
        }
        GucVariable::Int(conf) => {
            if let Some(slot) = conf.show_hook {
                return (slot.get())();
            }
            let mut result: i64 = if conf.variable.installed() {
                conf.variable.read() as i64
            } else {
                conf.value.unwrap_or(conf.reset_val) as i64
            };
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
            let mut result: f64 = if conf.variable.installed() {
                conf.variable.read()
            } else {
                conf.value.unwrap_or(conf.reset_val)
            };
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
            let v = if conf.variable.installed() {
                conf.variable.read()
            } else {
                conf.value.clone().flatten()
            };
            match v {
                Some(s) if !s.is_empty() => s,
                _ => String::new(),
            }
        }
        GucVariable::Enum(conf) => {
            if let Some(slot) = conf.show_hook {
                return (slot.get())();
            }
            let v = if conf.variable.installed() {
                conf.variable.read()
            } else {
                conf.value.unwrap_or(conf.reset_val)
            };
            config_enum_lookup_by_value(conf, v).unwrap_or("?").to_string()
        }
    }
}

/// Render a double the way C `snprintf(buf, "%g", v)` does (the renderer
/// `ShowGUCOption` uses for `PGC_REAL`). C `%g` uses precision `P = 6`
/// significant digits by default: it formats with `%e` style when the decimal
/// exponent `X` satisfies `X < -4` or `X >= P`, otherwise `%f` style, and then
/// strips trailing zeros and a trailing decimal point. See
/// [`crate::units::fmt_g`] for the shared implementation.
fn fmt_g(v: f64) -> String {
    crate::units::fmt_g(v)
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

/// `GetConfigOptionFlags(name, missing_ok)` (guc.c:4452): the variable's
/// `config_generic.flags` bitmask (the accessor pg_settings-style callers use).
/// Returns `Ok(0)` for a missing variable when `missing_ok`, else an
/// `unrecognized configuration parameter` error.
pub fn get_config_option_flags(
    reg: &GucRegistry,
    name: &str,
    missing_ok: bool,
) -> PgResult<i32> {
    match reg.find_option(name) {
        Some(record) => Ok(record.gen().flags),
        None => {
            if missing_ok {
                Ok(0)
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
    deferred_hooks: &mut Vec<DeferredAssignHook>,
) -> PgResult<i32> {
    let elevel = resolve_elevel(elevel, source);

    // Save the original context/source/srole before the reset-source rewrite
    // (guc.c:3654) overwrites `context`/`source`/`srole` below. The
    // session_authorization -> role kluge (guc.c:4116) must use these originals.
    let orig_context = context;
    let orig_source = source;
    let orig_srole = srole;

    // C: `record = find_option(name, true, false, elevel)` (guc.c:3439). The
    // `create_placeholders=true` path: an unknown name that is a syntactically
    // valid custom (`class.subname`) name gets a placeholder created on the spot;
    // any other unknown name is the `unrecognized configuration parameter` error.
    // `assignable_custom_variable_name(name, skip_errors=false)` reproduces the
    // exact error surface (invalid-name vs unrecognized) when no placeholder may
    // be created.
    let idx = match reg.find_index_pub(name) {
        Some(idx) => idx,
        None => match crate::assignable_custom_variable_name(name, false) {
            Ok(true) => match reg.add_placeholder_variable(name) {
                Ok(idx) => idx,
                Err(e) => return reject(elevel, e),
            },
            // skip_errors=false never returns Ok(false): it errors instead.
            Ok(false) => return Ok(0),
            Err(e) => return reject(elevel, e),
        },
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
            // reset provenance. C reuses the stored `reset_extra` pointer; the
            // model's `GucHookExtra` is a non-Clone `Box<dyn Any>` with no
            // identity, so we re-derive an equivalent extra by running the check
            // hook on the reset value (the check hooks are pure functions of the
            // value — e.g. `check_datestyle`/`check_timezone` recompute the same
            // `int myextra[2]`). This is what lets `RESET datestyle` fire its
            // assign hook, which requires a non-NULL extra.
            let record = &reg.vars[idx];
            let gen = record.gen();
            source = gen.reset_source;
            context = gen.reset_scontext;
            srole = gen.reset_srole;
            match reset_value_and_extra(record, source) {
                Ok(nv) => nv,
                Err(e) => return reject(elevel, e),
            }
        }
    };

    // prohibitValueChange (guc.c:3490-3495,3734-3751): re-reading a
    // PGC_POSTMASTER variable from postgresql.conf under PGC_SIGHUP. The value
    // can't be changed; compare the canonicalized newval against the live one.
    let prohibit_value_change =
        reg.vars[idx].gen().context == PGC_POSTMASTER && context == PGC_SIGHUP;
    if prohibit_value_change {
        // (newextra is dropped here, mirroring the C guc_free of non-reset
        // extra; Rust drops `newextra` at end of scope.)
        let differs = current_value_differs(&reg.vars[idx], &newval);
        if differs {
            reg.vars[idx].gen_mut().status |= GUC_PENDING_RESTART;
            let e = err(
                ERRCODE_CANT_CHANGE_RUNTIME_PARAM,
                format!(
                    "parameter \"{name}\" cannot be changed without restarting the server"
                ),
            );
            return reject(elevel, e);
        }
        reg.vars[idx].gen_mut().status &= !GUC_PENDING_RESTART;
        return Ok(-1);
    }

    // C threads the *same* `newextra` pointer through both the value path
    // (`conf->gen.extra`) and the makeDefault path (`conf->reset_extra` and each
    // matching `stack->prior.extra`). With `SharedExtra` = `Arc`, clone the
    // pointer for the makeDefault block before the value path consumes it.
    let make_default_extra = if make_default { newextra.clone() } else { None };
    if change_val {
        let record = &mut reg.vars[idx];
        // Save old value to support transaction abort (guc.c:3754). Skipped when
        // makeDefault, exactly as C.
        if !make_default {
            push_old_value(record, action);
        }
        if let Some(hook) = apply_value(record, newval.clone(), newextra, source, context, srole) {
            // Deferred: fired by the caller after the store borrow is released,
            // so a recursively re-entrant SetConfigOption (e.g. the
            // session_authorization -> is_superuser chain) does not re-lock the
            // store / alias the live `&mut reg`.
            deferred_hooks.push(hook);
        }

        // Ugly hack (guc.c:4116): during SET session_authorization, forcibly do
        // SET ROLE NONE with the same context/source/etc so the effects have
        // identical lifespan (required by the SQL spec; the variable's check/
        // assign hooks lack the info to do it). For RESET session_authorization
        // (value == None) pass NULL to "role" so it does RESET role rather than
        // SET ROLE NONE. Skipped when is_reload. The actual role change must run
        // outside the live store borrow, so it is deferred like the assign hook.
        if !is_reload && name == "session_authorization" {
            let role_value: Option<&'static str> = if value.is_some() { Some("none") } else { None };
            let role_source = if orig_source == PGC_S_OVERRIDE {
                PGC_S_DYNAMIC_DEFAULT
            } else {
                orig_source
            };
            deferred_hooks.push(Box::new(move || {
                let _ = crate::live::set_config_option_global(
                    "role",
                    role_value,
                    orig_context,
                    role_source,
                    orig_srole,
                    action,
                    true,
                    elevel,
                    false,
                );
            }));
        }
    }
    if make_default {
        let record = &mut reg.vars[idx];
        make_default_bookkeeping(record, &newval, make_default_extra, source, context, srole);
    }

    Ok(if change_val { 1 } else { -1 })
}

/// The NULL-value, `source == PGC_S_DEFAULT` arm: `newval = conf->boot_val` plus
/// the type's check hook.
fn boot_default_value(
    record: &GucVariable,
    source: GucSource,
) -> PgResult<(config_var_val, Option<SharedExtra>)> {
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

/// The NULL-value RESET arm with a re-derived `extra`: `newval = conf->reset_val`
/// plus the extra produced by running the variable's check hook on that value (C
/// reuses the stored `conf->reset_extra`; we recompute an equivalent one because
/// `GucHookExtra` is not `Clone`). The reset value is the already-canonicalized
/// boot/SET value, so the check hook accepts it; on the off chance a hook rejects
/// it the error is surfaced exactly like the SET path.
fn reset_value_and_extra(
    record: &GucVariable,
    source: GucSource,
) -> PgResult<(config_var_val, Option<SharedExtra>)> {
    match record {
        GucVariable::Bool(c) => {
            let mut v = c.reset_val;
            let extra = call_bool_check_hook(c, &mut v, source)?;
            Ok((config_var_val::Boolval(v), extra))
        }
        GucVariable::Int(c) => {
            let mut v = c.reset_val;
            let extra = call_int_check_hook(c, &mut v, source)?;
            Ok((config_var_val::Intval(v), extra))
        }
        GucVariable::Real(c) => {
            let mut v = c.reset_val;
            let extra = call_real_check_hook(c, &mut v, source)?;
            Ok((config_var_val::Realval(v), extra))
        }
        GucVariable::String(c) => {
            let mut v = c.reset_val.clone();
            let extra = call_string_check_hook(c, &mut v, source)?;
            Ok((config_var_val::Stringval(v), extra))
        }
        GucVariable::Enum(c) => {
            let mut v = c.reset_val;
            let extra = call_enum_check_hook(c, &mut v, source)?;
            Ok((config_var_val::Enumval(v), extra))
        }
    }
}

/// `*conf->variable != newval` (the `prohibitValueChange` comparison of each
/// per-type case, guc.c:3739/3837/3935/4052/4203). Reads the variable's live
/// storage when the owner has installed it, else the GUC's tracked value, and
/// reports whether it differs from the canonicalized `newval`. String values
/// follow the C NULL-aware comparison.
fn current_value_differs(record: &GucVariable, newval: &config_var_val) -> bool {
    // C's prohibitValueChange compares `*conf->variable` against newval. The
    // record's own `c.value` is the GUC store's authoritative copy of that same
    // storage: `apply_value` always writes the new value into BOTH `c.value` and
    // (when installed) the backing accessor, keeping them in lockstep. We read
    // `c.value` here rather than re-entering through `c.variable.read()` because
    // this runs with the live GUC store already mutably borrowed: some accessors
    // (io_method / io_max_concurrency / io_workers) resolve their value by
    // reading the live store itself, so `.read()` would re-enter the `RefCell`
    // and panic `already mutably borrowed`. The store copy is identical and
    // never re-enters.
    match (record, newval) {
        (GucVariable::Bool(c), config_var_val::Boolval(nv)) => {
            c.value.unwrap_or(c.reset_val) != *nv
        }
        (GucVariable::Int(c), config_var_val::Intval(nv)) => {
            c.value.unwrap_or(c.reset_val) != *nv
        }
        (GucVariable::Real(c), config_var_val::Realval(nv)) => {
            c.value.unwrap_or(c.reset_val) != *nv
        }
        (GucVariable::Enum(c), config_var_val::Enumval(nv)) => {
            c.value.unwrap_or(c.reset_val) != *nv
        }
        (GucVariable::String(c), config_var_val::Stringval(nv)) => {
            let cur = c.value.clone().unwrap_or_else(|| c.reset_val.clone());
            // C: (*conf->variable && newval) ? strcmp(...) != 0 : *conf->variable != newval
            match (cur, nv) {
                (Some(a), Some(b)) => a != *b,
                (None, None) => false,
                _ => true,
            }
        }
        _ => true,
    }
}

/// A captured assign-hook invocation, deferred out of the store-locked region
/// so it can recursively re-enter `SetConfigOption` (see [`apply_value`]).
pub type DeferredAssignHook = Box<dyn FnOnce()>;

/// Apply an evaluated value to a variable's live storage and update its
/// source/scontext/srole (the `if (changeVal)` body of each per-type case),
/// pushing the value to the owner's storage slot (`*conf->variable = newval`).
/// Returns the variable's assign hook captured for deferred firing (the C hook
/// call), which the caller runs after releasing the store borrow.
fn apply_value(
    record: &mut GucVariable,
    newval: config_var_val,
    extra: Option<SharedExtra>,
    source: GucSource,
    context: GucContext,
    srole: Oid,
) -> Option<DeferredAssignHook> {
    // The variable's `assign_hook` is NOT run inline: in C the hook may
    // recursively call `SetConfigOption` (e.g. `assign_session_authorization` ->
    // `SetSessionAuthorization` -> `SetOuterUserId` -> `SetConfigOption(
    // "is_superuser")`), re-entering the GUC store. C's store is plain process
    // memory so the recursion just works; here the store is borrowed `&mut`
    // across this call, so running the hook inline would re-enter
    // `with_store_mut` and deadlock / alias the live `&mut`. So the hook
    // invocation (which only re-reads/writes the store, not this `record`) is
    // captured and returned to be fired by the caller AFTER the store borrow is
    // released. The owner-storage `*conf->variable = newval` write stays inline
    // (it does not re-enter the GUC store). This preserves C's ordering: the
    // value is in the record before the hook fires.
    let deferred: Option<DeferredAssignHook> = match (&mut *record, newval) {
        (GucVariable::Bool(c), config_var_val::Boolval(b)) => {
            c.value = Some(b);
            if c.variable.installed() {
                c.variable.write(b);
            }
            c.assign_hook.map(|slot| {
                let f = slot.get();
                let extra = extra.clone();
                Box::new(move || f(b, extra.as_deref())) as DeferredAssignHook
            })
        }
        (GucVariable::Int(c), config_var_val::Intval(v)) => {
            c.value = Some(v);
            if c.variable.installed() {
                c.variable.write(v);
            }
            c.assign_hook.map(|slot| {
                let f = slot.get();
                let extra = extra.clone();
                Box::new(move || f(v, extra.as_deref())) as DeferredAssignHook
            })
        }
        (GucVariable::Real(c), config_var_val::Realval(v)) => {
            c.value = Some(v);
            if c.variable.installed() {
                c.variable.write(v);
            }
            c.assign_hook.map(|slot| {
                let f = slot.get();
                let extra = extra.clone();
                Box::new(move || f(v, extra.as_deref())) as DeferredAssignHook
            })
        }
        (GucVariable::String(c), config_var_val::Stringval(s)) => {
            if c.variable.installed() {
                c.variable.write(s.clone());
            }
            c.value = Some(s.clone());
            c.assign_hook.map(|slot| {
                let f = slot.get();
                let extra = extra.clone();
                Box::new(move || f(s.as_deref(), extra.as_deref())) as DeferredAssignHook
            })
        }
        (GucVariable::Enum(c), config_var_val::Enumval(v)) => {
            c.value = Some(v);
            if c.variable.installed() {
                c.variable.write(v);
            }
            c.assign_hook.map(|slot| {
                let f = slot.get();
                let extra = extra.clone();
                Box::new(move || f(v, extra.as_deref())) as DeferredAssignHook
            })
        }
        (_record, _) => None,
    };

    let gen = record.gen_mut();
    // C: `set_extra_field(&conf->gen, &conf->gen.extra, newextra)` (guc.c:3762).
    // The live variable must remember its current extra so the GUC stack
    // (`set_stack_value`) can copy it into a `prior`/`masked` slot, and so a
    // later `AtEOXact_GUC` restore re-runs the assign hook with the *matching*
    // extra. The timezone/datestyle assign hooks downcast a non-NULL extra, so a
    // dropped extra panics on rollback ("GUC assign hook reached with no extra
    // payload"). The deferred closure above holds its own clone of the same
    // `Arc`, mirroring C's shared pointer.
    gen.extra = extra;
    gen.source = source;
    gen.scontext = context;
    gen.srole = srole;

    deferred
}

/// The `if (makeDefault)` body of each per-type case (guc.c:3768 for bool;
/// identical shape for the others).
fn make_default_bookkeeping(
    record: &mut GucVariable,
    newval: &config_var_val,
    extra: Option<SharedExtra>,
    source: GucSource,
    context: GucContext,
    srole: Oid,
) {
    if record.gen().reset_source <= source {
        // C: `conf->reset_val = newval; set_extra_field(..., &conf->reset_extra,
        // newextra)` (guc.c:3774). The per-type `reset_extra` slot remembers the
        // default's extra so a later RESET re-runs the assign hook with it.
        match (&mut *record, newval) {
            (GucVariable::Bool(c), config_var_val::Boolval(v)) => {
                c.reset_val = *v;
                c.reset_extra = extra.clone();
            }
            (GucVariable::Int(c), config_var_val::Intval(v)) => {
                c.reset_val = *v;
                c.reset_extra = extra.clone();
            }
            (GucVariable::Real(c), config_var_val::Realval(v)) => {
                c.reset_val = *v;
                c.reset_extra = extra.clone();
            }
            (GucVariable::String(c), config_var_val::Stringval(v)) => {
                c.reset_val = v.clone();
                c.reset_extra = extra.clone();
            }
            (GucVariable::Enum(c), config_var_val::Enumval(v)) => {
                c.reset_val = *v;
                c.reset_extra = extra.clone();
            }
            (_record, _) => return,
        }
        let gen = record.gen_mut();
        gen.reset_source = source;
        gen.reset_scontext = context;
        gen.reset_srole = srole;
    }
    // for (stack = conf->gen.stack; stack; stack = stack->prev) ...
    // C copies `newextra` into each matching `stack->prior.extra`
    // (set_extra_field, guc.c:3786).
    let mut stack = record.gen_mut().stack.as_deref_mut();
    while let Some(s) = stack {
        if s.source <= source {
            s.prior.val = Some(newval.clone());
            s.prior.extra = extra.clone();
            s.source = source;
            s.scontext = context;
            s.srole = srole;
        }
        stack = s.prev.as_deref_mut();
    }
}

// ---------------------------------------------------------------------------
// The transactional GUC stack (guc_stack.c). In C this is the per-variable
// intrusive `GucStack` list (`conf->gen.stack`) chained from `guc_stack_list`;
// here every variable carries its own `Option<Box<GucStack>>` (model.rs) and
// `AtEOXact_GUC` iterates the whole registry instead of a separate slist (the
// per-variable list and the registry-wide scan are behavior-equivalent).
// ---------------------------------------------------------------------------

/// `set_stack_value(gconf, val)` (guc.c:812): copy the variable's *current* live
/// value into a stack-entry slot. The model's `config_*.value` mirrors
/// `*conf->variable` (the live storage), so reading it is the faithful analog of
/// C's `*((struct config_bool *) gconf)->variable`. The `extra` field is copied
/// too (C `set_extra_field(gconf, &val->extra, gconf->extra)`).
fn set_stack_value(record: &GucVariable, val: &mut config_var_value) {
    let cur = match record {
        GucVariable::Bool(c) => config_var_val::Boolval(current_bool(c)),
        GucVariable::Int(c) => config_var_val::Intval(current_int(c)),
        GucVariable::Real(c) => config_var_val::Realval(current_real(c)),
        GucVariable::String(c) => config_var_val::Stringval(current_string(c)),
        GucVariable::Enum(c) => config_var_val::Enumval(current_enum(c)),
    };
    val.val = Some(cur);
    // C: `set_extra_field(gconf, &(val->extra), gconf->extra)` (guc.c:838) —
    // copy the variable's *current* extra into the stack slot. With `SharedExtra`
    // = `Arc`, cloning copies the pointer (C's refcounted `set_extra_field`), so
    // an `AtEOXact_GUC` rollback re-runs the assign hook with the matching extra
    // instead of `None` (which made every extra-consuming assign hook panic).
    val.extra = record.gen().extra.clone();
}

/// `discard_stack_value(gconf, val)` (guc.c:846): clear a no-longer-needed stack
/// entry value (and its extra). For scalar types this is a no-op in C (the value
/// is inline); here we drop the owned value/extra, which is the faithful analog
/// of C's `set_string_field(NULL)` + `set_extra_field(NULL)`.
fn discard_stack_value(val: &mut config_var_value) {
    val.val = None;
    val.extra = None;
}

fn current_bool(c: &config_bool) -> bool {
    if c.variable.installed() {
        c.variable.read()
    } else {
        c.value.unwrap_or(c.reset_val)
    }
}
fn current_int(c: &config_int) -> i32 {
    if c.variable.installed() {
        c.variable.read()
    } else {
        c.value.unwrap_or(c.reset_val)
    }
}
fn current_real(c: &config_real) -> f64 {
    if c.variable.installed() {
        c.variable.read()
    } else {
        c.value.unwrap_or(c.reset_val)
    }
}
fn current_enum(c: &config_enum) -> i32 {
    if c.variable.installed() {
        c.variable.read()
    } else {
        c.value.unwrap_or(c.reset_val)
    }
}
fn current_string(c: &config_string) -> Option<String> {
    if c.variable.installed() {
        c.variable.read()
    } else {
        c.value.clone().unwrap_or_else(|| c.reset_val.clone())
    }
}

/// `push_old_value(gconf, action)` (guc.c:2134): push the variable's previous
/// state during a transactional assignment, so `AtEOXact_GUC` can roll it back.
/// A no-op when no nesting level is open (`GUCNestLevel == 0`).
fn push_old_value(record: &mut GucVariable, action: GucAction) {
    let nest_level = crate::guc_nest_level();
    // If we're not inside a nest level, do nothing.
    if nest_level == 0 {
        return;
    }

    // Do we already have a stack entry of the current nest level?
    let has_current = record
        .gen()
        .stack
        .as_ref()
        .is_some_and(|s| s.nest_level >= nest_level);
    if has_current {
        // Snapshot the live value first (needed for the SET-then-SET-LOCAL case),
        // before taking a mutable borrow of the stack entry.
        let masked_snapshot = if action == GUC_ACTION_LOCAL {
            let mut v = config_var_value::default();
            set_stack_value(record, &mut v);
            Some((record.gen().scontext, record.gen().srole, v))
        } else {
            None
        };
        let stack = record.gen_mut().stack.as_mut().unwrap();
        debug_assert!(stack.nest_level == nest_level);
        match action {
            GUC_ACTION_SET => {
                // SET overrides any prior action at same nest level.
                if stack.state == GUC_SET_LOCAL {
                    // must discard old masked value
                    discard_stack_value(&mut stack.masked);
                }
                stack.state = GUC_SET;
            }
            GUC_ACTION_LOCAL => {
                if stack.state == GUC_SET {
                    // SET followed by SET LOCAL: remember SET's value.
                    let (sc, sr, v) = masked_snapshot.unwrap();
                    stack.masked_scontext = sc;
                    stack.masked_srole = sr;
                    stack.masked = v;
                    stack.state = GUC_SET_LOCAL;
                }
                // in all other cases, no change to stack entry
            }
            _ /* GUC_ACTION_SAVE */ => {
                // Could only have a prior SAVE of same variable.
                debug_assert!(stack.state == GUC_SAVE);
            }
        }
        return;
    }

    // Push a new stack entry. Snapshot the current value first.
    let mut prior = config_var_value::default();
    set_stack_value(record, &mut prior);

    let gen = record.gen_mut();
    let prev = gen.stack.take();
    let state = match action {
        GUC_ACTION_SET => GUC_SET,
        GUC_ACTION_LOCAL => GUC_LOCAL,
        _ => GUC_SAVE,
    };
    let stack = GucStack {
        prev,
        nest_level,
        state,
        source: gen.source,
        scontext: gen.scontext,
        masked_scontext: gen.scontext,
        srole: gen.srole,
        masked_srole: gen.srole,
        prior,
        masked: config_var_value::default(),
    };
    gen.stack = Some(Box::new(stack));
}

/// Restore a stacked value onto a variable's live storage + run its assign hook,
/// the per-type body of the `if (restorePrior || restoreMasked)` block in
/// `AtEOXact_GUC` (guc.c:2376-2495). Returns whether the live value changed.
fn restore_stacked_value(
    record: &mut GucVariable,
    newvalue: &config_var_value,
    deferred_hooks: &mut Vec<DeferredAssignHook>,
) -> bool {
    let newval = newvalue.val.as_ref();
    // The restored extra travels with the stacked value (C's
    // `newvalue.extra`). `as_deref()` peels the `Arc` to the `&GucHookExtra` the
    // assign hook expects; `newextra` (the owned `Option<SharedExtra>`) is stored
    // back into `conf->gen.extra` after the hook fires (guc.c:2416 et al.).
    //
    // The variable's `assign_hook` is NOT fired inline: a hook may recursively
    // re-enter `SetConfigOption` (e.g. `assign_role` -> `SetCurrentRoleId` ->
    // `SetOuterUserId` -> `SetConfigOption("is_superuser")`), which re-locks the
    // process-global GUC store and would deadlock while `AtEOXact_GUC` still
    // holds the `with_store_mut` borrow. Mirroring [`apply_value`], the hook is
    // captured into `deferred_hooks` and fired by the caller AFTER the store
    // borrow is released. The owner-storage `*conf->variable = newval` write and
    // the `conf->gen.extra` update stay inline (neither re-enters the store), so
    // C's ordering (value in the record before the hook fires) is preserved.
    let newextra = newvalue.extra.clone();
    let newextra_ref: Option<&GucHookExtra> = newvalue.extra.as_deref();
    let mut changed = false;
    match (record, newval) {
        (GucVariable::Bool(c), Some(config_var_val::Boolval(nv))) => {
            if current_bool(c) != *nv || extra_differs(c.gen.extra.as_deref(), newextra_ref) {
                if let Some(slot) = c.assign_hook {
                    let f = slot.get();
                    let v = *nv;
                    let extra = newextra.clone();
                    deferred_hooks.push(Box::new(move || f(v, extra.as_deref())));
                }
                c.value = Some(*nv);
                if c.variable.installed() {
                    c.variable.write(*nv);
                }
                c.gen.extra = newextra;
                changed = true;
            }
        }
        (GucVariable::Int(c), Some(config_var_val::Intval(nv))) => {
            if current_int(c) != *nv || extra_differs(c.gen.extra.as_deref(), newextra_ref) {
                if let Some(slot) = c.assign_hook {
                    let f = slot.get();
                    let v = *nv;
                    let extra = newextra.clone();
                    deferred_hooks.push(Box::new(move || f(v, extra.as_deref())));
                }
                c.value = Some(*nv);
                if c.variable.installed() {
                    c.variable.write(*nv);
                }
                c.gen.extra = newextra;
                changed = true;
            }
        }
        (GucVariable::Real(c), Some(config_var_val::Realval(nv))) => {
            if current_real(c) != *nv || extra_differs(c.gen.extra.as_deref(), newextra_ref) {
                if let Some(slot) = c.assign_hook {
                    let f = slot.get();
                    let v = *nv;
                    let extra = newextra.clone();
                    deferred_hooks.push(Box::new(move || f(v, extra.as_deref())));
                }
                c.value = Some(*nv);
                if c.variable.installed() {
                    c.variable.write(*nv);
                }
                c.gen.extra = newextra;
                changed = true;
            }
        }
        (GucVariable::String(c), Some(config_var_val::Stringval(nv))) => {
            let differs = match (current_string(c), nv) {
                (Some(a), Some(b)) => a != *b,
                (None, None) => false,
                _ => true,
            };
            if differs || extra_differs(c.gen.extra.as_deref(), newextra_ref) {
                if let Some(slot) = c.assign_hook {
                    let f = slot.get();
                    let s = nv.clone();
                    let extra = newextra.clone();
                    deferred_hooks.push(Box::new(move || f(s.as_deref(), extra.as_deref())));
                }
                c.value = Some(nv.clone());
                if c.variable.installed() {
                    c.variable.write(nv.clone());
                }
                c.gen.extra = newextra;
                changed = true;
            }
        }
        (GucVariable::Enum(c), Some(config_var_val::Enumval(nv))) => {
            if current_enum(c) != *nv || extra_differs(c.gen.extra.as_deref(), newextra_ref) {
                if let Some(slot) = c.assign_hook {
                    let f = slot.get();
                    let v = *nv;
                    let extra = newextra.clone();
                    deferred_hooks.push(Box::new(move || f(v, extra.as_deref())));
                }
                c.value = Some(*nv);
                if c.variable.installed() {
                    c.variable.write(*nv);
                }
                c.gen.extra = newextra;
                changed = true;
            }
        }
        _ => {}
    }
    changed
}

/// `conf->gen.extra != newextra`: the C pointer-identity compare. The model's
/// `GucHookExtra` is an opaque payload with no identity; conservatively report a
/// difference only when one side is present and the other absent (so a present
/// extra is always re-installed, never dropped).
fn extra_differs(cur: Option<&GucHookExtra>, new: Option<&GucHookExtra>) -> bool {
    cur.is_some() != new.is_some()
}

/// `AtEOXact_GUC(isCommit, nestLevel)` (guc.c:2262), the per-variable rollback
/// walk. The caller ([`crate::at_eoxact_guc`]) owns the `GUCNestLevel` update.
/// During abort, discard all GUC settings applied at nesting levels >=
/// `nest_level`; on commit, fold/keep per the stack-state rules.
pub fn at_eoxact_guc(
    reg: &mut GucRegistry,
    is_commit: bool,
    nest_level: i32,
    deferred_hooks: &mut Vec<DeferredAssignHook>,
) {
    debug_assert!(nest_level > 0);
    for record in reg.iter_mut() {
        pop_var_stack(record, is_commit, nest_level, deferred_hooks);
    }
}

/// Process and pop one variable's stack entries within the nest level
/// (the `while ((stack = gconf->stack) ...)` loop body of `AtEOXact_GUC`).
fn pop_var_stack(
    record: &mut GucVariable,
    is_commit: bool,
    nest_level: i32,
    deferred_hooks: &mut Vec<DeferredAssignHook>,
) {
    loop {
        // Peek the top stack entry.
        let top_level = match record.gen().stack.as_ref() {
            Some(s) if s.nest_level >= nest_level => s.nest_level,
            _ => break,
        };

        // Pop the top entry off the chain so we can inspect/own it.
        let mut stack = record.gen_mut().stack.take().unwrap();
        let prev_opt = stack.prev.take(); // detaches prev from `stack`
        let prev_level = prev_opt.as_ref().map(|p| p.nest_level);

        let mut restore_prior = false;
        let mut restore_masked = false;

        if !is_commit {
            // if abort, always restore prior value
            restore_prior = true;
        } else if stack.state == GUC_SAVE {
            restore_prior = true;
        } else if stack.nest_level == 1 {
            // transaction commit
            if stack.state == GUC_SET_LOCAL {
                restore_masked = true;
            } else if stack.state == GUC_SET {
                // we keep the current active value
                discard_stack_value(&mut stack.prior);
            } else {
                // must be GUC_LOCAL
                restore_prior = true;
            }
        } else if prev_opt.is_none() || prev_level.unwrap() < stack.nest_level - 1 {
            // decrement entry's level and do not pop it: re-link prev and keep.
            stack.nest_level = top_level - 1;
            stack.prev = prev_opt;
            record.gen_mut().stack = Some(stack);
            continue;
        } else {
            // Merge this stack entry into prev.
            let mut prev = prev_opt.unwrap();
            match stack.state {
                GUC_SAVE => debug_assert!(false, "can't get here"),
                GUC_SET => {
                    // next level always becomes SET
                    discard_stack_value(&mut stack.prior);
                    if prev.state == GUC_SET_LOCAL {
                        discard_stack_value(&mut prev.masked);
                    }
                    prev.state = GUC_SET;
                }
                GUC_LOCAL => {
                    if prev.state == GUC_SET {
                        // LOCAL migrates down
                        prev.masked_scontext = stack.scontext;
                        prev.masked_srole = stack.srole;
                        prev.masked = core::mem::take(&mut stack.prior);
                        prev.state = GUC_SET_LOCAL;
                    } else {
                        // else just forget this stack level
                        discard_stack_value(&mut stack.prior);
                    }
                }
                _ /* GUC_SET_LOCAL */ => {
                    // prior state at this level no longer wanted
                    discard_stack_value(&mut stack.prior);
                    // copy down the masked state
                    prev.masked_scontext = stack.masked_scontext;
                    prev.masked_srole = stack.masked_srole;
                    if prev.state == GUC_SET_LOCAL {
                        discard_stack_value(&mut prev.masked);
                    }
                    prev.masked = core::mem::take(&mut stack.masked);
                    prev.state = GUC_SET_LOCAL;
                }
            }
            // Pop `stack`, prev becomes the new top.
            record.gen_mut().stack = Some(prev);
            continue;
        }

        let mut changed = false;
        if restore_prior || restore_masked {
            let (newvalue, newsource, newscontext, newsrole) = if restore_masked {
                (
                    core::mem::take(&mut stack.masked),
                    PGC_S_SESSION,
                    stack.masked_scontext,
                    stack.masked_srole,
                )
            } else {
                (
                    core::mem::take(&mut stack.prior),
                    stack.source,
                    stack.scontext,
                    stack.srole,
                )
            };

            changed = restore_stacked_value(record, &newvalue, deferred_hooks);

            // Release stacked values (already taken above / discard the rest).
            discard_stack_value(&mut stack.prior);
            discard_stack_value(&mut stack.masked);

            // Restore source information.
            let gen = record.gen_mut();
            gen.source = newsource;
            gen.scontext = newscontext;
            gen.srole = newsrole;
        }

        // Pop the GUC's state stack.
        record.gen_mut().stack = prev_opt;

        // Report new value if we changed it.
        if changed && (record.gen().flags & GUC_REPORT) != 0 {
            let gen = record.gen_mut();
            if gen.status & GUC_NEEDS_REPORT == 0 {
                gen.status |= GUC_NEEDS_REPORT;
            }
        }
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
            ::utils_error::emit_error_report_for(&e);
        }
        Ok(0)
    }
}

/// `ResetAllOptions()` (guc.c:2003), reduced to the core effect: reset every
/// USERSET/SUSET variable (that lacks GUC_NO_RESET_ALL) to its reset value and
/// reset_source. The stack/parallel/report bookkeeping is part of the stack
/// subsystem (deferred).
pub fn reset_all_options(reg: &mut GucRegistry) {
    use ::types_guc::GUC_NO_RESET_ALL;
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
    // C's `ResetAllOptions` re-applies the variable's `reset_val` *with* its
    // saved `reset_extra` (the boot/reset-time check-hook payload), and copies
    // `reset_extra` back into `gen.extra` (set_extra_field). Passing `None` for
    // the extra here panics any assign hook that downcasts a required payload
    // (e.g. client_encoding, datestyle, timezone, session_authorization).
    match var {
        GucVariable::Bool(c) => {
            c.value = Some(c.reset_val);
            if let Some(slot) = c.assign_hook {
                if slot.installed() {
                    (slot.get())(c.reset_val, c.reset_extra.as_deref());
                }
            }
            if c.variable.installed() {
                c.variable.write(c.reset_val);
            }
            c.gen.extra = c.reset_extra.clone();
        }
        GucVariable::Int(c) => {
            c.value = Some(c.reset_val);
            if let Some(slot) = c.assign_hook {
                if slot.installed() {
                    (slot.get())(c.reset_val, c.reset_extra.as_deref());
                }
            }
            if c.variable.installed() {
                c.variable.write(c.reset_val);
            }
            c.gen.extra = c.reset_extra.clone();
        }
        GucVariable::Real(c) => {
            c.value = Some(c.reset_val);
            if let Some(slot) = c.assign_hook {
                if slot.installed() {
                    (slot.get())(c.reset_val, c.reset_extra.as_deref());
                }
            }
            if c.variable.installed() {
                c.variable.write(c.reset_val);
            }
            c.gen.extra = c.reset_extra.clone();
        }
        GucVariable::String(c) => {
            if let Some(slot) = c.assign_hook {
                if slot.installed() {
                    (slot.get())(c.reset_val.as_deref(), c.reset_extra.as_deref());
                }
            }
            if c.variable.installed() {
                c.variable.write(c.reset_val.clone());
            }
            c.value = Some(c.reset_val.clone());
            c.gen.extra = c.reset_extra.clone();
        }
        GucVariable::Enum(c) => {
            c.value = Some(c.reset_val);
            if let Some(slot) = c.assign_hook {
                if slot.installed() {
                    (slot.get())(c.reset_val, c.reset_extra.as_deref());
                }
            }
            if c.variable.installed() {
                c.variable.write(c.reset_val);
            }
            c.gen.extra = c.reset_extra.clone();
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
