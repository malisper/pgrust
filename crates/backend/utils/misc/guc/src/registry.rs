use std::collections::HashMap;

use elog::{ereport, message_level_is_interesting};
use types_core::Oid;
use types_error::{
    ErrorLevel, PgError, PgResult, SqlState, DEBUG3, ERRCODE_CANT_CHANGE_RUNTIME_PARAM,
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_INVALID_TRANSACTION_STATE, ERRCODE_UNDEFINED_OBJECT, ERROR, LOG, WARNING,
};
use types_guc::{
    GucContext, GucSource, GUC_ALLOW_IN_PARALLEL, GUC_IS_NAME, GUC_NO_RESET, GUC_REPORT, GUC_UNIT,
    PGC_BACKEND, PGC_INTERNAL, PGC_POSTMASTER, PGC_SIGHUP, PGC_SUSET, PGC_SU_BACKEND, PGC_S_CLIENT,
    PGC_S_DATABASE, PGC_S_DATABASE_USER, PGC_S_DEFAULT, PGC_S_DYNAMIC_DEFAULT, PGC_S_FILE,
    PGC_S_GLOBAL, PGC_S_OVERRIDE, PGC_S_SESSION, PGC_S_USER, PGC_USERSET,
};

use guc_tables::GucHookExtra;

use crate::enum_lookup::{
    config_enum_get_options, config_enum_lookup_by_name, config_enum_lookup_by_value,
};
use crate::model::{
    config_bool, config_enum, config_generic, config_int, config_real, config_string,
    config_var_val, config_var_value, GucStack, SharedExtra, GUC_LOCAL, GUC_NEEDS_REPORT,
    GUC_PENDING_RESTART, GUC_SAVE, GUC_SET, GUC_SET_LOCAL,
};
use crate::name::{fold_name, guc_name_eq, GucNameHasherBuilder, MAP_OLD_GUC_NAMES};
use crate::units::{
    convert_int_from_base_unit, convert_real_from_base_unit, fmt_g, get_config_unit_name,
    parse_int, parse_real, ParseNum,
};
use crate::{GUC_ACTION_LOCAL, GUC_ACTION_SAVE, GUC_ACTION_SET};

pub type GucAction = u32;

#[derive(Clone, Debug)]
pub enum GucVariable {
    Bool(config_bool),
    Int(config_int),
    Real(config_real),
    String(config_string),
    Enum(config_enum),
}

impl GucVariable {
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

    pub fn name(&self) -> &'static str {
        self.gen().name
    }
}

// guc_hashtab plus the three intrusive lists (guc.c:226): guc_stack_list and
// guc_report_list make AtEOXact_GUC / ReportChangedGUCOptions O(changed) not
// O(~400); guc_nondef_list makes ResetAllOptions walk only non-default vars.
#[derive(Clone, Debug, Default)]
pub struct GucRegistry {
    vars: Vec<GucVariable>,
    index: HashMap<Box<str>, usize, GucNameHasherBuilder>,
    stacked: Vec<usize>,
    reported: Vec<usize>,
    nondef: Vec<usize>,
}

impl GucRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    fn note_stacked(&mut self, idx: usize) {
        self.stacked.push(idx);
        // A stale-true hint on a non-TLS registry is harmless (see store.rs).
        crate::store::set_has_stacked_hint(true);
    }

    // Invariant (C's): idx is on `reported` iff its GUC_NEEDS_REPORT bit is set.
    fn note_reportable(&mut self, idx: usize) {
        let g = self.vars[idx].gen_mut();
        if g.flags & GUC_REPORT != 0 && g.status & GUC_NEEDS_REPORT == 0 {
            g.status |= GUC_NEEDS_REPORT;
            self.reported.push(idx);
            crate::store::set_report_pending_hint(true);
        }
    }

    pub fn drain_report_list(&mut self) -> Vec<usize> {
        let indices = core::mem::take(&mut self.reported);
        for &idx in &indices {
            self.vars[idx].gen_mut().status &= !GUC_NEEDS_REPORT;
        }
        indices
    }

    // set_guc_source (guc.c): keeps guc_nondef_list membership in step with
    // source==PGC_S_DEFAULT transitions.
    fn set_source(&mut self, idx: usize, newsource: GucSource) {
        let g = self.vars[idx].gen_mut();
        if g.source == PGC_S_DEFAULT {
            if newsource != PGC_S_DEFAULT {
                self.nondef.push(idx);
            }
        } else if newsource == PGC_S_DEFAULT {
            self.nondef.retain(|&i| i != idx);
        }
        self.vars[idx].gen_mut().source = newsource;
    }

    pub fn define(&mut self, var: GucVariable) -> PgResult<()> {
        let idx = self.vars.len();
        let key = fold_name(var.name()).into_boxed_str();
        self.vars.try_reserve(1).map_err(|_| oom())?;
        self.index.try_reserve(1).map_err(|_| oom())?;
        self.vars.push(var);
        self.index.insert(key, idx);
        Ok(())
    }

    pub fn add_placeholder_variable(&mut self, name: &str) -> PgResult<usize> {
        use types_guc::{config_group, config_type};

        // C guc_strdups the name into GUCMemoryContext, never freed.
        let gen = config_generic::boot(
            Box::leak(name.to_string().into_boxed_str()),
            GucContext::PGC_USERSET,
            config_group::CUSTOM_OPTIONS,
            Some("GUC placeholder variable"),
            None,
            types_guc::GUC_NO_SHOW_ALL
                | types_guc::GUC_NOT_IN_SAMPLE
                | types_guc::GUC_CUSTOM_PLACEHOLDER,
            config_type::PGC_STRING,
        );
        let var = GucVariable::String(config_string {
            gen,
            variable: &guc_tables::vars::GucPlaceholderVariable,
            value: None,
            boot_val: None,
            check_hook: None,
            assign_hook: None,
            show_hook: None,
            reset_val: None,
            reset_extra: None,
        });
        let idx = self.vars.len();
        self.define(var)?;
        Ok(idx)
    }

    pub fn find_option(&self, name: &str) -> Option<&GucVariable> {
        self.find_index(name).map(|idx| &self.vars[idx])
    }

    pub fn find_option_mut(&mut self, name: &str) -> Option<&mut GucVariable> {
        self.find_index(name).map(|idx| &mut self.vars[idx])
    }

    pub fn find_index(&self, name: &str) -> Option<usize> {
        let direct = if name.bytes().any(|b| b.is_ascii_uppercase()) {
            self.index.get(fold_name(name).as_str()).copied()
        } else {
            self.index.get(name).copied()
        };
        if let Some(idx) = direct {
            return Some(idx);
        }
        for (old, new) in MAP_OLD_GUC_NAMES {
            if guc_name_eq(name, old) {
                return self.index.get(*new).copied();
            }
        }
        None
    }

    pub fn iter(&self) -> impl Iterator<Item = &GucVariable> {
        self.vars
            .iter()
            .filter(|v| v.gen().status & crate::model::GUC_REMOVED == 0)
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut GucVariable> {
        self.vars
            .iter_mut()
            .filter(|v| v.gen().status & crate::model::GUC_REMOVED == 0)
    }

    // MarkGUCPrefixReserved's placeholder purge (guc.c:5285): C removes the
    // guc_hashtab entry and unlinks the var from all lists; our vars slot must
    // keep its index, so it stays behind as a GUC_REMOVED tombstone that
    // find_index/iter can no longer reach. Returns the removed names.
    pub fn remove_reserved_placeholders(&mut self, class_name: &str) -> Vec<&'static str> {
        let mut removed = Vec::new();
        for idx in 0..self.vars.len() {
            let g = self.vars[idx].gen();
            let name = g.name;
            if g.flags & types_guc::GUC_CUSTOM_PLACEHOLDER == 0
                || g.status & crate::model::GUC_REMOVED != 0
                || name.len() <= class_name.len()
                || !name.starts_with(class_name)
                || name.as_bytes()[class_name.len()] != b'.'
            {
                continue;
            }
            self.index.remove(fold_name(name).as_str());
            self.stacked.retain(|&i| i != idx);
            self.reported.retain(|&i| i != idx);
            self.nondef.retain(|&i| i != idx);
            let g = self.vars[idx].gen_mut();
            g.status &= !GUC_NEEDS_REPORT;
            g.status |= crate::model::GUC_REMOVED;
            removed.push(name);
        }
        removed
    }

    pub fn len(&self) -> usize {
        self.vars.len()
    }

    pub fn is_empty(&self) -> bool {
        self.vars.is_empty()
    }

    pub fn has_stacked(&self) -> bool {
        !self.stacked.is_empty()
    }

    pub fn set_source_pub(&mut self, idx: usize, newsource: GucSource) {
        self.set_source(idx, newsource);
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

#[track_caller]
#[cold]
fn oom() -> Box<PgError> {
    ereport(ERROR)
        .errcode(types_error::ERRCODE_OUT_OF_MEMORY)
        .errmsg("out of memory")
        .into_error()
        .into()
}

fn resolve_elevel(elevel: ErrorLevel, source: GucSource) -> ErrorLevel {
    if elevel != ErrorLevel(0) {
        return elevel;
    }
    if source == PGC_S_DEFAULT || source == PGC_S_FILE {
        if init_small::globals::IsUnderPostmaster() {
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

#[cold]
fn err(sqlstate: SqlState, message: String) -> PgError {
    ereport(ERROR)
        .errcode(sqlstate)
        .errmsg(message)
        .into_error()
}

// C return convention: ereport(elevel) throws at >= ERROR, else logs and the
// set returns 0.
fn reject(elevel: ErrorLevel, e: PgError) -> PgResult<i32> {
    if elevel >= ERROR {
        Err(e.into())
    } else {
        if message_level_is_interesting(elevel) {
            elog::emit_error_report_for(&e);
        }
        Ok(0)
    }
}

enum AccessCheck {
    Ok,
    Skip,
    Reject(PgError),
}

fn parameter_acl_set_ok(name: &str, role: Oid) -> PgResult<bool> {
    aclchk_seams::pg_parameter_aclcheck_set::call(name, role)
}

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

    if xact_seams::is_in_parallel_mode::call()
        && change_val
        && action != GUC_ACTION_SAVE
        && (record.flags & GUC_ALLOW_IN_PARALLEL) == 0
    {
        return Ok(AccessCheck::Reject(err(
            ERRCODE_INVALID_TRANSACTION_STATE,
            format!("parameter \"{name}\" cannot be set during a parallel operation"),
        )));
    }

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
                // prohibitValueChange: handled after canonicalizing the value.
            } else if context != PGC_POSTMASTER {
                return Ok(AccessCheck::Reject(err(
                    ERRCODE_CANT_CHANGE_RUNTIME_PARAM,
                    format!("parameter \"{name}\" cannot be changed without restarting the server"),
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
        PGC_USERSET => {}
    }

    if record.flags & types_guc::GUC_NOT_WHILE_SEC_REST != 0 {
        if miscinit::InLocalUserIdChange() {
            return Ok(AccessCheck::Reject(err(
                ERRCODE_INSUFFICIENT_PRIVILEGE,
                format!("cannot set parameter \"{name}\" within security-definer function"),
            )));
        }
        if miscinit::InSecurityRestrictedOperation() {
            return Ok(AccessCheck::Reject(err(
                ERRCODE_INSUFFICIENT_PRIVILEGE,
                format!("cannot set parameter \"{name}\" within security-restricted operation"),
            )));
        }
    }

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

fn backend_context_rules(
    name: &str,
    context: GucContext,
    source: GucSource,
    change_val: bool,
    is_reload: bool,
) -> Option<AccessCheck> {
    if context == PGC_SIGHUP {
        // Accept in the postmaster, ignore in existing backends unless reloading.
        if init_small::globals::IsUnderPostmaster() && change_val && !is_reload {
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
                    )
                    .into())
                }
            };
            let extra = call_bool_check_hook(conf, &mut newval, source)?;
            Ok((config_var_val::Boolval(newval), extra))
        }
        GucVariable::Int(conf) => {
            let mut newval = match parse_int(value, gen.flags) {
                ParseNum::Ok(v) => v,
                ParseNum::Err { hint } => return Err(invalid_value_error(name, value, hint).into()),
            };
            if newval < conf.min || newval > conf.max {
                let (unit, sp) = unit_and_space(gen.flags);
                return Err(err(
                    ERRCODE_INVALID_PARAMETER_VALUE,
                    format!(
                        "{newval}{sp}{unit} is outside the valid range for parameter \"{name}\" ({}{sp}{unit} .. {}{sp}{unit})",
                        conf.min, conf.max
                    ),
                )
                .into());
            }
            let extra = call_int_check_hook(conf, &mut newval, source)?;
            Ok((config_var_val::Intval(newval), extra))
        }
        GucVariable::Real(conf) => {
            let mut newval = match parse_real(value, gen.flags) {
                ParseNum::Ok(v) => v,
                ParseNum::Err { hint } => return Err(invalid_value_error(name, value, hint).into()),
            };
            if newval < conf.min || newval > conf.max {
                let (unit, sp) = unit_and_space(gen.flags);
                return Err(err(
                    ERRCODE_INVALID_PARAMETER_VALUE,
                    format!(
                        "{newval}{sp}{unit} is outside the valid range for parameter \"{name}\" ({}{sp}{unit} .. {}{sp}{unit})",
                        conf.min, conf.max
                    ),
                )
                .into());
            }
            let extra = call_real_check_hook(conf, &mut newval, source)?;
            Ok((config_var_val::Realval(newval), extra))
        }
        GucVariable::String(conf) => {
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
                        .errmsg(format!(
                            "invalid value for parameter \"{name}\": \"{value}\""
                        ))
                        .errhint(hint)
                        .into_error()
                        .into());
                }
            };
            let extra = call_enum_check_hook(conf, &mut newval, source)?;
            Ok((config_var_val::Enumval(newval), extra))
        }
    }
}

// truncate_identifier(newval, strlen(newval), true) (scansup.c) for GUC_IS_NAME.
fn truncate_name(s: &str) -> PgResult<String> {
    if s.len() < parser_small1::NAMEDATALEN {
        return Ok(s.to_string());
    }
    let ctx = mcx::MemoryContext::new("guc truncate_identifier");
    let mcx = ctx.mcx();
    let mut ident = mcx::vec_with_capacity_in(mcx, s.len()).map_err(|_| oom())?;
    ident.extend_from_slice(s.as_bytes());
    let encoding = mbutils_seams::get_database_encoding::call();
    parser_small1::truncate_identifier(&mut ident, true, encoding)?;
    Ok(String::from_utf8_lossy(&ident).into_owned())
}

fn invalid_value_error(name: &str, value: &str, hint: Option<&str>) -> PgError {
    let mut b = ereport(ERROR)
        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
        .errmsg(format!(
            "invalid value for parameter \"{name}\": \"{value}\""
        ));
    if let Some(h) = hint {
        b = b.errhint(h.to_string());
    }
    b.into_error()
}

fn unit_and_space(flags: i32) -> (&'static str, &'static str) {
    match get_config_unit_name(flags & GUC_UNIT) {
        Some(u) => (u, " "),
        None => ("", ""),
    }
}

// call_*_check_hook (guc.c:6809..): an uninstalled slot is an unported owner;
// it behaves as C's check_hook == NULL (boot defaults are valid by construction).
fn check_hook_error(fallback: String) -> PgError {
    let check = crate::take_guc_check_error();
    if check.message.is_none()
        && check.detail.is_none()
        && check.hint.is_none()
        && check.sqlstate == ERRCODE_INVALID_PARAMETER_VALUE
    {
        return err(ERRCODE_INVALID_PARAMETER_VALUE, fallback);
    }
    let mut builder = ereport(ERROR)
        .errcode(check.sqlstate)
        .errmsg(check.message.unwrap_or(fallback));
    if let Some(detail) = check.detail {
        builder = builder.errdetail_internal(detail);
    }
    if let Some(hint) = check.hint {
        builder = builder.errhint(hint);
    }
    builder.into_error()
}

macro_rules! check_hook_caller {
    ($fname:ident, $conf:ty, $val:ty, $fallback:expr) => {
        fn $fname(
            conf: &$conf,
            newval: &mut $val,
            source: GucSource,
        ) -> PgResult<Option<SharedExtra>> {
            let Some(slot) = conf.check_hook else {
                return Ok(None);
            };
            if !slot.installed() {
                return Ok(None);
            }
            crate::reset_guc_check_error();
            let mut extra: Option<GucHookExtra> = None;
            if (slot.get())(newval, &mut extra, source)? {
                Ok(extra.map(SharedExtra::new))
            } else {
                Err(check_hook_error($fallback(conf, newval)).into())
            }
        }
    };
}

check_hook_caller!(
    call_bool_check_hook,
    config_bool,
    bool,
    |c: &config_bool, v: &bool| format!(
        "invalid value for parameter \"{}\": {}",
        c.gen.name, *v as i32
    )
);
check_hook_caller!(
    call_int_check_hook,
    config_int,
    i32,
    |c: &config_int, v: &i32| format!("invalid value for parameter \"{}\": {}", c.gen.name, v)
);
check_hook_caller!(
    call_real_check_hook,
    config_real,
    f64,
    |c: &config_real, v: &f64| format!("invalid value for parameter \"{}\": {}", c.gen.name, v)
);
check_hook_caller!(
    call_string_check_hook,
    config_string,
    Option<String>,
    |c: &config_string, v: &Option<String>| format!(
        "invalid value for parameter \"{}\": \"{}\"",
        c.gen.name,
        v.as_deref().unwrap_or("")
    )
);
check_hook_caller!(
    call_enum_check_hook,
    config_enum,
    i32,
    |c: &config_enum, v: &i32| format!(
        "invalid value for parameter \"{}\": \"{}\"",
        c.gen.name,
        config_enum_lookup_by_value(c, *v).unwrap_or("?")
    )
);

// InitializeOneGUCOption's hook step (guc.c:1644): run the check hook on the
// boot value for its extra, fire the assign hook, stash extra in gen.extra and
// reset_extra. `publish` false suppresses the assign hook and the backing-var
// write: in C both only touch the child's own address space, but our backing
// vars are process-shared, so a child bring-up installing a boot value that
// its snapshot restore immediately overwrites would publish a transient wrong
// value to concurrently running threads.
pub fn initialize_one_guc_option_hooks(var: &mut GucVariable, publish: bool) -> PgResult<()> {
    match var {
        GucVariable::Bool(conf) => {
            let mut newval = conf.boot_val;
            let extra = call_bool_check_hook(conf, &mut newval, PGC_S_DEFAULT)?;
            if publish {
                if let Some(slot) = conf.assign_hook {
                    if slot.installed() {
                        (slot.get())(newval, extra.as_deref());
                    }
                }
                if conf.variable.installed() {
                    conf.variable.write(newval);
                }
            }
            conf.value = Some(newval);
            conf.gen.extra = extra.clone();
            conf.reset_extra = extra;
        }
        GucVariable::Int(conf) => {
            let mut newval = conf.boot_val;
            let extra = call_int_check_hook(conf, &mut newval, PGC_S_DEFAULT)?;
            if publish {
                if let Some(slot) = conf.assign_hook {
                    if slot.installed() {
                        (slot.get())(newval, extra.as_deref());
                    }
                }
                if conf.variable.installed() {
                    conf.variable.write(newval);
                }
            }
            conf.value = Some(newval);
            conf.gen.extra = extra.clone();
            conf.reset_extra = extra;
        }
        GucVariable::Real(conf) => {
            let mut newval = conf.boot_val;
            let extra = call_real_check_hook(conf, &mut newval, PGC_S_DEFAULT)?;
            if publish {
                if let Some(slot) = conf.assign_hook {
                    if slot.installed() {
                        (slot.get())(newval, extra.as_deref());
                    }
                }
                if conf.variable.installed() {
                    conf.variable.write(newval);
                }
            }
            conf.value = Some(newval);
            conf.gen.extra = extra.clone();
            conf.reset_extra = extra;
        }
        GucVariable::String(conf) => {
            let mut newval = conf.boot_val.clone();
            let extra = call_string_check_hook(conf, &mut newval, PGC_S_DEFAULT)?;
            if publish {
                if let Some(slot) = conf.assign_hook {
                    if slot.installed() {
                        (slot.get())(newval.as_deref(), extra.as_deref());
                    }
                }
                if conf.variable.installed() {
                    conf.variable.write(newval.clone());
                }
            }
            conf.value = Some(newval);
            conf.gen.extra = extra.clone();
            conf.reset_extra = extra;
        }
        GucVariable::Enum(conf) => {
            let mut newval = conf.boot_val;
            let extra = call_enum_check_hook(conf, &mut newval, PGC_S_DEFAULT)?;
            if publish {
                if let Some(slot) = conf.assign_hook {
                    if slot.installed() {
                        (slot.get())(newval, extra.as_deref());
                    }
                }
                if conf.variable.installed() {
                    conf.variable.write(newval);
                }
            }
            conf.value = Some(newval);
            conf.gen.extra = extra.clone();
            conf.reset_extra = extra;
        }
    }
    Ok(())
}

// C ShowGUCOption reads *conf->variable live; read through an installed
// accessor so a direct write to the bound global (e.g. xact.c restoring
// XactReadOnly) is reflected, else the record's tracked value.
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

pub fn show_guc_option(record: &GucVariable, use_units: bool) -> String {
    match record {
        GucVariable::Bool(conf) => {
            if let Some(slot) = conf.show_hook {
                if slot.installed() {
                    return (slot.get())();
                }
            }
            if current_bool(conf) {
                "on".to_string()
            } else {
                "off".to_string()
            }
        }
        GucVariable::Int(conf) => {
            if let Some(slot) = conf.show_hook {
                if slot.installed() {
                    return (slot.get())();
                }
            }
            let mut result = current_int(conf) as i64;
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
                if slot.installed() {
                    return (slot.get())();
                }
            }
            let mut result = current_real(conf);
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
                if slot.installed() {
                    return (slot.get())();
                }
            }
            current_string(conf)
                .filter(|s| !s.is_empty())
                .unwrap_or_default()
        }
        GucVariable::Enum(conf) => {
            if let Some(slot) = conf.show_hook {
                if slot.installed() {
                    return (slot.get())();
                }
            }
            config_enum_lookup_by_value(conf, current_enum(conf))
                .unwrap_or("?")
                .to_string()
        }
    }
}

pub fn reset_value_string(record: &GucVariable) -> Option<String> {
    Some(match record {
        GucVariable::Bool(c) => {
            if c.reset_val {
                "on".to_string()
            } else {
                "off".to_string()
            }
        }
        GucVariable::Int(c) => format!("{}", c.reset_val),
        GucVariable::Real(c) => fmt_g(c.reset_val),
        GucVariable::String(c) => c.reset_val.clone()?,
        GucVariable::Enum(c) => config_enum_lookup_by_value(c, c.reset_val)?.to_string(),
    })
}

// get_explain_guc_options (guc.c): GUC_EXPLAIN vars whose current value
// differs from boot_val, as (name, GetConfigOptionByName value). C walks
// guc_nondef_list in hash order; table order here — EXPLAIN (SETTINGS)
// consumers are order-insensitive (regress greps the line / extracts JSON
// keys). `visible` is ConfigOptionIsVisible (lives with the ACL deps).
pub fn get_explain_guc_options(
    reg: &GucRegistry,
    visible: &mut dyn FnMut(&GucVariable) -> PgResult<bool>,
) -> PgResult<Vec<(&'static str, Option<String>)>> {
    let mut out = Vec::new();
    for conf in reg.iter() {
        let gen = conf.gen();
        if gen.flags & types_guc::GUC_EXPLAIN == 0 {
            continue;
        }
        // guc_nondef_list membership: only sources beyond the default.
        if gen.source == PGC_S_DEFAULT {
            continue;
        }
        if !visible(conf)? {
            continue;
        }
        let modified = match conf {
            GucVariable::Bool(c) => current_bool(c) != c.boot_val,
            GucVariable::Int(c) => current_int(c) != c.boot_val,
            GucVariable::Real(c) => current_real(c) != c.boot_val,
            GucVariable::String(c) => match (&c.boot_val, current_string(c)) {
                (None, None) => false,
                (None, Some(_)) | (Some(_), None) => true,
                (Some(b), Some(v)) => *b != v,
            },
            GucVariable::Enum(c) => current_enum(c) != c.boot_val,
        };
        if !modified {
            continue;
        }
        out.push((gen.name, Some(show_guc_option(conf, true))));
    }
    Ok(out)
}

pub fn get_config_option_by_name(
    reg: &GucRegistry,
    name: &str,
    missing_ok: bool,
) -> PgResult<Option<String>> {
    match reg.find_option(name) {
        Some(record) => Ok(Some(show_guc_option(record, true))),
        None if missing_ok => Ok(None),
        None => Err(unrecognized(name).into()),
    }
}

pub fn get_config_option_flags(reg: &GucRegistry, name: &str, missing_ok: bool) -> PgResult<i32> {
    match reg.find_option(name) {
        Some(record) => Ok(record.gen().flags),
        None if missing_ok => Ok(0),
        None => Err(unrecognized(name).into()),
    }
}

#[cold]
fn unrecognized(name: &str) -> PgError {
    err(
        ERRCODE_UNDEFINED_OBJECT,
        format!("unrecognized configuration parameter \"{name}\""),
    )
}

pub type DeferredAssignHook = Box<dyn FnOnce()>;

// set_config_with_handle core (guc.c:3405). Ok(1) applied, Ok(0)
// rejected-below-ERROR, Ok(-1) skipped; Err when the resolved elevel is ERROR.
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

    // Originals for the session_authorization -> role kluge below.
    let orig_context = context;
    let orig_source = source;
    let orig_srole = srole;

    // find_option(name, create_placeholders=true, skip_errors=false, elevel).
    let idx = match reg.find_index(name) {
        Some(idx) => idx,
        None => match crate::assignable_custom_variable_name(name, false) {
            Ok(true) => match reg.add_placeholder_variable(name) {
                Ok(idx) => idx,
                Err(e) => return reject(elevel, *e),
            },
            Ok(false) => return Ok(0),
            Err(e) => return reject(elevel, *e),
        },
    };

    let access = check_can_set(
        reg.vars[idx].gen(),
        value.is_none(),
        context,
        source,
        srole,
        action,
        change_val,
        is_reload,
    )?;
    match access {
        AccessCheck::Ok => {}
        AccessCheck::Skip => return Ok(-1),
        AccessCheck::Reject(e) => return reject(elevel, e),
    }

    let make_default =
        change_val && (source <= PGC_S_OVERRIDE) && (value.is_some() || source == PGC_S_DEFAULT);

    // Ignore attempted set if overridden by previously processed setting.
    let mut change_val = change_val;
    if reg.vars[idx].gen().source > source {
        if change_val && !make_default {
            return Ok(-1);
        }
        change_val = false;
    }

    let mut source = source;
    let mut context = context;
    let mut srole = srole;
    let (newval, newextra) = match value {
        Some(v) => match parse_and_validate_value(&reg.vars[idx], v, source) {
            Ok(nv) => nv,
            Err(e) => return reject(elevel, *e),
        },
        None if source == PGC_S_DEFAULT => match boot_default_value(&reg.vars[idx], source) {
            Ok(nv) => nv,
            Err(e) => return reject(elevel, *e),
        },
        None => {
            // RESET: newval = reset_val, newextra = reset_extra (Rc pointer
            // share), provenance from the reset_* fields.
            let record = &reg.vars[idx];
            let gen = record.gen();
            source = gen.reset_source;
            context = gen.reset_scontext;
            srole = gen.reset_srole;
            reset_value_and_extra(record)
        }
    };

    // Re-reading a PGC_POSTMASTER variable from postgresql.conf under SIGHUP.
    let prohibit_value_change =
        reg.vars[idx].gen().context == PGC_POSTMASTER && orig_context == PGC_SIGHUP;
    if prohibit_value_change {
        if current_value_differs(&reg.vars[idx], &newval) {
            reg.vars[idx].gen_mut().status |= GUC_PENDING_RESTART;
            return reject(
                elevel,
                err(
                    ERRCODE_CANT_CHANGE_RUNTIME_PARAM,
                    format!("parameter \"{name}\" cannot be changed without restarting the server"),
                ),
            );
        }
        reg.vars[idx].gen_mut().status &= !GUC_PENDING_RESTART;
        return Ok(-1);
    }

    let make_default_extra = if make_default { newextra.clone() } else { None };
    if change_val {
        let record = &mut reg.vars[idx];
        let mut newly_stacked = false;
        if !make_default {
            let was_empty = record.gen().stack.is_none();
            push_old_value(record, action);
            newly_stacked = was_empty && record.gen().stack.is_some();
        }
        if let Some(hook) = apply_value(record, newval.clone(), newextra, context, srole) {
            deferred_hooks.push(hook);
        }
        reg.set_source(idx, source);
        if newly_stacked {
            reg.note_stacked(idx);
        }

        // Ugly hack (guc.c:4116): SET session_authorization forces SET ROLE
        // NONE with the same lifetime; deferred past the store borrow like the
        // assign hooks (it re-enters set_config_option_global).
        if !is_reload && guc_name_eq(name, "session_authorization") {
            let role_value: Option<&'static str> =
                if value.is_some() { Some("none") } else { None };
            let role_source = if orig_source == PGC_S_OVERRIDE {
                PGC_S_DYNAMIC_DEFAULT
            } else {
                orig_source
            };
            deferred_hooks.push(Box::new(move || {
                let _ = crate::store::set_config_option_global(
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
        make_default_bookkeeping(
            &mut reg.vars[idx],
            &newval,
            make_default_extra,
            source,
            context,
            srole,
        );
    }

    // The guc_report_list link of every C set-site (guc.c:4267).
    if change_val {
        reg.note_reportable(idx);
    }

    Ok(if change_val { 1 } else { -1 })
}

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

// One leader GUC captured for a thread-native parallel worker (§3.4 P-guc):
// the leader-validated value plus its check-hook extra cross by pointer, so
// the worker never reruns check hooks (C contract: check hooks are pure
// validators — all session side effects live in the assign hooks, which DO
// rerun on the worker thread against the shared extra).
pub struct CapturedGuc {
    name: String,
    val: config_var_val,
    extra: Option<SharedExtra>,
    scontext: GucContext,
    source: GucSource,
    srole: Oid,
}

impl CapturedGuc {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The captured (leader-validated) value. Snapshot consumers diff on this
    /// — never on a live process-global backing (the reload dead-diff hazard,
    /// guc::layers).
    pub fn value(&self) -> &config_var_val {
        &self.val
    }

    /// Conservative content equality for the pin re-mint dedup
    /// (layers::current_query_pin): true only when binding `self` and
    /// binding `other` are indistinguishable to a worker. `extra` compares
    /// by Arc identity (a content-equal but re-minted hook extra is a dedup
    /// MISS, never a false hit — int/bool/enum vars, the SET-churn
    /// clientele, carry no extra).
    pub(crate) fn content_eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.val == other.val
            && self.scontext == other.scontext
            && self.source == other.source
            && self.srole == other.srole
            && match (&self.extra, &other.extra) {
                (None, None) => true,
                (Some(a), Some(b)) => std::sync::Arc::ptr_eq(a, b),
                _ => false,
            }
    }
}

fn current_value(record: &GucVariable) -> config_var_val {
    match record {
        GucVariable::Bool(c) => config_var_val::Boolval(current_bool(c)),
        GucVariable::Int(c) => config_var_val::Intval(current_int(c)),
        GucVariable::Real(c) => config_var_val::Realval(current_real(c)),
        GucVariable::String(c) => config_var_val::Stringval(current_string(c)),
        GucVariable::Enum(c) => config_var_val::Enumval(current_enum(c)),
    }
}

fn stored_value(record: &GucVariable) -> config_var_val {
    match record {
        GucVariable::Bool(c) => config_var_val::Boolval(c.value.unwrap_or(c.boot_val)),
        GucVariable::Int(c) => config_var_val::Intval(c.value.unwrap_or(c.boot_val)),
        GucVariable::Real(c) => config_var_val::Realval(c.value.unwrap_or(c.boot_val)),
        GucVariable::String(c) => {
            config_var_val::Stringval(c.value.clone().unwrap_or_else(|| c.boot_val.clone()))
        }
        GucVariable::Enum(c) => config_var_val::Enumval(c.value.unwrap_or(c.boot_val)),
    }
}

pub(crate) fn clone_current_state(reg: &GucRegistry) -> GucRegistry {
    let mut cloned = reg.clone();
    for (dst, src) in cloned.vars.iter_mut().zip(&reg.vars) {
        let value = current_value(src);
        match (dst, value) {
            (GucVariable::Bool(c), config_var_val::Boolval(v)) => c.value = Some(v),
            (GucVariable::Int(c), config_var_val::Intval(v)) => c.value = Some(v),
            (GucVariable::Real(c), config_var_val::Realval(v)) => c.value = Some(v),
            (GucVariable::String(c), config_var_val::Stringval(v)) => c.value = Some(v),
            (GucVariable::Enum(c), config_var_val::Enumval(v)) => c.value = Some(v),
            _ => unreachable!("GUC registry clone changed variable type"),
        }
    }
    cloned
}

pub(crate) fn activate_current_values(
    reg: &mut GucRegistry,
    deferred_hooks: &mut Vec<DeferredAssignHook>,
) {
    for record in &mut reg.vars {
        let value = stored_value(record);
        if !current_value_differs(record, &value) {
            continue;
        }
        let gen = record.gen();
        let extra = gen.extra.clone();
        let context = gen.scontext;
        let role = gen.srole;
        if let Some(hook) = apply_value(record, value, extra, context, role) {
            deferred_hooks.push(hook);
        }
    }
}

// SerializeGUCState, typed: same variable set as capture_nondefault_variables
// (both sides of a launch share the postmaster snapshot as baseline, so
// nondefault-only transfers the whole leader/worker difference).
pub(crate) fn capture_session_gucs(reg: &GucRegistry) -> Vec<CapturedGuc> {
    reg.iter()
        .filter(|v| v.gen().source != PGC_S_DEFAULT)
        .map(|v| CapturedGuc {
            name: v.name().to_string(),
            val: current_value(v),
            extra: v.gen().extra.clone(),
            scontext: v.gen().scontext,
            source: v.gen().source,
            srole: v.gen().srole,
        })
        .collect()
}

// RestoreGUCState for one variable, minus only the parse + check-hook rerun:
// the guard sequence (check_can_set with is_reload semantics, source
// priority, PGC_POSTMASTER reload prohibition) and the end-state writes
// (value, extra, source, scontext, srole, reset_* for source <=
// PGC_S_OVERRIDE, transaction stack push above it, report marking) follow
// set_config_option(value, GUC_ACTION_SET, is_reload=true) exactly.
pub(crate) fn bind_captured_guc(
    reg: &mut GucRegistry,
    cap: &CapturedGuc,
    deferred_hooks: &mut Vec<DeferredAssignHook>,
    exact: bool,
) -> PgResult<()> {
    let elevel = resolve_elevel(ErrorLevel(0), cap.source);
    let idx = match reg.find_index(&cap.name) {
        Some(idx) => idx,
        None => match crate::assignable_custom_variable_name(&cap.name, false) {
            Ok(true) => match reg.add_placeholder_variable(&cap.name) {
                Ok(idx) => idx,
                Err(e) => return reject(elevel, *e).map(drop),
            },
            Ok(false) => return Ok(()),
            Err(e) => return reject(elevel, *e).map(drop),
        },
    };

    match check_can_set(
        reg.vars[idx].gen(),
        false,
        cap.scontext,
        cap.source,
        cap.srole,
        GUC_ACTION_SET,
        true,
        true,
    )? {
        AccessCheck::Ok => {}
        AccessCheck::Skip => return Ok(()),
        AccessCheck::Reject(e) => return reject(elevel, e).map(drop),
    }

    let make_default = cap.source <= PGC_S_OVERRIDE;
    let mut change_val = true;
    if !exact && reg.vars[idx].gen().source > cap.source {
        if !make_default {
            return Ok(());
        }
        change_val = false;
    }

    if reg.vars[idx].gen().context == PGC_POSTMASTER && cap.scontext == PGC_SIGHUP {
        if current_value_differs(&reg.vars[idx], &cap.val) {
            reg.vars[idx].gen_mut().status |= GUC_PENDING_RESTART;
            return reject(
                elevel,
                err(
                    ERRCODE_CANT_CHANGE_RUNTIME_PARAM,
                    format!(
                        "parameter \"{}\" cannot be changed without restarting the server",
                        cap.name
                    ),
                ),
            )
            .map(drop);
        }
        reg.vars[idx].gen_mut().status &= !GUC_PENDING_RESTART;
        return Ok(());
    }

    if change_val {
        let record = &mut reg.vars[idx];
        let mut newly_stacked = false;
        if !make_default {
            let was_empty = record.gen().stack.is_none();
            push_old_value(record, GUC_ACTION_SET);
            newly_stacked = was_empty && record.gen().stack.is_some();
        }
        if let Some(hook) = apply_value(
            record,
            cap.val.clone(),
            cap.extra.clone(),
            cap.scontext,
            cap.srole,
        ) {
            deferred_hooks.push(hook);
        }
        reg.set_source(idx, cap.source);
        if newly_stacked {
            reg.note_stacked(idx);
        }
    }
    if make_default {
        make_default_bookkeeping(
            &mut reg.vars[idx],
            &cap.val,
            cap.extra.clone(),
            cap.source,
            cap.scontext,
            cap.srole,
        );
    }
    if change_val {
        reg.note_reportable(idx);
    }
    Ok(())
}

fn reset_value_and_extra(record: &GucVariable) -> (config_var_val, Option<SharedExtra>) {
    match record {
        GucVariable::Bool(c) => (config_var_val::Boolval(c.reset_val), c.reset_extra.clone()),
        GucVariable::Int(c) => (config_var_val::Intval(c.reset_val), c.reset_extra.clone()),
        GucVariable::Real(c) => (config_var_val::Realval(c.reset_val), c.reset_extra.clone()),
        GucVariable::String(c) => (
            config_var_val::Stringval(c.reset_val.clone()),
            c.reset_extra.clone(),
        ),
        GucVariable::Enum(c) => (config_var_val::Enumval(c.reset_val), c.reset_extra.clone()),
    }
}

fn current_value_differs(record: &GucVariable, newval: &config_var_val) -> bool {
    match (record, newval) {
        (GucVariable::Bool(c), config_var_val::Boolval(nv)) => current_bool(c) != *nv,
        (GucVariable::Int(c), config_var_val::Intval(nv)) => current_int(c) != *nv,
        (GucVariable::Real(c), config_var_val::Realval(nv)) => current_real(c) != *nv,
        (GucVariable::Enum(c), config_var_val::Enumval(nv)) => current_enum(c) != *nv,
        (GucVariable::String(c), config_var_val::Stringval(nv)) => match (current_string(c), nv) {
            (Some(a), Some(b)) => a != *b,
            (None, None) => false,
            _ => true,
        },
        _ => true,
    }
}

// C fires the assign hook, then writes *conf->variable (guc.c:3759). Here the
// value is written inline and the hook is deferred past the store borrow: a
// hook may recursively re-enter set_config_option (assign_session_authorization
// -> SetOuterUserId -> SetConfigOption("is_superuser")), which would re-borrow
// the RefCell store.
fn apply_value(
    record: &mut GucVariable,
    newval: config_var_val,
    extra: Option<SharedExtra>,
    context: GucContext,
    srole: Oid,
) -> Option<DeferredAssignHook> {
    let deferred: Option<DeferredAssignHook> = match (&mut *record, newval) {
        (GucVariable::Bool(c), config_var_val::Boolval(v)) => {
            c.value = Some(v);
            if c.variable.installed() {
                c.variable.write(v);
            }
            installed_hook(c.assign_hook).map(|f| {
                let extra = extra.clone();
                Box::new(move || f(v, extra.as_deref())) as DeferredAssignHook
            })
        }
        (GucVariable::Int(c), config_var_val::Intval(v)) => {
            c.value = Some(v);
            if c.variable.installed() {
                c.variable.write(v);
            }
            installed_hook(c.assign_hook).map(|f| {
                let extra = extra.clone();
                Box::new(move || f(v, extra.as_deref())) as DeferredAssignHook
            })
        }
        (GucVariable::Real(c), config_var_val::Realval(v)) => {
            c.value = Some(v);
            if c.variable.installed() {
                c.variable.write(v);
            }
            installed_hook(c.assign_hook).map(|f| {
                let extra = extra.clone();
                Box::new(move || f(v, extra.as_deref())) as DeferredAssignHook
            })
        }
        (GucVariable::String(c), config_var_val::Stringval(s)) => {
            c.value = Some(s.clone());
            if c.variable.installed() {
                c.variable.write(s.clone());
            }
            installed_hook(c.assign_hook).map(|f| {
                let extra = extra.clone();
                Box::new(move || f(s.as_deref(), extra.as_deref())) as DeferredAssignHook
            })
        }
        (GucVariable::Enum(c), config_var_val::Enumval(v)) => {
            c.value = Some(v);
            if c.variable.installed() {
                c.variable.write(v);
            }
            installed_hook(c.assign_hook).map(|f| {
                let extra = extra.clone();
                Box::new(move || f(v, extra.as_deref())) as DeferredAssignHook
            })
        }
        _ => None,
    };

    let gen = record.gen_mut();
    gen.extra = extra;
    gen.scontext = context;
    gen.srole = srole;
    // gen.source is set by the caller through GucRegistry::set_source.

    deferred
}

fn installed_hook<T: Copy + 'static>(slot: Option<&'static guc_tables::GucSlot<T>>) -> Option<T> {
    let slot = slot?;
    if slot.installed() {
        Some(slot.get())
    } else {
        None
    }
}

fn make_default_bookkeeping(
    record: &mut GucVariable,
    newval: &config_var_val,
    extra: Option<SharedExtra>,
    source: GucSource,
    context: GucContext,
    srole: Oid,
) {
    if record.gen().reset_source <= source {
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
            _ => return,
        }
        let gen = record.gen_mut();
        gen.reset_source = source;
        gen.reset_scontext = context;
        gen.reset_srole = srole;
    }
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

// set_stack_value (guc.c:812).
fn set_stack_value(record: &GucVariable, val: &mut config_var_value) {
    val.val = Some(match record {
        GucVariable::Bool(c) => config_var_val::Boolval(current_bool(c)),
        GucVariable::Int(c) => config_var_val::Intval(current_int(c)),
        GucVariable::Real(c) => config_var_val::Realval(current_real(c)),
        GucVariable::String(c) => config_var_val::Stringval(current_string(c)),
        GucVariable::Enum(c) => config_var_val::Enumval(current_enum(c)),
    });
    val.extra = record.gen().extra.clone();
}

fn discard_stack_value(val: &mut config_var_value) {
    val.val = None;
    val.extra = None;
}

// push_old_value (guc.c:2134).
fn push_old_value(record: &mut GucVariable, action: GucAction) {
    let nest_level = crate::guc_nest_level();
    if nest_level == 0 {
        return;
    }

    let has_current = record
        .gen()
        .stack
        .as_ref()
        .is_some_and(|s| s.nest_level >= nest_level);
    if has_current {
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
                if stack.state == GUC_SET_LOCAL {
                    discard_stack_value(&mut stack.masked);
                }
                stack.state = GUC_SET;
            }
            GUC_ACTION_LOCAL => {
                if stack.state == GUC_SET {
                    let (sc, sr, v) = masked_snapshot.unwrap();
                    stack.masked_scontext = sc;
                    stack.masked_srole = sr;
                    stack.masked = v;
                    stack.state = GUC_SET_LOCAL;
                }
            }
            _ => {
                debug_assert!(stack.state == GUC_SAVE);
            }
        }
        return;
    }

    let mut prior = config_var_value::default();
    set_stack_value(record, &mut prior);

    let gen = record.gen_mut();
    let prev = gen.stack.take();
    let state = match action {
        GUC_ACTION_SET => GUC_SET,
        GUC_ACTION_LOCAL => GUC_LOCAL,
        _ => GUC_SAVE,
    };
    gen.stack = Some(Box::new(GucStack {
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
    }));
}

fn restore_stacked_value(
    record: &mut GucVariable,
    newvalue: &config_var_value,
    deferred_hooks: &mut Vec<DeferredAssignHook>,
) -> bool {
    let newextra = newvalue.extra.clone();
    let mut changed = false;
    match (record, newvalue.val.as_ref()) {
        (GucVariable::Bool(c), Some(config_var_val::Boolval(nv))) => {
            if current_bool(c) != *nv || extra_differs(&c.gen.extra, &newextra) {
                if let Some(f) = installed_hook(c.assign_hook) {
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
            if current_int(c) != *nv || extra_differs(&c.gen.extra, &newextra) {
                if let Some(f) = installed_hook(c.assign_hook) {
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
            if current_real(c) != *nv || extra_differs(&c.gen.extra, &newextra) {
                if let Some(f) = installed_hook(c.assign_hook) {
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
            if differs || extra_differs(&c.gen.extra, &newextra) {
                if let Some(f) = installed_hook(c.assign_hook) {
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
            if current_enum(c) != *nv || extra_differs(&c.gen.extra, &newextra) {
                if let Some(f) = installed_hook(c.assign_hook) {
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

// C compares extra pointers; Rc gives pointer identity back.
fn extra_differs(cur: &Option<SharedExtra>, new: &Option<SharedExtra>) -> bool {
    match (cur, new) {
        (Some(a), Some(b)) => !std::sync::Arc::ptr_eq(a, b),
        (None, None) => false,
        _ => true,
    }
}

// The per-variable walk of AtEOXact_GUC (guc.c:2262); the caller owns the
// GUCNestLevel update.
pub fn at_eoxact_guc(
    reg: &mut GucRegistry,
    is_commit: bool,
    nest_level: i32,
    deferred_hooks: &mut Vec<DeferredAssignHook>,
) {
    debug_assert!(nest_level > 0);
    let indices = core::mem::take(&mut reg.stacked);
    let mut kept: Vec<usize> = Vec::with_capacity(indices.len());
    let mut to_report: Vec<usize> = Vec::new();
    for idx in indices {
        let (changed, newsource) =
            pop_var_stack(&mut reg.vars[idx], is_commit, nest_level, deferred_hooks);
        if let Some(src) = newsource {
            reg.set_source(idx, src);
        }
        if changed && reg.vars[idx].gen().flags & GUC_REPORT != 0 {
            to_report.push(idx);
        }
        if reg.vars[idx].gen().stack.is_some() {
            kept.push(idx);
        }
    }
    reg.stacked = kept;
    for idx in to_report {
        reg.note_reportable(idx);
    }
}

// The stack-popping loop body. Returns (value changed at any level, the last
// restored source if any restore happened).
fn pop_var_stack(
    record: &mut GucVariable,
    is_commit: bool,
    nest_level: i32,
    deferred_hooks: &mut Vec<DeferredAssignHook>,
) -> (bool, Option<GucSource>) {
    let mut any_changed = false;
    let mut last_source: Option<GucSource> = None;
    loop {
        let top_level = match record.gen().stack.as_ref() {
            Some(s) if s.nest_level >= nest_level => s.nest_level,
            _ => break,
        };

        let mut stack = record.gen_mut().stack.take().unwrap();
        let prev_opt = stack.prev.take();
        let prev_level = prev_opt.as_ref().map(|p| p.nest_level);

        let mut restore_prior = false;
        let mut restore_masked = false;

        if !is_commit {
            restore_prior = true;
        } else if stack.state == GUC_SAVE {
            restore_prior = true;
        } else if stack.nest_level == 1 {
            // Transaction commit.
            if stack.state == GUC_SET_LOCAL {
                restore_masked = true;
            } else if stack.state == GUC_SET {
                discard_stack_value(&mut stack.prior);
            } else {
                restore_prior = true;
            }
        } else if prev_opt.is_none() || prev_level.unwrap() < stack.nest_level - 1 {
            // Decrement entry's level and do not pop it.
            stack.nest_level = top_level - 1;
            stack.prev = prev_opt;
            record.gen_mut().stack = Some(stack);
            continue;
        } else {
            // Merge this stack entry into prev.
            let mut prev = prev_opt.unwrap();
            match stack.state {
                GUC_SET => {
                    discard_stack_value(&mut stack.prior);
                    if prev.state == GUC_SET_LOCAL {
                        discard_stack_value(&mut prev.masked);
                    }
                    prev.state = GUC_SET;
                }
                GUC_LOCAL => {
                    if prev.state == GUC_SET {
                        prev.masked_scontext = stack.scontext;
                        prev.masked_srole = stack.srole;
                        prev.masked = core::mem::take(&mut stack.prior);
                        prev.state = GUC_SET_LOCAL;
                    } else {
                        discard_stack_value(&mut stack.prior);
                    }
                }
                GUC_SET_LOCAL => {
                    discard_stack_value(&mut stack.prior);
                    prev.masked_scontext = stack.masked_scontext;
                    prev.masked_srole = stack.masked_srole;
                    if prev.state == GUC_SET_LOCAL {
                        discard_stack_value(&mut prev.masked);
                    }
                    prev.masked = core::mem::take(&mut stack.masked);
                    prev.state = GUC_SET_LOCAL;
                }
                _ => debug_assert!(false, "GUC_SAVE can't get here"),
            }
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

            discard_stack_value(&mut stack.prior);
            discard_stack_value(&mut stack.masked);

            last_source = Some(newsource);
            let gen = record.gen_mut();
            gen.scontext = newscontext;
            gen.srole = newsrole;
        }

        record.gen_mut().stack = prev_opt;
        any_changed |= changed;
    }
    (any_changed, last_source)
}

// ResetAllOptions (guc.c:2003): walks only guc_nondef_list.
pub fn reset_all_options(reg: &mut GucRegistry) {
    use types_guc::GUC_NO_RESET_ALL;

    let nondef = reg.nondef.clone();
    for idx in nondef {
        let gen = reg.vars[idx].gen();
        if gen.context != PGC_SUSET && gen.context != PGC_USERSET {
            continue;
        }
        if gen.flags & GUC_NO_RESET_ALL != 0 {
            continue;
        }
        if gen.source <= PGC_S_OVERRIDE {
            continue;
        }

        let was_empty = reg.vars[idx].gen().stack.is_none();
        push_old_value(&mut reg.vars[idx], GUC_ACTION_SET);
        if was_empty && reg.vars[idx].gen().stack.is_some() {
            reg.note_stacked(idx);
        }
        reset_one(&mut reg.vars[idx]);

        let reset_source = reg.vars[idx].gen().reset_source;
        reg.set_source(idx, reset_source);
        reg.note_reportable(idx);
    }
}

// The RESET ALL per-type body: reset_val re-applied with reset_extra.
fn reset_one(var: &mut GucVariable) {
    match var {
        GucVariable::Bool(c) => {
            if let Some(f) = installed_hook(c.assign_hook) {
                f(c.reset_val, c.reset_extra.as_deref());
            }
            c.value = Some(c.reset_val);
            if c.variable.installed() {
                c.variable.write(c.reset_val);
            }
            c.gen.extra = c.reset_extra.clone();
        }
        GucVariable::Int(c) => {
            if let Some(f) = installed_hook(c.assign_hook) {
                f(c.reset_val, c.reset_extra.as_deref());
            }
            c.value = Some(c.reset_val);
            if c.variable.installed() {
                c.variable.write(c.reset_val);
            }
            c.gen.extra = c.reset_extra.clone();
        }
        GucVariable::Real(c) => {
            if let Some(f) = installed_hook(c.assign_hook) {
                f(c.reset_val, c.reset_extra.as_deref());
            }
            c.value = Some(c.reset_val);
            if c.variable.installed() {
                c.variable.write(c.reset_val);
            }
            c.gen.extra = c.reset_extra.clone();
        }
        GucVariable::String(c) => {
            if let Some(f) = installed_hook(c.assign_hook) {
                f(c.reset_val.as_deref(), c.reset_extra.as_deref());
            }
            c.value = Some(c.reset_val.clone());
            if c.variable.installed() {
                c.variable.write(c.reset_val.clone());
            }
            c.gen.extra = c.reset_extra.clone();
        }
        GucVariable::Enum(c) => {
            if let Some(f) = installed_hook(c.assign_hook) {
                f(c.reset_val, c.reset_extra.as_deref());
            }
            c.value = Some(c.reset_val);
            if c.variable.installed() {
                c.variable.write(c.reset_val);
            }
            c.gen.extra = c.reset_extra.clone();
        }
    }
    let reset_scontext = var.gen().reset_scontext;
    let reset_srole = var.gen().reset_srole;
    let gen = var.gen_mut();
    gen.scontext = reset_scontext;
    gen.srole = reset_srole;
}
