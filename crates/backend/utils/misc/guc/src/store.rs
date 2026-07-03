use std::cell::{Cell, RefCell};

use guc_tables::{all_settings, GucDefaultValue, GucSetting};
use types_core::Oid;
use types_error::{ErrorLevel, PgResult, FATAL};
use types_guc::{config_type, GucContext, GucSource, PGC_POSTMASTER, PGC_S_ENV_VAR, PGC_S_OVERRIDE};

use crate::model::{config_bool, config_enum, config_generic, config_int, config_real, config_string};
use crate::registry::{DeferredAssignHook, GucRegistry, GucVariable};

thread_local! {
    // guc_hashtab + the runtime records; one backend = one thread, so this
    // thread_local IS the C file-static store. RefCell: re-entrant reads free,
    // re-entrant writes fail fast (assign hooks are deferred past the borrow).
    static GUC_STORE: RefCell<Option<GucRegistry>> = const { RefCell::new(None) };
    // TimestampTz PgReloadTime (guc.c).
    static PG_RELOAD_TIME: Cell<i64> = const { Cell::new(0) };
    // Fast flags for the per-statement no-op paths (C reads a bare list head;
    // reaching ours costs a RefCell borrow + Option). Contract: a flag is
    // NEVER false while the TLS store's list is non-empty; stale TRUE is fine
    // (the slow path re-checks the real list).
    static HAS_STACKED_HINT: Cell<bool> = const { Cell::new(false) };
    static REPORT_PENDING_HINT: Cell<bool> = const { Cell::new(false) };
}

#[inline]
pub(crate) fn has_stacked_hint() -> bool {
    HAS_STACKED_HINT.get()
}

#[inline]
pub(crate) fn set_has_stacked_hint(v: bool) {
    HAS_STACKED_HINT.set(v);
}

#[inline]
pub(crate) fn report_pending_hint() -> bool {
    REPORT_PENDING_HINT.get()
}

#[inline]
pub(crate) fn set_report_pending_hint(v: bool) {
    REPORT_PENDING_HINT.set(v);
}

pub fn set_pg_reload_time(t: i64) {
    PG_RELOAD_TIME.set(t);
}

pub fn pg_reload_time() -> i64 {
    PG_RELOAD_TIME.get()
}

fn build_variable(setting: GucSetting) -> Option<GucVariable> {
    let name = setting.name();
    let gen = |vartype: config_type| {
        config_generic::boot(
            name,
            setting.context(),
            setting.group(),
            None,
            None,
            setting.flags(),
            vartype,
        )
    };
    Some(match (setting, setting.default_value()) {
        (GucSetting::Bool(s), GucDefaultValue::Bool(b)) => GucVariable::Bool(config_bool {
            gen: gen(types_guc::PGC_BOOL),
            variable: s.variable,
            value: Some(b),
            boot_val: b,
            check_hook: s.check_hook,
            assign_hook: s.assign_hook,
            show_hook: s.show_hook,
            reset_val: b,
            reset_extra: None,
        }),
        (GucSetting::Int(s), GucDefaultValue::Int(i)) => GucVariable::Int(config_int {
            gen: gen(types_guc::PGC_INT),
            variable: s.variable,
            value: Some(i),
            boot_val: i,
            min: s.min,
            max: s.max,
            check_hook: s.check_hook,
            assign_hook: s.assign_hook,
            show_hook: s.show_hook,
            reset_val: i,
            reset_extra: None,
        }),
        (GucSetting::Real(s), GucDefaultValue::Real(r)) => GucVariable::Real(config_real {
            gen: gen(types_guc::PGC_REAL),
            variable: s.variable,
            value: Some(r),
            boot_val: r,
            min: s.min,
            max: s.max,
            check_hook: s.check_hook,
            assign_hook: s.assign_hook,
            show_hook: s.show_hook,
            reset_val: r,
            reset_extra: None,
        }),
        (GucSetting::String(s), GucDefaultValue::String(v)) => {
            let v: Option<String> = v.map(|s| s.to_string());
            GucVariable::String(config_string {
                gen: gen(types_guc::PGC_STRING),
                variable: s.variable,
                value: Some(v.clone()),
                boot_val: v.clone(),
                check_hook: s.check_hook,
                assign_hook: s.assign_hook,
                show_hook: s.show_hook,
                reset_val: v,
                reset_extra: None,
            })
        }
        (GucSetting::Enum(s), GucDefaultValue::Enum(e)) => GucVariable::Enum(config_enum {
            gen: gen(types_guc::PGC_ENUM),
            variable: s.variable,
            value: Some(e),
            boot_val: e,
            options: s.options,
            check_hook: s.check_hook,
            assign_hook: s.assign_hook,
            show_hook: s.show_hook,
            reset_val: e,
            reset_extra: None,
        }),
        _ => return None,
    })
}

// InitializeGUCOptions (guc.c:1530).
pub fn initialize_guc_options() -> PgResult<()> {
    initialize_guc_options_impl(|_| true)
}

// Child-thread bring-up (SubPostmasterMain shape). C's re-init writes only the
// child's own address space; our backing vars are process-shared, so boot
// values must not be published for variables the snapshot restore is about to
// overwrite — other threads (the postmaster's maybe_adjust_io_workers) read
// them concurrently.
pub fn initialize_guc_options_for_child(snapshot: &[NondefaultGuc]) -> PgResult<()> {
    initialize_guc_options_impl(|name| !snapshot.iter().any(|v| v.name == name))
}

fn initialize_guc_options_impl(publish: impl Fn(&str) -> bool) -> PgResult<()> {
    // Before log_line_prefix-style GUCs can demand elog timestamps.
    pgtz::pg_timezone_initialize();

    let mut reg = GucRegistry::new();
    for setting in all_settings() {
        let Some(mut var) = build_variable(setting) else { continue };
        // A check-hook failure on a boot value is C's elog(FATAL, "failed to
        // initialize %s to ...").
        crate::registry::initialize_one_guc_option_hooks(&mut var, publish(setting.name()))
            .map_err(|e| {
            Box::new(
                elog::ereport(FATAL)
                    .errmsg(format!(
                        "failed to initialize {} to {:?}: {}",
                        var.name(),
                        setting.default_value(),
                        e.message()
                    ))
                    .into_error(),
            )
        })?;
        reg.define(var)?;
    }
    GUC_STORE.with(|c| *c.borrow_mut() = Some(reg));

    crate::report::set_reporting_enabled(false);

    // Prevent any attempt to override the transaction modes from
    // non-interactive sources.
    crate::SetConfigOption("transaction_isolation", Some("read committed"), PGC_POSTMASTER, PGC_S_OVERRIDE)?;
    crate::SetConfigOption("transaction_read_only", Some("no"), PGC_POSTMASTER, PGC_S_OVERRIDE)?;
    crate::SetConfigOption("transaction_deferrable", Some("no"), PGC_POSTMASTER, PGC_S_OVERRIDE)?;

    initialize_guc_options_from_environment()?;
    Ok(())
}

// InitializeGUCOptionsFromEnvironment (guc.c:1589). The stack-rlimit branch
// needs get_stack_depth_rlimit (ported in stack_depth, which depends on guc);
// deferred for layering.
pub fn initialize_guc_options_from_environment() -> PgResult<()> {
    if let Ok(env) = std::env::var("PGPORT") {
        crate::SetConfigOption("port", Some(&env), PGC_POSTMASTER, PGC_S_ENV_VAR)?;
    }
    if let Ok(env) = std::env::var("PGDATESTYLE") {
        crate::SetConfigOption("datestyle", Some(&env), PGC_POSTMASTER, PGC_S_ENV_VAR)?;
    }
    if let Ok(env) = std::env::var("PGCLIENTENCODING") {
        crate::SetConfigOption("client_encoding", Some(&env), PGC_POSTMASTER, PGC_S_ENV_VAR)?;
    }
    Ok(())
}

pub fn is_initialized() -> bool {
    GUC_STORE.with(|c| c.borrow().is_some())
}

pub struct NondefaultGuc {
    pub name: String,
    pub value: Option<String>,
    pub scontext: GucContext,
    pub source: GucSource,
    pub srole: Oid,
}

// write_nondefault_variables (guc.c): the EXEC_BACKEND parameter file,
// rendered as an in-memory snapshot taken on the spawning (postmaster)
// thread; backend threads have no fork to inherit the TLS store through.
pub fn capture_nondefault_variables() -> Vec<NondefaultGuc> {
    with_store(|reg| {
        reg.iter()
            .filter(|v| v.gen().source != types_guc::GucSource::PGC_S_DEFAULT)
            .map(|v| NondefaultGuc {
                name: v.name().to_string(),
                value: Some(crate::registry::show_guc_option(v, false)),
                scontext: v.gen().scontext,
                source: v.gen().source,
                srole: v.gen().srole,
            })
            .collect()
    })
    .unwrap_or_default()
}

// read_nondefault_variables (guc.c): InitializeGUCOptions must already have
// run on this thread (SubPostmasterMain order).
pub fn restore_nondefault_variables(vars: &[NondefaultGuc]) -> PgResult<()> {
    for v in vars {
        crate::set_config_option_ext(
            &v.name,
            v.value.as_deref(),
            v.scontext,
            v.source,
            v.srole,
            crate::GUC_ACTION_SET,
            true,
            ErrorLevel(0),
            true,
        )?;
    }
    Ok(())
}

pub fn with_store<R>(f: impl FnOnce(&GucRegistry) -> R) -> Option<R> {
    GUC_STORE.with(|c| {
        let guard = c.borrow();
        Some(f(guard.as_ref()?))
    })
}

pub fn with_store_mut<R>(f: impl FnOnce(&mut GucRegistry) -> R) -> Option<R> {
    GUC_STORE.with(|c| {
        let mut guard = c.borrow_mut();
        Some(f(guard.as_mut()?))
    })
}

#[cold]
fn store_uninitialized(name: &str) -> ! {
    panic!("GUC store access for {name:?} before InitializeGUCOptions built it")
}

fn lookup_var<R>(name: &str, pick: impl FnOnce(&GucVariable) -> Option<R>) -> Option<R> {
    with_store(|reg| reg.find_option(name).and_then(pick))
        .unwrap_or_else(|| store_uninitialized(name))
}

pub fn get_bool(name: &str) -> Option<bool> {
    lookup_var(name, |v| match v {
        GucVariable::Bool(c) => c.value,
        _ => None,
    })
}

pub fn get_int(name: &str) -> Option<i32> {
    lookup_var(name, |v| match v {
        GucVariable::Int(c) => c.value,
        _ => None,
    })
}

pub fn get_real(name: &str) -> Option<f64> {
    lookup_var(name, |v| match v {
        GucVariable::Real(c) => c.value,
        _ => None,
    })
}

pub fn get_enum(name: &str) -> Option<i32> {
    lookup_var(name, |v| match v {
        GucVariable::Enum(c) => c.value,
        _ => None,
    })
}

pub fn get_string(name: &str) -> Option<Option<String>> {
    lookup_var(name, |v| match v {
        GucVariable::String(c) => c.value.clone(),
        _ => None,
    })
}

// set_config_option over the global store; assign hooks fire after the borrow
// is released (they may recursively re-enter, e.g. session_authorization ->
// is_superuser).
#[allow(clippy::too_many_arguments)]
pub fn set_config_option_global(
    name: &str,
    value: Option<&str>,
    context: GucContext,
    source: GucSource,
    srole: Oid,
    action: crate::registry::GucAction,
    change_val: bool,
    elevel: ErrorLevel,
    is_reload: bool,
) -> PgResult<i32> {
    let mut deferred_hooks: Vec<DeferredAssignHook> = Vec::new();
    let result = with_store_mut(|reg| {
        crate::registry::set_config_option(
            reg,
            name,
            value,
            context,
            source,
            srole,
            action,
            change_val,
            elevel,
            is_reload,
            &mut deferred_hooks,
        )
    })
    .unwrap_or_else(|| store_uninitialized(name));

    for hook in deferred_hooks {
        hook();
    }

    result
}

pub fn reset_all_options() {
    with_store_mut(crate::registry::reset_all_options)
        .unwrap_or_else(|| store_uninitialized("ResetAllOptions"));
}
