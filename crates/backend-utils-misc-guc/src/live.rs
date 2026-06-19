//! The **unified live GUC variable store** — the idiomatic analog of `guc.c`'s
//! single `guc_hashtab` of `struct config_generic *`, boot-initialized by
//! `InitializeGUCOptions` / `InitializeOneGUCOption`.
//!
//! The single store is the process-global [`GucRegistry`]. The boot init
//! ([`initialize_guc_options`], the `InitializeGUCOptions` analog) builds every
//! `config_*` record from [`backend_utils_misc_guc_tables`] and seeds each
//! record's `value`/`reset_val` from its compiled-in `boot_val` (the
//! `InitializeOneGUCOption` body, `*conf->variable = conf->reset_val =
//! boot_val`). The typed accessors below read **through** that store, so a `SET`
//! applied via [`set_config_option_global`] is immediately visible to every
//! value seam; reporting ([`crate::report`]) walks the same store.
//!
//! # Reconciliation with the restructured `guc_tables`
//!
//! Unlike the prior idiomatic tree (whose `guc_tables` carried unresolved
//! `IntExpr`/`EnumExpr`/`RealExpr` boot-value *placeholder strings* and symbolic
//! min/max *expression strings*), this repo's [`backend_utils_misc_guc_tables`]
//! is **fully resolved**: every `boot_val` is a concrete
//! [`GucDefaultValue::Bool`]/`Int`/`Real`/`String`/`Enum` (the C `#define`d
//! default, evaluated in `consts.rs`), and every int/real `min`/`max` is a real
//! `i32`/`f64`. So the prior tree's two documented honest deferrals —
//! *unresolved boot placeholders yield `variable = None` (skipped, never
//! guessed)* and *symbolic min/max fall back to the widest bound* — are **moot
//! here**: those settings (`checkpoint_flush_after`, `bgwriter_flush_after`, …)
//! now seed their real boot values and carry exact bounds, strictly improving
//! fidelity. The build still *skips* (never fabricates) any setting whose boot
//! value cannot be turned into a typed value — but with the resolved tables this
//! case does not arise.
//!
//! Hooks (check/assign/show) and the storage variable are not fn-pointer fields
//! of the record here; they are the typed install-once *slots* the `guc_tables`
//! `GucSetting` references, carried straight into the live record (see
//! [`crate::model`]). The registry calls them through those slots, the faithful
//! analog of the C record carrying the hook/variable pointers — so the prior
//! tree's `assign_interval_style`/`assign_datestyle_str`/`assign_timezone_str`
//! special-case seams are gone: an applied SET runs the variable's real
//! `assign_*` hook slot (when its owner installed it) and writes the owner's
//! storage through the `vars` slot, exactly as C's address-shared
//! `*conf->variable = newval` plus `assign_hook` does.
//!
//! # Storage model
//!
//! GUC state is process-local (not shared memory), exactly as `guc.c`'s file
//! statics are. The backend reaches `initialize_guc_options` single-threaded at
//! startup. The store is held in a process-global [`Mutex`] (the safe analog of
//! the C file-static `guc_hashtab`): one store per process, written once at boot
//! and thereafter mutated only by the `SET` path, but read/written safely even
//! when the broad test harness drives it from several threads.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;

use backend_utils_misc_guc_tables::{all_settings, GucDefaultValue, GucSetting};
use types_core::{Oid, TimestampTz};
use types_error::{ErrorLevel, PgResult};
use types_guc::{config_type, GucContext, GucSource};

use crate::model::{config_bool, config_enum, config_generic, config_int, config_real, config_string};
use crate::registry::{GucRegistry, GucVariable};

/// The process-global GUC store: the idiomatic `guc_hashtab`. A `Mutex<Option>`
/// — `None` until boot's `initialize_guc_options` builds it — gives the safe
/// process-global single store the C file static is, with interior mutability
/// for the `SET` write path.
static GUC_STORE: Mutex<Option<GucRegistry>> = Mutex::new(None);

/// `TimestampTz PgReloadTime` (guc.c file static): the time of the most recent
/// successful config-file load, surfaced by `pg_conf_load_time()`.
static PG_RELOAD_TIME: AtomicI64 = AtomicI64::new(0);

/// `PgReloadTime = GetCurrentTimestamp()` — record when the config file was last
/// successfully (re)loaded. The caller supplies the timestamp.
pub fn set_pg_reload_time(t: TimestampTz) {
    PG_RELOAD_TIME.store(t, Ordering::Relaxed);
}

/// Read `PgReloadTime` (the `pg_conf_load_time()` source value).
pub fn pg_reload_time() -> TimestampTz {
    PG_RELOAD_TIME.load(Ordering::Relaxed)
}

/// One resolved boot value (the result of evaluating a `guc_tables` `boot_val`).
enum BootValue {
    Bool(bool),
    Int(i32),
    Real(f64),
    String(Option<String>),
    Enum(i32),
}

/// Resolve a `guc_tables` setting's `boot_val` into a concrete [`BootValue`].
///
/// The resolved `guc_tables` only carries the five literal forms, so every
/// setting resolves; the `None` arm is unreachable with the current tables and
/// exists only so an unexpected future variant is *skipped*, never guessed.
fn resolve_boot_value(setting: GucSetting) -> Option<BootValue> {
    match setting.default_value() {
        GucDefaultValue::Bool(b) => Some(BootValue::Bool(b)),
        GucDefaultValue::Int(i) => Some(BootValue::Int(i)),
        GucDefaultValue::Real(r) => Some(BootValue::Real(r)),
        GucDefaultValue::String(s) => Some(BootValue::String(s.map(|v| v.to_string()))),
        GucDefaultValue::Enum(e) => Some(BootValue::Enum(e)),
    }
}

/// Build one [`GucVariable`] from a `guc_tables` setting, with `value` /
/// `reset_val` seeded from the resolved boot value (the `InitializeOneGUCOption`
/// `*conf->variable = conf->reset_val = boot_val` step), and the hook/variable
/// slots carried from the table.
fn build_variable(setting: GucSetting) -> Option<GucVariable> {
    let name = setting.name();
    let boot = resolve_boot_value(setting)?;
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
    Some(match (setting, boot) {
        (GucSetting::Bool(s), BootValue::Bool(b)) => GucVariable::Bool(config_bool {
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
        (GucSetting::Int(s), BootValue::Int(i)) => GucVariable::Int(config_int {
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
        (GucSetting::Real(s), BootValue::Real(r)) => GucVariable::Real(config_real {
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
        (GucSetting::String(s), BootValue::String(v)) => GucVariable::String(config_string {
            gen: gen(types_guc::PGC_STRING),
            variable: s.variable,
            value: Some(v.clone()),
            boot_val: v.clone(),
            check_hook: s.check_hook,
            assign_hook: s.assign_hook,
            show_hook: s.show_hook,
            reset_val: v,
            reset_extra: None,
        }),
        (GucSetting::Enum(s), BootValue::Enum(e)) => GucVariable::Enum(config_enum {
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
        // The kind tag and the resolved boot value always agree; a mismatch is
        // unreachable. Skip rather than guess.
        _ => return None,
    })
}

/// `InitializeGUCOptions()` (guc.c:1530), boot-scope subset.
///
/// `build_guc_variables()` + the `InitializeOneGUCOption` loop: build a
/// `config_*` record for every `guc_tables` setting and seed each one's live
/// `value`/`reset_val` from its compiled-in `boot_val`.
///
/// Idempotent: a second call rebuilds the store from scratch. Call once,
/// single-threaded, at startup.
pub fn initialize_guc_options() {
    try_initialize_guc_options()
        .unwrap_or_else(|_| panic!("initialize_guc_options: out of memory building the GUC store"));
}

/// Fallible [`initialize_guc_options`].
pub fn try_initialize_guc_options() -> PgResult<()> {
    let mut reg = GucRegistry::new();
    for setting in all_settings() {
        if let Some(var) = build_variable(setting) {
            // `InitializeOneGUCOption` (guc.c): after seeding `*conf->variable =
            // conf->reset_val = boot_val`, C fires the variable's `assign_hook`
            // with the boot value so hook-side global state derived from a GUC
            // (e.g. `SyncRepWaitMode` from `assign_synchronous_commit`, which
            // commit-time sync-rep indexes `WalSndCtl->lsn[mode]` with — a stale
            // `-1` sentinel otherwise crashes the commit path) is initialized to
            // agree with the boot value. C runs this for every variable that has
            // an `assign_hook`.
            //
            // We fire each variable's boot hooks exactly as C's
            // `InitializeOneGUCOption` does: run the variable's own check hook on
            // the boot value to produce the `extra` payload, then call the assign
            // hook with `(boot_val, extra)`. The earlier port called the assign
            // hook with `extra = None`, which made every extra-consuming hook
            // (DateStyle/TimeZone/log_timezone/client_encoding/role/seed/
            // log_destination) panic at boot trying to downcast a missing payload.
            //
            // The call is still wrapped in `catch_unwind`: a few assign hooks have
            // genuinely-unported bodies that `panic!("… not yet ported")` (they
            // panic on a runtime `SET` for the same reason), so catching here
            // defers that one hook's boot side effect — no worse than before,
            // where no hook ran at all — while every fully-ported hook (and now
            // every extra-consuming hook with a ported check hook) runs and seeds
            // its hook-side global state, which the sync-rep commit invariant
            // needs.
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = crate::registry::initialize_one_guc_option_hooks(&var);
            }));
            reg.define(var)?;
        }
    }
    *GUC_STORE.lock().unwrap() = Some(reg);
    Ok(())
}

/// True once [`initialize_guc_options`] has built the store.
pub fn is_initialized() -> bool {
    GUC_STORE.lock().unwrap().is_some()
}

/// Borrow the global GUC store, if initialized.
pub fn with_store<R>(f: impl FnOnce(&GucRegistry) -> R) -> Option<R> {
    let guard = GUC_STORE.lock().unwrap();
    let store = guard.as_ref()?;
    Some(f(store))
}

/// Mutably borrow the global GUC store (the SET write path), if initialized.
pub fn with_store_mut<R>(f: impl FnOnce(&mut GucRegistry) -> R) -> Option<R> {
    let mut guard = GUC_STORE.lock().unwrap();
    let store = guard.as_mut()?;
    Some(f(store))
}

/// `ResetAllOptions()` over the process-global GUC store. Panics (loud) if the
/// live store is not initialized — never a silent no-op.
pub fn reset_all_options_global() {
    with_store_mut(crate::registry::reset_all_options)
        .expect("reset_all_options_global: live GUC store not initialized");
}

/// Look up a variable's live value (the C `*conf->variable` dereference).
fn lookup_var<R>(name: &str, pick: impl FnOnce(&GucVariable) -> Option<R>) -> Option<R> {
    with_store(|reg| reg.find_option(name).and_then(pick)).flatten()
}

/// Read the current value of a `PGC_BOOL` GUC.
pub fn get_bool(name: &str) -> Option<bool> {
    lookup_var(name, |v| match v {
        GucVariable::Bool(c) => c.value,
        _ => None,
    })
}

/// Read the current value of a `PGC_INT` GUC.
pub fn get_int(name: &str) -> Option<i32> {
    lookup_var(name, |v| match v {
        GucVariable::Int(c) => c.value,
        _ => None,
    })
}

/// Read the current value of a `PGC_REAL` GUC.
pub fn get_real(name: &str) -> Option<f64> {
    lookup_var(name, |v| match v {
        GucVariable::Real(c) => c.value,
        _ => None,
    })
}

/// Read the integer encoding of a `PGC_ENUM` GUC.
pub fn get_enum(name: &str) -> Option<i32> {
    lookup_var(name, |v| match v {
        GucVariable::Enum(c) => c.value,
        _ => None,
    })
}

/// Read the current value of a `PGC_STRING` GUC. `Some(None)` for a present GUC
/// whose value is NULL; `None` if absent / not a string.
pub fn get_string(name: &str) -> Option<Option<String>> {
    lookup_var(name, |v| match v {
        GucVariable::String(c) => c.value.clone(),
        _ => None,
    })
}

/// `GetConfigOptionResetString(name)`'s store lookup: the variable's RESET value
/// rendered as a string.
pub fn get_reset_string(name: &str) -> Option<Option<String>> {
    with_store(|reg| {
        reg.find_option(name)
            .map(crate::registry::reset_value_string)
    })
    .flatten()
}

/// `set_config_option(name, value, context, source)` over the **global** store.
///
/// Routes a write through [`crate::registry::set_config_option`] on the one
/// process-global [`GucRegistry`], so the change is immediately visible to every
/// read-through accessor and to [`crate::report`]. On a successful applied change
/// (`Ok(1)`) the variable is marked `GUC_NEEDS_REPORT`.
///
/// Returns the C `set_config_option` convention: `Ok(1)` applied, `Ok(0)`
/// rejected-below-ERROR, `Ok(-1)` skipped, `Err` when elevel >= ERROR and
/// rejected. Panics loudly if called before [`initialize_guc_options`].
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
    // Collect the variable's assign hook(s) to fire AFTER the store borrow is
    // released: a hook may recursively call SetConfigOption (the C
    // assign_session_authorization -> SetSessionAuthorization -> SetOuterUserId
    // -> SetConfigOption("is_superuser") chain), which would re-lock the store
    // and deadlock if fired while the `with_store_mut` guard is held.
    let mut deferred_hooks: Vec<crate::registry::DeferredAssignHook> = Vec::new();
    let result = with_store_mut(|reg| {
        let rc = crate::registry::set_config_option(
            reg, name, value, context, source, srole, action, change_val, elevel, is_reload,
            &mut deferred_hooks,
        )?;
        if rc == 1 {
            // Mark for the next ReportChangedGUCOptions.
            if let Some(var) = reg.find_option_mut(name) {
                var.gen_mut().status |= crate::model::GUC_NEEDS_REPORT;
            }
        }
        Ok(rc)
    })
    .unwrap_or_else(|| {
        panic!(
            "set_config_option_global({name:?}) called before initialize_guc_options seeded the \
             global GUC store"
        )
    });

    // Store borrow is now released: fire the assign hook(s), which may
    // recursively re-enter set_config_option_global, in registration order.
    for hook in deferred_hooks {
        hook();
    }

    result
}
