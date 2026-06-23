//! Custom (extension-defined) GUC registration — `guc.c`'s
//! `DefineCustomBoolVariable` / `DefineCustomIntVariable` /
//! `DefineCustomRealVariable` / `DefineCustomStringVariable` /
//! `DefineCustomEnumVariable`, their shared subroutines `init_custom_variable` /
//! `define_custom_variable` / `reapply_stacked_values`, and
//! `MarkGUCPrefixReserved`.
//!
//! In C an extension's `_PG_init` calls these at library load time to register a
//! `class.subname` configuration variable wired to the extension's own storage
//! (`bool *`/`int *`/… `valueAddr`) plus optional check/assign/show hooks. The
//! new record *replaces* any placeholder the GUC machinery created for a `SET
//! class.subname = …` that ran before the extension loaded
//! (`define_custom_variable`), re-applying the placeholder's pending value with
//! the real datatype's validation.
//!
//! ## Storage model in this repo
//!
//! The live `config_*` records ([`crate::model`]) reference their storage and
//! hooks through `&'static` install-once *slots*
//! ([`::guc_tables::GucSlot`]). Core GUCs declare those slots as
//! file statics in `guc_tables`; a *custom* GUC has no compiled-in slot, so —
//! exactly as C `guc_malloc`s the `config_*` struct into `GUCMemoryContext` and
//! never frees it — we leak a freshly-`Box`ed slot to `'static` per registration
//! (one per session per variable; the extension load runs once). The extension
//! supplies the storage accessors ([`GucVarAccessors`], reading/writing its own
//! per-backend cell) and the hook `fn` pointers by value; we install them into
//! the leaked slots and build the record from them.

use ::types_core::BOOTSTRAP_SUPERUSERID;
use ::types_error::{PgResult, WARNING};
use ::types_guc::{
    config_enum_entry, config_group, config_type, GucContext, GucSource, GUC_CUSTOM_PLACEHOLDER,
    GUC_LIST_QUOTE, PGC_POSTMASTER, PGC_SUSET, PGC_USERSET,
};

use ::guc_tables::{
    GucBoolAssignFn, GucBoolCheckFn, GucBoolVar, GucEnumAssignFn, GucEnumCheckFn, GucEnumOptions,
    GucEnumOptionsSlot, GucEnumVar, GucIntAssignFn, GucIntCheckFn, GucIntVar, GucRealAssignFn,
    GucRealCheckFn, GucRealVar, GucShowFn, GucStringAssignFn, GucStringCheckFn, GucStringVar,
    GucVarAccessors,
};

use crate::guc_store_uninitialized;
use crate::model::{
    config_bool, config_enum, config_generic, config_int, config_real, config_string,
};
use crate::registry::GucVariable;
use crate::GUC_ACTION_SET;

/// `reserved_class_prefix` (guc.c file static): the class prefixes
/// `MarkGUCPrefixReserved` has reserved. Process-lifetime, per-backend (the
/// child inherits the parent's list across `fork()`). Read by
/// [`crate::assignable_custom_variable_name`].
mod reserved {
    use std::cell::RefCell;

    thread_local! {
        static RESERVED_CLASS_PREFIX: RefCell<Vec<&'static str>> =
            const { RefCell::new(Vec::new()) };
    }

    /// `lappend(reserved_class_prefix, pstrdup(className))` — remember a reserved
    /// prefix (leaked to `'static`, the `pstrdup` into `GUCMemoryContext` analog).
    pub fn add(prefix: &str) {
        let owned: &'static str = Box::leak(prefix.to_string().into_boxed_str());
        RESERVED_CLASS_PREFIX.with(|c| c.borrow_mut().push(owned));
    }

    /// Whether `class` (the `name` up to its first `.`) matches a reserved prefix.
    pub fn matches(class: &str) -> bool {
        RESERVED_CLASS_PREFIX.with(|c| c.borrow().iter().any(|p| *p == class))
    }
}

/// True iff `class` is a reserved GUC class prefix
/// (`assignable_custom_variable_name`'s reserved-prefix check).
pub(crate) fn is_reserved_class_prefix(class: &str) -> bool {
    reserved::matches(class)
}

/// `init_custom_variable`'s context-validation + USERSET-pljava downgrade
/// (guc.c:4876), shared by all five `DefineCustomXXXVariable`. Returns the
/// adjusted `GucContext`, or the C `elog(FATAL, …)` as a `PgError`.
fn init_custom_context(name: &str, context: GucContext, flags: i32) -> PgResult<GucContext> {
    // Only allow custom PGC_POSTMASTER variables during shared-preload. There is
    // no `process_shared_preload_libraries_in_progress` machinery here (static
    // link), so this guard cannot trip; keep the structural check.
    let _ = PGC_POSTMASTER;

    // We can't support custom GUC_LIST_QUOTE variables.
    if flags & GUC_LIST_QUOTE != 0 {
        return Err(::types_error::PgError::error(
            "extensions cannot define GUC_LIST_QUOTE variables",
        ));
    }

    // pljava pre-2015 escalation guard: force these USERSET vars to SUSET.
    let context = if context == PGC_USERSET
        && (name == "pljava.classpath" || name == "pljava.vmoptions")
    {
        PGC_SUSET
    } else {
        context
    };

    Ok(context)
}

/// `define_custom_variable(variable)` (guc.c:4937): insert the freshly-built
/// custom record into the live store, replacing any same-named placeholder and
/// re-applying its pending value.
///
/// `var` is the new typed record, already seeded to its default value
/// (`InitializeOneGUCOption` ran in the per-type builder); the accessors are
/// written to the default. `pending` is the placeholder's stored current value
/// (the `*(pHolder->variable)` C reads), `None` when there was no placeholder or
/// the placeholder was still at its NULL default.
fn define_custom_variable(var: GucVariable, pending: Option<String>) -> PgResult<()> {
    let name = var.name_pub().to_string();

    // See if there's a placeholder by the same name (find_option, HASH_FIND).
    let placeholder_idx = crate::live::with_store(|reg| reg.find_index_pub(&name)).flatten();

    match placeholder_idx {
        None => {
            // No placeholder to replace — just add it. The record already holds
            // its default value (InitializeOneGUCOption ran in the builder).
            let defined = crate::live::with_store_mut(|reg| reg.define(var))
                .ok_or_else(guc_store_uninitialized)?;
            defined?;
            return Ok(());
        }
        Some(idx) => {
            // This better be a placeholder.
            let is_placeholder = crate::live::with_store(|reg| {
                reg[idx].gen().flags & GUC_CUSTOM_PLACEHOLDER != 0
            })
            .unwrap_or(false);
            if !is_placeholder {
                return Err(::types_error::PgError::error(format!(
                    "attempt to redefine parameter \"{name}\""
                ))
                .with_sqlstate(::types_error::ERRCODE_INTERNAL_ERROR));
            }

            // Replace the placeholder in the store with the real, default-valued
            // record (guc.c: `hentry->gucvar = variable`). The new record already
            // had InitializeOneGUCOption applied.
            crate::live::with_store_mut(move |reg| {
                reg[idx] = var;
            })
            .ok_or_else(guc_store_uninitialized)?;
        }
    }

    // Assign the value the placeholder was carrying to the real variable. C
    // duplicates the placeholder's reset value and its active + stacked values
    // (reapply_stacked_values) with the real datatype's validation; here we
    // model the single active session value (the common case — a `SET
    // class.subname = …` before load). An assignment failure reports a WARNING
    // and keeps going (C: bad values must not bollix the half-loaded module).
    if let Some(value) = pending {
        let _ = crate::live::set_config_option_global(
            &name,
            Some(&value),
            PGC_USERSET,
            GucSource::PGC_S_SESSION,
            BOOTSTRAP_SUPERUSERID,
            GUC_ACTION_SET,
            true,
            WARNING,
            false,
        );
    }

    Ok(())
}

/// Read a placeholder's pending current value (the `*(pHolder->variable)` C
/// reads in `define_custom_variable`), as a string, before it is replaced.
fn placeholder_pending_value(name: &str) -> Option<String> {
    crate::live::with_store(|reg| {
        let idx = reg.find_index_pub(name)?;
        match &reg[idx] {
            GucVariable::String(c) if c.gen.flags & GUC_CUSTOM_PLACEHOLDER != 0 => {
                // `value` is Option<Option<String>>: the inner Some is a non-NULL
                // stored value (a pending SET); a NULL/None inner means the
                // placeholder is still at its default and there is nothing to
                // re-apply.
                c.value.clone().flatten()
            }
            _ => None,
        }
    })
    .flatten()
}

// ---------------------------------------------------------------------------
// DefineCustomXXXVariable
// ---------------------------------------------------------------------------

/// `DefineCustomBoolVariable` (guc.c:5138).
#[allow(clippy::too_many_arguments)]
pub fn define_custom_bool_variable(
    name: &'static str,
    short_desc: Option<&'static str>,
    long_desc: Option<&'static str>,
    accessors: GucVarAccessors<bool>,
    boot_value: bool,
    context: GucContext,
    flags: i32,
    check_hook: Option<GucBoolCheckFn>,
    assign_hook: Option<GucBoolAssignFn>,
    show_hook: Option<GucShowFn>,
) -> PgResult<()> {
    let context = init_custom_context(name, context, flags)?;
    let pending = placeholder_pending_value(name);

    // Leak the storage/hook slots to `'static` (the guc_malloc-into-
    // GUCMemoryContext analog) and install the extension's accessors/hooks.
    let var_slot: &'static GucBoolVar = Box::leak(Box::new(GucBoolVar::new(name)));
    var_slot.install(accessors);
    let check = check_hook.map(|f| leak_check_bool(name, f));
    let assign = assign_hook.map(|f| leak_assign_bool(name, f));
    let show = show_hook.map(|f| leak_show(name, f));

    let mut record = GucVariable::Bool(config_bool {
        gen: custom_gen(name, context, short_desc, long_desc, flags, config_type::PGC_BOOL),
        variable: var_slot,
        value: Some(boot_value),
        boot_val: boot_value,
        check_hook: check,
        assign_hook: assign,
        show_hook: show,
        reset_val: boot_value,
        reset_extra: None,
    });
    // InitializeOneGUCOption: seed the storage with the boot value + fire hooks.
    let _ = crate::registry::initialize_one_guc_option_hooks(&mut record)?;
    define_custom_variable(record, pending)
}

/// `DefineCustomIntVariable` (guc.c:5164).
#[allow(clippy::too_many_arguments)]
pub fn define_custom_int_variable(
    name: &'static str,
    short_desc: Option<&'static str>,
    long_desc: Option<&'static str>,
    accessors: GucVarAccessors<i32>,
    boot_value: i32,
    min_value: i32,
    max_value: i32,
    context: GucContext,
    flags: i32,
    check_hook: Option<GucIntCheckFn>,
    assign_hook: Option<GucIntAssignFn>,
    show_hook: Option<GucShowFn>,
) -> PgResult<()> {
    let context = init_custom_context(name, context, flags)?;
    let pending = placeholder_pending_value(name);

    let var_slot: &'static GucIntVar = Box::leak(Box::new(GucIntVar::new(name)));
    var_slot.install(accessors);
    let check = check_hook.map(|f| leak_check_int(name, f));
    let assign = assign_hook.map(|f| leak_assign_int(name, f));
    let show = show_hook.map(|f| leak_show(name, f));

    let mut record = GucVariable::Int(config_int {
        gen: custom_gen(name, context, short_desc, long_desc, flags, config_type::PGC_INT),
        variable: var_slot,
        value: Some(boot_value),
        boot_val: boot_value,
        min: min_value,
        max: max_value,
        check_hook: check,
        assign_hook: assign,
        show_hook: show,
        reset_val: boot_value,
        reset_extra: None,
    });
    let _ = crate::registry::initialize_one_guc_option_hooks(&mut record)?;
    define_custom_variable(record, pending)
}

/// `DefineCustomRealVariable` (guc.c:5194).
#[allow(clippy::too_many_arguments)]
pub fn define_custom_real_variable(
    name: &'static str,
    short_desc: Option<&'static str>,
    long_desc: Option<&'static str>,
    accessors: GucVarAccessors<f64>,
    boot_value: f64,
    min_value: f64,
    max_value: f64,
    context: GucContext,
    flags: i32,
    check_hook: Option<GucRealCheckFn>,
    assign_hook: Option<GucRealAssignFn>,
    show_hook: Option<GucShowFn>,
) -> PgResult<()> {
    let context = init_custom_context(name, context, flags)?;
    let pending = placeholder_pending_value(name);

    let var_slot: &'static GucRealVar = Box::leak(Box::new(GucRealVar::new(name)));
    var_slot.install(accessors);
    let check = check_hook.map(|f| leak_check_real(name, f));
    let assign = assign_hook.map(|f| leak_assign_real(name, f));
    let show = show_hook.map(|f| leak_show(name, f));

    let mut record = GucVariable::Real(config_real {
        gen: custom_gen(name, context, short_desc, long_desc, flags, config_type::PGC_REAL),
        variable: var_slot,
        value: Some(boot_value),
        boot_val: boot_value,
        min: min_value,
        max: max_value,
        check_hook: check,
        assign_hook: assign,
        show_hook: show,
        reset_val: boot_value,
        reset_extra: None,
    });
    let _ = crate::registry::initialize_one_guc_option_hooks(&mut record)?;
    define_custom_variable(record, pending)
}

/// `DefineCustomStringVariable` (guc.c:5224).
#[allow(clippy::too_many_arguments)]
pub fn define_custom_string_variable(
    name: &'static str,
    short_desc: Option<&'static str>,
    long_desc: Option<&'static str>,
    accessors: GucVarAccessors<Option<String>>,
    boot_value: Option<&str>,
    context: GucContext,
    flags: i32,
    check_hook: Option<GucStringCheckFn>,
    assign_hook: Option<GucStringAssignFn>,
    show_hook: Option<GucShowFn>,
) -> PgResult<()> {
    let context = init_custom_context(name, context, flags)?;
    let pending = placeholder_pending_value(name);

    let var_slot: &'static GucStringVar = Box::leak(Box::new(GucStringVar::new(name)));
    var_slot.install(accessors);
    let check = check_hook.map(|f| leak_check_string(name, f));
    let assign = assign_hook.map(|f| leak_assign_string(name, f));
    let show = show_hook.map(|f| leak_show(name, f));

    let boot = boot_value.map(|s| s.to_string());
    let mut record = GucVariable::String(config_string {
        gen: custom_gen(name, context, short_desc, long_desc, flags, config_type::PGC_STRING),
        variable: var_slot,
        value: Some(boot.clone()),
        boot_val: boot.clone(),
        check_hook: check,
        assign_hook: assign,
        show_hook: show,
        reset_val: boot,
        reset_extra: None,
    });
    let _ = crate::registry::initialize_one_guc_option_hooks(&mut record)?;
    define_custom_variable(record, pending)
}

/// `DefineCustomEnumVariable` (guc.c:5249).
#[allow(clippy::too_many_arguments)]
pub fn define_custom_enum_variable(
    name: &'static str,
    short_desc: Option<&'static str>,
    long_desc: Option<&'static str>,
    accessors: GucVarAccessors<i32>,
    boot_value: i32,
    options: &'static [config_enum_entry],
    context: GucContext,
    flags: i32,
    check_hook: Option<GucEnumCheckFn>,
    assign_hook: Option<GucEnumAssignFn>,
    show_hook: Option<GucShowFn>,
) -> PgResult<()> {
    let context = init_custom_context(name, context, flags)?;
    let pending = placeholder_pending_value(name);

    let var_slot: &'static GucEnumVar = Box::leak(Box::new(GucEnumVar::new(name)));
    var_slot.install(accessors);
    // The option array is an `&'static` slice already; wrap it in a leaked
    // external options slot (the C `extern const struct config_enum_entry[]`).
    let opts_slot: &'static GucEnumOptionsSlot =
        Box::leak(Box::new(GucEnumOptionsSlot::new(name)));
    opts_slot.install(options);
    let check = check_hook.map(|f| leak_check_enum(name, f));
    let assign = assign_hook.map(|f| leak_assign_enum(name, f));
    let show = show_hook.map(|f| leak_show(name, f));

    let mut record = GucVariable::Enum(config_enum {
        gen: custom_gen(name, context, short_desc, long_desc, flags, config_type::PGC_ENUM),
        variable: var_slot,
        value: Some(boot_value),
        boot_val: boot_value,
        options: GucEnumOptions::External(opts_slot),
        check_hook: check,
        assign_hook: assign,
        show_hook: show,
        reset_val: boot_value,
        reset_extra: None,
    });
    let _ = crate::registry::initialize_one_guc_option_hooks(&mut record)?;
    define_custom_variable(record, pending)
}

/// `init_custom_variable`'s `config_generic` setup (guc.c:4918): a defaulted
/// header with `group = CUSTOM_OPTIONS` and the supplied metadata.
fn custom_gen(
    name: &'static str,
    context: GucContext,
    short_desc: Option<&'static str>,
    long_desc: Option<&'static str>,
    flags: i32,
    vartype: config_type,
) -> config_generic {
    config_generic::boot(
        name,
        context,
        config_group::CUSTOM_OPTIONS,
        short_desc,
        long_desc,
        flags,
        vartype,
    )
}

// ---------------------------------------------------------------------------
// MarkGUCPrefixReserved
// ---------------------------------------------------------------------------

/// `MarkGUCPrefixReserved(className)` (guc.c:5285): remove any
/// `GUC_CUSTOM_PLACEHOLDER` variable under `className.`, then remember the
/// prefix so future placeholders cannot be created under it.
pub fn mark_guc_prefix_reserved(class_name: &str) {
    // Remove existing placeholders under this class. C: hash_seq over guc_hashtab,
    // removing any placeholder whose name is `className` + '.' + subname.
    let prefix = format!("{class_name}.");
    crate::live::with_store_mut(|reg| {
        // Collect indices to remove (a placeholder under the reserved class).
        // The store is a Vec; rebuild it without the matching placeholders.
        let to_remove: Vec<String> = reg
            .iter()
            .filter(|v| {
                let g = v.gen();
                g.flags & GUC_CUSTOM_PLACEHOLDER != 0 && g.name.starts_with(&prefix)
            })
            .map(|v| v.name_pub().to_string())
            .collect();
        for victim in to_remove {
            // ereport(WARNING, "invalid configuration parameter name … removing it").
            let e = utils_error::ereport(WARNING)
                .errcode(::types_error::ERRCODE_INVALID_NAME)
                .errmsg(format!(
                    "invalid configuration parameter name \"{victim}\", removing it"
                ))
                .errdetail(format!("\"{class_name}\" is now a reserved prefix."))
                .into_error();
            utils_error::emit_error_report_for(&e);
            reg.remove_by_name(&victim);
        }
    });

    // Remember the reserved prefix.
    reserved::add(class_name);
}

// ---------------------------------------------------------------------------
// Hook-slot leaking. A custom GUC's hooks have no compiled-in slot; build one
// per registration, install the fn pointer, and leak it to `'static`.
// ---------------------------------------------------------------------------

macro_rules! leak_hook {
    ($fn_name:ident, $hook_ty:path, $fn_ty:path) => {
        fn $fn_name(c_symbol: &'static str, f: $fn_ty) -> &'static $hook_ty {
            let slot: &'static $hook_ty = Box::leak(Box::new(<$hook_ty>::new(c_symbol)));
            slot.install(f);
            slot
        }
    };
}

leak_hook!(leak_check_bool, ::guc_tables::GucBoolCheckHook, GucBoolCheckFn);
leak_hook!(leak_assign_bool, ::guc_tables::GucBoolAssignHook, GucBoolAssignFn);
leak_hook!(leak_check_int, ::guc_tables::GucIntCheckHook, GucIntCheckFn);
leak_hook!(leak_assign_int, ::guc_tables::GucIntAssignHook, GucIntAssignFn);
leak_hook!(leak_check_real, ::guc_tables::GucRealCheckHook, GucRealCheckFn);
leak_hook!(leak_assign_real, ::guc_tables::GucRealAssignHook, GucRealAssignFn);
leak_hook!(leak_check_string, ::guc_tables::GucStringCheckHook, GucStringCheckFn);
leak_hook!(leak_assign_string, ::guc_tables::GucStringAssignHook, GucStringAssignFn);
leak_hook!(leak_check_enum, ::guc_tables::GucEnumCheckHook, GucEnumCheckFn);
leak_hook!(leak_assign_enum, ::guc_tables::GucEnumAssignHook, GucEnumAssignFn);
leak_hook!(leak_show, ::guc_tables::GucShowHook, GucShowFn);
