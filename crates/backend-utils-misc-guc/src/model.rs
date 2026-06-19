//! The live GUC variable records (`struct config_generic` + the five typed
//! `config_bool`/`config_int`/`config_real`/`config_string`/`config_enum`
//! shapes, `utils/guc_tables.h`) and the GUC stack node.
//!
//! In `guc.c` these are the runtime objects the `guc_hashtab` holds; their
//! static metadata (name/context/group/flags/boot_val/min/max) comes from
//! `guc_tables.c`, and the cross-unit pointers (`conf->variable`, the
//! check/assign/show hooks, the extern enum-option arrays) point into other
//! translation units. In this repo that static metadata + the cross-unit
//! pointers live in [`backend_utils_misc_guc_tables`] as typed install-once
//! *slots*; the live records below hold **references to those slots** (the
//! faithful analog of the C record carrying the hook/variable pointers) plus
//! the per-variable runtime state guc.c mutates: the live `variable` value,
//! `reset_val`, `source`/`scontext`/`srole` provenance, the GUC stack, and the
//! status bits.
//!
//! This model is crate-local: no other crate consumes the live `config_*`
//! records (the GUC value surface other crates use is the typed `vars` slots in
//! guc_tables + the read-through accessors in [`crate::live`]).

use backend_utils_misc_guc_tables::{
    GucBoolAssignHook, GucBoolCheckHook, GucEnumAssignHook, GucEnumCheckHook, GucEnumOptions,
    GucHookExtra, GucIntAssignHook, GucIntCheckHook, GucRealAssignHook, GucRealCheckHook,
    GucShowHook, GucStringAssignHook, GucStringCheckHook,
};
use types_core::{Oid, BOOTSTRAP_SUPERUSERID};
use types_guc::{
    config_enum_entry, config_group, config_type, GucContext, GucSource, PGC_INTERNAL,
    PGC_S_DEFAULT,
};

/// `GUC_IS_IN_FILE` (`utils/guc_tables.h`): found in the config file. Transient
/// state for `ProcessConfigFile`.
pub const GUC_IS_IN_FILE: i32 = 0x0001;
/// `GUC_PENDING_RESTART`: a changed value that cannot be applied without a
/// restart.
pub const GUC_PENDING_RESTART: i32 = 0x0002;
/// `GUC_NEEDS_REPORT`: a new value must be reported to the client.
pub const GUC_NEEDS_REPORT: i32 = 0x0004;

/// `GucStackState` (`utils/guc_tables.h`).
pub type GucStackState = u32;
pub const GUC_SAVE: GucStackState = 0;
pub const GUC_SET: GucStackState = 1;
pub const GUC_LOCAL: GucStackState = 2;
pub const GUC_SET_LOCAL: GucStackState = 3;

/// The opaque "extra" payload a check hook hands its paired assign hook
/// (C's `void *extra`). Re-exported from guc_tables (one definition).
pub use backend_utils_misc_guc_tables::GucHookExtra as HookExtra;

/// A *shared* GUC extra payload, the faithful analog of C's `void *extra`.
///
/// In `guc.c` the `extra` blob a check hook produces is a single refcounted
/// `malloc`'d allocation whose *pointer* is copied (not deep-copied) into
/// `conf->gen.extra`, `conf->reset_extra`, and every GUC-stack
/// `prior.extra`/`masked.extra` slot that needs it (`set_extra_field`,
/// guc.c:792); `guc_extra_field_used` tracks whether any live slot still points
/// at it. Because our [`GucHookExtra`] is a `Box<dyn Any + Send>` with no
/// pointer identity and is not `Clone`, we model that single shared allocation
/// with [`Arc`]: cloning the `Arc` copies the pointer (C's `set_extra_field`),
/// and the payload is freed when the last slot drops it (C's `guc_free` once
/// `!guc_extra_field_used`). The `Arc` derefs to `&GucHookExtra`, so the assign
/// hook signature (`Option<&GucHookExtra>`) is unchanged.
pub type SharedExtra = std::sync::Arc<GucHookExtra>;

/// `union config_var_val` (`utils/guc_tables.h`); originally a C union.
#[derive(Debug)]
pub enum config_var_val {
    Boolval(bool),
    Intval(i32),
    Realval(f64),
    Stringval(Option<String>),
    Enumval(i32),
}

impl Clone for config_var_val {
    fn clone(&self) -> Self {
        match self {
            config_var_val::Boolval(b) => config_var_val::Boolval(*b),
            config_var_val::Intval(v) => config_var_val::Intval(*v),
            config_var_val::Realval(v) => config_var_val::Realval(*v),
            config_var_val::Stringval(s) => config_var_val::Stringval(s.clone()),
            config_var_val::Enumval(v) => config_var_val::Enumval(*v),
        }
    }
}

/// `config_var_value` (`utils/guc_tables.h`): a value plus its `extra`. The
/// `extra` is a [`SharedExtra`] (`Arc`), so a stack slot's extra can *share* the
/// live variable's extra by pointer — the faithful analog of C's
/// `set_extra_field`, which copies the refcounted `void *` pointer rather than
/// deep-copying. This is what lets `AtEOXact_GUC` rollback re-run the assign
/// hook with the matching extra.
#[derive(Debug, Default)]
pub struct config_var_value {
    pub val: Option<config_var_val>,
    pub extra: Option<SharedExtra>,
}

/// `GucStack` (`utils/guc_tables.h`): one transactional save level.
#[derive(Debug)]
pub struct GucStack {
    pub prev: Option<Box<GucStack>>,
    pub nest_level: i32,
    pub state: GucStackState,
    pub source: GucSource,
    pub scontext: GucContext,
    pub masked_scontext: GucContext,
    pub srole: Oid,
    pub masked_srole: Oid,
    pub prior: config_var_value,
    pub masked: config_var_value,
}

/// `struct config_generic` (`utils/guc_tables.h`): the header every typed
/// `config_*` record embeds. `name`/`context`/`group`/`flags` mirror the
/// `guc_tables` static metadata; the rest is runtime state.
#[derive(Debug)]
pub struct config_generic {
    pub name: &'static str,
    pub context: GucContext,
    pub group: config_group,
    pub short_desc: Option<&'static str>,
    pub long_desc: Option<&'static str>,
    pub flags: i32,
    pub vartype: config_type,
    pub status: i32,
    pub source: GucSource,
    pub reset_source: GucSource,
    pub scontext: GucContext,
    pub reset_scontext: GucContext,
    pub srole: Oid,
    pub reset_srole: Oid,
    pub stack: Option<Box<GucStack>>,
    pub extra: Option<SharedExtra>,
    pub last_reported: Option<String>,
    pub sourcefile: Option<String>,
    pub sourceline: i32,
}

impl config_generic {
    /// A freshly-defaulted header for a boot-built record, matching what
    /// `InitializeOneGUCOption` leaves on every variable (status 0,
    /// source/reset_source PGC_S_DEFAULT, scontext/reset_scontext PGC_INTERNAL,
    /// srole/reset_srole BOOTSTRAP_SUPERUSERID).
    pub fn boot(
        name: &'static str,
        context: GucContext,
        group: config_group,
        short_desc: Option<&'static str>,
        long_desc: Option<&'static str>,
        flags: i32,
        vartype: config_type,
    ) -> Self {
        config_generic {
            name,
            context,
            group,
            short_desc,
            long_desc,
            flags,
            vartype,
            status: 0,
            source: PGC_S_DEFAULT,
            reset_source: PGC_S_DEFAULT,
            scontext: PGC_INTERNAL,
            reset_scontext: PGC_INTERNAL,
            srole: BOOTSTRAP_SUPERUSERID,
            reset_srole: BOOTSTRAP_SUPERUSERID,
            stack: None,
            extra: None,
            last_reported: None,
            sourcefile: None,
            sourceline: 0,
        }
    }
}

/// `struct config_bool`.
#[derive(Debug)]
pub struct config_bool {
    pub gen: config_generic,
    /// The owner's storage accessors (`conf->variable`).
    pub variable: &'static backend_utils_misc_guc_tables::GucBoolVar,
    /// The live value cached in this record (mirrors `*conf->variable`).
    pub value: Option<bool>,
    pub boot_val: bool,
    pub check_hook: Option<&'static GucBoolCheckHook>,
    pub assign_hook: Option<&'static GucBoolAssignHook>,
    pub show_hook: Option<&'static GucShowHook>,
    pub reset_val: bool,
    pub reset_extra: Option<SharedExtra>,
}

/// `struct config_int`.
#[derive(Debug)]
pub struct config_int {
    pub gen: config_generic,
    pub variable: &'static backend_utils_misc_guc_tables::GucIntVar,
    pub value: Option<i32>,
    pub boot_val: i32,
    pub min: i32,
    pub max: i32,
    pub check_hook: Option<&'static GucIntCheckHook>,
    pub assign_hook: Option<&'static GucIntAssignHook>,
    pub show_hook: Option<&'static GucShowHook>,
    pub reset_val: i32,
    pub reset_extra: Option<SharedExtra>,
}

/// `struct config_real`.
#[derive(Debug)]
pub struct config_real {
    pub gen: config_generic,
    pub variable: &'static backend_utils_misc_guc_tables::GucRealVar,
    pub value: Option<f64>,
    pub boot_val: f64,
    pub min: f64,
    pub max: f64,
    pub check_hook: Option<&'static GucRealCheckHook>,
    pub assign_hook: Option<&'static GucRealAssignHook>,
    pub show_hook: Option<&'static GucShowHook>,
    pub reset_val: f64,
    pub reset_extra: Option<SharedExtra>,
}

/// `struct config_string`.
#[derive(Debug)]
pub struct config_string {
    pub gen: config_generic,
    pub variable: &'static backend_utils_misc_guc_tables::GucStringVar,
    pub value: Option<Option<String>>,
    pub boot_val: Option<String>,
    pub check_hook: Option<&'static GucStringCheckHook>,
    pub assign_hook: Option<&'static GucStringAssignHook>,
    pub show_hook: Option<&'static GucShowHook>,
    pub reset_val: Option<String>,
    pub reset_extra: Option<SharedExtra>,
}

/// `struct config_enum`.
#[derive(Debug)]
pub struct config_enum {
    pub gen: config_generic,
    pub variable: &'static backend_utils_misc_guc_tables::GucEnumVar,
    pub value: Option<i32>,
    pub boot_val: i32,
    pub options: GucEnumOptions,
    pub check_hook: Option<&'static GucEnumCheckHook>,
    pub assign_hook: Option<&'static GucEnumAssignHook>,
    pub show_hook: Option<&'static GucShowHook>,
    pub reset_val: i32,
    pub reset_extra: Option<SharedExtra>,
}

impl config_enum {
    /// The enum option entries (`config_enum.options`).
    pub fn entries(&self) -> &'static [config_enum_entry] {
        self.options.entries()
    }
}

/// The `void **extra` typed alias the check-hook callers pass through.
pub type ExtraSlot = Option<SharedExtra>;
