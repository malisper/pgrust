use guc_tables::{
    GucBoolAssignHook, GucBoolCheckHook, GucEnumAssignHook, GucEnumCheckHook, GucEnumOptions,
    GucHookExtra, GucIntAssignHook, GucIntCheckHook, GucRealAssignHook, GucRealCheckHook,
    GucShowHook, GucStringAssignHook, GucStringCheckHook,
};
use types_core::{Oid, BOOTSTRAP_SUPERUSERID};
use types_guc::{
    config_enum_entry, config_group, config_type, GucContext, GucSource, PGC_INTERNAL,
    PGC_S_DEFAULT,
};

// guc_tables.h transient status bits.
pub const GUC_IS_IN_FILE: i32 = 0x0001;
pub const GUC_PENDING_RESTART: i32 = 0x0002;
pub const GUC_NEEDS_REPORT: i32 = 0x0004;
// Rust-only status bit: placeholder purged by MarkGUCPrefixReserved. C frees
// the guc_hashtab entry; our registry Vec keeps the slot as a tombstone so
// existing indices stay valid.
pub const GUC_REMOVED: i32 = 0x8000;

pub type GucStackState = u32;
pub const GUC_SAVE: GucStackState = 0;
pub const GUC_SET: GucStackState = 1;
pub const GUC_LOCAL: GucStackState = 2;
pub const GUC_SET_LOCAL: GucStackState = 3;

// C's `void *extra` is one refcounted guc_malloc block whose pointer is shared
// across gen.extra / reset_extra / stack slots (set_extra_field). Arc, not Rc:
// a parallel-worker session bind (store.rs) hands the leader's validated
// extras to the claiming pool thread so check hooks need not rerun there.
pub type SharedExtra = std::sync::Arc<GucHookExtra>;

#[derive(Clone, Debug)]
pub enum config_var_val {
    Boolval(bool),
    Intval(i32),
    Realval(f64),
    Stringval(Option<String>),
    Enumval(i32),
}

impl PartialEq for config_var_val {
    /// Exact-representation equality (Realval compares by bit pattern, so the
    /// relation stays reflexive under NaN and never conflates values with
    /// distinct representations). Used by the pin re-mint dedup
    /// (layers::current_query_pin): "equal" must imply the captured states
    /// are interchangeable, so the compare is conservative — bitwise on
    /// floats, discriminant-strict across types.
    fn eq(&self, other: &Self) -> bool {
        use config_var_val::*;
        match (self, other) {
            (Boolval(a), Boolval(b)) => a == b,
            (Intval(a), Intval(b)) => a == b,
            (Realval(a), Realval(b)) => a.to_bits() == b.to_bits(),
            (Stringval(a), Stringval(b)) => a == b,
            (Enumval(a), Enumval(b)) => a == b,
            _ => false,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct config_var_value {
    pub val: Option<config_var_val>,
    pub extra: Option<SharedExtra>,
}

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
pub struct config_bool {
    pub gen: config_generic,
    pub variable: &'static guc_tables::GucBoolVar,
    pub value: Option<bool>,
    pub boot_val: bool,
    pub check_hook: Option<&'static GucBoolCheckHook>,
    pub assign_hook: Option<&'static GucBoolAssignHook>,
    pub show_hook: Option<&'static GucShowHook>,
    pub reset_val: bool,
    pub reset_extra: Option<SharedExtra>,
}

#[derive(Clone, Debug)]
pub struct config_int {
    pub gen: config_generic,
    pub variable: &'static guc_tables::GucIntVar,
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

#[derive(Clone, Debug)]
pub struct config_real {
    pub gen: config_generic,
    pub variable: &'static guc_tables::GucRealVar,
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

#[derive(Clone, Debug)]
pub struct config_string {
    pub gen: config_generic,
    pub variable: &'static guc_tables::GucStringVar,
    pub value: Option<Option<String>>,
    pub boot_val: Option<String>,
    pub check_hook: Option<&'static GucStringCheckHook>,
    pub assign_hook: Option<&'static GucStringAssignHook>,
    pub show_hook: Option<&'static GucShowHook>,
    pub reset_val: Option<String>,
    pub reset_extra: Option<SharedExtra>,
}

#[derive(Clone, Debug)]
pub struct config_enum {
    pub gen: config_generic,
    pub variable: &'static guc_tables::GucEnumVar,
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
    pub fn entries(&self) -> &'static [config_enum_entry] {
        self.options.entries()
    }
}
