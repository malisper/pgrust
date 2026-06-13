//! Typed binding slots for the pieces of a `config_*` entry that C wires up
//! with pointers into other translation units: the storage variable
//! (`conf->variable`), the check/assign/show hook function pointers, and the
//! `extern const struct config_enum_entry ...[]` option arrays.
//!
//! Each slot is a named static, declared here (mirroring the C extern
//! declaration) and installed exactly once by the owning unit's
//! `init_seams()`. Using a slot before its owner installed it panics loudly
//! with the C symbol name — never a silent no-op.

use std::any::Any;
use std::sync::OnceLock;

use types_error::PgResult;
use types_guc::{config_enum_entry, GucSource};

/// The opaque payload a check hook hands to its paired assign hook — C's
/// `void **extra` (`utils/guc.h`). Each hook defines its own private struct,
/// so this is the genuinely heterogeneous kind of opacity that stays opaque
/// (docs/types.md rule 6).
pub type GucHookExtra = Box<dyn Any + Send>;

// The C hook typedefs (`utils/guc.h`). A check hook may canonicalize
// `newval` in place and may produce an `extra` payload for the paired assign
// hook; `Ok(false)` is the C `return false` rejection and `Err` is the
// hook's ereport(ERROR) surface. Assign hooks mirror C's `void` return:
// they cannot fail.
pub type GucBoolCheckFn =
    fn(newval: &mut bool, extra: &mut Option<GucHookExtra>, source: GucSource) -> PgResult<bool>;
pub type GucIntCheckFn =
    fn(newval: &mut i32, extra: &mut Option<GucHookExtra>, source: GucSource) -> PgResult<bool>;
pub type GucRealCheckFn =
    fn(newval: &mut f64, extra: &mut Option<GucHookExtra>, source: GucSource) -> PgResult<bool>;
/// `char **newval`: the pointee may be NULL (a string GUC with a NULL
/// `boot_val`), and hooks distinguish NULL from empty — hence `Option`.
pub type GucStringCheckFn = fn(
    newval: &mut Option<String>,
    extra: &mut Option<GucHookExtra>,
    source: GucSource,
) -> PgResult<bool>;
pub type GucEnumCheckFn =
    fn(newval: &mut i32, extra: &mut Option<GucHookExtra>, source: GucSource) -> PgResult<bool>;
pub type GucBoolAssignFn = fn(newval: bool, extra: Option<&GucHookExtra>);
pub type GucIntAssignFn = fn(newval: i32, extra: Option<&GucHookExtra>);
pub type GucRealAssignFn = fn(newval: f64, extra: Option<&GucHookExtra>);
pub type GucStringAssignFn = fn(newval: Option<&str>, extra: Option<&GucHookExtra>);
pub type GucEnumAssignFn = fn(newval: i32, extra: Option<&GucHookExtra>);
pub type GucShowFn = fn() -> String;

/// A named install-once slot for a value owned by another unit. `c_symbol`
/// is the C symbol the slot stands in for.
pub struct GucSlot<T: Copy + 'static> {
    c_symbol: &'static str,
    slot: OnceLock<T>,
}

impl<T: Copy> GucSlot<T> {
    pub const fn new(c_symbol: &'static str) -> Self {
        Self {
            c_symbol,
            slot: OnceLock::new(),
        }
    }

    pub fn c_symbol(&self) -> &'static str {
        self.c_symbol
    }

    /// Install the owning unit's implementation. Exactly one installer per
    /// slot; a second install panics.
    pub fn install(&self, value: T) {
        if self.slot.set(value).is_err() {
            panic!("GUC slot {} installed twice", self.c_symbol);
        }
    }

    pub fn installed(&self) -> bool {
        self.slot.get().is_some()
    }

    /// The installed value; panics with the C symbol if the owning unit has
    /// not installed it yet.
    pub fn get(&self) -> T {
        *self.slot.get().unwrap_or_else(|| {
            panic!(
                "GUC slot {} used before its owning unit installed it",
                self.c_symbol
            )
        })
    }
}

impl<T: Copy> std::fmt::Debug for GucSlot<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GucSlot({})", self.c_symbol)
    }
}

/// Slot identity is the static itself (one slot per C symbol).
impl<T: Copy> PartialEq for GucSlot<T> {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}

/// Typed accessors over a GUC's runtime storage — C's `conf->variable`
/// pointer. The owning subsystem installs functions reading/writing its own
/// `thread_local` backing store.
pub struct GucVarAccessors<T> {
    pub get: fn() -> T,
    pub set: fn(T),
}

impl<T> Clone for GucVarAccessors<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for GucVarAccessors<T> {}

impl<T: 'static> GucSlot<GucVarAccessors<T>> {
    /// Read the variable through the installed accessors (`*conf->variable`).
    pub fn read(&self) -> T {
        (self.get().get)()
    }

    /// Write the variable through the installed accessors.
    pub fn write(&self, value: T) {
        (self.get().set)(value)
    }
}

// Hook slots (one alias per C hook typedef).
pub type GucBoolCheckHook = GucSlot<GucBoolCheckFn>;
pub type GucIntCheckHook = GucSlot<GucIntCheckFn>;
pub type GucRealCheckHook = GucSlot<GucRealCheckFn>;
pub type GucStringCheckHook = GucSlot<GucStringCheckFn>;
pub type GucEnumCheckHook = GucSlot<GucEnumCheckFn>;
pub type GucBoolAssignHook = GucSlot<GucBoolAssignFn>;
pub type GucIntAssignHook = GucSlot<GucIntAssignFn>;
pub type GucRealAssignHook = GucSlot<GucRealAssignFn>;
pub type GucStringAssignHook = GucSlot<GucStringAssignFn>;
pub type GucEnumAssignHook = GucSlot<GucEnumAssignFn>;
pub type GucShowHook = GucSlot<GucShowFn>;

// Variable slots (one alias per `config_*` variable type).
pub type GucBoolVar = GucSlot<GucVarAccessors<bool>>;
pub type GucIntVar = GucSlot<GucVarAccessors<i32>>;
pub type GucRealVar = GucSlot<GucVarAccessors<f64>>;
/// `char **variable`: NULL stays distinguishable from empty.
pub type GucStringVar = GucSlot<GucVarAccessors<Option<String>>>;
pub type GucEnumVar = GucSlot<GucVarAccessors<i32>>;

/// Slot for an `extern const struct config_enum_entry ...[]` option array
/// owned by another unit.
pub type GucEnumOptionsSlot = GucSlot<&'static [config_enum_entry]>;

/// `config_enum.options`: either an array defined in guc_tables.c itself or
/// one of the extern arrays owned (and installed) by another unit.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum GucEnumOptions {
    Inline(&'static [config_enum_entry]),
    External(&'static GucEnumOptionsSlot),
}

impl GucEnumOptions {
    pub fn entries(self) -> &'static [config_enum_entry] {
        match self {
            GucEnumOptions::Inline(entries) => entries,
            GucEnumOptions::External(slot) => slot.get(),
        }
    }
}
