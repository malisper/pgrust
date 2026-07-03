use std::any::Any;
use std::sync::OnceLock;

use ::types_error::PgResult;
use ::types_guc::{config_enum_entry, GucSource};

// C's `void **extra` check-hook payload; shared via Arc across GUC stack slots,
// hence Send + Sync.
pub type GucHookExtra = Box<dyn Any + Send + Sync>;

pub type GucBoolCheckFn =
    fn(newval: &mut bool, extra: &mut Option<GucHookExtra>, source: GucSource) -> PgResult<bool>;
pub type GucIntCheckFn =
    fn(newval: &mut i32, extra: &mut Option<GucHookExtra>, source: GucSource) -> PgResult<bool>;
pub type GucRealCheckFn =
    fn(newval: &mut f64, extra: &mut Option<GucHookExtra>, source: GucSource) -> PgResult<bool>;
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

    pub fn install(&self, value: T) {
        if self.slot.set(value).is_err() {
            panic!("GUC slot {} installed twice", self.c_symbol);
        }
    }

    pub fn installed(&self) -> bool {
        self.slot.get().is_some()
    }

    // Idempotent variant for owners whose init can race between test threads
    // (the checked install's check-then-set is not atomic).
    pub fn install_if_absent(&self, value: T) {
        let _ = self.slot.set(value);
    }

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

impl<T: Copy> PartialEq for GucSlot<T> {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}

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
    pub fn read(&self) -> T {
        (self.get().get)()
    }

    pub fn write(&self, value: T) {
        (self.get().set)(value)
    }
}

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

pub type GucBoolVar = GucSlot<GucVarAccessors<bool>>;
pub type GucIntVar = GucSlot<GucVarAccessors<i32>>;
pub type GucRealVar = GucSlot<GucVarAccessors<f64>>;
pub type GucStringVar = GucSlot<GucVarAccessors<Option<String>>>;
pub type GucEnumVar = GucSlot<GucVarAccessors<i32>>;

pub type GucEnumOptionsSlot = GucSlot<&'static [config_enum_entry]>;

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
