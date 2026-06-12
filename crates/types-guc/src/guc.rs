//! `enum GucContext` (`utils/guc.h`): in which context a GUC variable may be
//! set. The discriminants match the C enum order.

pub type GucContext = u32;
pub const PGC_INTERNAL: GucContext = 0;
pub const PGC_POSTMASTER: GucContext = 1;
pub const PGC_SIGHUP: GucContext = 2;
pub const PGC_SU_BACKEND: GucContext = 3;
pub const PGC_BACKEND: GucContext = 4;
pub const PGC_SUSET: GucContext = 5;
pub const PGC_USERSET: GucContext = 6;

/// `struct config_enum_entry` (`utils/guc.h`): one allowed value of an enum
/// GUC. C's NULL-terminated arrays become plain slices.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct config_enum_entry {
    pub name: &'static str,
    pub val: i32,
    pub hidden: bool,
}
