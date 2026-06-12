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

/// `enum GucSource` (`utils/guc.h`): where a GUC setting came from. The
/// discriminants match the C enum order.
pub type GucSource = u32;
pub const PGC_S_DEFAULT: GucSource = 0;
pub const PGC_S_DYNAMIC_DEFAULT: GucSource = 1;
pub const PGC_S_ENV_VAR: GucSource = 2;
pub const PGC_S_FILE: GucSource = 3;
pub const PGC_S_ARGV: GucSource = 4;
pub const PGC_S_GLOBAL: GucSource = 5;
pub const PGC_S_DATABASE: GucSource = 6;
pub const PGC_S_USER: GucSource = 7;
pub const PGC_S_DATABASE_USER: GucSource = 8;
pub const PGC_S_CLIENT: GucSource = 9;
pub const PGC_S_OVERRIDE: GucSource = 10;
pub const PGC_S_INTERACTIVE: GucSource = 11;
pub const PGC_S_TEST: GucSource = 12;
pub const PGC_S_SESSION: GucSource = 13;
