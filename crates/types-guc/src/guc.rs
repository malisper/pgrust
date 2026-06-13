//! GUC vocabulary from `utils/guc.h` / `utils/guc_tables.h`, trimmed to the
//! items current ports consume. Discriminants match the C enum order.

/// `enum GucContext` (`utils/guc.h`): in which context a GUC variable may be
/// set. `Ord` mirrors the C `context >= PGC_*` privilege comparisons.
#[repr(i32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum GucContext {
    PGC_INTERNAL = 0,
    PGC_POSTMASTER = 1,
    PGC_SIGHUP = 2,
    PGC_SU_BACKEND = 3,
    PGC_BACKEND = 4,
    PGC_SUSET = 5,
    PGC_USERSET = 6,
}

pub use GucContext::*;

/// `enum GucSource` (`utils/guc.h`): where a setting's current value came
/// from. `Ord` mirrors the C comparisons (higher sources override lower
/// ones; `source <= PGC_S_OVERRIDE` sets the RESET default).
#[repr(i32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum GucSource {
    PGC_S_DEFAULT = 0,
    PGC_S_DYNAMIC_DEFAULT = 1,
    PGC_S_ENV_VAR = 2,
    PGC_S_FILE = 3,
    PGC_S_ARGV = 4,
    PGC_S_GLOBAL = 5,
    PGC_S_DATABASE = 6,
    PGC_S_USER = 7,
    PGC_S_DATABASE_USER = 8,
    PGC_S_CLIENT = 9,
    PGC_S_OVERRIDE = 10,
    PGC_S_INTERACTIVE = 11,
    PGC_S_TEST = 12,
    PGC_S_SESSION = 13,
}

pub use GucSource::*;

/// `enum config_type` (`utils/guc_tables.h`).
#[repr(i32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum config_type {
    PGC_BOOL = 0,
    PGC_INT = 1,
    PGC_REAL = 2,
    PGC_STRING = 3,
    PGC_ENUM = 4,
}

pub use config_type::*;

/// `enum config_group` (`utils/guc_tables.h`).
#[repr(i32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum config_group {
    UNGROUPED = 0,
    FILE_LOCATIONS = 1,
    CONN_AUTH_SETTINGS = 2,
    CONN_AUTH_TCP = 3,
    CONN_AUTH_AUTH = 4,
    CONN_AUTH_SSL = 5,
    RESOURCES_MEM = 6,
    RESOURCES_DISK = 7,
    RESOURCES_KERNEL = 8,
    RESOURCES_BGWRITER = 9,
    RESOURCES_IO = 10,
    RESOURCES_WORKER_PROCESSES = 11,
    WAL_SETTINGS = 12,
    WAL_CHECKPOINTS = 13,
    WAL_ARCHIVING = 14,
    WAL_RECOVERY = 15,
    WAL_ARCHIVE_RECOVERY = 16,
    WAL_RECOVERY_TARGET = 17,
    WAL_SUMMARIZATION = 18,
    REPLICATION_SENDING = 19,
    REPLICATION_PRIMARY = 20,
    REPLICATION_STANDBY = 21,
    REPLICATION_SUBSCRIBERS = 22,
    QUERY_TUNING_METHOD = 23,
    QUERY_TUNING_COST = 24,
    QUERY_TUNING_GEQO = 25,
    QUERY_TUNING_OTHER = 26,
    LOGGING_WHERE = 27,
    LOGGING_WHEN = 28,
    LOGGING_WHAT = 29,
    PROCESS_TITLE = 30,
    STATS_MONITORING = 31,
    STATS_CUMULATIVE = 32,
    VACUUM_AUTOVACUUM = 33,
    VACUUM_COST_DELAY = 34,
    VACUUM_DEFAULT = 35,
    VACUUM_FREEZING = 36,
    CLIENT_CONN_STATEMENT = 37,
    CLIENT_CONN_LOCALE = 38,
    CLIENT_CONN_PRELOAD = 39,
    CLIENT_CONN_OTHER = 40,
    LOCK_MANAGEMENT = 41,
    COMPAT_OPTIONS_PREVIOUS = 42,
    COMPAT_OPTIONS_OTHER = 43,
    ERROR_HANDLING_OPTIONS = 44,
    PRESET_OPTIONS = 45,
    CUSTOM_OPTIONS = 46,
    DEVELOPER_OPTIONS = 47,
}

pub use config_group::*;

// GUC flag bits (`utils/guc.h`).
pub const GUC_LIST_INPUT: i32 = 0x000001;
pub const GUC_LIST_QUOTE: i32 = 0x000002;
pub const GUC_NO_SHOW_ALL: i32 = 0x000004;
pub const GUC_NO_RESET: i32 = 0x000008;
pub const GUC_NO_RESET_ALL: i32 = 0x000010;
pub const GUC_EXPLAIN: i32 = 0x000020;
pub const GUC_REPORT: i32 = 0x000040;
pub const GUC_NOT_IN_SAMPLE: i32 = 0x000080;
pub const GUC_DISALLOW_IN_FILE: i32 = 0x000100;
pub const GUC_CUSTOM_PLACEHOLDER: i32 = 0x000200;
pub const GUC_SUPERUSER_ONLY: i32 = 0x000400;
pub const GUC_IS_NAME: i32 = 0x000800;
pub const GUC_NOT_WHILE_SEC_REST: i32 = 0x001000;
pub const GUC_DISALLOW_IN_AUTO_FILE: i32 = 0x002000;
pub const GUC_RUNTIME_COMPUTED: i32 = 0x004000;
pub const GUC_ALLOW_IN_PARALLEL: i32 = 0x008000;
pub const GUC_UNIT_KB: i32 = 0x01000000;
pub const GUC_UNIT_BLOCKS: i32 = 0x02000000;
pub const GUC_UNIT_XBLOCKS: i32 = 0x03000000;
pub const GUC_UNIT_MB: i32 = 0x04000000;
pub const GUC_UNIT_BYTE: i32 = 0x05000000;
pub const GUC_UNIT_MEMORY: i32 = 0x0F000000;
pub const GUC_UNIT_MS: i32 = 0x10000000;
pub const GUC_UNIT_S: i32 = 0x20000000;
pub const GUC_UNIT_MIN: i32 = 0x30000000;
pub const GUC_UNIT_TIME: i32 = 0x70000000;
pub const GUC_UNIT: i32 = GUC_UNIT_MEMORY | GUC_UNIT_TIME;

/// `struct config_enum_entry` (`utils/guc.h`): one allowed value of an enum
/// GUC. C's NULL-terminated arrays become plain slices.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct config_enum_entry {
    pub name: &'static str,
    pub val: i32,
    pub hidden: bool,
}
