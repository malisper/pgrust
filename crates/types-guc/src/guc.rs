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
/// from. Ordered so that higher values override lower ones.
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

/// `enum config_type` (`utils/guc_tables.h`).
pub type config_type = u32;
pub const PGC_BOOL: config_type = 0;
pub const PGC_INT: config_type = 1;
pub const PGC_REAL: config_type = 2;
pub const PGC_STRING: config_type = 3;
pub const PGC_ENUM: config_type = 4;

/// `enum config_group` (`utils/guc_tables.h`).
pub type config_group = u32;
pub const UNGROUPED: config_group = 0;
pub const FILE_LOCATIONS: config_group = 1;
pub const CONN_AUTH_SETTINGS: config_group = 2;
pub const CONN_AUTH_TCP: config_group = 3;
pub const CONN_AUTH_AUTH: config_group = 4;
pub const CONN_AUTH_SSL: config_group = 5;
pub const RESOURCES_MEM: config_group = 6;
pub const RESOURCES_DISK: config_group = 7;
pub const RESOURCES_KERNEL: config_group = 8;
pub const RESOURCES_BGWRITER: config_group = 9;
pub const RESOURCES_IO: config_group = 10;
pub const RESOURCES_WORKER_PROCESSES: config_group = 11;
pub const WAL_SETTINGS: config_group = 12;
pub const WAL_CHECKPOINTS: config_group = 13;
pub const WAL_ARCHIVING: config_group = 14;
pub const WAL_RECOVERY: config_group = 15;
pub const WAL_ARCHIVE_RECOVERY: config_group = 16;
pub const WAL_RECOVERY_TARGET: config_group = 17;
pub const WAL_SUMMARIZATION: config_group = 18;
pub const REPLICATION_SENDING: config_group = 19;
pub const REPLICATION_PRIMARY: config_group = 20;
pub const REPLICATION_STANDBY: config_group = 21;
pub const REPLICATION_SUBSCRIBERS: config_group = 22;
pub const QUERY_TUNING_METHOD: config_group = 23;
pub const QUERY_TUNING_COST: config_group = 24;
pub const QUERY_TUNING_GEQO: config_group = 25;
pub const QUERY_TUNING_OTHER: config_group = 26;
pub const LOGGING_WHERE: config_group = 27;
pub const LOGGING_WHEN: config_group = 28;
pub const LOGGING_WHAT: config_group = 29;
pub const PROCESS_TITLE: config_group = 30;
pub const STATS_MONITORING: config_group = 31;
pub const STATS_CUMULATIVE: config_group = 32;
pub const VACUUM_AUTOVACUUM: config_group = 33;
pub const VACUUM_COST_DELAY: config_group = 34;
pub const VACUUM_DEFAULT: config_group = 35;
pub const VACUUM_FREEZING: config_group = 36;
pub const CLIENT_CONN_STATEMENT: config_group = 37;
pub const CLIENT_CONN_LOCALE: config_group = 38;
pub const CLIENT_CONN_PRELOAD: config_group = 39;
pub const CLIENT_CONN_OTHER: config_group = 40;
pub const LOCK_MANAGEMENT: config_group = 41;
pub const COMPAT_OPTIONS_PREVIOUS: config_group = 42;
pub const COMPAT_OPTIONS_OTHER: config_group = 43;
pub const ERROR_HANDLING_OPTIONS: config_group = 44;
pub const PRESET_OPTIONS: config_group = 45;
pub const CUSTOM_OPTIONS: config_group = 46;
pub const DEVELOPER_OPTIONS: config_group = 47;

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
