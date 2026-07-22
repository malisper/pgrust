use crate::consts::*;
use crate::slots::*;
use crate::{hooks, option_sets, vars};
use types_guc::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GucValueKind {
    Bool,
    Int,
    Real,
    String,
    Enum,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum GucDefaultValue {
    Bool(bool),
    Int(i32),
    Real(f64),
    String(Option<&'static str>),
    Enum(i32),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GucBoolSetting {
    pub name: &'static str,
    pub context: GucContext,
    pub group: config_group,
    pub short_desc: Option<&'static str>,
    pub long_desc: Option<&'static str>,
    pub flags: i32,
    pub variable: &'static GucBoolVar,
    pub boot_val: GucDefaultValue,
    pub check_hook: Option<&'static GucBoolCheckHook>,
    pub assign_hook: Option<&'static GucBoolAssignHook>,
    pub show_hook: Option<&'static GucShowHook>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GucIntSetting {
    pub name: &'static str,
    pub context: GucContext,
    pub group: config_group,
    pub short_desc: Option<&'static str>,
    pub long_desc: Option<&'static str>,
    pub flags: i32,
    pub variable: &'static GucIntVar,
    pub boot_val: GucDefaultValue,
    pub min: i32,
    pub max: i32,
    pub check_hook: Option<&'static GucIntCheckHook>,
    pub assign_hook: Option<&'static GucIntAssignHook>,
    pub show_hook: Option<&'static GucShowHook>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GucRealSetting {
    pub name: &'static str,
    pub context: GucContext,
    pub group: config_group,
    pub short_desc: Option<&'static str>,
    pub long_desc: Option<&'static str>,
    pub flags: i32,
    pub variable: &'static GucRealVar,
    pub boot_val: GucDefaultValue,
    pub min: f64,
    pub max: f64,
    pub check_hook: Option<&'static GucRealCheckHook>,
    pub assign_hook: Option<&'static GucRealAssignHook>,
    pub show_hook: Option<&'static GucShowHook>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GucStringSetting {
    pub name: &'static str,
    pub context: GucContext,
    pub group: config_group,
    pub short_desc: Option<&'static str>,
    pub long_desc: Option<&'static str>,
    pub flags: i32,
    pub variable: &'static GucStringVar,
    pub boot_val: GucDefaultValue,
    pub check_hook: Option<&'static GucStringCheckHook>,
    pub assign_hook: Option<&'static GucStringAssignHook>,
    pub show_hook: Option<&'static GucShowHook>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GucEnumSetting {
    pub name: &'static str,
    pub context: GucContext,
    pub group: config_group,
    pub short_desc: Option<&'static str>,
    pub long_desc: Option<&'static str>,
    pub flags: i32,
    pub variable: &'static GucEnumVar,
    pub boot_val: GucDefaultValue,
    pub options: GucEnumOptions,
    pub check_hook: Option<&'static GucEnumCheckHook>,
    pub assign_hook: Option<&'static GucEnumAssignHook>,
    pub show_hook: Option<&'static GucShowHook>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum GucSetting {
    Bool(GucBoolSetting),
    Int(GucIntSetting),
    Real(GucRealSetting),
    String(GucStringSetting),
    Enum(GucEnumSetting),
}

impl GucSetting {
    pub fn name(self) -> &'static str {
        match self {
            GucSetting::Bool(s) => s.name,
            GucSetting::Int(s) => s.name,
            GucSetting::Real(s) => s.name,
            GucSetting::String(s) => s.name,
            GucSetting::Enum(s) => s.name,
        }
    }
    pub fn value_kind(self) -> GucValueKind {
        match self {
            GucSetting::Bool(_) => GucValueKind::Bool,
            GucSetting::Int(_) => GucValueKind::Int,
            GucSetting::Real(_) => GucValueKind::Real,
            GucSetting::String(_) => GucValueKind::String,
            GucSetting::Enum(_) => GucValueKind::Enum,
        }
    }
    pub fn context(self) -> GucContext {
        match self {
            GucSetting::Bool(s) => s.context,
            GucSetting::Int(s) => s.context,
            GucSetting::Real(s) => s.context,
            GucSetting::String(s) => s.context,
            GucSetting::Enum(s) => s.context,
        }
    }
    pub fn group(self) -> config_group {
        match self {
            GucSetting::Bool(s) => s.group,
            GucSetting::Int(s) => s.group,
            GucSetting::Real(s) => s.group,
            GucSetting::String(s) => s.group,
            GucSetting::Enum(s) => s.group,
        }
    }
    pub fn short_desc(self) -> Option<&'static str> {
        match self {
            GucSetting::Bool(s) => s.short_desc,
            GucSetting::Int(s) => s.short_desc,
            GucSetting::Real(s) => s.short_desc,
            GucSetting::String(s) => s.short_desc,
            GucSetting::Enum(s) => s.short_desc,
        }
    }
    pub fn long_desc(self) -> Option<&'static str> {
        match self {
            GucSetting::Bool(s) => s.long_desc,
            GucSetting::Int(s) => s.long_desc,
            GucSetting::Real(s) => s.long_desc,
            GucSetting::String(s) => s.long_desc,
            GucSetting::Enum(s) => s.long_desc,
        }
    }
    pub fn flags(self) -> i32 {
        match self {
            GucSetting::Bool(s) => s.flags,
            GucSetting::Int(s) => s.flags,
            GucSetting::Real(s) => s.flags,
            GucSetting::String(s) => s.flags,
            GucSetting::Enum(s) => s.flags,
        }
    }
    pub fn variable_c_symbol(self) -> &'static str {
        match self {
            GucSetting::Bool(s) => s.variable.c_symbol(),
            GucSetting::Int(s) => s.variable.c_symbol(),
            GucSetting::Real(s) => s.variable.c_symbol(),
            GucSetting::String(s) => s.variable.c_symbol(),
            GucSetting::Enum(s) => s.variable.c_symbol(),
        }
    }
    pub fn default_value(self) -> GucDefaultValue {
        match self {
            GucSetting::Bool(s) => s.boot_val,
            GucSetting::Int(s) => s.boot_val,
            GucSetting::Real(s) => s.boot_val,
            GucSetting::String(s) => s.boot_val,
            GucSetting::Enum(s) => s.boot_val,
        }
    }
    pub fn show_hook(self) -> Option<&'static GucShowHook> {
        match self {
            GucSetting::Bool(s) => s.show_hook,
            GucSetting::Int(s) => s.show_hook,
            GucSetting::Real(s) => s.show_hook,
            GucSetting::String(s) => s.show_hook,
            GucSetting::Enum(s) => s.show_hook,
        }
    }
    pub fn options(self) -> Option<GucEnumOptions> {
        match self {
            GucSetting::Enum(s) => Some(s.options),
            _ => None,
        }
    }
}

pub static bytea_output_options: &[config_enum_entry] = &[
    config_enum_entry { name: "escape", val: BYTEA_OUTPUT_ESCAPE, hidden: false },
    config_enum_entry { name: "hex", val: BYTEA_OUTPUT_HEX, hidden: false },
];

pub static client_message_level_options: &[config_enum_entry] = &[
    config_enum_entry { name: "debug5", val: DEBUG5, hidden: false },
    config_enum_entry { name: "debug4", val: DEBUG4, hidden: false },
    config_enum_entry { name: "debug3", val: DEBUG3, hidden: false },
    config_enum_entry { name: "debug2", val: DEBUG2, hidden: false },
    config_enum_entry { name: "debug1", val: DEBUG1, hidden: false },
    config_enum_entry { name: "debug", val: DEBUG2, hidden: true },
    config_enum_entry { name: "log", val: LOG, hidden: false },
    config_enum_entry { name: "info", val: INFO, hidden: true },
    config_enum_entry { name: "notice", val: NOTICE, hidden: false },
    config_enum_entry { name: "warning", val: WARNING, hidden: false },
    config_enum_entry { name: "error", val: ERROR, hidden: false },
];

pub static server_message_level_options: &[config_enum_entry] = &[
    config_enum_entry { name: "debug5", val: DEBUG5, hidden: false },
    config_enum_entry { name: "debug4", val: DEBUG4, hidden: false },
    config_enum_entry { name: "debug3", val: DEBUG3, hidden: false },
    config_enum_entry { name: "debug2", val: DEBUG2, hidden: false },
    config_enum_entry { name: "debug1", val: DEBUG1, hidden: false },
    config_enum_entry { name: "debug", val: DEBUG2, hidden: true },
    config_enum_entry { name: "info", val: INFO, hidden: false },
    config_enum_entry { name: "notice", val: NOTICE, hidden: false },
    config_enum_entry { name: "warning", val: WARNING, hidden: false },
    config_enum_entry { name: "error", val: ERROR, hidden: false },
    config_enum_entry { name: "log", val: LOG, hidden: false },
    config_enum_entry { name: "fatal", val: FATAL, hidden: false },
    config_enum_entry { name: "panic", val: PANIC, hidden: false },
];

pub static hnsw_iterative_scan_options: &[config_enum_entry] = &[
    config_enum_entry { name: "off", val: 0, hidden: false },
    config_enum_entry { name: "relaxed_order", val: 1, hidden: false },
    config_enum_entry { name: "strict_order", val: 2, hidden: false },
];

pub static intervalstyle_options: &[config_enum_entry] = &[
    config_enum_entry { name: "postgres", val: INTSTYLE_POSTGRES, hidden: false },
    config_enum_entry { name: "postgres_verbose", val: INTSTYLE_POSTGRES_VERBOSE, hidden: false },
    config_enum_entry { name: "sql_standard", val: INTSTYLE_SQL_STANDARD, hidden: false },
    config_enum_entry { name: "iso_8601", val: INTSTYLE_ISO_8601, hidden: false },
];

pub static icu_validation_level_options: &[config_enum_entry] = &[
    config_enum_entry { name: "disabled", val: -(1), hidden: false },
    config_enum_entry { name: "debug5", val: DEBUG5, hidden: false },
    config_enum_entry { name: "debug4", val: DEBUG4, hidden: false },
    config_enum_entry { name: "debug3", val: DEBUG3, hidden: false },
    config_enum_entry { name: "debug2", val: DEBUG2, hidden: false },
    config_enum_entry { name: "debug1", val: DEBUG1, hidden: false },
    config_enum_entry { name: "debug", val: DEBUG2, hidden: true },
    config_enum_entry { name: "log", val: LOG, hidden: false },
    config_enum_entry { name: "info", val: INFO, hidden: true },
    config_enum_entry { name: "notice", val: NOTICE, hidden: false },
    config_enum_entry { name: "warning", val: WARNING, hidden: false },
    config_enum_entry { name: "error", val: ERROR, hidden: false },
];

pub static log_error_verbosity_options: &[config_enum_entry] = &[
    config_enum_entry { name: "terse", val: PGERROR_TERSE, hidden: false },
    config_enum_entry { name: "default", val: PGERROR_DEFAULT, hidden: false },
    config_enum_entry { name: "verbose", val: PGERROR_VERBOSE, hidden: false },
];

pub static log_statement_options: &[config_enum_entry] = &[
    config_enum_entry { name: "none", val: LOGSTMT_NONE, hidden: false },
    config_enum_entry { name: "ddl", val: LOGSTMT_DDL, hidden: false },
    config_enum_entry { name: "mod", val: LOGSTMT_MOD, hidden: false },
    config_enum_entry { name: "all", val: LOGSTMT_ALL, hidden: false },
];

pub static isolation_level_options: &[config_enum_entry] = &[
    config_enum_entry { name: "serializable", val: XACT_SERIALIZABLE, hidden: false },
    config_enum_entry { name: "repeatable read", val: XACT_REPEATABLE_READ, hidden: false },
    config_enum_entry { name: "read committed", val: XACT_READ_COMMITTED, hidden: false },
    config_enum_entry { name: "read uncommitted", val: XACT_READ_UNCOMMITTED, hidden: false },
];

pub static session_replication_role_options: &[config_enum_entry] = &[
    config_enum_entry { name: "origin", val: SESSION_REPLICATION_ROLE_ORIGIN, hidden: false },
    config_enum_entry { name: "replica", val: SESSION_REPLICATION_ROLE_REPLICA, hidden: false },
    config_enum_entry { name: "local", val: SESSION_REPLICATION_ROLE_LOCAL, hidden: false },
];

pub static syslog_facility_options: &[config_enum_entry] = &[
    config_enum_entry { name: "local0", val: LOG_LOCAL0, hidden: false },
    config_enum_entry { name: "local1", val: LOG_LOCAL1, hidden: false },
    config_enum_entry { name: "local2", val: LOG_LOCAL2, hidden: false },
    config_enum_entry { name: "local3", val: LOG_LOCAL3, hidden: false },
    config_enum_entry { name: "local4", val: LOG_LOCAL4, hidden: false },
    config_enum_entry { name: "local5", val: LOG_LOCAL5, hidden: false },
    config_enum_entry { name: "local6", val: LOG_LOCAL6, hidden: false },
    config_enum_entry { name: "local7", val: LOG_LOCAL7, hidden: false },
];

pub static track_function_options: &[config_enum_entry] = &[
    config_enum_entry { name: "none", val: TRACK_FUNC_OFF, hidden: false },
    config_enum_entry { name: "pl", val: TRACK_FUNC_PL, hidden: false },
    config_enum_entry { name: "all", val: TRACK_FUNC_ALL, hidden: false },
];

pub static stats_fetch_consistency: &[config_enum_entry] = &[
    config_enum_entry { name: "none", val: PGSTAT_FETCH_CONSISTENCY_NONE, hidden: false },
    config_enum_entry { name: "cache", val: PGSTAT_FETCH_CONSISTENCY_CACHE, hidden: false },
    config_enum_entry { name: "snapshot", val: PGSTAT_FETCH_CONSISTENCY_SNAPSHOT, hidden: false },
];

pub static xmlbinary_options: &[config_enum_entry] = &[
    config_enum_entry { name: "base64", val: XMLBINARY_BASE64, hidden: false },
    config_enum_entry { name: "hex", val: XMLBINARY_HEX, hidden: false },
];

pub static xmloption_options: &[config_enum_entry] = &[
    config_enum_entry { name: "content", val: XMLOPTION_CONTENT, hidden: false },
    config_enum_entry { name: "document", val: XMLOPTION_DOCUMENT, hidden: false },
];

pub static regex_engine_options: &[config_enum_entry] = &[
    config_enum_entry { name: "auto", val: REGEX_ENGINE_AUTO, hidden: false },
    config_enum_entry { name: "spencer", val: REGEX_ENGINE_SPENCER, hidden: false },
    config_enum_entry { name: "re2", val: REGEX_ENGINE_RE2, hidden: false },
];

pub static pgrust_parallel_engine_options: &[config_enum_entry] = &[
    config_enum_entry { name: "legacy", val: PARALLEL_ENGINE_LEGACY, hidden: false },
    config_enum_entry { name: "runtime", val: PARALLEL_ENGINE_RUNTIME, hidden: false },
];

pub static backslash_quote_options: &[config_enum_entry] = &[
    config_enum_entry { name: "safe_encoding", val: BACKSLASH_QUOTE_SAFE_ENCODING, hidden: false },
    config_enum_entry { name: "on", val: BACKSLASH_QUOTE_ON, hidden: false },
    config_enum_entry { name: "off", val: BACKSLASH_QUOTE_OFF, hidden: false },
    config_enum_entry { name: "true", val: BACKSLASH_QUOTE_ON, hidden: true },
    config_enum_entry { name: "false", val: BACKSLASH_QUOTE_OFF, hidden: true },
    config_enum_entry { name: "yes", val: BACKSLASH_QUOTE_ON, hidden: true },
    config_enum_entry { name: "no", val: BACKSLASH_QUOTE_OFF, hidden: true },
    config_enum_entry { name: "1", val: BACKSLASH_QUOTE_ON, hidden: true },
    config_enum_entry { name: "0", val: BACKSLASH_QUOTE_OFF, hidden: true },
];

pub static compute_query_id_options: &[config_enum_entry] = &[
    config_enum_entry { name: "auto", val: COMPUTE_QUERY_ID_AUTO, hidden: false },
    config_enum_entry { name: "regress", val: COMPUTE_QUERY_ID_REGRESS, hidden: false },
    config_enum_entry { name: "on", val: COMPUTE_QUERY_ID_ON, hidden: false },
    config_enum_entry { name: "off", val: COMPUTE_QUERY_ID_OFF, hidden: false },
    config_enum_entry { name: "true", val: COMPUTE_QUERY_ID_ON, hidden: true },
    config_enum_entry { name: "false", val: COMPUTE_QUERY_ID_OFF, hidden: true },
    config_enum_entry { name: "yes", val: COMPUTE_QUERY_ID_ON, hidden: true },
    config_enum_entry { name: "no", val: COMPUTE_QUERY_ID_OFF, hidden: true },
    config_enum_entry { name: "1", val: COMPUTE_QUERY_ID_ON, hidden: true },
    config_enum_entry { name: "0", val: COMPUTE_QUERY_ID_OFF, hidden: true },
];

pub static pgss_track_options: &[config_enum_entry] = &[
    config_enum_entry { name: "none", val: PGSS_TRACK_NONE, hidden: false },
    config_enum_entry { name: "top", val: PGSS_TRACK_TOP, hidden: false },
    config_enum_entry { name: "all", val: PGSS_TRACK_ALL, hidden: false },
];

// auto_explain.c format_options.
pub static auto_explain_format_options: &[config_enum_entry] = &[
    config_enum_entry { name: "text", val: EXPLAIN_FORMAT_TEXT, hidden: false },
    config_enum_entry { name: "xml", val: EXPLAIN_FORMAT_XML, hidden: false },
    config_enum_entry { name: "json", val: EXPLAIN_FORMAT_JSON, hidden: false },
    config_enum_entry { name: "yaml", val: EXPLAIN_FORMAT_YAML, hidden: false },
];

// auto_explain.c loglevel_options (a strict subset of the server levels).
pub static auto_explain_loglevel_options: &[config_enum_entry] = &[
    config_enum_entry { name: "debug5", val: DEBUG5, hidden: false },
    config_enum_entry { name: "debug4", val: DEBUG4, hidden: false },
    config_enum_entry { name: "debug3", val: DEBUG3, hidden: false },
    config_enum_entry { name: "debug2", val: DEBUG2, hidden: false },
    config_enum_entry { name: "debug1", val: DEBUG1, hidden: false },
    config_enum_entry { name: "debug", val: DEBUG2, hidden: true },
    config_enum_entry { name: "info", val: INFO, hidden: false },
    config_enum_entry { name: "notice", val: NOTICE, hidden: false },
    config_enum_entry { name: "warning", val: WARNING, hidden: false },
    config_enum_entry { name: "log", val: LOG, hidden: false },
];

pub static constraint_exclusion_options: &[config_enum_entry] = &[
    config_enum_entry { name: "partition", val: CONSTRAINT_EXCLUSION_PARTITION, hidden: false },
    config_enum_entry { name: "on", val: CONSTRAINT_EXCLUSION_ON, hidden: false },
    config_enum_entry { name: "off", val: CONSTRAINT_EXCLUSION_OFF, hidden: false },
    config_enum_entry { name: "true", val: CONSTRAINT_EXCLUSION_ON, hidden: true },
    config_enum_entry { name: "false", val: CONSTRAINT_EXCLUSION_OFF, hidden: true },
    config_enum_entry { name: "yes", val: CONSTRAINT_EXCLUSION_ON, hidden: true },
    config_enum_entry { name: "no", val: CONSTRAINT_EXCLUSION_OFF, hidden: true },
    config_enum_entry { name: "1", val: CONSTRAINT_EXCLUSION_ON, hidden: true },
    config_enum_entry { name: "0", val: CONSTRAINT_EXCLUSION_OFF, hidden: true },
];

pub static synchronous_commit_options: &[config_enum_entry] = &[
    config_enum_entry { name: "local", val: SYNCHRONOUS_COMMIT_LOCAL_FLUSH, hidden: false },
    config_enum_entry { name: "remote_write", val: SYNCHRONOUS_COMMIT_REMOTE_WRITE, hidden: false },
    config_enum_entry { name: "remote_apply", val: SYNCHRONOUS_COMMIT_REMOTE_APPLY, hidden: false },
    config_enum_entry { name: "on", val: SYNCHRONOUS_COMMIT_REMOTE_FLUSH, hidden: false },
    config_enum_entry { name: "off", val: SYNCHRONOUS_COMMIT_OFF, hidden: false },
    config_enum_entry { name: "true", val: SYNCHRONOUS_COMMIT_REMOTE_FLUSH, hidden: true },
    config_enum_entry { name: "false", val: SYNCHRONOUS_COMMIT_OFF, hidden: true },
    config_enum_entry { name: "yes", val: SYNCHRONOUS_COMMIT_REMOTE_FLUSH, hidden: true },
    config_enum_entry { name: "no", val: SYNCHRONOUS_COMMIT_OFF, hidden: true },
    config_enum_entry { name: "1", val: SYNCHRONOUS_COMMIT_REMOTE_FLUSH, hidden: true },
    config_enum_entry { name: "0", val: SYNCHRONOUS_COMMIT_OFF, hidden: true },
];

pub static huge_pages_options: &[config_enum_entry] = &[
    config_enum_entry { name: "off", val: HUGE_PAGES_OFF, hidden: false },
    config_enum_entry { name: "on", val: HUGE_PAGES_ON, hidden: false },
    config_enum_entry { name: "try", val: HUGE_PAGES_TRY, hidden: false },
    config_enum_entry { name: "true", val: HUGE_PAGES_ON, hidden: true },
    config_enum_entry { name: "false", val: HUGE_PAGES_OFF, hidden: true },
    config_enum_entry { name: "yes", val: HUGE_PAGES_ON, hidden: true },
    config_enum_entry { name: "no", val: HUGE_PAGES_OFF, hidden: true },
    config_enum_entry { name: "1", val: HUGE_PAGES_ON, hidden: true },
    config_enum_entry { name: "0", val: HUGE_PAGES_OFF, hidden: true },
];

pub static huge_pages_status_options: &[config_enum_entry] = &[
    config_enum_entry { name: "off", val: HUGE_PAGES_OFF, hidden: false },
    config_enum_entry { name: "on", val: HUGE_PAGES_ON, hidden: false },
    config_enum_entry { name: "unknown", val: HUGE_PAGES_UNKNOWN, hidden: false },
];

pub static recovery_prefetch_options: &[config_enum_entry] = &[
    config_enum_entry { name: "off", val: RECOVERY_PREFETCH_OFF, hidden: false },
    config_enum_entry { name: "on", val: RECOVERY_PREFETCH_ON, hidden: false },
    config_enum_entry { name: "try", val: RECOVERY_PREFETCH_TRY, hidden: false },
    config_enum_entry { name: "true", val: RECOVERY_PREFETCH_ON, hidden: true },
    config_enum_entry { name: "false", val: RECOVERY_PREFETCH_OFF, hidden: true },
    config_enum_entry { name: "yes", val: RECOVERY_PREFETCH_ON, hidden: true },
    config_enum_entry { name: "no", val: RECOVERY_PREFETCH_OFF, hidden: true },
    config_enum_entry { name: "1", val: RECOVERY_PREFETCH_ON, hidden: true },
    config_enum_entry { name: "0", val: RECOVERY_PREFETCH_OFF, hidden: true },
];

pub static debug_parallel_query_options: &[config_enum_entry] = &[
    config_enum_entry { name: "off", val: DEBUG_PARALLEL_OFF, hidden: false },
    config_enum_entry { name: "on", val: DEBUG_PARALLEL_ON, hidden: false },
    config_enum_entry { name: "regress", val: DEBUG_PARALLEL_REGRESS, hidden: false },
    config_enum_entry { name: "true", val: DEBUG_PARALLEL_ON, hidden: true },
    config_enum_entry { name: "false", val: DEBUG_PARALLEL_OFF, hidden: true },
    config_enum_entry { name: "yes", val: DEBUG_PARALLEL_ON, hidden: true },
    config_enum_entry { name: "no", val: DEBUG_PARALLEL_OFF, hidden: true },
    config_enum_entry { name: "1", val: DEBUG_PARALLEL_ON, hidden: true },
    config_enum_entry { name: "0", val: DEBUG_PARALLEL_OFF, hidden: true },
];

pub static plan_cache_mode_options: &[config_enum_entry] = &[
    config_enum_entry { name: "auto", val: PLAN_CACHE_MODE_AUTO, hidden: false },
    config_enum_entry { name: "force_generic_plan", val: PLAN_CACHE_MODE_FORCE_GENERIC_PLAN, hidden: false },
    config_enum_entry { name: "force_custom_plan", val: PLAN_CACHE_MODE_FORCE_CUSTOM_PLAN, hidden: false },
];

pub static password_encryption_options: &[config_enum_entry] = &[
    config_enum_entry { name: "md5", val: PASSWORD_TYPE_MD5, hidden: false },
    config_enum_entry { name: "scram-sha-256", val: PASSWORD_TYPE_SCRAM_SHA_256, hidden: false },
];

const SSL_PROTOCOL_VERSIONS_INFO: [config_enum_entry; 5] = [
    config_enum_entry { name: "", val: PG_TLS_ANY, hidden: false },
    config_enum_entry { name: "TLSv1", val: PG_TLS1_VERSION, hidden: false },
    config_enum_entry { name: "TLSv1.1", val: PG_TLS1_1_VERSION, hidden: false },
    config_enum_entry { name: "TLSv1.2", val: PG_TLS1_2_VERSION, hidden: false },
    config_enum_entry { name: "TLSv1.3", val: PG_TLS1_3_VERSION, hidden: false },
];

pub static ssl_protocol_versions_info: &[config_enum_entry] = &SSL_PROTOCOL_VERSIONS_INFO;

pub static ssl_protocol_versions_info_without_any: &[config_enum_entry] = {
    const REST: &[config_enum_entry] = SSL_PROTOCOL_VERSIONS_INFO.split_first().unwrap().1;
    REST
};

pub static debug_logical_replication_streaming_options: &[config_enum_entry] = &[
    config_enum_entry { name: "buffered", val: DEBUG_LOGICAL_REP_STREAMING_BUFFERED, hidden: false },
    config_enum_entry { name: "immediate", val: DEBUG_LOGICAL_REP_STREAMING_IMMEDIATE, hidden: false },
];

pub static recovery_init_sync_method_options: &[config_enum_entry] = &[
    config_enum_entry { name: "fsync", val: DATA_DIR_SYNC_METHOD_FSYNC, hidden: false },
];

pub static shared_memory_options: &[config_enum_entry] = &[
    config_enum_entry { name: "sysv", val: SHMEM_TYPE_SYSV, hidden: false },
    config_enum_entry { name: "mmap", val: SHMEM_TYPE_MMAP, hidden: false },
];

// Reference build (../pgrust/postgres-18.3 pg_config.h): USE_LZ4, USE_ZSTD
// undefined — entries/boot values follow the C #else branches. USE_SSL /
// USE_OPENSSL are DEFINED here (be_secure_openssl): ssl_library, ssl_ciphers,
// ssl_groups carry the OpenSSL boot values.
pub static default_toast_compression_options: &[config_enum_entry] = &[
    config_enum_entry { name: "pglz", val: TOAST_PGLZ_COMPRESSION, hidden: false },
];

pub static wal_compression_options: &[config_enum_entry] = &[
    config_enum_entry { name: "pglz", val: WAL_COMPRESSION_PGLZ, hidden: false },
    config_enum_entry { name: "on", val: WAL_COMPRESSION_PGLZ, hidden: false },
    config_enum_entry { name: "off", val: WAL_COMPRESSION_NONE, hidden: false },
    config_enum_entry { name: "true", val: WAL_COMPRESSION_PGLZ, hidden: true },
    config_enum_entry { name: "false", val: WAL_COMPRESSION_NONE, hidden: true },
    config_enum_entry { name: "yes", val: WAL_COMPRESSION_PGLZ, hidden: true },
    config_enum_entry { name: "no", val: WAL_COMPRESSION_NONE, hidden: true },
    config_enum_entry { name: "1", val: WAL_COMPRESSION_PGLZ, hidden: true },
    config_enum_entry { name: "0", val: WAL_COMPRESSION_NONE, hidden: true },
];

// C compile-gates "clone" on platform support (guc_tables.c:489); pgrust has
// no clone_file port yet, so the entry is absent — the same surface as a C
// build without HAVE_COPYFILE/HAVE_COPY_FILE_RANGE. SET file_copy_method=clone
// is a clean invalid-value ERROR instead of copydir's unported-arm panic at
// CREATE DATABASE time. Restore the entry when clone_file (copydir.c) lands.
pub static file_copy_method_options: &[config_enum_entry] = &[
    config_enum_entry { name: "copy", val: FILE_COPY_METHOD_COPY, hidden: false },
];

pub static file_extend_method_options: &[config_enum_entry] = &[
    config_enum_entry { name: "write_zeros", val: FILE_EXTEND_METHOD_WRITE_ZEROS, hidden: false },
];

pub static ConfigureNamesBool: &[GucBoolSetting] = &[
    GucBoolSetting { name: "enable_seqscan", context: PGC_USERSET, group: QUERY_TUNING_METHOD, short_desc: Some("Enables the planner's use of sequential-scan plans."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::enable_seqscan, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "enable_indexscan", context: PGC_USERSET, group: QUERY_TUNING_METHOD, short_desc: Some("Enables the planner's use of index-scan plans."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::enable_indexscan, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "enable_indexonlyscan", context: PGC_USERSET, group: QUERY_TUNING_METHOD, short_desc: Some("Enables the planner's use of index-only-scan plans."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::enable_indexonlyscan, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "enable_bitmapscan", context: PGC_USERSET, group: QUERY_TUNING_METHOD, short_desc: Some("Enables the planner's use of bitmap-scan plans."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::enable_bitmapscan, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "enable_tidscan", context: PGC_USERSET, group: QUERY_TUNING_METHOD, short_desc: Some("Enables the planner's use of TID scan plans."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::enable_tidscan, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "enable_sort", context: PGC_USERSET, group: QUERY_TUNING_METHOD, short_desc: Some("Enables the planner's use of explicit sort steps."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::enable_sort, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "enable_incremental_sort", context: PGC_USERSET, group: QUERY_TUNING_METHOD, short_desc: Some("Enables the planner's use of incremental sort steps."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::enable_incremental_sort, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "enable_hashagg", context: PGC_USERSET, group: QUERY_TUNING_METHOD, short_desc: Some("Enables the planner's use of hashed aggregation plans."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::enable_hashagg, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "enable_material", context: PGC_USERSET, group: QUERY_TUNING_METHOD, short_desc: Some("Enables the planner's use of materialization."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::enable_material, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "enable_memoize", context: PGC_USERSET, group: QUERY_TUNING_METHOD, short_desc: Some("Enables the planner's use of memoization."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::enable_memoize, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "enable_nestloop", context: PGC_USERSET, group: QUERY_TUNING_METHOD, short_desc: Some("Enables the planner's use of nested-loop join plans."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::enable_nestloop, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "enable_mergejoin", context: PGC_USERSET, group: QUERY_TUNING_METHOD, short_desc: Some("Enables the planner's use of merge join plans."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::enable_mergejoin, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "enable_hashjoin", context: PGC_USERSET, group: QUERY_TUNING_METHOD, short_desc: Some("Enables the planner's use of hash join plans."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::enable_hashjoin, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "enable_gathermerge", context: PGC_USERSET, group: QUERY_TUNING_METHOD, short_desc: Some("Enables the planner's use of gather merge plans."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::enable_gathermerge, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "enable_partitionwise_join", context: PGC_USERSET, group: QUERY_TUNING_METHOD, short_desc: Some("Enables partitionwise join."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::enable_partitionwise_join, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "enable_partitionwise_aggregate", context: PGC_USERSET, group: QUERY_TUNING_METHOD, short_desc: Some("Enables partitionwise aggregation and grouping."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::enable_partitionwise_aggregate, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "enable_parallel_append", context: PGC_USERSET, group: QUERY_TUNING_METHOD, short_desc: Some("Enables the planner's use of parallel append plans."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::enable_parallel_append, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "enable_parallel_hash", context: PGC_USERSET, group: QUERY_TUNING_METHOD, short_desc: Some("Enables the planner's use of parallel hash plans."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::enable_parallel_hash, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "enable_partition_pruning", context: PGC_USERSET, group: QUERY_TUNING_METHOD, short_desc: Some("Enables plan-time and execution-time partition pruning."), long_desc: Some("Allows the query planner and executor to compare partition bounds to conditions in the query to determine which partitions must be scanned."), flags: GUC_EXPLAIN, variable: &vars::enable_partition_pruning, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "enable_presorted_aggregate", context: PGC_USERSET, group: QUERY_TUNING_METHOD, short_desc: Some("Enables the planner's ability to produce plans that provide presorted input for ORDER BY / DISTINCT aggregate functions."), long_desc: Some("Allows the query planner to build plans that provide presorted input for aggregate functions with an ORDER BY / DISTINCT clause.  When disabled, implicit sorts are always performed during execution."), flags: GUC_EXPLAIN, variable: &vars::enable_presorted_aggregate, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "enable_async_append", context: PGC_USERSET, group: QUERY_TUNING_METHOD, short_desc: Some("Enables the planner's use of async append plans."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::enable_async_append, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "enable_self_join_elimination", context: PGC_USERSET, group: QUERY_TUNING_METHOD, short_desc: Some("Enables removal of unique self-joins."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::enable_self_join_elimination, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "enable_group_by_reordering", context: PGC_USERSET, group: QUERY_TUNING_METHOD, short_desc: Some("Enables reordering of GROUP BY keys."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::enable_group_by_reordering, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "enable_distinct_reordering", context: PGC_USERSET, group: QUERY_TUNING_METHOD, short_desc: Some("Enables reordering of DISTINCT keys."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::enable_distinct_reordering, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "geqo", context: PGC_USERSET, group: QUERY_TUNING_GEQO, short_desc: Some("Enables genetic query optimization."), long_desc: Some("This algorithm attempts to do planning without exhaustive searching."), flags: GUC_EXPLAIN, variable: &vars::enable_geqo, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "is_superuser", context: PGC_INTERNAL, group: UNGROUPED, short_desc: Some("Shows whether the current user is a superuser."), long_desc: None, flags: GUC_REPORT | GUC_NO_SHOW_ALL | GUC_NO_RESET_ALL | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE | GUC_ALLOW_IN_PARALLEL, variable: &vars::current_role_is_superuser, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "allow_alter_system", context: PGC_SIGHUP, group: COMPAT_OPTIONS_OTHER, short_desc: Some("Allows running the ALTER SYSTEM command."), long_desc: Some("Can be set to off for environments where global configuration changes should be made using a different method."), flags: GUC_DISALLOW_IN_AUTO_FILE, variable: &vars::AllowAlterSystem, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "bonjour", context: PGC_POSTMASTER, group: CONN_AUTH_SETTINGS, short_desc: Some("Enables advertising the server via Bonjour."), long_desc: None, flags: 0, variable: &vars::enable_bonjour, boot_val: GucDefaultValue::Bool(false), check_hook: Some(&hooks::check_bonjour), assign_hook: None, show_hook: None },
    GucBoolSetting { name: "track_commit_timestamp", context: PGC_POSTMASTER, group: REPLICATION_SENDING, short_desc: Some("Collects transaction commit time."), long_desc: None, flags: 0, variable: &vars::track_commit_timestamp, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "ssl", context: PGC_SIGHUP, group: CONN_AUTH_SSL, short_desc: Some("Enables SSL connections."), long_desc: None, flags: 0, variable: &vars::EnableSSL, boot_val: GucDefaultValue::Bool(false), check_hook: Some(&hooks::check_ssl), assign_hook: None, show_hook: None },
    GucBoolSetting { name: "ssl_passphrase_command_supports_reload", context: PGC_SIGHUP, group: CONN_AUTH_SSL, short_desc: Some("Controls whether \"ssl_passphrase_command\" is called during server reload."), long_desc: None, flags: 0, variable: &vars::ssl_passphrase_command_supports_reload, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "ssl_prefer_server_ciphers", context: PGC_SIGHUP, group: CONN_AUTH_SSL, short_desc: Some("Give priority to server ciphersuite order."), long_desc: None, flags: 0, variable: &vars::SSLPreferServerCiphers, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "fsync", context: PGC_SIGHUP, group: WAL_SETTINGS, short_desc: Some("Forces synchronization of updates to disk."), long_desc: Some("The server will use the fsync() system call in several places to make sure that updates are physically written to disk. This ensures that a database cluster will recover to a consistent state after an operating system or hardware crash."), flags: 0, variable: &vars::enableFsync, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "ignore_checksum_failure", context: PGC_SUSET, group: DEVELOPER_OPTIONS, short_desc: Some("Continues processing after a checksum failure."), long_desc: Some("Detection of a checksum failure normally causes PostgreSQL to report an error, aborting the current transaction. Setting ignore_checksum_failure to true causes the system to ignore the failure (but still report a warning), and continue processing. This behavior could cause crashes or other serious problems. Only has an effect if checksums are enabled."), flags: GUC_NOT_IN_SAMPLE, variable: &vars::ignore_checksum_failure, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "zero_damaged_pages", context: PGC_SUSET, group: DEVELOPER_OPTIONS, short_desc: Some("Continues processing past damaged page headers."), long_desc: Some("Detection of a damaged page header normally causes PostgreSQL to report an error, aborting the current transaction. Setting \"zero_damaged_pages\" to true causes the system to instead report a warning, zero out the damaged page, and continue processing. This behavior will destroy data, namely all the rows on the damaged page."), flags: GUC_NOT_IN_SAMPLE, variable: &vars::zero_damaged_pages, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "ignore_invalid_pages", context: PGC_POSTMASTER, group: DEVELOPER_OPTIONS, short_desc: Some("Continues recovery after an invalid pages failure."), long_desc: Some("Detection of WAL records having references to invalid pages during recovery causes PostgreSQL to raise a PANIC-level error, aborting the recovery. Setting \"ignore_invalid_pages\" to true causes the system to ignore invalid page references in WAL records (but still report a warning), and continue recovery. This behavior may cause crashes, data loss, propagate or hide corruption, or other serious problems. Only has an effect during recovery or in standby mode."), flags: GUC_NOT_IN_SAMPLE, variable: &vars::ignore_invalid_pages, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "full_page_writes", context: PGC_SIGHUP, group: WAL_SETTINGS, short_desc: Some("Writes full pages to WAL when first modified after a checkpoint."), long_desc: Some("A page write in process during an operating system crash might be only partially written to disk.  During recovery, the row changes stored in WAL are not enough to recover.  This option writes pages when first modified after a checkpoint to WAL so full recovery is possible."), flags: 0, variable: &vars::fullPageWrites, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "wal_log_hints", context: PGC_POSTMASTER, group: WAL_SETTINGS, short_desc: Some("Writes full pages to WAL when first modified after a checkpoint, even for a non-critical modification."), long_desc: None, flags: 0, variable: &vars::wal_log_hints, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "wal_init_zero", context: PGC_SUSET, group: WAL_SETTINGS, short_desc: Some("Writes zeroes to new WAL files before first use."), long_desc: None, flags: 0, variable: &vars::wal_init_zero, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "wal_recycle", context: PGC_SUSET, group: WAL_SETTINGS, short_desc: Some("Recycles WAL files by renaming them."), long_desc: None, flags: 0, variable: &vars::wal_recycle, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "log_checkpoints", context: PGC_SIGHUP, group: LOGGING_WHAT, short_desc: Some("Logs each checkpoint."), long_desc: None, flags: 0, variable: &vars::log_checkpoints, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "trace_connection_negotiation", context: PGC_POSTMASTER, group: DEVELOPER_OPTIONS, short_desc: Some("Logs details of pre-authentication connection handshake."), long_desc: None, flags: GUC_NOT_IN_SAMPLE, variable: &vars::Trace_connection_negotiation, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "log_disconnections", context: PGC_SU_BACKEND, group: LOGGING_WHAT, short_desc: Some("Logs end of a session, including duration."), long_desc: None, flags: 0, variable: &vars::Log_disconnections, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "log_replication_commands", context: PGC_SUSET, group: LOGGING_WHAT, short_desc: Some("Logs each replication command."), long_desc: None, flags: 0, variable: &vars::log_replication_commands, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "debug_assertions", context: PGC_INTERNAL, group: PRESET_OPTIONS, short_desc: Some("Shows whether the running server has assertion checks enabled."), long_desc: None, flags: GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE, variable: &vars::assert_enabled, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "exit_on_error", context: PGC_USERSET, group: ERROR_HANDLING_OPTIONS, short_desc: Some("Terminate session on any error."), long_desc: None, flags: 0, variable: &vars::ExitOnAnyError, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "restart_after_crash", context: PGC_SIGHUP, group: ERROR_HANDLING_OPTIONS, short_desc: Some("Reinitialize server after backend crash."), long_desc: None, flags: 0, variable: &vars::restart_after_crash, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "remove_temp_files_after_crash", context: PGC_SIGHUP, group: DEVELOPER_OPTIONS, short_desc: Some("Remove temporary files after backend crash."), long_desc: None, flags: GUC_NOT_IN_SAMPLE, variable: &vars::remove_temp_files_after_crash, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "send_abort_for_crash", context: PGC_SIGHUP, group: DEVELOPER_OPTIONS, short_desc: Some("Send SIGABRT not SIGQUIT to child processes after backend crash."), long_desc: None, flags: GUC_NOT_IN_SAMPLE, variable: &vars::send_abort_for_crash, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "send_abort_for_kill", context: PGC_SIGHUP, group: DEVELOPER_OPTIONS, short_desc: Some("Send SIGABRT not SIGKILL to stuck child processes."), long_desc: None, flags: GUC_NOT_IN_SAMPLE, variable: &vars::send_abort_for_kill, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "log_duration", context: PGC_SUSET, group: LOGGING_WHAT, short_desc: Some("Logs the duration of each completed SQL statement."), long_desc: None, flags: 0, variable: &vars::log_duration, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "debug_print_parse", context: PGC_USERSET, group: LOGGING_WHAT, short_desc: Some("Logs each query's parse tree."), long_desc: None, flags: 0, variable: &vars::Debug_print_parse, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "debug_print_rewritten", context: PGC_USERSET, group: LOGGING_WHAT, short_desc: Some("Logs each query's rewritten parse tree."), long_desc: None, flags: 0, variable: &vars::Debug_print_rewritten, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "debug_print_plan", context: PGC_USERSET, group: LOGGING_WHAT, short_desc: Some("Logs each query's execution plan."), long_desc: None, flags: 0, variable: &vars::Debug_print_plan, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "debug_pretty_print", context: PGC_USERSET, group: LOGGING_WHAT, short_desc: Some("Indents parse and plan tree displays."), long_desc: None, flags: 0, variable: &vars::Debug_pretty_print, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "log_parser_stats", context: PGC_SUSET, group: STATS_MONITORING, short_desc: Some("Writes parser performance statistics to the server log."), long_desc: None, flags: 0, variable: &vars::log_parser_stats, boot_val: GucDefaultValue::Bool(false), check_hook: Some(&hooks::check_stage_log_stats), assign_hook: None, show_hook: None },
    GucBoolSetting { name: "log_planner_stats", context: PGC_SUSET, group: STATS_MONITORING, short_desc: Some("Writes planner performance statistics to the server log."), long_desc: None, flags: 0, variable: &vars::log_planner_stats, boot_val: GucDefaultValue::Bool(false), check_hook: Some(&hooks::check_stage_log_stats), assign_hook: None, show_hook: None },
    GucBoolSetting { name: "log_executor_stats", context: PGC_SUSET, group: STATS_MONITORING, short_desc: Some("Writes executor performance statistics to the server log."), long_desc: None, flags: 0, variable: &vars::log_executor_stats, boot_val: GucDefaultValue::Bool(false), check_hook: Some(&hooks::check_stage_log_stats), assign_hook: None, show_hook: None },
    GucBoolSetting { name: "log_statement_stats", context: PGC_SUSET, group: STATS_MONITORING, short_desc: Some("Writes cumulative performance statistics to the server log."), long_desc: None, flags: 0, variable: &vars::log_statement_stats, boot_val: GucDefaultValue::Bool(false), check_hook: Some(&hooks::check_log_stats), assign_hook: None, show_hook: None },
    GucBoolSetting { name: "track_activities", context: PGC_SUSET, group: STATS_CUMULATIVE, short_desc: Some("Collects information about executing commands."), long_desc: Some("Enables the collection of information on the currently executing command of each session, along with the time at which that command began execution."), flags: 0, variable: &vars::pgstat_track_activities, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "track_counts", context: PGC_SUSET, group: STATS_CUMULATIVE, short_desc: Some("Collects statistics on database activity."), long_desc: None, flags: 0, variable: &vars::pgstat_track_counts, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "track_cost_delay_timing", context: PGC_SUSET, group: STATS_CUMULATIVE, short_desc: Some("Collects timing statistics for cost-based vacuum delay."), long_desc: None, flags: 0, variable: &vars::track_cost_delay_timing, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "track_io_timing", context: PGC_SUSET, group: STATS_CUMULATIVE, short_desc: Some("Collects timing statistics for database I/O activity."), long_desc: None, flags: 0, variable: &vars::track_io_timing, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "track_wal_io_timing", context: PGC_SUSET, group: STATS_CUMULATIVE, short_desc: Some("Collects timing statistics for WAL I/O activity."), long_desc: None, flags: 0, variable: &vars::track_wal_io_timing, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "update_process_title", context: PGC_SUSET, group: PROCESS_TITLE, short_desc: Some("Updates the process title to show the active SQL command."), long_desc: Some("Enables updating of the process title every time a new SQL command is received by the server."), flags: 0, variable: &vars::update_process_title, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "autovacuum", context: PGC_SIGHUP, group: VACUUM_AUTOVACUUM, short_desc: Some("Starts the autovacuum subprocess."), long_desc: None, flags: 0, variable: &vars::autovacuum_start_daemon, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "trace_notify", context: PGC_USERSET, group: DEVELOPER_OPTIONS, short_desc: Some("Generates debugging output for LISTEN and NOTIFY."), long_desc: None, flags: GUC_NOT_IN_SAMPLE, variable: &vars::Trace_notify, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "log_lock_waits", context: PGC_SUSET, group: LOGGING_WHAT, short_desc: Some("Logs long lock waits."), long_desc: None, flags: 0, variable: &vars::log_lock_waits, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "log_lock_failures", context: PGC_SUSET, group: LOGGING_WHAT, short_desc: Some("Logs lock failures."), long_desc: None, flags: 0, variable: &vars::log_lock_failures, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "log_recovery_conflict_waits", context: PGC_SIGHUP, group: LOGGING_WHAT, short_desc: Some("Logs standby recovery conflict waits."), long_desc: None, flags: 0, variable: &vars::log_recovery_conflict_waits, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "log_hostname", context: PGC_SIGHUP, group: LOGGING_WHAT, short_desc: Some("Logs the host name in the connection logs."), long_desc: Some("By default, connection logs only show the IP address of the connecting host. If you want them to show the host name you can turn this on, but depending on your host name resolution setup it might impose a non-negligible performance penalty."), flags: 0, variable: &vars::log_hostname, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "transform_null_equals", context: PGC_USERSET, group: COMPAT_OPTIONS_OTHER, short_desc: Some("Treats \"expr=NULL\" as \"expr IS NULL\"."), long_desc: Some("When turned on, expressions of the form expr = NULL (or NULL = expr) are treated as expr IS NULL, that is, they return true if expr evaluates to the null value, and false otherwise. The correct behavior of expr = NULL is to always return null (unknown)."), flags: 0, variable: &vars::Transform_null_equals, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "default_transaction_read_only", context: PGC_USERSET, group: CLIENT_CONN_STATEMENT, short_desc: Some("Sets the default read-only status of new transactions."), long_desc: None, flags: GUC_REPORT, variable: &vars::DefaultXactReadOnly, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "transaction_read_only", context: PGC_USERSET, group: CLIENT_CONN_STATEMENT, short_desc: Some("Sets the current transaction's read-only status."), long_desc: None, flags: GUC_NO_RESET | GUC_NO_RESET_ALL | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE, variable: &vars::XactReadOnly, boot_val: GucDefaultValue::Bool(false), check_hook: Some(&hooks::check_transaction_read_only), assign_hook: None, show_hook: None },
    GucBoolSetting { name: "default_transaction_deferrable", context: PGC_USERSET, group: CLIENT_CONN_STATEMENT, short_desc: Some("Sets the default deferrable status of new transactions."), long_desc: None, flags: 0, variable: &vars::DefaultXactDeferrable, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "transaction_deferrable", context: PGC_USERSET, group: CLIENT_CONN_STATEMENT, short_desc: Some("Whether to defer a read-only serializable transaction until it can be executed with no possible serialization failures."), long_desc: None, flags: GUC_NO_RESET | GUC_NO_RESET_ALL | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE, variable: &vars::XactDeferrable, boot_val: GucDefaultValue::Bool(false), check_hook: Some(&hooks::check_transaction_deferrable), assign_hook: None, show_hook: None },
    GucBoolSetting { name: "row_security", context: PGC_USERSET, group: CLIENT_CONN_STATEMENT, short_desc: Some("Enables row security."), long_desc: Some("When enabled, row security will be applied to all users."), flags: 0, variable: &vars::row_security, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "check_function_bodies", context: PGC_USERSET, group: CLIENT_CONN_STATEMENT, short_desc: Some("Check routine bodies during CREATE FUNCTION and CREATE PROCEDURE."), long_desc: None, flags: 0, variable: &vars::check_function_bodies, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "array_nulls", context: PGC_USERSET, group: COMPAT_OPTIONS_PREVIOUS, short_desc: Some("Enables input of NULL elements in arrays."), long_desc: Some("When turned on, unquoted NULL in an array input value means a null value; otherwise it is taken literally."), flags: 0, variable: &vars::Array_nulls, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "default_with_oids", context: PGC_USERSET, group: COMPAT_OPTIONS_PREVIOUS, short_desc: Some("WITH OIDS is no longer supported; this can only be false."), long_desc: None, flags: GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE, variable: &vars::default_with_oids, boot_val: GucDefaultValue::Bool(false), check_hook: Some(&hooks::check_default_with_oids), assign_hook: None, show_hook: None },
    GucBoolSetting { name: "logging_collector", context: PGC_POSTMASTER, group: LOGGING_WHERE, short_desc: Some("Start a subprocess to capture stderr, csvlog and/or jsonlog into log files."), long_desc: None, flags: 0, variable: &vars::Logging_collector, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "log_truncate_on_rotation", context: PGC_SIGHUP, group: LOGGING_WHERE, short_desc: Some("Truncate existing log files of same name during log rotation."), long_desc: None, flags: 0, variable: &vars::Log_truncate_on_rotation, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "trace_sort", context: PGC_USERSET, group: DEVELOPER_OPTIONS, short_desc: Some("Emit information about resource usage in sorting."), long_desc: None, flags: GUC_NOT_IN_SAMPLE, variable: &vars::trace_sort, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "integer_datetimes", context: PGC_INTERNAL, group: PRESET_OPTIONS, short_desc: Some("Shows whether datetimes are integer based."), long_desc: None, flags: GUC_REPORT | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE, variable: &vars::integer_datetimes, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "krb_caseins_users", context: PGC_SIGHUP, group: CONN_AUTH_AUTH, short_desc: Some("Sets whether Kerberos and GSSAPI user names should be treated as case-insensitive."), long_desc: None, flags: 0, variable: &vars::pg_krb_caseins_users, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "gss_accept_delegation", context: PGC_SIGHUP, group: CONN_AUTH_AUTH, short_desc: Some("Sets whether GSSAPI delegation should be accepted from the client."), long_desc: None, flags: 0, variable: &vars::pg_gss_accept_delegation, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "escape_string_warning", context: PGC_USERSET, group: COMPAT_OPTIONS_PREVIOUS, short_desc: Some("Warn about backslash escapes in ordinary string literals."), long_desc: None, flags: 0, variable: &vars::escape_string_warning, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "standard_conforming_strings", context: PGC_USERSET, group: COMPAT_OPTIONS_PREVIOUS, short_desc: Some("Causes '...' strings to treat backslashes literally."), long_desc: None, flags: GUC_REPORT, variable: &vars::standard_conforming_strings, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "synchronize_seqscans", context: PGC_USERSET, group: COMPAT_OPTIONS_PREVIOUS, short_desc: Some("Enables synchronized sequential scans."), long_desc: None, flags: 0, variable: &vars::synchronize_seqscans, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "recovery_target_inclusive", context: PGC_POSTMASTER, group: WAL_RECOVERY_TARGET, short_desc: Some("Sets whether to include or exclude transaction with recovery target."), long_desc: None, flags: 0, variable: &vars::recoveryTargetInclusive, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "summarize_wal", context: PGC_SIGHUP, group: WAL_SUMMARIZATION, short_desc: Some("Starts the WAL summarizer process to enable incremental backup."), long_desc: None, flags: 0, variable: &vars::summarize_wal, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "hot_standby", context: PGC_POSTMASTER, group: REPLICATION_STANDBY, short_desc: Some("Allows connections and queries during recovery."), long_desc: None, flags: 0, variable: &vars::EnableHotStandby, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "hot_standby_feedback", context: PGC_SIGHUP, group: REPLICATION_STANDBY, short_desc: Some("Allows feedback from a hot standby to the primary that will avoid query conflicts."), long_desc: None, flags: 0, variable: &vars::hot_standby_feedback, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "in_hot_standby", context: PGC_INTERNAL, group: PRESET_OPTIONS, short_desc: Some("Shows whether hot standby is currently active."), long_desc: None, flags: GUC_REPORT | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE, variable: &vars::in_hot_standby_guc, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: Some(&hooks::show_in_hot_standby) },
    GucBoolSetting { name: "allow_system_table_mods", context: PGC_SUSET, group: DEVELOPER_OPTIONS, short_desc: Some("Allows modifications of the structure of system tables."), long_desc: None, flags: GUC_NOT_IN_SAMPLE, variable: &vars::allowSystemTableMods, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "ignore_system_indexes", context: PGC_BACKEND, group: DEVELOPER_OPTIONS, short_desc: Some("Disables reading from system indexes."), long_desc: Some("It does not prevent updating the indexes, so it is safe to use.  The worst consequence is slowness."), flags: GUC_NOT_IN_SAMPLE, variable: &vars::IgnoreSystemIndexes, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "allow_in_place_tablespaces", context: PGC_SUSET, group: DEVELOPER_OPTIONS, short_desc: Some("Allows tablespaces directly inside pg_tblspc, for testing."), long_desc: None, flags: GUC_NOT_IN_SAMPLE, variable: &vars::allow_in_place_tablespaces, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "lo_compat_privileges", context: PGC_SUSET, group: COMPAT_OPTIONS_PREVIOUS, short_desc: Some("Enables backward compatibility mode for privilege checks on large objects."), long_desc: Some("Skips privilege checks when reading or modifying large objects, for compatibility with PostgreSQL releases prior to 9.0."), flags: 0, variable: &vars::lo_compat_privileges, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "quote_all_identifiers", context: PGC_USERSET, group: COMPAT_OPTIONS_PREVIOUS, short_desc: Some("When generating SQL fragments, quote all identifiers."), long_desc: None, flags: 0, variable: &vars::quote_all_identifiers, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "data_checksums", context: PGC_INTERNAL, group: PRESET_OPTIONS, short_desc: Some("Shows whether data checksums are turned on for this cluster."), long_desc: None, flags: GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE | GUC_RUNTIME_COMPUTED, variable: &vars::data_checksums, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: Some(&hooks::show_data_checksums) },
    GucBoolSetting { name: "syslog_sequence_numbers", context: PGC_SIGHUP, group: LOGGING_WHERE, short_desc: Some("Add sequence number to syslog messages to avoid duplicate suppression."), long_desc: None, flags: 0, variable: &vars::syslog_sequence_numbers, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "syslog_split_messages", context: PGC_SIGHUP, group: LOGGING_WHERE, short_desc: Some("Split messages sent to syslog by lines and to fit into 1024 bytes."), long_desc: None, flags: 0, variable: &vars::syslog_split_messages, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "parallel_leader_participation", context: PGC_USERSET, group: RESOURCES_WORKER_PROCESSES, short_desc: Some("Controls whether Gather and Gather Merge also run subplans."), long_desc: Some("Should gather nodes also run subplans or just gather tuples?"), flags: GUC_EXPLAIN, variable: &vars::parallel_leader_participation, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "jit", context: PGC_USERSET, group: QUERY_TUNING_OTHER, short_desc: Some("Allow JIT compilation."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::jit_enabled, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "jit_debugging_support", context: PGC_SU_BACKEND, group: DEVELOPER_OPTIONS, short_desc: Some("Register JIT-compiled functions with debugger."), long_desc: None, flags: GUC_NOT_IN_SAMPLE, variable: &vars::jit_debugging_support, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "jit_dump_bitcode", context: PGC_SUSET, group: DEVELOPER_OPTIONS, short_desc: Some("Write out LLVM bitcode to facilitate JIT debugging."), long_desc: None, flags: GUC_NOT_IN_SAMPLE, variable: &vars::jit_dump_bitcode, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "jit_expressions", context: PGC_USERSET, group: DEVELOPER_OPTIONS, short_desc: Some("Allow JIT compilation of expressions."), long_desc: None, flags: GUC_NOT_IN_SAMPLE, variable: &vars::jit_expressions, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "jit_profiling_support", context: PGC_SU_BACKEND, group: DEVELOPER_OPTIONS, short_desc: Some("Register JIT-compiled functions with perf profiler."), long_desc: None, flags: GUC_NOT_IN_SAMPLE, variable: &vars::jit_profiling_support, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "jit_tuple_deforming", context: PGC_USERSET, group: DEVELOPER_OPTIONS, short_desc: Some("Allow JIT compilation of tuple deforming."), long_desc: None, flags: GUC_NOT_IN_SAMPLE, variable: &vars::jit_tuple_deforming, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "data_sync_retry", context: PGC_POSTMASTER, group: ERROR_HANDLING_OPTIONS, short_desc: Some("Whether to continue running after a failure to sync data files."), long_desc: None, flags: 0, variable: &vars::data_sync_retry, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "wal_receiver_create_temp_slot", context: PGC_SIGHUP, group: REPLICATION_STANDBY, short_desc: Some("Sets whether a WAL receiver should create a temporary replication slot if no permanent slot is configured."), long_desc: None, flags: 0, variable: &vars::wal_receiver_create_temp_slot, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "event_triggers", context: PGC_SUSET, group: CLIENT_CONN_STATEMENT, short_desc: Some("Enables event triggers."), long_desc: Some("When enabled, event triggers will fire for all applicable statements."), flags: 0, variable: &vars::event_triggers, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "sync_replication_slots", context: PGC_SIGHUP, group: REPLICATION_STANDBY, short_desc: Some("Enables a physical standby to synchronize logical failover replication slots from the primary server."), long_desc: None, flags: 0, variable: &vars::sync_replication_slots, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "md5_password_warnings", context: PGC_USERSET, group: CONN_AUTH_AUTH, short_desc: Some("Enables deprecation warnings for MD5 passwords."), long_desc: None, flags: 0, variable: &vars::md5_password_warnings, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "vacuum_truncate", context: PGC_USERSET, group: VACUUM_DEFAULT, short_desc: Some("Enables vacuum to truncate empty pages at the end of the table."), long_desc: None, flags: 0, variable: &vars::vacuum_truncate, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    // pgrust-only (no C counterpart): the lane-v2 push executor's master
    // switch. Default ON (2026-07-14); the PGRUST_LANE_V2 boot env var sets
    // the startup default (=0|off -> default off) via
    // initialize_guc_options_from_environment (PGC_S_ENV_VAR) — the fleet
    // harness / kill-switch path. The session backing cell IS the gate the
    // executor reads, so SET / SET LOCAL re-evaluates it on the next query.
    GucBoolSetting { name: "pgrust.lane_executor", context: PGC_USERSET, group: CUSTOM_OPTIONS, short_desc: Some("Enables the lane-v2 push executor."), long_desc: None, flags: 0, variable: &vars::pgrust_lane_executor, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    // pgrust.condition_cache: the pgrcolumnar per-granule qual-verdict cache
    // (ClickHouse QueryConditionCache counterpart; approved 2026-07-10 as
    // the one sanctioned cross-query in-memory cache, GUC-gated). Default
    // OFF; benchmark arms enable it explicitly and record it in manifests.
    GucBoolSetting { name: "pgrust.condition_cache", context: PGC_USERSET, group: CUSTOM_OPTIONS, short_desc: Some("Enables the cbstore condition cache (cross-query cached qual verdicts per granule)."), long_desc: None, flags: 0, variable: &vars::pgrust_condition_cache, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    // pgrust.runtime (pgrust-only, env-to-guc train): the M0 master switch for
    // the morsel runtime worker pool, replacing the PGRUST_RUNTIME env var as
    // the product surface. PGC_POSTMASTER (the pool spawns once at boot),
    // default ON. PGRUST_RUNTIME=0 seeds this off at boot; postgresql.conf may
    // also set it. Off => the pool is never spawned, so the whole runtime
    // engine stays inert (every arm falls back to the serial/Gather plan).
    GucBoolSetting { name: "pgrust.runtime", context: PGC_POSTMASTER, group: CUSTOM_OPTIONS, short_desc: Some("Enables the pgrust morsel runtime worker pool (the parallel analytics engine)."), long_desc: Some("Off restores the pool-less process exactly. The PGRUST_RUNTIME environment variable seeds the startup default."), flags: 0, variable: &vars::pgrust_runtime, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    // pgrust.runtime_vacuum_pool (pgrust-only, GL-M41-3 flip): parallel
    // VACUUM's driver on the morsel-pool workers (M4.1 ⊕ Q2). PGC_POSTMASTER
    // (the reader is a per-process cell consulted at each vacuum; the pool
    // identity glue it composes with is spawn-time), default ON since the
    // train-40 flip. Off restores the launched bgworker gang exactly.
    GucBoolSetting { name: "pgrust.runtime_vacuum_pool", context: PGC_POSTMASTER, group: CUSTOM_OPTIONS, short_desc: Some("Runs parallel VACUUM's workers on the pgrust runtime worker pool."), long_desc: Some("Off restores the launched background-worker vacuum gang exactly. The PGRUST_RUNTIME_VACUUM_POOL environment variable seeds the startup default (0 or off disables)."), flags: 0, variable: &vars::pgrust_runtime_vacuum_pool, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    // pgrust.memory_watchdog (pgrust-only, GL-MEMWATCH-1): master switch for
    // the postmaster memory-watchdog sampler. PGC_SIGHUP: the watchdog thread
    // re-reads the cell each tick, so ops can arm/disarm on a running server.
    // Default ON — the sampler is off every query path (a 1s tick reading
    // /proc and two atomics); under an unbounded cgroup with no configured
    // limit it idles after one boot log line.
    GucBoolSetting { name: "pgrust.memory_watchdog", context: PGC_SIGHUP, group: CUSTOM_OPTIONS, short_desc: Some("Enables the process memory watchdog (logs memory ledgers and context dumps at escalating thresholds below the memory limit)."), long_desc: Some("The limit is pgrust.memory_watchdog_limit, or the cgroup v2 memory.max when the limit is 0."), flags: 0, variable: &vars::pgrust_memory_watchdog, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    // pgrust.memory_watchdog_dump (pgrust-only, GL-MEMWATCH-1): on a threshold
    // breach, additionally signal every live backend to log its memory-context
    // tree (the pg_log_backend_memory_contexts machinery, fanned out).
    GucBoolSetting { name: "pgrust.memory_watchdog_dump", context: PGC_SIGHUP, group: CUSTOM_OPTIONS, short_desc: Some("Requests memory-context dumps from all backends when the memory watchdog fires."), long_desc: None, flags: 0, variable: &vars::pgrust_memory_watchdog_dump, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    // pgrust.mem_autotune (pgrust-only, env-to-guc train): gates the boot-time
    // machine-scaled memory/parallel default auto-tune (autotune.rs, assembled
    // from night/mem-defaults). PGC_POSTMASTER, default OFF (stock boot values,
    // so SHOW ALL / pg_settings conformance is unaffected unless opted in).
    // Seeded by the PGRUST_MEM_AUTOTUNE boot env var.
    GucBoolSetting { name: "pgrust.mem_autotune", context: PGC_POSTMASTER, group: CUSTOM_OPTIONS, short_desc: Some("Applies machine-scaled memory and parallelism defaults at server startup."), long_desc: Some("Off keeps the stock boot defaults. The PGRUST_MEM_AUTOTUNE environment variable seeds the startup default."), flags: 0, variable: &vars::pgrust_mem_autotune, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    // pgrust.regex_pattern_program: the anchored pattern-program regex fast
    // tier under the auto RE2 dispatch (regexp_alt::program). Hidden like
    // regex_engine; OFF restores the exact pre-tier RE2 arm — the toggle is
    // the four-engine differential's fourth arm and the escape hatch.
    GucBoolSetting { name: "pgrust.regex_pattern_program", context: PGC_USERSET, group: DEVELOPER_OPTIONS, short_desc: Some("Enables the anchored pattern-program fast tier for RE2-dispatched regexps."), long_desc: None, flags: GUC_NOT_IN_SAMPLE | GUC_NO_SHOW_ALL, variable: &vars::pgrust_regex_pattern_program, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    // pgrust.regex_re2_linked: build-property preset (debug_assertions
    // shape) — the runtime witness that this binary carries the RE2 tier.
    // regexp_alt installs the accessor; SHOW reports the build cfg, not the
    // boot_val below.
    GucBoolSetting { name: "pgrust.regex_re2_linked", context: PGC_INTERNAL, group: PRESET_OPTIONS, short_desc: Some("Shows whether the RE2 regexp engine was linked into this build."), long_desc: Some("Off means regex_engine=auto has only the Spencer tier and SET regex_engine=re2 errors at run time."), flags: GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE, variable: &vars::pgrust_regex_re2_linked, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    // auto_explain custom GUCs (statically defined; see vars.rs note).
    GucBoolSetting { name: "auto_explain.log_analyze", context: PGC_SUSET, group: CUSTOM_OPTIONS, short_desc: Some("Use EXPLAIN ANALYZE for plan logging."), long_desc: None, flags: 0, variable: &vars::aex_log_analyze, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "auto_explain.log_settings", context: PGC_SUSET, group: CUSTOM_OPTIONS, short_desc: Some("Log modified configuration parameters affecting query planning."), long_desc: None, flags: 0, variable: &vars::aex_log_settings, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "auto_explain.log_verbose", context: PGC_SUSET, group: CUSTOM_OPTIONS, short_desc: Some("Use EXPLAIN VERBOSE for plan logging."), long_desc: None, flags: 0, variable: &vars::aex_log_verbose, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "auto_explain.log_buffers", context: PGC_SUSET, group: CUSTOM_OPTIONS, short_desc: Some("Log buffers usage."), long_desc: None, flags: 0, variable: &vars::aex_log_buffers, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "auto_explain.log_wal", context: PGC_SUSET, group: CUSTOM_OPTIONS, short_desc: Some("Log WAL usage."), long_desc: None, flags: 0, variable: &vars::aex_log_wal, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "auto_explain.log_triggers", context: PGC_SUSET, group: CUSTOM_OPTIONS, short_desc: Some("Include trigger statistics in plans."), long_desc: Some("This has no effect unless log_analyze is also set."), flags: 0, variable: &vars::aex_log_triggers, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "auto_explain.log_timing", context: PGC_SUSET, group: CUSTOM_OPTIONS, short_desc: Some("Collect timing data, not just row counts."), long_desc: None, flags: 0, variable: &vars::aex_log_timing, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "auto_explain.log_nested_statements", context: PGC_SUSET, group: CUSTOM_OPTIONS, short_desc: Some("Log nested statements."), long_desc: None, flags: 0, variable: &vars::aex_log_nested_statements, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    // pg_stat_statements custom GUCs (statically defined; see vars.rs note).
    GucBoolSetting { name: "pg_stat_statements.track_utility", context: PGC_SUSET, group: CUSTOM_OPTIONS, short_desc: Some("Selects whether utility commands are tracked by pg_stat_statements."), long_desc: None, flags: 0, variable: &vars::pgss_track_utility, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "pg_stat_statements.track_planning", context: PGC_SUSET, group: CUSTOM_OPTIONS, short_desc: Some("Selects whether planning duration is tracked by pg_stat_statements."), long_desc: None, flags: 0, variable: &vars::pgss_track_planning, boot_val: GucDefaultValue::Bool(false), check_hook: None, assign_hook: None, show_hook: None },
    GucBoolSetting { name: "pg_stat_statements.save", context: PGC_SIGHUP, group: CUSTOM_OPTIONS, short_desc: Some("Save pg_stat_statements statistics across server shutdowns."), long_desc: None, flags: 0, variable: &vars::pgss_save, boot_val: GucDefaultValue::Bool(true), check_hook: None, assign_hook: None, show_hook: None },
];

pub static ConfigureNamesInt: &[GucIntSetting] = &[
    GucIntSetting { name: "auto_explain.log_min_duration", context: PGC_SUSET, group: CUSTOM_OPTIONS, short_desc: Some("Sets the minimum execution time above which plans will be logged."), long_desc: Some("-1 disables logging plans. 0 means log all plans."), flags: GUC_UNIT_MS, variable: &vars::aex_log_min_duration, boot_val: GucDefaultValue::Int(-1), min: -(1), max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "auto_explain.log_parameter_max_length", context: PGC_SUSET, group: CUSTOM_OPTIONS, short_desc: Some("Sets the maximum length of query parameter values to log."), long_desc: Some("-1 means log values in full."), flags: GUC_UNIT_BYTE, variable: &vars::aex_log_parameter_max_length, boot_val: GucDefaultValue::Int(-1), min: -(1), max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "pg_stat_statements.max", context: PGC_POSTMASTER, group: CUSTOM_OPTIONS, short_desc: Some("Sets the maximum number of statements tracked by pg_stat_statements."), long_desc: None, flags: 0, variable: &vars::pgss_max, boot_val: GucDefaultValue::Int(5000), min: 100, max: i32::MAX / 2, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "archive_timeout", context: PGC_SIGHUP, group: WAL_ARCHIVING, short_desc: Some("Sets the amount of time to wait before forcing a switch to the next WAL file."), long_desc: Some("0 disables the timeout."), flags: GUC_UNIT_S, variable: &vars::XLogArchiveTimeout, boot_val: GucDefaultValue::Int(0), min: 0, max: i32::MAX / 2, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "post_auth_delay", context: PGC_BACKEND, group: DEVELOPER_OPTIONS, short_desc: Some("Sets the amount of time to wait after authentication on connection startup."), long_desc: Some("This allows attaching a debugger to the process."), flags: GUC_NOT_IN_SAMPLE | GUC_UNIT_S, variable: &vars::PostAuthDelay, boot_val: GucDefaultValue::Int(0), min: 0, max: i32::MAX / 1000000, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "default_statistics_target", context: PGC_USERSET, group: QUERY_TUNING_OTHER, short_desc: Some("Sets the default statistics target."), long_desc: Some("This applies to table columns that have not had a column-specific target set via ALTER TABLE SET STATISTICS."), flags: 0, variable: &vars::default_statistics_target, boot_val: GucDefaultValue::Int(100), min: 1, max: MAX_STATISTICS_TARGET, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "from_collapse_limit", context: PGC_USERSET, group: QUERY_TUNING_OTHER, short_desc: Some("Sets the FROM-list size beyond which subqueries are not collapsed."), long_desc: Some("The planner will merge subqueries into upper queries if the resulting FROM list would have no more than this many items."), flags: GUC_EXPLAIN, variable: &vars::from_collapse_limit, boot_val: GucDefaultValue::Int(8), min: 1, max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "join_collapse_limit", context: PGC_USERSET, group: QUERY_TUNING_OTHER, short_desc: Some("Sets the FROM-list size beyond which JOIN constructs are not flattened."), long_desc: Some("The planner will flatten explicit JOIN constructs into lists of FROM items whenever a list of no more than this many items would result."), flags: GUC_EXPLAIN, variable: &vars::join_collapse_limit, boot_val: GucDefaultValue::Int(8), min: 1, max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "geqo_threshold", context: PGC_USERSET, group: QUERY_TUNING_GEQO, short_desc: Some("Sets the threshold of FROM items beyond which GEQO is used."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::geqo_threshold, boot_val: GucDefaultValue::Int(12), min: 2, max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "geqo_effort", context: PGC_USERSET, group: QUERY_TUNING_GEQO, short_desc: Some("GEQO: effort is used to set the default for other GEQO parameters."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::Geqo_effort, boot_val: GucDefaultValue::Int(DEFAULT_GEQO_EFFORT), min: MIN_GEQO_EFFORT, max: MAX_GEQO_EFFORT, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "geqo_pool_size", context: PGC_USERSET, group: QUERY_TUNING_GEQO, short_desc: Some("GEQO: number of individuals in the population."), long_desc: Some("0 means use a suitable default value."), flags: GUC_EXPLAIN, variable: &vars::Geqo_pool_size, boot_val: GucDefaultValue::Int(0), min: 0, max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "geqo_generations", context: PGC_USERSET, group: QUERY_TUNING_GEQO, short_desc: Some("GEQO: number of iterations of the algorithm."), long_desc: Some("0 means use a suitable default value."), flags: GUC_EXPLAIN, variable: &vars::Geqo_generations, boot_val: GucDefaultValue::Int(0), min: 0, max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "deadlock_timeout", context: PGC_SUSET, group: LOCK_MANAGEMENT, short_desc: Some("Sets the time to wait on a lock before checking for deadlock."), long_desc: None, flags: GUC_UNIT_MS, variable: &vars::DeadlockTimeout, boot_val: GucDefaultValue::Int(1000), min: 1, max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "max_standby_archive_delay", context: PGC_SIGHUP, group: REPLICATION_STANDBY, short_desc: Some("Sets the maximum delay before canceling queries when a hot standby server is processing archived WAL data."), long_desc: Some("-1 means wait forever."), flags: GUC_UNIT_MS, variable: &vars::max_standby_archive_delay, boot_val: GucDefaultValue::Int(30 * 1000), min: -(1), max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "max_standby_streaming_delay", context: PGC_SIGHUP, group: REPLICATION_STANDBY, short_desc: Some("Sets the maximum delay before canceling queries when a hot standby server is processing streamed WAL data."), long_desc: Some("-1 means wait forever."), flags: GUC_UNIT_MS, variable: &vars::max_standby_streaming_delay, boot_val: GucDefaultValue::Int(30 * 1000), min: -(1), max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "recovery_min_apply_delay", context: PGC_SIGHUP, group: REPLICATION_STANDBY, short_desc: Some("Sets the minimum delay for applying changes during recovery."), long_desc: None, flags: GUC_UNIT_MS, variable: &vars::recovery_min_apply_delay, boot_val: GucDefaultValue::Int(0), min: 0, max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "wal_receiver_status_interval", context: PGC_SIGHUP, group: REPLICATION_STANDBY, short_desc: Some("Sets the maximum interval between WAL receiver status reports to the sending server."), long_desc: None, flags: GUC_UNIT_S, variable: &vars::wal_receiver_status_interval, boot_val: GucDefaultValue::Int(10), min: 0, max: i32::MAX / 1000, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "wal_receiver_timeout", context: PGC_SIGHUP, group: REPLICATION_STANDBY, short_desc: Some("Sets the maximum wait time to receive data from the sending server."), long_desc: Some("0 disables the timeout."), flags: GUC_UNIT_MS, variable: &vars::wal_receiver_timeout, boot_val: GucDefaultValue::Int(60 * 1000), min: 0, max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "max_connections", context: PGC_POSTMASTER, group: CONN_AUTH_SETTINGS, short_desc: Some("Sets the maximum number of concurrent connections."), long_desc: None, flags: 0, variable: &vars::MaxConnections, boot_val: GucDefaultValue::Int(100), min: 1, max: MAX_BACKENDS, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "superuser_reserved_connections", context: PGC_POSTMASTER, group: CONN_AUTH_SETTINGS, short_desc: Some("Sets the number of connection slots reserved for superusers."), long_desc: None, flags: 0, variable: &vars::SuperuserReservedConnections, boot_val: GucDefaultValue::Int(3), min: 0, max: MAX_BACKENDS, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "reserved_connections", context: PGC_POSTMASTER, group: CONN_AUTH_SETTINGS, short_desc: Some("Sets the number of connection slots reserved for roles with privileges of pg_use_reserved_connections."), long_desc: None, flags: 0, variable: &vars::ReservedConnections, boot_val: GucDefaultValue::Int(0), min: 0, max: MAX_BACKENDS, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "min_dynamic_shared_memory", context: PGC_POSTMASTER, group: RESOURCES_MEM, short_desc: Some("Amount of dynamic shared memory reserved at startup."), long_desc: None, flags: GUC_UNIT_MB, variable: &vars::min_dynamic_shared_memory, boot_val: GucDefaultValue::Int(0), min: 0, max: 2147483647, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "shared_buffers", context: PGC_POSTMASTER, group: RESOURCES_MEM, short_desc: Some("Sets the number of shared memory buffers used by the server."), long_desc: None, flags: GUC_UNIT_BLOCKS, variable: &vars::NBuffers, boot_val: GucDefaultValue::Int(16384), min: 16, max: i32::MAX / 2, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "vacuum_buffer_usage_limit", context: PGC_USERSET, group: RESOURCES_MEM, short_desc: Some("Sets the buffer pool size for VACUUM, ANALYZE, and autovacuum."), long_desc: None, flags: GUC_UNIT_KB, variable: &vars::VacuumBufferUsageLimit, boot_val: GucDefaultValue::Int(2048), min: 0, max: MAX_BAS_VAC_RING_SIZE_KB, check_hook: Some(&hooks::check_vacuum_buffer_usage_limit), assign_hook: None, show_hook: None },
    GucIntSetting { name: "shared_memory_size", context: PGC_INTERNAL, group: PRESET_OPTIONS, short_desc: Some("Shows the size of the server's main shared memory area (rounded up to the nearest MB)."), long_desc: None, flags: GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE | GUC_UNIT_MB | GUC_RUNTIME_COMPUTED, variable: &vars::shared_memory_size_mb, boot_val: GucDefaultValue::Int(0), min: 0, max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "shared_memory_size_in_huge_pages", context: PGC_INTERNAL, group: PRESET_OPTIONS, short_desc: Some("Shows the number of huge pages needed for the main shared memory area."), long_desc: Some("-1 means huge pages are not supported."), flags: GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE | GUC_RUNTIME_COMPUTED, variable: &vars::shared_memory_size_in_huge_pages, boot_val: GucDefaultValue::Int(-(1)), min: -(1), max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "num_os_semaphores", context: PGC_INTERNAL, group: PRESET_OPTIONS, short_desc: Some("Shows the number of semaphores required for the server."), long_desc: None, flags: GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE | GUC_RUNTIME_COMPUTED, variable: &vars::num_os_semaphores, boot_val: GucDefaultValue::Int(0), min: 0, max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "commit_timestamp_buffers", context: PGC_POSTMASTER, group: RESOURCES_MEM, short_desc: Some("Sets the size of the dedicated buffer pool used for the commit timestamp cache."), long_desc: Some("0 means use a fraction of \"shared_buffers\"."), flags: GUC_UNIT_BLOCKS, variable: &vars::commit_timestamp_buffers, boot_val: GucDefaultValue::Int(0), min: 0, max: SLRU_MAX_ALLOWED_BUFFERS, check_hook: Some(&hooks::check_commit_ts_buffers), assign_hook: None, show_hook: None },
    GucIntSetting { name: "multixact_member_buffers", context: PGC_POSTMASTER, group: RESOURCES_MEM, short_desc: Some("Sets the size of the dedicated buffer pool used for the MultiXact member cache."), long_desc: None, flags: GUC_UNIT_BLOCKS, variable: &vars::multixact_member_buffers, boot_val: GucDefaultValue::Int(32), min: 16, max: SLRU_MAX_ALLOWED_BUFFERS, check_hook: Some(&hooks::check_multixact_member_buffers), assign_hook: None, show_hook: None },
    GucIntSetting { name: "multixact_offset_buffers", context: PGC_POSTMASTER, group: RESOURCES_MEM, short_desc: Some("Sets the size of the dedicated buffer pool used for the MultiXact offset cache."), long_desc: None, flags: GUC_UNIT_BLOCKS, variable: &vars::multixact_offset_buffers, boot_val: GucDefaultValue::Int(16), min: 16, max: SLRU_MAX_ALLOWED_BUFFERS, check_hook: Some(&hooks::check_multixact_offset_buffers), assign_hook: None, show_hook: None },
    GucIntSetting { name: "notify_buffers", context: PGC_POSTMASTER, group: RESOURCES_MEM, short_desc: Some("Sets the size of the dedicated buffer pool used for the LISTEN/NOTIFY message cache."), long_desc: None, flags: GUC_UNIT_BLOCKS, variable: &vars::notify_buffers, boot_val: GucDefaultValue::Int(16), min: 16, max: SLRU_MAX_ALLOWED_BUFFERS, check_hook: Some(&hooks::check_notify_buffers), assign_hook: None, show_hook: None },
    GucIntSetting { name: "serializable_buffers", context: PGC_POSTMASTER, group: RESOURCES_MEM, short_desc: Some("Sets the size of the dedicated buffer pool used for the serializable transaction cache."), long_desc: None, flags: GUC_UNIT_BLOCKS, variable: &vars::serializable_buffers, boot_val: GucDefaultValue::Int(32), min: 16, max: SLRU_MAX_ALLOWED_BUFFERS, check_hook: Some(&hooks::check_serial_buffers), assign_hook: None, show_hook: None },
    GucIntSetting { name: "subtransaction_buffers", context: PGC_POSTMASTER, group: RESOURCES_MEM, short_desc: Some("Sets the size of the dedicated buffer pool used for the subtransaction cache."), long_desc: Some("0 means use a fraction of \"shared_buffers\"."), flags: GUC_UNIT_BLOCKS, variable: &vars::subtransaction_buffers, boot_val: GucDefaultValue::Int(0), min: 0, max: SLRU_MAX_ALLOWED_BUFFERS, check_hook: Some(&hooks::check_subtrans_buffers), assign_hook: None, show_hook: None },
    GucIntSetting { name: "transaction_buffers", context: PGC_POSTMASTER, group: RESOURCES_MEM, short_desc: Some("Sets the size of the dedicated buffer pool used for the transaction status cache."), long_desc: Some("0 means use a fraction of \"shared_buffers\"."), flags: GUC_UNIT_BLOCKS, variable: &vars::transaction_buffers, boot_val: GucDefaultValue::Int(0), min: 0, max: SLRU_MAX_ALLOWED_BUFFERS, check_hook: Some(&hooks::check_transaction_buffers), assign_hook: None, show_hook: None },
    GucIntSetting { name: "temp_buffers", context: PGC_USERSET, group: RESOURCES_MEM, short_desc: Some("Sets the maximum number of temporary buffers used by each session."), long_desc: None, flags: GUC_UNIT_BLOCKS | GUC_EXPLAIN, variable: &vars::num_temp_buffers, boot_val: GucDefaultValue::Int(1024), min: 100, max: i32::MAX / 2, check_hook: Some(&hooks::check_temp_buffers), assign_hook: None, show_hook: None },
    GucIntSetting { name: "port", context: PGC_POSTMASTER, group: CONN_AUTH_SETTINGS, short_desc: Some("Sets the TCP port the server listens on."), long_desc: None, flags: 0, variable: &vars::PostPortNumber, boot_val: GucDefaultValue::Int(DEF_PGPORT), min: 1, max: 65535, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "unix_socket_permissions", context: PGC_POSTMASTER, group: CONN_AUTH_SETTINGS, short_desc: Some("Sets the access permissions of the Unix-domain socket."), long_desc: Some("Unix-domain sockets use the usual Unix file system permission set. The parameter value is expected to be a numeric mode specification in the form accepted by the chmod and umask system calls. (To use the customary octal format the number must start with a 0 (zero).)"), flags: 0, variable: &vars::Unix_socket_permissions, boot_val: GucDefaultValue::Int(0o777), min: 0, max: 0o777, check_hook: None, assign_hook: None, show_hook: Some(&hooks::show_unix_socket_permissions) },
    GucIntSetting { name: "log_file_mode", context: PGC_SIGHUP, group: LOGGING_WHERE, short_desc: Some("Sets the file permissions for log files."), long_desc: Some("The parameter value is expected to be a numeric mode specification in the form accepted by the chmod and umask system calls. (To use the customary octal format the number must start with a 0 (zero).)"), flags: 0, variable: &vars::Log_file_mode, boot_val: GucDefaultValue::Int(0o600), min: 0, max: 0o777, check_hook: None, assign_hook: None, show_hook: Some(&hooks::show_log_file_mode) },
    GucIntSetting { name: "data_directory_mode", context: PGC_INTERNAL, group: PRESET_OPTIONS, short_desc: Some("Shows the mode of the data directory."), long_desc: Some("The parameter value is a numeric mode specification in the form accepted by the chmod and umask system calls. (To use the customary octal format the number must start with a 0 (zero).)"), flags: GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE | GUC_RUNTIME_COMPUTED, variable: &vars::data_directory_mode, boot_val: GucDefaultValue::Int(0o700), min: 0, max: 0o777, check_hook: None, assign_hook: None, show_hook: Some(&hooks::show_data_directory_mode) },
    GucIntSetting { name: "work_mem", context: PGC_USERSET, group: RESOURCES_MEM, short_desc: Some("Sets the maximum memory to be used for query workspaces."), long_desc: Some("This much memory can be used by each internal sort operation and hash table before switching to temporary disk files."), flags: GUC_UNIT_KB | GUC_EXPLAIN, variable: &vars::work_mem, boot_val: GucDefaultValue::Int(4096), min: 64, max: MAX_KILOBYTES, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "maintenance_work_mem", context: PGC_USERSET, group: RESOURCES_MEM, short_desc: Some("Sets the maximum memory to be used for maintenance operations."), long_desc: Some("This includes operations such as VACUUM and CREATE INDEX."), flags: GUC_UNIT_KB, variable: &vars::maintenance_work_mem, boot_val: GucDefaultValue::Int(65536), min: 64, max: MAX_KILOBYTES, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "logical_decoding_work_mem", context: PGC_USERSET, group: RESOURCES_MEM, short_desc: Some("Sets the maximum memory to be used for logical decoding."), long_desc: Some("This much memory can be used by each internal reorder buffer before spilling to disk."), flags: GUC_UNIT_KB, variable: &vars::logical_decoding_work_mem, boot_val: GucDefaultValue::Int(65536), min: 64, max: MAX_KILOBYTES, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "max_stack_depth", context: PGC_SUSET, group: RESOURCES_MEM, short_desc: Some("Sets the maximum stack depth, in kilobytes."), long_desc: None, flags: GUC_UNIT_KB, variable: &vars::max_stack_depth, boot_val: GucDefaultValue::Int(100), min: 100, max: MAX_KILOBYTES, check_hook: Some(&hooks::check_max_stack_depth), assign_hook: Some(&hooks::assign_max_stack_depth), show_hook: None },
    GucIntSetting { name: "temp_file_limit", context: PGC_SUSET, group: RESOURCES_DISK, short_desc: Some("Limits the total size of all temporary files used by each process."), long_desc: Some("-1 means no limit."), flags: GUC_UNIT_KB, variable: &vars::temp_file_limit, boot_val: GucDefaultValue::Int(-(1)), min: -(1), max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "vacuum_cost_page_hit", context: PGC_USERSET, group: VACUUM_COST_DELAY, short_desc: Some("Vacuum cost for a page found in the buffer cache."), long_desc: None, flags: 0, variable: &vars::VacuumCostPageHit, boot_val: GucDefaultValue::Int(1), min: 0, max: 10000, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "vacuum_cost_page_miss", context: PGC_USERSET, group: VACUUM_COST_DELAY, short_desc: Some("Vacuum cost for a page not found in the buffer cache."), long_desc: None, flags: 0, variable: &vars::VacuumCostPageMiss, boot_val: GucDefaultValue::Int(2), min: 0, max: 10000, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "vacuum_cost_page_dirty", context: PGC_USERSET, group: VACUUM_COST_DELAY, short_desc: Some("Vacuum cost for a page dirtied by vacuum."), long_desc: None, flags: 0, variable: &vars::VacuumCostPageDirty, boot_val: GucDefaultValue::Int(20), min: 0, max: 10000, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "vacuum_cost_limit", context: PGC_USERSET, group: VACUUM_COST_DELAY, short_desc: Some("Vacuum cost amount available before napping."), long_desc: None, flags: 0, variable: &vars::VacuumCostLimit, boot_val: GucDefaultValue::Int(200), min: 1, max: 10000, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "autovacuum_vacuum_cost_limit", context: PGC_SIGHUP, group: VACUUM_AUTOVACUUM, short_desc: Some("Vacuum cost amount available before napping, for autovacuum."), long_desc: Some("-1 means use \"vacuum_cost_limit\"."), flags: 0, variable: &vars::autovacuum_vac_cost_limit, boot_val: GucDefaultValue::Int(-(1)), min: -(1), max: 10000, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "max_files_per_process", context: PGC_POSTMASTER, group: RESOURCES_KERNEL, short_desc: Some("Sets the maximum number of files each server process is allowed to open simultaneously."), long_desc: None, flags: 0, variable: &vars::max_files_per_process, boot_val: GucDefaultValue::Int(1000), min: 64, max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "max_prepared_transactions", context: PGC_POSTMASTER, group: RESOURCES_MEM, short_desc: Some("Sets the maximum number of simultaneously prepared transactions."), long_desc: None, flags: 0, variable: &vars::max_prepared_xacts, boot_val: GucDefaultValue::Int(0), min: 0, max: MAX_BACKENDS, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "statement_timeout", context: PGC_USERSET, group: CLIENT_CONN_STATEMENT, short_desc: Some("Sets the maximum allowed duration of any statement."), long_desc: Some("0 disables the timeout."), flags: GUC_UNIT_MS, variable: &vars::StatementTimeout, boot_val: GucDefaultValue::Int(0), min: 0, max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "lock_timeout", context: PGC_USERSET, group: CLIENT_CONN_STATEMENT, short_desc: Some("Sets the maximum allowed duration of any wait for a lock."), long_desc: Some("0 disables the timeout."), flags: GUC_UNIT_MS, variable: &vars::LockTimeout, boot_val: GucDefaultValue::Int(0), min: 0, max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "idle_in_transaction_session_timeout", context: PGC_USERSET, group: CLIENT_CONN_STATEMENT, short_desc: Some("Sets the maximum allowed idle time between queries, when in a transaction."), long_desc: Some("0 disables the timeout."), flags: GUC_UNIT_MS, variable: &vars::IdleInTransactionSessionTimeout, boot_val: GucDefaultValue::Int(0), min: 0, max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "transaction_timeout", context: PGC_USERSET, group: CLIENT_CONN_STATEMENT, short_desc: Some("Sets the maximum allowed duration of any transaction within a session (not a prepared transaction)."), long_desc: Some("0 disables the timeout."), flags: GUC_UNIT_MS, variable: &vars::TransactionTimeout, boot_val: GucDefaultValue::Int(0), min: 0, max: i32::MAX, check_hook: None, assign_hook: Some(&hooks::assign_transaction_timeout), show_hook: None },
    GucIntSetting { name: "idle_session_timeout", context: PGC_USERSET, group: CLIENT_CONN_STATEMENT, short_desc: Some("Sets the maximum allowed idle time between queries, when not in a transaction."), long_desc: Some("0 disables the timeout."), flags: GUC_UNIT_MS, variable: &vars::IdleSessionTimeout, boot_val: GucDefaultValue::Int(0), min: 0, max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "vacuum_freeze_min_age", context: PGC_USERSET, group: VACUUM_FREEZING, short_desc: Some("Minimum age at which VACUUM should freeze a table row."), long_desc: None, flags: 0, variable: &vars::vacuum_freeze_min_age, boot_val: GucDefaultValue::Int(50000000), min: 0, max: 1000000000, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "vacuum_freeze_table_age", context: PGC_USERSET, group: VACUUM_FREEZING, short_desc: Some("Age at which VACUUM should scan whole table to freeze tuples."), long_desc: None, flags: 0, variable: &vars::vacuum_freeze_table_age, boot_val: GucDefaultValue::Int(150000000), min: 0, max: 2000000000, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "vacuum_multixact_freeze_min_age", context: PGC_USERSET, group: VACUUM_FREEZING, short_desc: Some("Minimum age at which VACUUM should freeze a MultiXactId in a table row."), long_desc: None, flags: 0, variable: &vars::vacuum_multixact_freeze_min_age, boot_val: GucDefaultValue::Int(5000000), min: 0, max: 1000000000, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "vacuum_multixact_freeze_table_age", context: PGC_USERSET, group: VACUUM_FREEZING, short_desc: Some("Multixact age at which VACUUM should scan whole table to freeze tuples."), long_desc: None, flags: 0, variable: &vars::vacuum_multixact_freeze_table_age, boot_val: GucDefaultValue::Int(150000000), min: 0, max: 2000000000, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "vacuum_failsafe_age", context: PGC_USERSET, group: VACUUM_FREEZING, short_desc: Some("Age at which VACUUM should trigger failsafe to avoid a wraparound outage."), long_desc: None, flags: 0, variable: &vars::vacuum_failsafe_age, boot_val: GucDefaultValue::Int(1600000000), min: 0, max: 2100000000, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "vacuum_multixact_failsafe_age", context: PGC_USERSET, group: VACUUM_FREEZING, short_desc: Some("Multixact age at which VACUUM should trigger failsafe to avoid a wraparound outage."), long_desc: None, flags: 0, variable: &vars::vacuum_multixact_failsafe_age, boot_val: GucDefaultValue::Int(1600000000), min: 0, max: 2100000000, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "max_locks_per_transaction", context: PGC_POSTMASTER, group: LOCK_MANAGEMENT, short_desc: Some("Sets the maximum number of locks per transaction."), long_desc: Some("The shared lock table is sized on the assumption that at most \"max_locks_per_transaction\" objects per server process or prepared transaction will need to be locked at any one time."), flags: 0, variable: &vars::max_locks_per_xact, boot_val: GucDefaultValue::Int(64), min: 10, max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "max_pred_locks_per_transaction", context: PGC_POSTMASTER, group: LOCK_MANAGEMENT, short_desc: Some("Sets the maximum number of predicate locks per transaction."), long_desc: Some("The shared predicate lock table is sized on the assumption that at most \"max_pred_locks_per_transaction\" objects per server process or prepared transaction will need to be locked at any one time."), flags: 0, variable: &vars::max_predicate_locks_per_xact, boot_val: GucDefaultValue::Int(64), min: 10, max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "max_pred_locks_per_relation", context: PGC_SIGHUP, group: LOCK_MANAGEMENT, short_desc: Some("Sets the maximum number of predicate-locked pages and tuples per relation."), long_desc: Some("If more than this total of pages and tuples in the same relation are locked by a connection, those locks are replaced by a relation-level lock."), flags: 0, variable: &vars::max_predicate_locks_per_relation, boot_val: GucDefaultValue::Int(-(2)), min: i32::MIN, max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "max_pred_locks_per_page", context: PGC_SIGHUP, group: LOCK_MANAGEMENT, short_desc: Some("Sets the maximum number of predicate-locked tuples per page."), long_desc: Some("If more than this number of tuples on the same page are locked by a connection, those locks are replaced by a page-level lock."), flags: 0, variable: &vars::max_predicate_locks_per_page, boot_val: GucDefaultValue::Int(2), min: 0, max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "authentication_timeout", context: PGC_SIGHUP, group: CONN_AUTH_AUTH, short_desc: Some("Sets the maximum allowed time to complete client authentication."), long_desc: None, flags: GUC_UNIT_S, variable: &vars::AuthenticationTimeout, boot_val: GucDefaultValue::Int(60), min: 1, max: 600, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "pre_auth_delay", context: PGC_SIGHUP, group: DEVELOPER_OPTIONS, short_desc: Some("Sets the amount of time to wait before authentication on connection startup."), long_desc: Some("This allows attaching a debugger to the process."), flags: GUC_NOT_IN_SAMPLE | GUC_UNIT_S, variable: &vars::PreAuthDelay, boot_val: GucDefaultValue::Int(0), min: 0, max: 60, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "max_notify_queue_pages", context: PGC_POSTMASTER, group: RESOURCES_DISK, short_desc: Some("Sets the maximum number of allocated pages for NOTIFY / LISTEN queue."), long_desc: None, flags: 0, variable: &vars::max_notify_queue_pages, boot_val: GucDefaultValue::Int(1048576), min: 64, max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "wal_decode_buffer_size", context: PGC_POSTMASTER, group: WAL_RECOVERY, short_desc: Some("Buffer size for reading ahead in the WAL during recovery."), long_desc: Some("Maximum distance to read ahead in the WAL to prefetch referenced data blocks."), flags: GUC_UNIT_BYTE, variable: &vars::wal_decode_buffer_size, boot_val: GucDefaultValue::Int(512 * 1024), min: 64 * 1024, max: MaxAllocSize, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "wal_keep_size", context: PGC_SIGHUP, group: REPLICATION_SENDING, short_desc: Some("Sets the size of WAL files held for standby servers."), long_desc: None, flags: GUC_UNIT_MB, variable: &vars::wal_keep_size_mb, boot_val: GucDefaultValue::Int(0), min: 0, max: MAX_KILOBYTES, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "min_wal_size", context: PGC_SIGHUP, group: WAL_CHECKPOINTS, short_desc: Some("Sets the minimum size to shrink the WAL to."), long_desc: None, flags: GUC_UNIT_MB, variable: &vars::min_wal_size_mb, boot_val: GucDefaultValue::Int(DEFAULT_MIN_WAL_SEGS * (DEFAULT_XLOG_SEG_SIZE / (1024 * 1024))), min: 2, max: MAX_KILOBYTES, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "max_wal_size", context: PGC_SIGHUP, group: WAL_CHECKPOINTS, short_desc: Some("Sets the WAL size that triggers a checkpoint."), long_desc: None, flags: GUC_UNIT_MB, variable: &vars::max_wal_size_mb, boot_val: GucDefaultValue::Int(DEFAULT_MAX_WAL_SEGS * (DEFAULT_XLOG_SEG_SIZE / (1024 * 1024))), min: 2, max: MAX_KILOBYTES, check_hook: None, assign_hook: Some(&hooks::assign_max_wal_size), show_hook: None },
    GucIntSetting { name: "checkpoint_timeout", context: PGC_SIGHUP, group: WAL_CHECKPOINTS, short_desc: Some("Sets the maximum time between automatic WAL checkpoints."), long_desc: None, flags: GUC_UNIT_S, variable: &vars::CheckPointTimeout, boot_val: GucDefaultValue::Int(300), min: 30, max: 86400, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "checkpoint_warning", context: PGC_SIGHUP, group: WAL_CHECKPOINTS, short_desc: Some("Sets the maximum time before warning if checkpoints triggered by WAL volume happen too frequently."), long_desc: Some("Write a message to the server log if checkpoints caused by the filling of WAL segment files happen more frequently than this amount of time. 0 disables the warning."), flags: GUC_UNIT_S, variable: &vars::CheckPointWarning, boot_val: GucDefaultValue::Int(30), min: 0, max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "checkpoint_flush_after", context: PGC_SIGHUP, group: WAL_CHECKPOINTS, short_desc: Some("Number of pages after which previously performed writes are flushed to disk."), long_desc: Some("0 disables forced writeback."), flags: GUC_UNIT_BLOCKS, variable: &vars::checkpoint_flush_after, boot_val: GucDefaultValue::Int(DEFAULT_CHECKPOINT_FLUSH_AFTER), min: 0, max: WRITEBACK_MAX_PENDING_FLUSHES, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "wal_buffers", context: PGC_POSTMASTER, group: WAL_SETTINGS, short_desc: Some("Sets the number of disk-page buffers in shared memory for WAL."), long_desc: Some("-1 means use a fraction of \"shared_buffers\"."), flags: GUC_UNIT_XBLOCKS, variable: &vars::XLOGbuffers, boot_val: GucDefaultValue::Int(-(1)), min: -(1), max: i32::MAX / XLOG_BLCKSZ, check_hook: Some(&hooks::check_wal_buffers), assign_hook: None, show_hook: None },
    GucIntSetting { name: "wal_writer_delay", context: PGC_SIGHUP, group: WAL_SETTINGS, short_desc: Some("Time between WAL flushes performed in the WAL writer."), long_desc: None, flags: GUC_UNIT_MS, variable: &vars::WalWriterDelay, boot_val: GucDefaultValue::Int(200), min: 1, max: 10000, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "wal_writer_flush_after", context: PGC_SIGHUP, group: WAL_SETTINGS, short_desc: Some("Amount of WAL written out by WAL writer that triggers a flush."), long_desc: None, flags: GUC_UNIT_XBLOCKS, variable: &vars::WalWriterFlushAfter, boot_val: GucDefaultValue::Int(DEFAULT_WAL_WRITER_FLUSH_AFTER), min: 0, max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "wal_skip_threshold", context: PGC_USERSET, group: WAL_SETTINGS, short_desc: Some("Minimum size of new file to fsync instead of writing WAL."), long_desc: None, flags: GUC_UNIT_KB, variable: &vars::wal_skip_threshold, boot_val: GucDefaultValue::Int(2048), min: 0, max: MAX_KILOBYTES, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "max_wal_senders", context: PGC_POSTMASTER, group: REPLICATION_SENDING, short_desc: Some("Sets the maximum number of simultaneously running WAL sender processes."), long_desc: None, flags: 0, variable: &vars::max_wal_senders, boot_val: GucDefaultValue::Int(10), min: 0, max: MAX_BACKENDS, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "max_replication_slots", context: PGC_POSTMASTER, group: REPLICATION_SENDING, short_desc: Some("Sets the maximum number of simultaneously defined replication slots."), long_desc: None, flags: 0, variable: &vars::max_replication_slots, boot_val: GucDefaultValue::Int(10), min: 0, max: MAX_BACKENDS, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "max_slot_wal_keep_size", context: PGC_SIGHUP, group: REPLICATION_SENDING, short_desc: Some("Sets the maximum WAL size that can be reserved by replication slots."), long_desc: Some("Replication slots will be marked as failed, and segments released for deletion or recycling, if this much space is occupied by WAL on disk. -1 means no maximum."), flags: GUC_UNIT_MB, variable: &vars::max_slot_wal_keep_size_mb, boot_val: GucDefaultValue::Int(-(1)), min: -(1), max: MAX_KILOBYTES, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "wal_sender_timeout", context: PGC_USERSET, group: REPLICATION_SENDING, short_desc: Some("Sets the maximum time to wait for WAL replication."), long_desc: None, flags: GUC_UNIT_MS, variable: &vars::wal_sender_timeout, boot_val: GucDefaultValue::Int(60 * 1000), min: 0, max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "idle_replication_slot_timeout", context: PGC_SIGHUP, group: REPLICATION_SENDING, short_desc: Some("Sets the duration a replication slot can remain idle before it is invalidated."), long_desc: None, flags: GUC_UNIT_S, variable: &vars::idle_replication_slot_timeout_secs, boot_val: GucDefaultValue::Int(0), min: 0, max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "commit_delay", context: PGC_SUSET, group: WAL_SETTINGS, short_desc: Some("Sets the delay in microseconds between transaction commit and flushing WAL to disk."), long_desc: None, flags: 0, variable: &vars::CommitDelay, boot_val: GucDefaultValue::Int(0), min: 0, max: 100000, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "commit_siblings", context: PGC_USERSET, group: WAL_SETTINGS, short_desc: Some("Sets the minimum number of concurrent open transactions required before performing \"commit_delay\"."), long_desc: None, flags: 0, variable: &vars::CommitSiblings, boot_val: GucDefaultValue::Int(5), min: 0, max: 1000, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "extra_float_digits", context: PGC_USERSET, group: CLIENT_CONN_LOCALE, short_desc: Some("Sets the number of digits displayed for floating-point values."), long_desc: Some("This affects real, double precision, and geometric data types. A zero or negative parameter value is added to the standard number of digits (FLT_DIG or DBL_DIG as appropriate). Any value greater than zero selects precise output mode."), flags: 0, variable: &vars::extra_float_digits, boot_val: GucDefaultValue::Int(1), min: -(15), max: 3, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "log_min_duration_sample", context: PGC_SUSET, group: LOGGING_WHEN, short_desc: Some("Sets the minimum execution time above which a sample of statements will be logged. Sampling is determined by \"log_statement_sample_rate\"."), long_desc: Some("-1 disables sampling. 0 means sample all statements."), flags: GUC_UNIT_MS, variable: &vars::log_min_duration_sample, boot_val: GucDefaultValue::Int(-(1)), min: -(1), max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "log_min_duration_statement", context: PGC_SUSET, group: LOGGING_WHEN, short_desc: Some("Sets the minimum execution time above which all statements will be logged."), long_desc: Some("-1 disables logging statement durations. 0 means log all statement durations."), flags: GUC_UNIT_MS, variable: &vars::log_min_duration_statement, boot_val: GucDefaultValue::Int(-(1)), min: -(1), max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "log_autovacuum_min_duration", context: PGC_SIGHUP, group: LOGGING_WHAT, short_desc: Some("Sets the minimum execution time above which autovacuum actions will be logged."), long_desc: Some("-1 disables logging autovacuum actions. 0 means log all autovacuum actions."), flags: GUC_UNIT_MS, variable: &vars::Log_autovacuum_min_duration, boot_val: GucDefaultValue::Int(600000), min: -(1), max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "log_parameter_max_length", context: PGC_SUSET, group: LOGGING_WHAT, short_desc: Some("Sets the maximum length in bytes of data logged for bind parameter values when logging statements."), long_desc: Some("-1 means log values in full."), flags: GUC_UNIT_BYTE, variable: &vars::log_parameter_max_length, boot_val: GucDefaultValue::Int(-(1)), min: -(1), max: i32::MAX / 2, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "log_parameter_max_length_on_error", context: PGC_USERSET, group: LOGGING_WHAT, short_desc: Some("Sets the maximum length in bytes of data logged for bind parameter values when logging statements, on error."), long_desc: Some("-1 means log values in full."), flags: GUC_UNIT_BYTE, variable: &vars::log_parameter_max_length_on_error, boot_val: GucDefaultValue::Int(0), min: -(1), max: i32::MAX / 2, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "bgwriter_delay", context: PGC_SIGHUP, group: RESOURCES_BGWRITER, short_desc: Some("Background writer sleep time between rounds."), long_desc: None, flags: GUC_UNIT_MS, variable: &vars::BgWriterDelay, boot_val: GucDefaultValue::Int(200), min: 10, max: 10000, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "bgwriter_lru_maxpages", context: PGC_SIGHUP, group: RESOURCES_BGWRITER, short_desc: Some("Background writer maximum number of LRU pages to flush per round."), long_desc: Some("0 disables background writing."), flags: 0, variable: &vars::bgwriter_lru_maxpages, boot_val: GucDefaultValue::Int(100), min: 0, max: i32::MAX / 2, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "bgwriter_flush_after", context: PGC_SIGHUP, group: RESOURCES_BGWRITER, short_desc: Some("Number of pages after which previously performed writes are flushed to disk."), long_desc: Some("0 disables forced writeback."), flags: GUC_UNIT_BLOCKS, variable: &vars::bgwriter_flush_after, boot_val: GucDefaultValue::Int(DEFAULT_BGWRITER_FLUSH_AFTER), min: 0, max: WRITEBACK_MAX_PENDING_FLUSHES, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "effective_io_concurrency", context: PGC_USERSET, group: RESOURCES_IO, short_desc: Some("Number of simultaneous requests that can be handled efficiently by the disk subsystem."), long_desc: Some("0 disables simultaneous requests."), flags: GUC_EXPLAIN, variable: &vars::effective_io_concurrency, boot_val: GucDefaultValue::Int(DEFAULT_EFFECTIVE_IO_CONCURRENCY), min: 0, max: MAX_IO_CONCURRENCY, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "maintenance_io_concurrency", context: PGC_USERSET, group: RESOURCES_IO, short_desc: Some("A variant of \"effective_io_concurrency\" that is used for maintenance work."), long_desc: Some("0 disables simultaneous requests."), flags: GUC_EXPLAIN, variable: &vars::maintenance_io_concurrency, boot_val: GucDefaultValue::Int(DEFAULT_MAINTENANCE_IO_CONCURRENCY), min: 0, max: MAX_IO_CONCURRENCY, check_hook: None, assign_hook: Some(&hooks::assign_maintenance_io_concurrency), show_hook: None },
    GucIntSetting { name: "io_max_combine_limit", context: PGC_POSTMASTER, group: RESOURCES_IO, short_desc: Some("Server-wide limit that clamps io_combine_limit."), long_desc: None, flags: GUC_UNIT_BLOCKS, variable: &vars::io_max_combine_limit, boot_val: GucDefaultValue::Int(16), min: 1, max: 128, check_hook: None, assign_hook: Some(&hooks::assign_io_max_combine_limit), show_hook: None },
    GucIntSetting { name: "io_combine_limit", context: PGC_USERSET, group: RESOURCES_IO, short_desc: Some("Limit on the size of data reads and writes."), long_desc: None, flags: GUC_UNIT_BLOCKS, variable: &vars::io_combine_limit_guc, boot_val: GucDefaultValue::Int(16), min: 1, max: 128, check_hook: None, assign_hook: Some(&hooks::assign_io_combine_limit), show_hook: None },
    GucIntSetting { name: "io_max_concurrency", context: PGC_POSTMASTER, group: RESOURCES_IO, short_desc: Some("Max number of IOs that one process can execute simultaneously."), long_desc: None, flags: 0, variable: &vars::io_max_concurrency, boot_val: GucDefaultValue::Int(-(1)), min: -(1), max: 1024, check_hook: Some(&hooks::check_io_max_concurrency), assign_hook: None, show_hook: None },
    GucIntSetting { name: "io_workers", context: PGC_SIGHUP, group: RESOURCES_IO, short_desc: Some("Number of IO worker processes, for io_method=worker."), long_desc: None, flags: 0, variable: &vars::io_workers, boot_val: GucDefaultValue::Int(3), min: 1, max: MAX_IO_WORKERS, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "backend_flush_after", context: PGC_USERSET, group: RESOURCES_IO, short_desc: Some("Number of pages after which previously performed writes are flushed to disk."), long_desc: Some("0 disables forced writeback."), flags: GUC_UNIT_BLOCKS, variable: &vars::backend_flush_after, boot_val: GucDefaultValue::Int(DEFAULT_BACKEND_FLUSH_AFTER), min: 0, max: WRITEBACK_MAX_PENDING_FLUSHES, check_hook: None, assign_hook: None, show_hook: None },
    // Raised 8->16 to keep max_parallel_workers=16 schedulable on the legacy
    // C-exact Gather bgworker path (parallel workers draw from this pool). The
    // shipped runtime work-stealing pool is separate (self-sized to cores, not
    // from this cap) — this matters for legacy-engine + router-uncovered
    // shapes. docs/design/jit-parallel-defaults.md.
    GucIntSetting { name: "max_worker_processes", context: PGC_POSTMASTER, group: RESOURCES_WORKER_PROCESSES, short_desc: Some("Maximum number of concurrent worker processes."), long_desc: None, flags: 0, variable: &vars::max_worker_processes, boot_val: GucDefaultValue::Int(16), min: 0, max: MAX_BACKENDS, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "max_logical_replication_workers", context: PGC_POSTMASTER, group: REPLICATION_SUBSCRIBERS, short_desc: Some("Maximum number of logical replication worker processes."), long_desc: None, flags: 0, variable: &vars::max_logical_replication_workers, boot_val: GucDefaultValue::Int(4), min: 0, max: MAX_BACKENDS, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "max_sync_workers_per_subscription", context: PGC_SIGHUP, group: REPLICATION_SUBSCRIBERS, short_desc: Some("Maximum number of table synchronization workers per subscription."), long_desc: None, flags: 0, variable: &vars::max_sync_workers_per_subscription, boot_val: GucDefaultValue::Int(2), min: 0, max: MAX_BACKENDS, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "max_parallel_apply_workers_per_subscription", context: PGC_SIGHUP, group: REPLICATION_SUBSCRIBERS, short_desc: Some("Maximum number of parallel apply workers per subscription."), long_desc: None, flags: 0, variable: &vars::max_parallel_apply_workers_per_subscription, boot_val: GucDefaultValue::Int(2), min: 0, max: MAX_PARALLEL_WORKER_LIMIT, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "max_active_replication_origins", context: PGC_POSTMASTER, group: REPLICATION_SUBSCRIBERS, short_desc: Some("Sets the maximum number of active replication origins."), long_desc: None, flags: 0, variable: &vars::max_active_replication_origins, boot_val: GucDefaultValue::Int(10), min: 0, max: MAX_BACKENDS, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "log_rotation_age", context: PGC_SIGHUP, group: LOGGING_WHERE, short_desc: Some("Sets the amount of time to wait before forcing log file rotation."), long_desc: Some("0 disables time-based creation of new log files."), flags: GUC_UNIT_MIN, variable: &vars::Log_RotationAge, boot_val: GucDefaultValue::Int(HOURS_PER_DAY * MINS_PER_HOUR), min: 0, max: i32::MAX / SECS_PER_MINUTE, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "log_rotation_size", context: PGC_SIGHUP, group: LOGGING_WHERE, short_desc: Some("Sets the maximum size a log file can reach before being rotated."), long_desc: Some("0 disables size-based creation of new log files."), flags: GUC_UNIT_KB, variable: &vars::Log_RotationSize, boot_val: GucDefaultValue::Int(10 * 1024), min: 0, max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "max_function_args", context: PGC_INTERNAL, group: PRESET_OPTIONS, short_desc: Some("Shows the maximum number of function arguments."), long_desc: None, flags: GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE, variable: &vars::max_function_args, boot_val: GucDefaultValue::Int(FUNC_MAX_ARGS), min: FUNC_MAX_ARGS, max: FUNC_MAX_ARGS, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "max_index_keys", context: PGC_INTERNAL, group: PRESET_OPTIONS, short_desc: Some("Shows the maximum number of index keys."), long_desc: None, flags: GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE, variable: &vars::max_index_keys, boot_val: GucDefaultValue::Int(INDEX_MAX_KEYS), min: INDEX_MAX_KEYS, max: INDEX_MAX_KEYS, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "max_identifier_length", context: PGC_INTERNAL, group: PRESET_OPTIONS, short_desc: Some("Shows the maximum identifier length."), long_desc: None, flags: GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE, variable: &vars::max_identifier_length, boot_val: GucDefaultValue::Int(NAMEDATALEN - 1), min: NAMEDATALEN - 1, max: NAMEDATALEN - 1, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "block_size", context: PGC_INTERNAL, group: PRESET_OPTIONS, short_desc: Some("Shows the size of a disk block."), long_desc: None, flags: GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE, variable: &vars::block_size, boot_val: GucDefaultValue::Int(BLCKSZ), min: BLCKSZ, max: BLCKSZ, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "segment_size", context: PGC_INTERNAL, group: PRESET_OPTIONS, short_desc: Some("Shows the number of pages per disk file."), long_desc: None, flags: GUC_UNIT_BLOCKS | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE, variable: &vars::segment_size, boot_val: GucDefaultValue::Int(RELSEG_SIZE), min: RELSEG_SIZE, max: RELSEG_SIZE, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "wal_block_size", context: PGC_INTERNAL, group: PRESET_OPTIONS, short_desc: Some("Shows the block size in the write ahead log."), long_desc: None, flags: GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE, variable: &vars::wal_block_size, boot_val: GucDefaultValue::Int(XLOG_BLCKSZ), min: XLOG_BLCKSZ, max: XLOG_BLCKSZ, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "wal_retrieve_retry_interval", context: PGC_SIGHUP, group: REPLICATION_STANDBY, short_desc: Some("Sets the time to wait before retrying to retrieve WAL after a failed attempt."), long_desc: None, flags: GUC_UNIT_MS, variable: &vars::wal_retrieve_retry_interval, boot_val: GucDefaultValue::Int(5000), min: 1, max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "wal_segment_size", context: PGC_INTERNAL, group: PRESET_OPTIONS, short_desc: Some("Shows the size of write ahead log segments."), long_desc: None, flags: GUC_UNIT_BYTE | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE | GUC_RUNTIME_COMPUTED, variable: &vars::wal_segment_size, boot_val: GucDefaultValue::Int(DEFAULT_XLOG_SEG_SIZE), min: WalSegMinSize, max: WalSegMaxSize, check_hook: Some(&hooks::check_wal_segment_size), assign_hook: None, show_hook: None },
    GucIntSetting { name: "wal_summary_keep_time", context: PGC_SIGHUP, group: WAL_SUMMARIZATION, short_desc: Some("Time for which WAL summary files should be kept."), long_desc: Some("0 disables automatic summary file deletion."), flags: GUC_UNIT_MIN, variable: &vars::wal_summary_keep_time, boot_val: GucDefaultValue::Int(10 * HOURS_PER_DAY * MINS_PER_HOUR), min: 0, max: i32::MAX / SECS_PER_MINUTE, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "autovacuum_naptime", context: PGC_SIGHUP, group: VACUUM_AUTOVACUUM, short_desc: Some("Time to sleep between autovacuum runs."), long_desc: None, flags: GUC_UNIT_S, variable: &vars::autovacuum_naptime, boot_val: GucDefaultValue::Int(60), min: 1, max: i32::MAX / 1000, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "autovacuum_vacuum_threshold", context: PGC_SIGHUP, group: VACUUM_AUTOVACUUM, short_desc: Some("Minimum number of tuple updates or deletes prior to vacuum."), long_desc: None, flags: 0, variable: &vars::autovacuum_vac_thresh, boot_val: GucDefaultValue::Int(50), min: 0, max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "autovacuum_vacuum_max_threshold", context: PGC_SIGHUP, group: VACUUM_AUTOVACUUM, short_desc: Some("Maximum number of tuple updates or deletes prior to vacuum."), long_desc: Some("-1 disables the maximum threshold."), flags: 0, variable: &vars::autovacuum_vac_max_thresh, boot_val: GucDefaultValue::Int(100000000), min: -(1), max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "autovacuum_vacuum_insert_threshold", context: PGC_SIGHUP, group: VACUUM_AUTOVACUUM, short_desc: Some("Minimum number of tuple inserts prior to vacuum."), long_desc: Some("-1 disables insert vacuums."), flags: 0, variable: &vars::autovacuum_vac_ins_thresh, boot_val: GucDefaultValue::Int(1000), min: -(1), max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "autovacuum_analyze_threshold", context: PGC_SIGHUP, group: VACUUM_AUTOVACUUM, short_desc: Some("Minimum number of tuple inserts, updates, or deletes prior to analyze."), long_desc: None, flags: 0, variable: &vars::autovacuum_anl_thresh, boot_val: GucDefaultValue::Int(50), min: 0, max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "autovacuum_freeze_max_age", context: PGC_POSTMASTER, group: VACUUM_AUTOVACUUM, short_desc: Some("Age at which to autovacuum a table to prevent transaction ID wraparound."), long_desc: None, flags: 0, variable: &vars::autovacuum_freeze_max_age, boot_val: GucDefaultValue::Int(200000000), min: 100000, max: 2000000000, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "autovacuum_multixact_freeze_max_age", context: PGC_POSTMASTER, group: VACUUM_AUTOVACUUM, short_desc: Some("Multixact age at which to autovacuum a table to prevent multixact wraparound."), long_desc: None, flags: 0, variable: &vars::autovacuum_multixact_freeze_max_age, boot_val: GucDefaultValue::Int(400000000), min: 10000, max: 2000000000, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "autovacuum_worker_slots", context: PGC_POSTMASTER, group: VACUUM_AUTOVACUUM, short_desc: Some("Sets the number of backend slots to allocate for autovacuum workers."), long_desc: None, flags: 0, variable: &vars::autovacuum_worker_slots, boot_val: GucDefaultValue::Int(16), min: 1, max: MAX_BACKENDS, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "autovacuum_max_workers", context: PGC_SIGHUP, group: VACUUM_AUTOVACUUM, short_desc: Some("Sets the maximum number of simultaneously running autovacuum worker processes."), long_desc: None, flags: 0, variable: &vars::autovacuum_max_workers, boot_val: GucDefaultValue::Int(3), min: 1, max: MAX_BACKENDS, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "max_parallel_maintenance_workers", context: PGC_USERSET, group: RESOURCES_WORKER_PROCESSES, short_desc: Some("Sets the maximum number of parallel processes per maintenance operation."), long_desc: None, flags: 0, variable: &vars::max_parallel_maintenance_workers, boot_val: GucDefaultValue::Int(2), min: 0, max: MAX_PARALLEL_WORKER_LIMIT, check_hook: None, assign_hook: None, show_hook: None },
    // Raised 2->4: warm-pool setup is cheap and per-gather is just the ceiling
    // compute_parallel_worker's log3 table-size rule fills up to; 4 lets a
    // single analytical scan use up to 5 threads. Measured scaling is
    // near-linear through DOP 8 on joins (notes/m3-hashjoin.md:227).
    GucIntSetting { name: "max_parallel_workers_per_gather", context: PGC_USERSET, group: RESOURCES_WORKER_PROCESSES, short_desc: Some("Sets the maximum number of parallel processes per executor node."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::max_parallel_workers_per_gather, boot_val: GucDefaultValue::Int(4), min: 0, max: MAX_PARALLEL_WORKER_LIMIT, check_hook: None, assign_hook: None, show_hook: None },
    // Raised 8->16. Under WORK-STEALING this cap stops being a thrash-avoidance
    // knob: pooled threads are not dedicated to a gather for the query's life;
    // idle threads are dynamically reassigned and a hard permit cap (=physical
    // cores) bounds RUNNING threads regardless of how many are "planned"
    // (runtime_pool.rs:43, docs/design/dop192-readiness.md). Over-provisioning
    // is therefore harmless — excess planned workers park — so the cap can sit
    // at/above common core counts. 16 covers typical Graviton sizes; the
    // runtime pool itself self-sizes to available_parallelism().
    GucIntSetting { name: "max_parallel_workers", context: PGC_USERSET, group: RESOURCES_WORKER_PROCESSES, short_desc: Some("Sets the maximum number of parallel workers that can be active at one time."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::max_parallel_workers, boot_val: GucDefaultValue::Int(16), min: 0, max: MAX_PARALLEL_WORKER_LIMIT, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "autovacuum_work_mem", context: PGC_SIGHUP, group: RESOURCES_MEM, short_desc: Some("Sets the maximum memory to be used by each autovacuum worker process."), long_desc: Some("-1 means use \"maintenance_work_mem\"."), flags: GUC_UNIT_KB, variable: &vars::autovacuum_work_mem, boot_val: GucDefaultValue::Int(-(1)), min: -(1), max: MAX_KILOBYTES, check_hook: Some(&hooks::check_autovacuum_work_mem), assign_hook: None, show_hook: None },
    GucIntSetting { name: "tcp_keepalives_idle", context: PGC_USERSET, group: CONN_AUTH_TCP, short_desc: Some("Time between issuing TCP keepalives."), long_desc: Some("0 means use the system default."), flags: GUC_UNIT_S, variable: &vars::tcp_keepalives_idle, boot_val: GucDefaultValue::Int(0), min: 0, max: i32::MAX, check_hook: None, assign_hook: Some(&hooks::assign_tcp_keepalives_idle), show_hook: Some(&hooks::show_tcp_keepalives_idle) },
    GucIntSetting { name: "tcp_keepalives_interval", context: PGC_USERSET, group: CONN_AUTH_TCP, short_desc: Some("Time between TCP keepalive retransmits."), long_desc: Some("0 means use the system default."), flags: GUC_UNIT_S, variable: &vars::tcp_keepalives_interval, boot_val: GucDefaultValue::Int(0), min: 0, max: i32::MAX, check_hook: None, assign_hook: Some(&hooks::assign_tcp_keepalives_interval), show_hook: Some(&hooks::show_tcp_keepalives_interval) },
    GucIntSetting { name: "ssl_renegotiation_limit", context: PGC_USERSET, group: COMPAT_OPTIONS_PREVIOUS, short_desc: Some("SSL renegotiation is no longer supported; this can only be 0."), long_desc: None, flags: GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE, variable: &vars::ssl_renegotiation_limit, boot_val: GucDefaultValue::Int(0), min: 0, max: 0, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "tcp_keepalives_count", context: PGC_USERSET, group: CONN_AUTH_TCP, short_desc: Some("Maximum number of TCP keepalive retransmits."), long_desc: Some("Number of consecutive keepalive retransmits that can be lost before a connection is considered dead. 0 means use the system default."), flags: 0, variable: &vars::tcp_keepalives_count, boot_val: GucDefaultValue::Int(0), min: 0, max: i32::MAX, check_hook: None, assign_hook: Some(&hooks::assign_tcp_keepalives_count), show_hook: Some(&hooks::show_tcp_keepalives_count) },
    GucIntSetting { name: "gin_fuzzy_search_limit", context: PGC_USERSET, group: CLIENT_CONN_OTHER, short_desc: Some("Sets the maximum allowed result for exact search by GIN."), long_desc: Some("0 means no limit."), flags: 0, variable: &vars::GinFuzzySearchLimit, boot_val: GucDefaultValue::Int(0), min: 0, max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "effective_cache_size", context: PGC_USERSET, group: QUERY_TUNING_COST, short_desc: Some("Sets the planner's assumption about the total size of the data caches."), long_desc: Some("That is, the total size of the caches (kernel cache and shared buffers) used for PostgreSQL data files. This is measured in disk pages, which are normally 8 kB each."), flags: GUC_UNIT_BLOCKS | GUC_EXPLAIN, variable: &vars::effective_cache_size, boot_val: GucDefaultValue::Int(DEFAULT_EFFECTIVE_CACHE_SIZE), min: 1, max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "min_parallel_table_scan_size", context: PGC_USERSET, group: QUERY_TUNING_COST, short_desc: Some("Sets the minimum amount of table data for a parallel scan."), long_desc: Some("If the planner estimates that it will read a number of table pages too small to reach this limit, a parallel scan will not be considered."), flags: GUC_UNIT_BLOCKS | GUC_EXPLAIN, variable: &vars::min_parallel_table_scan_size, boot_val: GucDefaultValue::Int(1024 * 1024 / BLCKSZ), min: 0, max: i32::MAX / 3, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "min_parallel_index_scan_size", context: PGC_USERSET, group: QUERY_TUNING_COST, short_desc: Some("Sets the minimum amount of index data for a parallel scan."), long_desc: Some("If the planner estimates that it will read a number of index pages too small to reach this limit, a parallel scan will not be considered."), flags: GUC_UNIT_BLOCKS | GUC_EXPLAIN, variable: &vars::min_parallel_index_scan_size, boot_val: GucDefaultValue::Int(64 * 1024 / BLCKSZ), min: 0, max: i32::MAX / 3, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "server_version_num", context: PGC_INTERNAL, group: PRESET_OPTIONS, short_desc: Some("Shows the server version as an integer."), long_desc: None, flags: GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE, variable: &vars::server_version_num, boot_val: GucDefaultValue::Int(PG_VERSION_NUM), min: PG_VERSION_NUM, max: PG_VERSION_NUM, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "log_temp_files", context: PGC_SUSET, group: LOGGING_WHAT, short_desc: Some("Log the use of temporary files larger than this number of kilobytes."), long_desc: Some("-1 disables logging temporary files. 0 means log all temporary files."), flags: GUC_UNIT_KB, variable: &vars::log_temp_files, boot_val: GucDefaultValue::Int(-(1)), min: -(1), max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "track_activity_query_size", context: PGC_POSTMASTER, group: STATS_CUMULATIVE, short_desc: Some("Sets the size reserved for pg_stat_activity.query, in bytes."), long_desc: None, flags: GUC_UNIT_BYTE, variable: &vars::pgstat_track_activity_query_size, boot_val: GucDefaultValue::Int(1024), min: 100, max: 1048576, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "gin_pending_list_limit", context: PGC_USERSET, group: CLIENT_CONN_STATEMENT, short_desc: Some("Sets the maximum size of the pending list for GIN index."), long_desc: None, flags: GUC_UNIT_KB, variable: &vars::gin_pending_list_limit, boot_val: GucDefaultValue::Int(4096), min: 64, max: MAX_KILOBYTES, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "tcp_user_timeout", context: PGC_USERSET, group: CONN_AUTH_TCP, short_desc: Some("TCP user timeout."), long_desc: Some("0 means use the system default."), flags: GUC_UNIT_MS, variable: &vars::tcp_user_timeout, boot_val: GucDefaultValue::Int(0), min: 0, max: i32::MAX, check_hook: None, assign_hook: Some(&hooks::assign_tcp_user_timeout), show_hook: Some(&hooks::show_tcp_user_timeout) },
    GucIntSetting { name: "huge_page_size", context: PGC_POSTMASTER, group: RESOURCES_MEM, short_desc: Some("The size of huge page that should be requested."), long_desc: Some("0 means use the system default."), flags: GUC_UNIT_KB, variable: &vars::huge_page_size, boot_val: GucDefaultValue::Int(0), min: 0, max: i32::MAX, check_hook: Some(&hooks::check_huge_page_size), assign_hook: None, show_hook: None },
    GucIntSetting { name: "debug_discard_caches", context: PGC_SUSET, group: DEVELOPER_OPTIONS, short_desc: Some("Aggressively flush system caches for debugging purposes."), long_desc: Some("0 means use normal caching behavior."), flags: GUC_NOT_IN_SAMPLE, variable: &vars::debug_discard_caches, boot_val: GucDefaultValue::Int(0), min: 0, max: 0, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "client_connection_check_interval", context: PGC_USERSET, group: CONN_AUTH_TCP, short_desc: Some("Sets the time interval between checks for disconnection while running queries."), long_desc: Some("0 disables connection checks."), flags: GUC_UNIT_MS, variable: &vars::client_connection_check_interval, boot_val: GucDefaultValue::Int(0), min: 0, max: i32::MAX, check_hook: Some(&hooks::check_client_connection_check_interval), assign_hook: None, show_hook: None },
    GucIntSetting { name: "log_startup_progress_interval", context: PGC_SIGHUP, group: LOGGING_WHEN, short_desc: Some("Time between progress updates for long-running startup operations."), long_desc: Some("0 disables progress updates."), flags: GUC_UNIT_MS, variable: &vars::log_startup_progress_interval, boot_val: GucDefaultValue::Int(10000), min: 0, max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "scram_iterations", context: PGC_USERSET, group: CONN_AUTH_AUTH, short_desc: Some("Sets the iteration count for SCRAM secret generation."), long_desc: None, flags: GUC_REPORT, variable: &vars::scram_sha_256_iterations, boot_val: GucDefaultValue::Int(SCRAM_SHA_256_DEFAULT_ITERATIONS), min: 1, max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "hnsw.ef_search", context: PGC_USERSET, group: CUSTOM_OPTIONS, short_desc: Some("Sets the size of the dynamic candidate list for search"), long_desc: Some("Valid range is 1..1000."), flags: 0, variable: &vars::hnsw_ef_search, boot_val: GucDefaultValue::Int(40), min: 1, max: 1000, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "hnsw.max_scan_tuples", context: PGC_USERSET, group: CUSTOM_OPTIONS, short_desc: Some("Sets the max number of tuples to visit for iterative scans"), long_desc: None, flags: 0, variable: &vars::hnsw_max_scan_tuples, boot_val: GucDefaultValue::Int(20000), min: 1, max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    // pgrust.condition_cache_size: the condition cache's LRU byte budget
    // (default 100MB — ClickHouse's query_condition_cache_size default).
    GucIntSetting { name: "pgrust.condition_cache_size", context: PGC_USERSET, group: CUSTOM_OPTIONS, short_desc: Some("Sets the memory budget of the cbstore condition cache."), long_desc: None, flags: GUC_UNIT_KB, variable: &vars::pgrust_condition_cache_size, boot_val: GucDefaultValue::Int(102400), min: 0, max: MAX_KILOBYTES, check_hook: None, assign_hook: None, show_hook: None },
    // pgrust.memory_watchdog family ints (pgrust-only, GL-MEMWATCH-1).
    GucIntSetting { name: "pgrust.memory_watchdog_interval", context: PGC_SIGHUP, group: CUSTOM_OPTIONS, short_desc: Some("Sets the memory watchdog sampling interval."), long_desc: None, flags: GUC_UNIT_MS, variable: &vars::pgrust_memory_watchdog_interval, boot_val: GucDefaultValue::Int(1000), min: 100, max: 60 * 1000, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "pgrust.memory_watchdog_threshold", context: PGC_SIGHUP, group: CUSTOM_OPTIONS, short_desc: Some("Sets the memory watchdog's base warning threshold as a percent of the memory limit."), long_desc: Some("Escalation tiers derive from the base T as T, T + (100-T)/2 and T + 3*(100-T)/4 (80 -> 80/90/95); each tier logs once per excursion above the base."), flags: 0, variable: &vars::pgrust_memory_watchdog_threshold, boot_val: GucDefaultValue::Int(80), min: 1, max: 100, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "pgrust.memory_watchdog_limit", context: PGC_SIGHUP, group: CUSTOM_OPTIONS, short_desc: Some("Sets the absolute memory limit the watchdog thresholds apply to (0 = use the cgroup v2 memory limit)."), long_desc: None, flags: GUC_UNIT_MB, variable: &vars::pgrust_memory_watchdog_limit, boot_val: GucDefaultValue::Int(0), min: 0, max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
    // Developer knob for the watchdog's standing e2e: each simple query leaks
    // this many MB into a session-lifetime "WatchdogTestHog" context. Hidden
    // (NOT_IN_SAMPLE + NO_SHOW_ALL): a deliberate leak is never a product knob.
    GucIntSetting { name: "pgrust.memory_watchdog_test_hog", context: PGC_USERSET, group: DEVELOPER_OPTIONS, short_desc: Some("Leaks this many MB per query into a named memory context (memory watchdog test instrumentation)."), long_desc: None, flags: GUC_UNIT_MB | GUC_NOT_IN_SAMPLE | GUC_NO_SHOW_ALL, variable: &vars::pgrust_memory_watchdog_test_hog, boot_val: GucDefaultValue::Int(0), min: 0, max: 1024 * 1024, check_hook: None, assign_hook: None, show_hook: None },
    // pgrust.runtime_dop (M5-0, docs/design/m5-planner.md §2.2): the product
    // DOP cap for runtime-engine engagements, consulted ONLY under
    // pgrust.parallel_engine=runtime (the M5-1 router reads it; the per-arm
    // bench pool GUCs never do). 0 = auto (available cores at engagement).
    GucIntSetting { name: "pgrust.runtime_dop", context: PGC_USERSET, group: CUSTOM_OPTIONS, short_desc: Some("Sets the degree of parallelism for runtime-engine engagements (0 = number of cores)."), long_desc: None, flags: 0, variable: &vars::pgrust_runtime_dop, boot_val: GucDefaultValue::Int(0), min: 0, max: 1024, check_hook: None, assign_hook: None, show_hook: None },
    // Per-arm runtime pool DOP force-overrides (env-to-guc train; deferred
    // pool-GUC recipe, docs/design/jit-parallel-defaults.md §3). Registered
    // faces of the formerly-unregistered `pgrust.*` placeholder options; the
    // arm readers (runtime_pool.rs / lane_pool.rs) resolve them through the
    // get_config_option seam, which now returns these registered cells.
    // 0 = auto: inherit pgrust.runtime_dop under pgrust.parallel_engine=runtime
    // (behavior-neutral at the default). PGC_USERSET, same bounds as runtime_dop.
    GucIntSetting { name: "pgrust.runtime_scan_pool", context: PGC_USERSET, group: CUSTOM_OPTIONS, short_desc: Some("Forces the runtime scan arm's degree of parallelism (0 = auto)."), long_desc: Some("0 = auto: inherit pgrust.runtime_dop under pgrust.parallel_engine=runtime."), flags: 0, variable: &vars::pgrust_runtime_scan_pool, boot_val: GucDefaultValue::Int(0), min: 0, max: 1024, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "pgrust.runtime_agg_pool", context: PGC_USERSET, group: CUSTOM_OPTIONS, short_desc: Some("Forces the runtime aggregation-sink arm's degree of parallelism (0 = auto)."), long_desc: Some("0 = auto: inherit pgrust.runtime_dop under pgrust.parallel_engine=runtime."), flags: 0, variable: &vars::pgrust_runtime_agg_pool, boot_val: GucDefaultValue::Int(0), min: 0, max: 1024, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "pgrust.runtime_distinct_pool", context: PGC_USERSET, group: CUSTOM_OPTIONS, short_desc: Some("Forces the runtime distinct-sink arm's degree of parallelism (0 = auto)."), long_desc: Some("0 = auto: inherit pgrust.runtime_dop (or pgrust.runtime_scan_pool) under pgrust.parallel_engine=runtime."), flags: 0, variable: &vars::pgrust_runtime_distinct_pool, boot_val: GucDefaultValue::Int(0), min: 0, max: 1024, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "pgrust.runtime_hashjoin_pool", context: PGC_USERSET, group: CUSTOM_OPTIONS, short_desc: Some("Forces the runtime hash-join arm's degree of parallelism (0 = auto)."), long_desc: Some("0 = auto: inherit pgrust.runtime_dop under pgrust.parallel_engine=runtime."), flags: 0, variable: &vars::pgrust_runtime_hashjoin_pool, boot_val: GucDefaultValue::Int(0), min: 0, max: 1024, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "pgrust.runtime_sort_pool", context: PGC_USERSET, group: CUSTOM_OPTIONS, short_desc: Some("Forces the runtime sort arm's degree of parallelism (0 = auto)."), long_desc: Some("0 = auto: inherit pgrust.runtime_dop under pgrust.parallel_engine=runtime."), flags: 0, variable: &vars::pgrust_runtime_sort_pool, boot_val: GucDefaultValue::Int(0), min: 0, max: 1024, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "pgrust.runtime_bitmap_pool", context: PGC_USERSET, group: CUSTOM_OPTIONS, short_desc: Some("Forces the runtime bitmap-heap arm's degree of parallelism (0 = auto)."), long_desc: Some("0 = auto: inherit pgrust.runtime_dop under pgrust.parallel_engine=runtime."), flags: 0, variable: &vars::pgrust_runtime_bitmap_pool, boot_val: GucDefaultValue::Int(0), min: 0, max: 1024, check_hook: None, assign_hook: None, show_hook: None },
    GucIntSetting { name: "pgrust.lane_parallel_pool", context: PGC_USERSET, group: CUSTOM_OPTIONS, short_desc: Some("Forces the lane-v2 parallel arm's degree of parallelism (0 = auto)."), long_desc: Some("0 = auto: inherit pgrust.runtime_dop under pgrust.parallel_engine=runtime."), flags: 0, variable: &vars::pgrust_lane_parallel_pool, boot_val: GucDefaultValue::Int(0), min: 0, max: 1024, check_hook: None, assign_hook: None, show_hook: None },
    // Gather read-fairness stride: after N consecutive tuples from one queue
    // the Gather leader advances its read cursor round-robin. 0 = C parity
    // (drain one queue until it would block). A tuple count, not a DOP.
    GucIntSetting { name: "pgrust.gather_fair_stride", context: PGC_USERSET, group: CUSTOM_OPTIONS, short_desc: Some("Round-robins the Gather leader's queue reads after this many tuples (0 = C behavior)."), long_desc: None, flags: 0, variable: &vars::pgrust_gather_fair_stride, boot_val: GucDefaultValue::Int(0), min: 0, max: i32::MAX, check_hook: None, assign_hook: None, show_hook: None },
];

pub static ConfigureNamesReal: &[GucRealSetting] = &[
    GucRealSetting { name: "auto_explain.sample_rate", context: PGC_SUSET, group: CUSTOM_OPTIONS, short_desc: Some("Fraction of queries to process."), long_desc: None, flags: 0, variable: &vars::aex_sample_rate, boot_val: GucDefaultValue::Real(1.0f64), min: 0.0f64, max: 1.0f64, check_hook: None, assign_hook: None, show_hook: None },
    GucRealSetting { name: "seq_page_cost", context: PGC_USERSET, group: QUERY_TUNING_COST, short_desc: Some("Sets the planner's estimate of the cost of a sequentially fetched disk page."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::seq_page_cost, boot_val: GucDefaultValue::Real(DEFAULT_SEQ_PAGE_COST), min: 0.0, max: f64::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucRealSetting { name: "random_page_cost", context: PGC_USERSET, group: QUERY_TUNING_COST, short_desc: Some("Sets the planner's estimate of the cost of a nonsequentially fetched disk page."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::random_page_cost, boot_val: GucDefaultValue::Real(DEFAULT_RANDOM_PAGE_COST), min: 0.0, max: f64::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucRealSetting { name: "cpu_tuple_cost", context: PGC_USERSET, group: QUERY_TUNING_COST, short_desc: Some("Sets the planner's estimate of the cost of processing each tuple (row)."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::cpu_tuple_cost, boot_val: GucDefaultValue::Real(DEFAULT_CPU_TUPLE_COST), min: 0.0, max: f64::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucRealSetting { name: "cpu_index_tuple_cost", context: PGC_USERSET, group: QUERY_TUNING_COST, short_desc: Some("Sets the planner's estimate of the cost of processing each index entry during an index scan."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::cpu_index_tuple_cost, boot_val: GucDefaultValue::Real(DEFAULT_CPU_INDEX_TUPLE_COST), min: 0.0, max: f64::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucRealSetting { name: "cpu_operator_cost", context: PGC_USERSET, group: QUERY_TUNING_COST, short_desc: Some("Sets the planner's estimate of the cost of processing each operator or function call."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::cpu_operator_cost, boot_val: GucDefaultValue::Real(DEFAULT_CPU_OPERATOR_COST), min: 0.0, max: f64::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucRealSetting { name: "parallel_tuple_cost", context: PGC_USERSET, group: QUERY_TUNING_COST, short_desc: Some("Sets the planner's estimate of the cost of passing each tuple (row) from worker to leader backend."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::parallel_tuple_cost, boot_val: GucDefaultValue::Real(DEFAULT_PARALLEL_TUPLE_COST), min: 0.0, max: f64::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucRealSetting { name: "parallel_setup_cost", context: PGC_USERSET, group: QUERY_TUNING_COST, short_desc: Some("Sets the planner's estimate of the cost of starting up worker processes for parallel query."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::parallel_setup_cost, boot_val: GucDefaultValue::Real(DEFAULT_PARALLEL_SETUP_COST), min: 0.0, max: f64::MAX, check_hook: None, assign_hook: None, show_hook: None },
    // pgrust JIT is copy-and-patch (~4-5us/kernel, ~808-1426 user instr), not
    // LLVM (~10-50ms) — but expression kernels have NO cross-execution cache:
    // they are estate-owned (execexpr/src/jit.rs), so every EXECUTE of a
    // prepared statement recompiles every Program-shape expression (~4-6
    // kernels, ~17-25us, ~6-9k instr per execution; each install pays two
    // mprotects + icache flush even arena-warm — jit_deform alloc_code). The
    // threshold must therefore clear the WHOLE per-execution compile bill over
    // expected executions = 1. Break-even: ~20us compile / ~14ns-per-row
    // saving = ~1.4k rows ~= seqscan+filter plan cost ~40-60; a 3-5x margin
    // for index-shaped plans (whose cost/row is ~10x a seqscan's, so equal
    // cost = far fewer rows) puts the default at 200. Cross-check: C's 100000
    // amortizes ~10-50ms of LLVM compile; pgrust's ~20us per execution is
    // ~500-2500x cheaper -> 100000/500..2500 = 40..200. 200 keeps the whole
    // mid-cost prepared-OLTP band (plan cost 10-100) interpreted — the old
    // default of 10 taxed it 5-15% per execution — while analytics scans
    // (cost in the thousands, 8-14x per-row JIT wins) still compile. Full
    // derivation: docs/design/jit-parallel-defaults.md.
    GucRealSetting { name: "jit_above_cost", context: PGC_USERSET, group: QUERY_TUNING_COST, short_desc: Some("Perform JIT compilation if query is more expensive."), long_desc: Some("-1 disables JIT compilation."), flags: GUC_EXPLAIN, variable: &vars::jit_above_cost, boot_val: GucDefaultValue::Real(200.0), min: -(1.0), max: f64::MAX, check_hook: None, assign_hook: None, show_hook: None },
    // INERT under copy-and-patch: PGJIT_OPT3/PGJIT_INLINE have no consumer that
    // does work (only the planner sets them and EXPLAIN prints them; LLVM
    // phase counters print 0.000 — jit-qual.md:415). Copy-and-patch emits its
    // final code in one pass; there is no separate optimize/inline phase to
    // gate. Kept for pg_settings compatibility, set equal to jit_above_cost to
    // document "one JIT tier engaged together" rather than C's misleading 5x
    // ordering (which would imply an optimizer that does not exist).
    GucRealSetting { name: "jit_optimize_above_cost", context: PGC_USERSET, group: QUERY_TUNING_COST, short_desc: Some("Optimize JIT-compiled functions if query is more expensive."), long_desc: Some("-1 disables optimization."), flags: GUC_EXPLAIN, variable: &vars::jit_optimize_above_cost, boot_val: GucDefaultValue::Real(200.0), min: -(1.0), max: f64::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucRealSetting { name: "jit_inline_above_cost", context: PGC_USERSET, group: QUERY_TUNING_COST, short_desc: Some("Perform JIT inlining if query is more expensive."), long_desc: Some("-1 disables inlining."), flags: GUC_EXPLAIN, variable: &vars::jit_inline_above_cost, boot_val: GucDefaultValue::Real(200.0), min: -(1.0), max: f64::MAX, check_hook: None, assign_hook: None, show_hook: None },
    GucRealSetting { name: "cursor_tuple_fraction", context: PGC_USERSET, group: QUERY_TUNING_OTHER, short_desc: Some("Sets the planner's estimate of the fraction of a cursor's rows that will be retrieved."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::cursor_tuple_fraction, boot_val: GucDefaultValue::Real(DEFAULT_CURSOR_TUPLE_FRACTION), min: 0.0f64, max: 1.0f64, check_hook: None, assign_hook: None, show_hook: None },
    GucRealSetting { name: "recursive_worktable_factor", context: PGC_USERSET, group: QUERY_TUNING_OTHER, short_desc: Some("Sets the planner's estimate of the average size of a recursive query's working table."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::recursive_worktable_factor, boot_val: GucDefaultValue::Real(DEFAULT_RECURSIVE_WORKTABLE_FACTOR), min: 0.001f64, max: 1000000.0f64, check_hook: None, assign_hook: None, show_hook: None },
    GucRealSetting { name: "geqo_selection_bias", context: PGC_USERSET, group: QUERY_TUNING_GEQO, short_desc: Some("GEQO: selective pressure within the population."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::Geqo_selection_bias, boot_val: GucDefaultValue::Real(DEFAULT_GEQO_SELECTION_BIAS), min: MIN_GEQO_SELECTION_BIAS, max: MAX_GEQO_SELECTION_BIAS, check_hook: None, assign_hook: None, show_hook: None },
    GucRealSetting { name: "geqo_seed", context: PGC_USERSET, group: QUERY_TUNING_GEQO, short_desc: Some("GEQO: seed for random path selection."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::Geqo_seed, boot_val: GucDefaultValue::Real(0.0f64), min: 0.0f64, max: 1.0f64, check_hook: None, assign_hook: None, show_hook: None },
    GucRealSetting { name: "hash_mem_multiplier", context: PGC_USERSET, group: RESOURCES_MEM, short_desc: Some("Multiple of \"work_mem\" to use for hash tables."), long_desc: None, flags: GUC_EXPLAIN, variable: &vars::hash_mem_multiplier, boot_val: GucDefaultValue::Real(2.0f64), min: 1.0f64, max: 1000.0f64, check_hook: None, assign_hook: None, show_hook: None },
    GucRealSetting { name: "bgwriter_lru_multiplier", context: PGC_SIGHUP, group: RESOURCES_BGWRITER, short_desc: Some("Multiple of the average buffer usage to free per round."), long_desc: None, flags: 0, variable: &vars::bgwriter_lru_multiplier, boot_val: GucDefaultValue::Real(2.0f64), min: 0.0f64, max: 10.0f64, check_hook: None, assign_hook: None, show_hook: None },
    GucRealSetting { name: "seed", context: PGC_USERSET, group: UNGROUPED, short_desc: Some("Sets the seed for random-number generation."), long_desc: None, flags: GUC_NO_SHOW_ALL | GUC_NO_RESET | GUC_NO_RESET_ALL | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE, variable: &vars::phony_random_seed, boot_val: GucDefaultValue::Real(0.0f64), min: -1.0f64, max: 1.0f64, check_hook: Some(&hooks::check_random_seed), assign_hook: Some(&hooks::assign_random_seed), show_hook: Some(&hooks::show_random_seed) },
    GucRealSetting { name: "vacuum_cost_delay", context: PGC_USERSET, group: VACUUM_COST_DELAY, short_desc: Some("Vacuum cost delay in milliseconds."), long_desc: None, flags: GUC_UNIT_MS, variable: &vars::VacuumCostDelay, boot_val: GucDefaultValue::Real(0.0), min: 0.0, max: 100.0, check_hook: None, assign_hook: None, show_hook: None },
    GucRealSetting { name: "autovacuum_vacuum_cost_delay", context: PGC_SIGHUP, group: VACUUM_AUTOVACUUM, short_desc: Some("Vacuum cost delay in milliseconds, for autovacuum."), long_desc: Some("-1 means use \"vacuum_cost_delay\"."), flags: GUC_UNIT_MS, variable: &vars::autovacuum_vac_cost_delay, boot_val: GucDefaultValue::Real(2.0), min: -(1.0), max: 100.0, check_hook: None, assign_hook: None, show_hook: None },
    GucRealSetting { name: "autovacuum_vacuum_scale_factor", context: PGC_SIGHUP, group: VACUUM_AUTOVACUUM, short_desc: Some("Number of tuple updates or deletes prior to vacuum as a fraction of reltuples."), long_desc: None, flags: 0, variable: &vars::autovacuum_vac_scale, boot_val: GucDefaultValue::Real(0.2f64), min: 0.0f64, max: 100.0f64, check_hook: None, assign_hook: None, show_hook: None },
    GucRealSetting { name: "autovacuum_vacuum_insert_scale_factor", context: PGC_SIGHUP, group: VACUUM_AUTOVACUUM, short_desc: Some("Number of tuple inserts prior to vacuum as a fraction of reltuples."), long_desc: None, flags: 0, variable: &vars::autovacuum_vac_ins_scale, boot_val: GucDefaultValue::Real(0.2f64), min: 0.0f64, max: 100.0f64, check_hook: None, assign_hook: None, show_hook: None },
    GucRealSetting { name: "autovacuum_analyze_scale_factor", context: PGC_SIGHUP, group: VACUUM_AUTOVACUUM, short_desc: Some("Number of tuple inserts, updates, or deletes prior to analyze as a fraction of reltuples."), long_desc: None, flags: 0, variable: &vars::autovacuum_anl_scale, boot_val: GucDefaultValue::Real(0.1f64), min: 0.0f64, max: 100.0f64, check_hook: None, assign_hook: None, show_hook: None },
    GucRealSetting { name: "checkpoint_completion_target", context: PGC_SIGHUP, group: WAL_CHECKPOINTS, short_desc: Some("Time spent flushing dirty buffers during checkpoint, as fraction of checkpoint interval."), long_desc: None, flags: 0, variable: &vars::CheckPointCompletionTarget, boot_val: GucDefaultValue::Real(0.9f64), min: 0.0f64, max: 1.0f64, check_hook: None, assign_hook: Some(&hooks::assign_checkpoint_completion_target), show_hook: None },
    GucRealSetting { name: "log_statement_sample_rate", context: PGC_SUSET, group: LOGGING_WHEN, short_desc: Some("Fraction of statements exceeding \"log_min_duration_sample\" to be logged."), long_desc: Some("Use a value between 0.0 (never log) and 1.0 (always log)."), flags: 0, variable: &vars::log_statement_sample_rate, boot_val: GucDefaultValue::Real(1.0f64), min: 0.0f64, max: 1.0f64, check_hook: None, assign_hook: None, show_hook: None },
    GucRealSetting { name: "log_transaction_sample_rate", context: PGC_SUSET, group: LOGGING_WHEN, short_desc: Some("Sets the fraction of transactions from which to log all statements."), long_desc: Some("Use a value between 0.0 (never log) and 1.0 (log all statements for all transactions)."), flags: 0, variable: &vars::log_xact_sample_rate, boot_val: GucDefaultValue::Real(0.0f64), min: 0.0f64, max: 1.0f64, check_hook: None, assign_hook: None, show_hook: None },
    GucRealSetting { name: "vacuum_max_eager_freeze_failure_rate", context: PGC_USERSET, group: VACUUM_FREEZING, short_desc: Some("Fraction of pages in a relation vacuum can scan and fail to freeze before disabling eager scanning."), long_desc: Some("A value of 0.0 disables eager scanning and a value of 1.0 will eagerly scan up to 100 percent of the all-visible pages in the relation. If vacuum successfully freezes these pages, the cap is lower than 100 percent, because the goal is to amortize page freezing across multiple vacuums."), flags: 0, variable: &vars::vacuum_max_eager_freeze_failure_rate, boot_val: GucDefaultValue::Real(0.03f64), min: 0.0f64, max: 1.0f64, check_hook: None, assign_hook: None, show_hook: None },
    GucRealSetting { name: "hnsw.scan_mem_multiplier", context: PGC_USERSET, group: CUSTOM_OPTIONS, short_desc: Some("Sets the multiple of work_mem to use for iterative scans"), long_desc: None, flags: 0, variable: &vars::hnsw_scan_mem_multiplier, boot_val: GucDefaultValue::Real(1.0f64), min: 1.0f64, max: 1000.0f64, check_hook: None, assign_hook: None, show_hook: None },
];

pub static ConfigureNamesString: &[GucStringSetting] = &[
    GucStringSetting { name: "archive_command", context: PGC_SIGHUP, group: WAL_ARCHIVING, short_desc: Some("Sets the shell command that will be called to archive a WAL file."), long_desc: Some("An empty string means use \"archive_library\"."), flags: 0, variable: &vars::XLogArchiveCommand, boot_val: GucDefaultValue::String(Some("")), check_hook: None, assign_hook: None, show_hook: Some(&hooks::show_archive_command) },
    GucStringSetting { name: "archive_library", context: PGC_SIGHUP, group: WAL_ARCHIVING, short_desc: Some("Sets the library that will be called to archive a WAL file."), long_desc: Some("An empty string means use \"archive_command\"."), flags: 0, variable: &vars::XLogArchiveLibrary, boot_val: GucDefaultValue::String(Some("")), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "restore_command", context: PGC_SIGHUP, group: WAL_ARCHIVE_RECOVERY, short_desc: Some("Sets the shell command that will be called to retrieve an archived WAL file."), long_desc: None, flags: 0, variable: &vars::recoveryRestoreCommand, boot_val: GucDefaultValue::String(Some("")), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "archive_cleanup_command", context: PGC_SIGHUP, group: WAL_ARCHIVE_RECOVERY, short_desc: Some("Sets the shell command that will be executed at every restart point."), long_desc: None, flags: 0, variable: &vars::archiveCleanupCommand, boot_val: GucDefaultValue::String(Some("")), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "recovery_end_command", context: PGC_SIGHUP, group: WAL_ARCHIVE_RECOVERY, short_desc: Some("Sets the shell command that will be executed once at the end of recovery."), long_desc: None, flags: 0, variable: &vars::recoveryEndCommand, boot_val: GucDefaultValue::String(Some("")), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "recovery_target_timeline", context: PGC_POSTMASTER, group: WAL_RECOVERY_TARGET, short_desc: Some("Specifies the timeline to recover into."), long_desc: None, flags: 0, variable: &vars::recovery_target_timeline_string, boot_val: GucDefaultValue::String(Some("latest")), check_hook: Some(&hooks::check_recovery_target_timeline), assign_hook: Some(&hooks::assign_recovery_target_timeline), show_hook: None },
    GucStringSetting { name: "recovery_target", context: PGC_POSTMASTER, group: WAL_RECOVERY_TARGET, short_desc: Some("Set to \"immediate\" to end recovery as soon as a consistent state is reached."), long_desc: None, flags: 0, variable: &vars::recovery_target_string, boot_val: GucDefaultValue::String(Some("")), check_hook: Some(&hooks::check_recovery_target), assign_hook: Some(&hooks::assign_recovery_target), show_hook: None },
    GucStringSetting { name: "recovery_target_xid", context: PGC_POSTMASTER, group: WAL_RECOVERY_TARGET, short_desc: Some("Sets the transaction ID up to which recovery will proceed."), long_desc: None, flags: 0, variable: &vars::recovery_target_xid_string, boot_val: GucDefaultValue::String(Some("")), check_hook: Some(&hooks::check_recovery_target_xid), assign_hook: Some(&hooks::assign_recovery_target_xid), show_hook: None },
    GucStringSetting { name: "recovery_target_time", context: PGC_POSTMASTER, group: WAL_RECOVERY_TARGET, short_desc: Some("Sets the time stamp up to which recovery will proceed."), long_desc: None, flags: 0, variable: &vars::recovery_target_time_string, boot_val: GucDefaultValue::String(Some("")), check_hook: Some(&hooks::check_recovery_target_time), assign_hook: Some(&hooks::assign_recovery_target_time), show_hook: None },
    GucStringSetting { name: "recovery_target_name", context: PGC_POSTMASTER, group: WAL_RECOVERY_TARGET, short_desc: Some("Sets the named restore point up to which recovery will proceed."), long_desc: None, flags: 0, variable: &vars::recovery_target_name_string, boot_val: GucDefaultValue::String(Some("")), check_hook: Some(&hooks::check_recovery_target_name), assign_hook: Some(&hooks::assign_recovery_target_name), show_hook: None },
    GucStringSetting { name: "recovery_target_lsn", context: PGC_POSTMASTER, group: WAL_RECOVERY_TARGET, short_desc: Some("Sets the LSN of the write-ahead log location up to which recovery will proceed."), long_desc: None, flags: 0, variable: &vars::recovery_target_lsn_string, boot_val: GucDefaultValue::String(Some("")), check_hook: Some(&hooks::check_recovery_target_lsn), assign_hook: Some(&hooks::assign_recovery_target_lsn), show_hook: None },
    GucStringSetting { name: "primary_conninfo", context: PGC_SIGHUP, group: REPLICATION_STANDBY, short_desc: Some("Sets the connection string to be used to connect to the sending server."), long_desc: None, flags: GUC_SUPERUSER_ONLY, variable: &vars::PrimaryConnInfo, boot_val: GucDefaultValue::String(Some("")), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "primary_slot_name", context: PGC_SIGHUP, group: REPLICATION_STANDBY, short_desc: Some("Sets the name of the replication slot to use on the sending server."), long_desc: None, flags: 0, variable: &vars::PrimarySlotName, boot_val: GucDefaultValue::String(Some("")), check_hook: Some(&hooks::check_primary_slot_name), assign_hook: None, show_hook: None },
    GucStringSetting { name: "client_encoding", context: PGC_USERSET, group: CLIENT_CONN_LOCALE, short_desc: Some("Sets the client's character set encoding."), long_desc: None, flags: GUC_IS_NAME | GUC_REPORT, variable: &vars::client_encoding_string, boot_val: GucDefaultValue::String(Some("SQL_ASCII")), check_hook: Some(&hooks::check_client_encoding), assign_hook: Some(&hooks::assign_client_encoding), show_hook: None },
    GucStringSetting { name: "log_line_prefix", context: PGC_SIGHUP, group: LOGGING_WHAT, short_desc: Some("Controls information prefixed to each log line."), long_desc: Some("An empty string means no prefix."), flags: 0, variable: &vars::Log_line_prefix, boot_val: GucDefaultValue::String(Some("%m [%p] ")), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "log_timezone", context: PGC_SIGHUP, group: LOGGING_WHAT, short_desc: Some("Sets the time zone to use in log messages."), long_desc: None, flags: 0, variable: &vars::log_timezone_string, boot_val: GucDefaultValue::String(Some("GMT")), check_hook: Some(&hooks::check_log_timezone), assign_hook: Some(&hooks::assign_log_timezone), show_hook: Some(&hooks::show_log_timezone) },
    GucStringSetting { name: "DateStyle", context: PGC_USERSET, group: CLIENT_CONN_LOCALE, short_desc: Some("Sets the display format for date and time values."), long_desc: Some("Also controls interpretation of ambiguous date inputs."), flags: GUC_LIST_INPUT | GUC_REPORT, variable: &vars::datestyle_string, boot_val: GucDefaultValue::String(Some("ISO, MDY")), check_hook: Some(&hooks::check_datestyle), assign_hook: Some(&hooks::assign_datestyle), show_hook: None },
    GucStringSetting { name: "default_table_access_method", context: PGC_USERSET, group: CLIENT_CONN_STATEMENT, short_desc: Some("Sets the default table access method for new tables."), long_desc: None, flags: GUC_IS_NAME, variable: &vars::default_table_access_method, boot_val: GucDefaultValue::String(Some("heap")), check_hook: Some(&hooks::check_default_table_access_method), assign_hook: None, show_hook: None },
    GucStringSetting { name: "default_tablespace", context: PGC_USERSET, group: CLIENT_CONN_STATEMENT, short_desc: Some("Sets the default tablespace to create tables and indexes in."), long_desc: Some("An empty string means use the database's default tablespace."), flags: GUC_IS_NAME, variable: &vars::default_tablespace, boot_val: GucDefaultValue::String(Some("")), check_hook: Some(&hooks::check_default_tablespace), assign_hook: None, show_hook: None },
    GucStringSetting { name: "temp_tablespaces", context: PGC_USERSET, group: CLIENT_CONN_STATEMENT, short_desc: Some("Sets the tablespace(s) to use for temporary tables and sort files."), long_desc: Some("An empty string means use the database's default tablespace."), flags: GUC_LIST_INPUT | GUC_LIST_QUOTE, variable: &vars::temp_tablespaces, boot_val: GucDefaultValue::String(Some("")), check_hook: Some(&hooks::check_temp_tablespaces), assign_hook: Some(&hooks::assign_temp_tablespaces), show_hook: None },
    GucStringSetting { name: "createrole_self_grant", context: PGC_USERSET, group: CLIENT_CONN_STATEMENT, short_desc: Some("Sets whether a CREATEROLE user automatically grants the role to themselves, and with which options."), long_desc: Some("An empty string disables automatic self grants."), flags: GUC_LIST_INPUT, variable: &vars::createrole_self_grant, boot_val: GucDefaultValue::String(Some("")), check_hook: Some(&hooks::check_createrole_self_grant), assign_hook: Some(&hooks::assign_createrole_self_grant), show_hook: None },
    GucStringSetting { name: "dynamic_library_path", context: PGC_SUSET, group: CLIENT_CONN_OTHER, short_desc: Some("Sets the path for dynamically loadable modules."), long_desc: Some("If a dynamically loadable module needs to be opened and the specified name does not have a directory component (i.e., the name does not contain a slash), the system will search this path for the specified file."), flags: GUC_SUPERUSER_ONLY, variable: &vars::Dynamic_library_path, boot_val: GucDefaultValue::String(Some("$libdir")), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "extension_control_path", context: PGC_SUSET, group: CLIENT_CONN_OTHER, short_desc: Some("Sets the path for extension control files."), long_desc: Some("The remaining extension script and secondary control files are then loaded from the same directory where the primary control file was found."), flags: GUC_SUPERUSER_ONLY, variable: &vars::Extension_control_path, boot_val: GucDefaultValue::String(Some("$system")), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "krb_server_keyfile", context: PGC_SIGHUP, group: CONN_AUTH_AUTH, short_desc: Some("Sets the location of the Kerberos server key file."), long_desc: None, flags: GUC_SUPERUSER_ONLY, variable: &vars::pg_krb_server_keyfile, boot_val: GucDefaultValue::String(Some("")), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "bonjour_name", context: PGC_POSTMASTER, group: CONN_AUTH_SETTINGS, short_desc: Some("Sets the Bonjour service name."), long_desc: Some("An empty string means use the computer name."), flags: 0, variable: &vars::bonjour_name, boot_val: GucDefaultValue::String(Some("")), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "lc_messages", context: PGC_SUSET, group: CLIENT_CONN_LOCALE, short_desc: Some("Sets the language in which messages are displayed."), long_desc: Some("An empty string means use the operating system setting."), flags: 0, variable: &vars::locale_messages, boot_val: GucDefaultValue::String(Some("")), check_hook: Some(&hooks::check_locale_messages), assign_hook: Some(&hooks::assign_locale_messages), show_hook: None },
    GucStringSetting { name: "lc_monetary", context: PGC_USERSET, group: CLIENT_CONN_LOCALE, short_desc: Some("Sets the locale for formatting monetary amounts."), long_desc: Some("An empty string means use the operating system setting."), flags: 0, variable: &vars::locale_monetary, boot_val: GucDefaultValue::String(Some("C")), check_hook: Some(&hooks::check_locale_monetary), assign_hook: Some(&hooks::assign_locale_monetary), show_hook: None },
    GucStringSetting { name: "lc_numeric", context: PGC_USERSET, group: CLIENT_CONN_LOCALE, short_desc: Some("Sets the locale for formatting numbers."), long_desc: Some("An empty string means use the operating system setting."), flags: 0, variable: &vars::locale_numeric, boot_val: GucDefaultValue::String(Some("C")), check_hook: Some(&hooks::check_locale_numeric), assign_hook: Some(&hooks::assign_locale_numeric), show_hook: None },
    GucStringSetting { name: "lc_time", context: PGC_USERSET, group: CLIENT_CONN_LOCALE, short_desc: Some("Sets the locale for formatting date and time values."), long_desc: Some("An empty string means use the operating system setting."), flags: 0, variable: &vars::locale_time, boot_val: GucDefaultValue::String(Some("C")), check_hook: Some(&hooks::check_locale_time), assign_hook: Some(&hooks::assign_locale_time), show_hook: None },
    GucStringSetting { name: "session_preload_libraries", context: PGC_SUSET, group: CLIENT_CONN_PRELOAD, short_desc: Some("Lists shared libraries to preload into each backend."), long_desc: None, flags: GUC_LIST_INPUT | GUC_LIST_QUOTE | GUC_SUPERUSER_ONLY, variable: &vars::session_preload_libraries_string, boot_val: GucDefaultValue::String(Some("")), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "shared_preload_libraries", context: PGC_POSTMASTER, group: CLIENT_CONN_PRELOAD, short_desc: Some("Lists shared libraries to preload into server."), long_desc: None, flags: GUC_LIST_INPUT | GUC_LIST_QUOTE | GUC_SUPERUSER_ONLY, variable: &vars::shared_preload_libraries_string, boot_val: GucDefaultValue::String(Some("")), check_hook: None, assign_hook: None, show_hook: None },
    // Compiled-in-contrib analog of shared_preload_libraries (hook-surface.md
    // section 6 open question 2): names in the dfmgr builtin-library registry
    // whose pg_init should run at boot, single-threaded, before any tap can
    // install (docs/design/hook-surface.md).
    GucStringSetting { name: "preload_contrib", context: PGC_POSTMASTER, group: CLIENT_CONN_PRELOAD, short_desc: Some("Lists compiled-in contrib modules to preload into server."), long_desc: None, flags: GUC_LIST_INPUT | GUC_LIST_QUOTE | GUC_SUPERUSER_ONLY, variable: &vars::preload_contrib_string, boot_val: GucDefaultValue::String(Some("")), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "local_preload_libraries", context: PGC_USERSET, group: CLIENT_CONN_PRELOAD, short_desc: Some("Lists unprivileged shared libraries to preload into each backend."), long_desc: None, flags: GUC_LIST_INPUT | GUC_LIST_QUOTE, variable: &vars::local_preload_libraries_string, boot_val: GucDefaultValue::String(Some("")), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "search_path", context: PGC_USERSET, group: CLIENT_CONN_STATEMENT, short_desc: Some("Sets the schema search order for names that are not schema-qualified."), long_desc: None, flags: GUC_LIST_INPUT | GUC_LIST_QUOTE | GUC_EXPLAIN | GUC_REPORT, variable: &vars::namespace_search_path, boot_val: GucDefaultValue::String(Some("\"$user\", public")), check_hook: Some(&hooks::check_search_path), assign_hook: Some(&hooks::assign_search_path), show_hook: None },
    GucStringSetting { name: "server_encoding", context: PGC_INTERNAL, group: PRESET_OPTIONS, short_desc: Some("Shows the server (database) character set encoding."), long_desc: None, flags: GUC_IS_NAME | GUC_REPORT | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE, variable: &vars::server_encoding_string, boot_val: GucDefaultValue::String(Some("SQL_ASCII")), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "server_version", context: PGC_INTERNAL, group: PRESET_OPTIONS, short_desc: Some("Shows the server version."), long_desc: None, flags: GUC_REPORT | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE, variable: &vars::server_version_string, boot_val: GucDefaultValue::String(Some("18.3")), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "role", context: PGC_USERSET, group: UNGROUPED, short_desc: Some("Sets the current role."), long_desc: None, flags: GUC_IS_NAME | GUC_NO_SHOW_ALL | GUC_NO_RESET_ALL | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE | GUC_NOT_WHILE_SEC_REST, variable: &vars::role_string, boot_val: GucDefaultValue::String(Some("none")), check_hook: Some(&hooks::check_role), assign_hook: Some(&hooks::assign_role), show_hook: Some(&hooks::show_role) },
    GucStringSetting { name: "session_authorization", context: PGC_USERSET, group: UNGROUPED, short_desc: Some("Sets the session user name."), long_desc: None, flags: GUC_IS_NAME | GUC_REPORT | GUC_NO_SHOW_ALL | GUC_NO_RESET_ALL | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE | GUC_NOT_WHILE_SEC_REST, variable: &vars::session_authorization_string, boot_val: GucDefaultValue::String(None), check_hook: Some(&hooks::check_session_authorization), assign_hook: Some(&hooks::assign_session_authorization), show_hook: None },
    GucStringSetting { name: "log_destination", context: PGC_SIGHUP, group: LOGGING_WHERE, short_desc: Some("Sets the destination for server log output."), long_desc: Some("Valid values are combinations of \"stderr\", \"syslog\", \"csvlog\", \"jsonlog\", and \"eventlog\", depending on the platform."), flags: GUC_LIST_INPUT, variable: &vars::Log_destination_string, boot_val: GucDefaultValue::String(Some("stderr")), check_hook: Some(&hooks::check_log_destination), assign_hook: Some(&hooks::assign_log_destination), show_hook: None },
    GucStringSetting { name: "log_directory", context: PGC_SIGHUP, group: LOGGING_WHERE, short_desc: Some("Sets the destination directory for log files."), long_desc: Some("Can be specified as relative to the data directory or as absolute path."), flags: GUC_SUPERUSER_ONLY, variable: &vars::Log_directory, boot_val: GucDefaultValue::String(Some("log")), check_hook: Some(&hooks::check_canonical_path), assign_hook: None, show_hook: None },
    GucStringSetting { name: "log_filename", context: PGC_SIGHUP, group: LOGGING_WHERE, short_desc: Some("Sets the file name pattern for log files."), long_desc: None, flags: GUC_SUPERUSER_ONLY, variable: &vars::Log_filename, boot_val: GucDefaultValue::String(Some("postgresql-%Y-%m-%d_%H%M%S.log")), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "syslog_ident", context: PGC_SIGHUP, group: LOGGING_WHERE, short_desc: Some("Sets the program name used to identify PostgreSQL messages in syslog."), long_desc: None, flags: 0, variable: &vars::syslog_ident_str, boot_val: GucDefaultValue::String(Some("postgres")), check_hook: None, assign_hook: Some(&hooks::assign_syslog_ident), show_hook: None },
    GucStringSetting { name: "event_source", context: PGC_POSTMASTER, group: LOGGING_WHERE, short_desc: Some("Sets the application name used to identify PostgreSQL messages in the event log."), long_desc: None, flags: 0, variable: &vars::event_source, boot_val: GucDefaultValue::String(Some("PostgreSQL")), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "TimeZone", context: PGC_USERSET, group: CLIENT_CONN_LOCALE, short_desc: Some("Sets the time zone for displaying and interpreting time stamps."), long_desc: None, flags: GUC_REPORT, variable: &vars::timezone_string, boot_val: GucDefaultValue::String(Some("GMT")), check_hook: Some(&hooks::check_timezone), assign_hook: Some(&hooks::assign_timezone), show_hook: Some(&hooks::show_timezone) },
    GucStringSetting { name: "timezone_abbreviations", context: PGC_USERSET, group: CLIENT_CONN_LOCALE, short_desc: Some("Selects a file of time zone abbreviations."), long_desc: None, flags: 0, variable: &vars::timezone_abbreviations_string, boot_val: GucDefaultValue::String(None), check_hook: Some(&hooks::check_timezone_abbreviations), assign_hook: Some(&hooks::assign_timezone_abbreviations), show_hook: None },
    GucStringSetting { name: "unix_socket_group", context: PGC_POSTMASTER, group: CONN_AUTH_SETTINGS, short_desc: Some("Sets the owning group of the Unix-domain socket."), long_desc: Some("The owning user of the socket is always the user that starts the server. An empty string means use the user's default group."), flags: 0, variable: &vars::Unix_socket_group, boot_val: GucDefaultValue::String(Some("")), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "unix_socket_directories", context: PGC_POSTMASTER, group: CONN_AUTH_SETTINGS, short_desc: Some("Sets the directories where Unix-domain sockets will be created."), long_desc: None, flags: GUC_LIST_INPUT | GUC_LIST_QUOTE | GUC_SUPERUSER_ONLY, variable: &vars::Unix_socket_directories, boot_val: GucDefaultValue::String(Some("/tmp")), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "listen_addresses", context: PGC_POSTMASTER, group: CONN_AUTH_SETTINGS, short_desc: Some("Sets the host name or IP address(es) to listen to."), long_desc: None, flags: GUC_LIST_INPUT, variable: &vars::ListenAddresses, boot_val: GucDefaultValue::String(Some("localhost")), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "data_directory", context: PGC_POSTMASTER, group: FILE_LOCATIONS, short_desc: Some("Sets the server's data directory."), long_desc: None, flags: GUC_SUPERUSER_ONLY | GUC_DISALLOW_IN_AUTO_FILE, variable: &vars::data_directory, boot_val: GucDefaultValue::String(None), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "config_file", context: PGC_POSTMASTER, group: FILE_LOCATIONS, short_desc: Some("Sets the server's main configuration file."), long_desc: None, flags: GUC_DISALLOW_IN_FILE | GUC_SUPERUSER_ONLY, variable: &vars::ConfigFileName, boot_val: GucDefaultValue::String(None), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "hba_file", context: PGC_POSTMASTER, group: FILE_LOCATIONS, short_desc: Some("Sets the server's \"hba\" configuration file."), long_desc: None, flags: GUC_SUPERUSER_ONLY, variable: &vars::HbaFileName, boot_val: GucDefaultValue::String(None), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "ident_file", context: PGC_POSTMASTER, group: FILE_LOCATIONS, short_desc: Some("Sets the server's \"ident\" configuration file."), long_desc: None, flags: GUC_SUPERUSER_ONLY, variable: &vars::IdentFileName, boot_val: GucDefaultValue::String(None), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "external_pid_file", context: PGC_POSTMASTER, group: FILE_LOCATIONS, short_desc: Some("Writes the postmaster PID to the specified file."), long_desc: None, flags: GUC_SUPERUSER_ONLY, variable: &vars::external_pid_file, boot_val: GucDefaultValue::String(None), check_hook: Some(&hooks::check_canonical_path), assign_hook: None, show_hook: None },
    GucStringSetting { name: "ssl_library", context: PGC_INTERNAL, group: PRESET_OPTIONS, short_desc: Some("Shows the name of the SSL library."), long_desc: None, flags: GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE, variable: &vars::ssl_library, boot_val: GucDefaultValue::String(Some("OpenSSL")), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "ssl_cert_file", context: PGC_SIGHUP, group: CONN_AUTH_SSL, short_desc: Some("Location of the SSL server certificate file."), long_desc: None, flags: 0, variable: &vars::ssl_cert_file, boot_val: GucDefaultValue::String(Some("server.crt")), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "ssl_key_file", context: PGC_SIGHUP, group: CONN_AUTH_SSL, short_desc: Some("Location of the SSL server private key file."), long_desc: None, flags: 0, variable: &vars::ssl_key_file, boot_val: GucDefaultValue::String(Some("server.key")), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "ssl_ca_file", context: PGC_SIGHUP, group: CONN_AUTH_SSL, short_desc: Some("Location of the SSL certificate authority file."), long_desc: None, flags: 0, variable: &vars::ssl_ca_file, boot_val: GucDefaultValue::String(Some("")), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "ssl_crl_file", context: PGC_SIGHUP, group: CONN_AUTH_SSL, short_desc: Some("Location of the SSL certificate revocation list file."), long_desc: None, flags: 0, variable: &vars::ssl_crl_file, boot_val: GucDefaultValue::String(Some("")), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "ssl_crl_dir", context: PGC_SIGHUP, group: CONN_AUTH_SSL, short_desc: Some("Location of the SSL certificate revocation list directory."), long_desc: None, flags: 0, variable: &vars::ssl_crl_dir, boot_val: GucDefaultValue::String(Some("")), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "synchronous_standby_names", context: PGC_SIGHUP, group: REPLICATION_PRIMARY, short_desc: Some("Number of synchronous standbys and list of names of potential synchronous ones."), long_desc: None, flags: GUC_LIST_INPUT, variable: &vars::SyncRepStandbyNames, boot_val: GucDefaultValue::String(Some("")), check_hook: Some(&hooks::check_synchronous_standby_names), assign_hook: Some(&hooks::assign_synchronous_standby_names), show_hook: None },
    GucStringSetting { name: "default_text_search_config", context: PGC_USERSET, group: CLIENT_CONN_LOCALE, short_desc: Some("Sets default text search configuration."), long_desc: None, flags: 0, variable: &vars::TSCurrentConfig, boot_val: GucDefaultValue::String(Some("pg_catalog.simple")), check_hook: Some(&hooks::check_default_text_search_config), assign_hook: Some(&hooks::assign_default_text_search_config), show_hook: None },
    GucStringSetting { name: "ssl_tls13_ciphers", context: PGC_SIGHUP, group: CONN_AUTH_SSL, short_desc: Some("Sets the list of allowed TLSv1.3 cipher suites."), long_desc: Some("An empty string means use the default cipher suites."), flags: GUC_SUPERUSER_ONLY, variable: &vars::SSLCipherSuites, boot_val: GucDefaultValue::String(Some("")), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "ssl_ciphers", context: PGC_SIGHUP, group: CONN_AUTH_SSL, short_desc: Some("Sets the list of allowed TLSv1.2 (and lower) ciphers."), long_desc: None, flags: GUC_SUPERUSER_ONLY, variable: &vars::SSLCipherList, boot_val: GucDefaultValue::String(Some("HIGH:MEDIUM:+3DES:!aNULL")), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "ssl_groups", context: PGC_SIGHUP, group: CONN_AUTH_SSL, short_desc: Some("Sets the group(s) to use for Diffie-Hellman key exchange."), long_desc: Some("Multiple groups can be specified using a colon-separated list."), flags: GUC_SUPERUSER_ONLY, variable: &vars::SSLECDHCurve, boot_val: GucDefaultValue::String(Some("X25519:prime256v1")), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "ssl_dh_params_file", context: PGC_SIGHUP, group: CONN_AUTH_SSL, short_desc: Some("Location of the SSL DH parameters file."), long_desc: Some("An empty string means use compiled-in default parameters."), flags: GUC_SUPERUSER_ONLY, variable: &vars::ssl_dh_params_file, boot_val: GucDefaultValue::String(Some("")), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "ssl_passphrase_command", context: PGC_SIGHUP, group: CONN_AUTH_SSL, short_desc: Some("Command to obtain passphrases for SSL."), long_desc: Some("An empty string means use the built-in prompting mechanism."), flags: GUC_SUPERUSER_ONLY, variable: &vars::ssl_passphrase_command, boot_val: GucDefaultValue::String(Some("")), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "application_name", context: PGC_USERSET, group: LOGGING_WHAT, short_desc: Some("Sets the application name to be reported in statistics and logs."), long_desc: None, flags: GUC_IS_NAME | GUC_REPORT | GUC_NOT_IN_SAMPLE, variable: &vars::application_name, boot_val: GucDefaultValue::String(Some("")), check_hook: Some(&hooks::check_application_name), assign_hook: Some(&hooks::assign_application_name), show_hook: None },
    GucStringSetting { name: "cluster_name", context: PGC_POSTMASTER, group: PROCESS_TITLE, short_desc: Some("Sets the name of the cluster, which is included in the process title."), long_desc: None, flags: GUC_IS_NAME, variable: &vars::cluster_name, boot_val: GucDefaultValue::String(Some("")), check_hook: Some(&hooks::check_cluster_name), assign_hook: None, show_hook: None },
    GucStringSetting { name: "wal_consistency_checking", context: PGC_SUSET, group: DEVELOPER_OPTIONS, short_desc: Some("Sets the WAL resource managers for which WAL consistency checks are done."), long_desc: Some("Full-page images will be logged for all data blocks and cross-checked against the results of WAL replay."), flags: GUC_LIST_INPUT | GUC_NOT_IN_SAMPLE, variable: &vars::wal_consistency_checking_string, boot_val: GucDefaultValue::String(Some("")), check_hook: Some(&hooks::check_wal_consistency_checking), assign_hook: Some(&hooks::assign_wal_consistency_checking), show_hook: None },
    GucStringSetting { name: "jit_provider", context: PGC_POSTMASTER, group: CLIENT_CONN_PRELOAD, short_desc: Some("JIT provider to use."), long_desc: None, flags: GUC_SUPERUSER_ONLY, variable: &vars::jit_provider, boot_val: GucDefaultValue::String(Some("llvmjit")), check_hook: None, assign_hook: None, show_hook: None },
    GucStringSetting { name: "backtrace_functions", context: PGC_SUSET, group: DEVELOPER_OPTIONS, short_desc: Some("Log backtrace for errors in these functions."), long_desc: None, flags: GUC_NOT_IN_SAMPLE, variable: &vars::backtrace_functions, boot_val: GucDefaultValue::String(Some("")), check_hook: Some(&hooks::check_backtrace_functions), assign_hook: Some(&hooks::assign_backtrace_functions), show_hook: None },
    GucStringSetting { name: "debug_io_direct", context: PGC_POSTMASTER, group: DEVELOPER_OPTIONS, short_desc: Some("Use direct I/O for file access."), long_desc: Some("An empty string disables direct I/O."), flags: GUC_LIST_INPUT | GUC_NOT_IN_SAMPLE, variable: &vars::debug_io_direct_string, boot_val: GucDefaultValue::String(Some("")), check_hook: Some(&hooks::check_debug_io_direct), assign_hook: Some(&hooks::assign_debug_io_direct), show_hook: None },
    GucStringSetting { name: "synchronized_standby_slots", context: PGC_SIGHUP, group: REPLICATION_PRIMARY, short_desc: Some("Lists streaming replication standby server replication slot names that logical WAL sender processes will wait for."), long_desc: Some("Logical WAL sender processes will send decoded changes to output plugins only after the specified replication slots have confirmed receiving WAL."), flags: GUC_LIST_INPUT, variable: &vars::synchronized_standby_slots, boot_val: GucDefaultValue::String(Some("")), check_hook: Some(&hooks::check_synchronized_standby_slots), assign_hook: Some(&hooks::assign_synchronized_standby_slots), show_hook: None },
    GucStringSetting { name: "restrict_nonsystem_relation_kind", context: PGC_USERSET, group: CLIENT_CONN_STATEMENT, short_desc: Some("Prohibits access to non-system relations of specified kinds."), long_desc: None, flags: GUC_LIST_INPUT | GUC_NOT_IN_SAMPLE, variable: &vars::restrict_nonsystem_relation_kind_string, boot_val: GucDefaultValue::String(Some("")), check_hook: Some(&hooks::check_restrict_nonsystem_relation_kind), assign_hook: Some(&hooks::assign_restrict_nonsystem_relation_kind), show_hook: None },
    GucStringSetting { name: "oauth_validator_libraries", context: PGC_SIGHUP, group: CONN_AUTH_AUTH, short_desc: Some("Lists libraries that may be called to validate OAuth v2 bearer tokens."), long_desc: None, flags: GUC_LIST_INPUT | GUC_LIST_QUOTE | GUC_SUPERUSER_ONLY, variable: &vars::oauth_validator_libraries_string, boot_val: GucDefaultValue::String(Some("")), check_hook: None, assign_hook: None, show_hook: None },
    // pgrust-only (no C counterpart): read-only computed channel for the
    // simharness F8 resource-baseline hook (`SHOW pgrust.resource_counters`).
    // PGC_INTERNAL: SET is refused; the value comes from the show hook (the
    // fd crate reports above-VFD-cache counters — allocated transient descs,
    // the allocated-desc cap, max_safe_fds, max_files_per_process).
    GucStringSetting { name: "pgrust.resource_counters", context: PGC_INTERNAL, group: DEVELOPER_OPTIONS, short_desc: Some("Shows per-backend fd-class resource counters (test-harness channel)."), long_desc: None, flags: GUC_NOT_IN_SAMPLE | GUC_NO_SHOW_ALL | GUC_DISALLOW_IN_FILE, variable: &vars::pgrust_resource_counters, boot_val: GucDefaultValue::String(Some("")), check_hook: None, assign_hook: None, show_hook: Some(&hooks::show_resource_counters) },
    GucStringSetting { name: "log_connections", context: PGC_SU_BACKEND, group: LOGGING_WHAT, short_desc: Some("Logs specified aspects of connection establishment and setup."), long_desc: None, flags: GUC_LIST_INPUT, variable: &vars::log_connections_string, boot_val: GucDefaultValue::String(Some("")), check_hook: Some(&hooks::check_log_connections), assign_hook: Some(&hooks::assign_log_connections), show_hook: None },
];

pub static ConfigureNamesEnum: &[GucEnumSetting] = &[
    GucEnumSetting { name: "backslash_quote", context: PGC_USERSET, group: COMPAT_OPTIONS_PREVIOUS, short_desc: Some("Sets whether \"\\'\" is allowed in string literals."), long_desc: None, flags: 0, variable: &vars::backslash_quote, boot_val: GucDefaultValue::Enum(BACKSLASH_QUOTE_SAFE_ENCODING), options: GucEnumOptions::Inline(backslash_quote_options), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "bytea_output", context: PGC_USERSET, group: CLIENT_CONN_STATEMENT, short_desc: Some("Sets the output format for bytea."), long_desc: None, flags: 0, variable: &vars::bytea_output, boot_val: GucDefaultValue::Enum(BYTEA_OUTPUT_HEX), options: GucEnumOptions::Inline(bytea_output_options), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "client_min_messages", context: PGC_USERSET, group: CLIENT_CONN_STATEMENT, short_desc: Some("Sets the message levels that are sent to the client."), long_desc: Some("Each level includes all the levels that follow it. The later the level, the fewer messages are sent."), flags: 0, variable: &vars::client_min_messages, boot_val: GucDefaultValue::Enum(NOTICE), options: GucEnumOptions::Inline(client_message_level_options), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "compute_query_id", context: PGC_SUSET, group: STATS_MONITORING, short_desc: Some("Enables in-core computation of query identifiers."), long_desc: None, flags: 0, variable: &vars::compute_query_id, boot_val: GucDefaultValue::Enum(COMPUTE_QUERY_ID_AUTO), options: GucEnumOptions::Inline(compute_query_id_options), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "auto_explain.log_format", context: PGC_SUSET, group: CUSTOM_OPTIONS, short_desc: Some("EXPLAIN format to be used for plan logging."), long_desc: None, flags: 0, variable: &vars::aex_log_format, boot_val: GucDefaultValue::Enum(EXPLAIN_FORMAT_TEXT), options: GucEnumOptions::Inline(auto_explain_format_options), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "auto_explain.log_level", context: PGC_SUSET, group: CUSTOM_OPTIONS, short_desc: Some("Log level for the plan."), long_desc: None, flags: 0, variable: &vars::aex_log_level, boot_val: GucDefaultValue::Enum(LOG), options: GucEnumOptions::Inline(auto_explain_loglevel_options), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "pg_stat_statements.track", context: PGC_SUSET, group: CUSTOM_OPTIONS, short_desc: Some("Selects which statements are tracked by pg_stat_statements."), long_desc: None, flags: 0, variable: &vars::pgss_track, boot_val: GucDefaultValue::Enum(PGSS_TRACK_TOP), options: GucEnumOptions::Inline(pgss_track_options), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "constraint_exclusion", context: PGC_USERSET, group: QUERY_TUNING_OTHER, short_desc: Some("Enables the planner to use constraints to optimize queries."), long_desc: Some("Table scans will be skipped if their constraints guarantee that no rows match the query."), flags: GUC_EXPLAIN, variable: &vars::constraint_exclusion, boot_val: GucDefaultValue::Enum(CONSTRAINT_EXCLUSION_PARTITION), options: GucEnumOptions::Inline(constraint_exclusion_options), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "default_toast_compression", context: PGC_USERSET, group: CLIENT_CONN_STATEMENT, short_desc: Some("Sets the default compression method for compressible values."), long_desc: None, flags: 0, variable: &vars::default_toast_compression, boot_val: GucDefaultValue::Enum(TOAST_PGLZ_COMPRESSION), options: GucEnumOptions::Inline(default_toast_compression_options), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "default_transaction_isolation", context: PGC_USERSET, group: CLIENT_CONN_STATEMENT, short_desc: Some("Sets the transaction isolation level of each new transaction."), long_desc: None, flags: 0, variable: &vars::DefaultXactIsoLevel, boot_val: GucDefaultValue::Enum(XACT_READ_COMMITTED), options: GucEnumOptions::Inline(isolation_level_options), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "transaction_isolation", context: PGC_USERSET, group: CLIENT_CONN_STATEMENT, short_desc: Some("Sets the current transaction's isolation level."), long_desc: None, flags: GUC_NO_RESET | GUC_NO_RESET_ALL | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE, variable: &vars::XactIsoLevel, boot_val: GucDefaultValue::Enum(XACT_READ_COMMITTED), options: GucEnumOptions::Inline(isolation_level_options), check_hook: Some(&hooks::check_transaction_isolation), assign_hook: None, show_hook: None },
    GucEnumSetting { name: "IntervalStyle", context: PGC_USERSET, group: CLIENT_CONN_LOCALE, short_desc: Some("Sets the display format for interval values."), long_desc: None, flags: GUC_REPORT, variable: &vars::IntervalStyle, boot_val: GucDefaultValue::Enum(INTSTYLE_POSTGRES), options: GucEnumOptions::Inline(intervalstyle_options), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "icu_validation_level", context: PGC_USERSET, group: CLIENT_CONN_LOCALE, short_desc: Some("Log level for reporting invalid ICU locale strings."), long_desc: None, flags: 0, variable: &vars::icu_validation_level, boot_val: GucDefaultValue::Enum(WARNING), options: GucEnumOptions::Inline(icu_validation_level_options), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "log_error_verbosity", context: PGC_SUSET, group: LOGGING_WHAT, short_desc: Some("Sets the verbosity of logged messages."), long_desc: None, flags: 0, variable: &vars::Log_error_verbosity, boot_val: GucDefaultValue::Enum(PGERROR_DEFAULT), options: GucEnumOptions::Inline(log_error_verbosity_options), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "log_min_messages", context: PGC_SUSET, group: LOGGING_WHEN, short_desc: Some("Sets the message levels that are logged."), long_desc: Some("Each level includes all the levels that follow it. The later the level, the fewer messages are sent."), flags: 0, variable: &vars::log_min_messages, boot_val: GucDefaultValue::Enum(WARNING), options: GucEnumOptions::Inline(server_message_level_options), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "log_min_error_statement", context: PGC_SUSET, group: LOGGING_WHEN, short_desc: Some("Causes all statements generating error at or above this level to be logged."), long_desc: Some("Each level includes all the levels that follow it. The later the level, the fewer messages are sent."), flags: 0, variable: &vars::log_min_error_statement, boot_val: GucDefaultValue::Enum(ERROR), options: GucEnumOptions::Inline(server_message_level_options), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "log_statement", context: PGC_SUSET, group: LOGGING_WHAT, short_desc: Some("Sets the type of statements logged."), long_desc: None, flags: 0, variable: &vars::log_statement, boot_val: GucDefaultValue::Enum(LOGSTMT_NONE), options: GucEnumOptions::Inline(log_statement_options), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "syslog_facility", context: PGC_SIGHUP, group: LOGGING_WHERE, short_desc: Some("Sets the syslog \"facility\" to be used when syslog enabled."), long_desc: None, flags: 0, variable: &vars::syslog_facility, boot_val: GucDefaultValue::Enum(DEFAULT_SYSLOG_FACILITY), options: GucEnumOptions::Inline(syslog_facility_options), check_hook: None, assign_hook: Some(&hooks::assign_syslog_facility), show_hook: None },
    GucEnumSetting { name: "session_replication_role", context: PGC_SUSET, group: CLIENT_CONN_STATEMENT, short_desc: Some("Sets the session's behavior for triggers and rewrite rules."), long_desc: None, flags: 0, variable: &vars::SessionReplicationRole, boot_val: GucDefaultValue::Enum(SESSION_REPLICATION_ROLE_ORIGIN), options: GucEnumOptions::Inline(session_replication_role_options), check_hook: None, assign_hook: Some(&hooks::assign_session_replication_role), show_hook: None },
    GucEnumSetting { name: "synchronous_commit", context: PGC_USERSET, group: WAL_SETTINGS, short_desc: Some("Sets the current transaction's synchronization level."), long_desc: None, flags: 0, variable: &vars::synchronous_commit, boot_val: GucDefaultValue::Enum(SYNCHRONOUS_COMMIT_REMOTE_FLUSH), options: GucEnumOptions::Inline(synchronous_commit_options), check_hook: None, assign_hook: Some(&hooks::assign_synchronous_commit), show_hook: None },
    GucEnumSetting { name: "archive_mode", context: PGC_POSTMASTER, group: WAL_ARCHIVING, short_desc: Some("Allows archiving of WAL files using \"archive_command\"."), long_desc: None, flags: 0, variable: &vars::XLogArchiveMode, boot_val: GucDefaultValue::Enum(ARCHIVE_MODE_OFF), options: GucEnumOptions::External(&option_sets::archive_mode_options), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "recovery_target_action", context: PGC_POSTMASTER, group: WAL_RECOVERY_TARGET, short_desc: Some("Sets the action to perform upon reaching the recovery target."), long_desc: None, flags: 0, variable: &vars::recoveryTargetAction, boot_val: GucDefaultValue::Enum(RECOVERY_TARGET_ACTION_PAUSE), options: GucEnumOptions::External(&option_sets::recovery_target_action_options), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "track_functions", context: PGC_SUSET, group: STATS_CUMULATIVE, short_desc: Some("Collects function-level statistics on database activity."), long_desc: None, flags: 0, variable: &vars::pgstat_track_functions, boot_val: GucDefaultValue::Enum(TRACK_FUNC_OFF), options: GucEnumOptions::Inline(track_function_options), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "stats_fetch_consistency", context: PGC_USERSET, group: STATS_CUMULATIVE, short_desc: Some("Sets the consistency of accesses to statistics data."), long_desc: None, flags: 0, variable: &vars::pgstat_fetch_consistency, boot_val: GucDefaultValue::Enum(PGSTAT_FETCH_CONSISTENCY_CACHE), options: GucEnumOptions::Inline(stats_fetch_consistency), check_hook: None, assign_hook: Some(&hooks::assign_stats_fetch_consistency), show_hook: None },
    GucEnumSetting { name: "wal_compression", context: PGC_SUSET, group: WAL_SETTINGS, short_desc: Some("Compresses full-page writes written in WAL file with specified method."), long_desc: None, flags: 0, variable: &vars::wal_compression, boot_val: GucDefaultValue::Enum(WAL_COMPRESSION_NONE), options: GucEnumOptions::Inline(wal_compression_options), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "wal_level", context: PGC_POSTMASTER, group: WAL_SETTINGS, short_desc: Some("Sets the level of information written to the WAL."), long_desc: None, flags: 0, variable: &vars::wal_level, boot_val: GucDefaultValue::Enum(WAL_LEVEL_REPLICA), options: GucEnumOptions::External(&option_sets::wal_level_options), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "dynamic_shared_memory_type", context: PGC_POSTMASTER, group: RESOURCES_MEM, short_desc: Some("Selects the dynamic shared memory implementation used."), long_desc: None, flags: 0, variable: &vars::dynamic_shared_memory_type, boot_val: GucDefaultValue::Enum(DEFAULT_DYNAMIC_SHARED_MEMORY_TYPE), options: GucEnumOptions::External(&option_sets::dynamic_shared_memory_options), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "shared_memory_type", context: PGC_POSTMASTER, group: RESOURCES_MEM, short_desc: Some("Selects the shared memory implementation used for the main shared memory region."), long_desc: None, flags: 0, variable: &vars::shared_memory_type, boot_val: GucDefaultValue::Enum(SHMEM_TYPE_MMAP), options: GucEnumOptions::Inline(shared_memory_options), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "file_copy_method", context: PGC_USERSET, group: RESOURCES_DISK, short_desc: Some("Selects the file copy method."), long_desc: None, flags: 0, variable: &vars::file_copy_method, boot_val: GucDefaultValue::Enum(FILE_COPY_METHOD_COPY), options: GucEnumOptions::Inline(file_copy_method_options), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "file_extend_method", context: PGC_SIGHUP, group: RESOURCES_DISK, short_desc: Some("Selects the method used for extending data files."), long_desc: None, flags: 0, variable: &vars::file_extend_method, boot_val: GucDefaultValue::Enum(DEFAULT_FILE_EXTEND_METHOD), options: GucEnumOptions::Inline(file_extend_method_options), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "wal_sync_method", context: PGC_SIGHUP, group: WAL_SETTINGS, short_desc: Some("Selects the method used for forcing WAL updates to disk."), long_desc: None, flags: 0, variable: &vars::wal_sync_method, boot_val: GucDefaultValue::Enum(DEFAULT_WAL_SYNC_METHOD), options: GucEnumOptions::External(&option_sets::wal_sync_method_options), check_hook: None, assign_hook: Some(&hooks::assign_wal_sync_method), show_hook: None },
    GucEnumSetting { name: "xmlbinary", context: PGC_USERSET, group: CLIENT_CONN_STATEMENT, short_desc: Some("Sets how binary values are to be encoded in XML."), long_desc: None, flags: 0, variable: &vars::xmlbinary, boot_val: GucDefaultValue::Enum(XMLBINARY_BASE64), options: GucEnumOptions::Inline(xmlbinary_options), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "xmloption", context: PGC_USERSET, group: CLIENT_CONN_STATEMENT, short_desc: Some("Sets whether XML data in implicit parsing and serialization operations is to be considered as documents or content fragments."), long_desc: None, flags: 0, variable: &vars::xmloption, boot_val: GucDefaultValue::Enum(XMLOPTION_CONTENT), options: GucEnumOptions::Inline(xmloption_options), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "huge_pages", context: PGC_POSTMASTER, group: RESOURCES_MEM, short_desc: Some("Use of huge pages on Linux or Windows."), long_desc: None, flags: 0, variable: &vars::huge_pages, boot_val: GucDefaultValue::Enum(HUGE_PAGES_TRY), options: GucEnumOptions::Inline(huge_pages_options), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "huge_pages_status", context: PGC_INTERNAL, group: PRESET_OPTIONS, short_desc: Some("Indicates the status of huge pages."), long_desc: None, flags: GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE, variable: &vars::huge_pages_status, boot_val: GucDefaultValue::Enum(HUGE_PAGES_UNKNOWN), options: GucEnumOptions::Inline(huge_pages_status_options), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "recovery_prefetch", context: PGC_SIGHUP, group: WAL_RECOVERY, short_desc: Some("Prefetch referenced blocks during recovery."), long_desc: Some("Look ahead in the WAL to find references to uncached data."), flags: 0, variable: &vars::recovery_prefetch, boot_val: GucDefaultValue::Enum(RECOVERY_PREFETCH_TRY), options: GucEnumOptions::Inline(recovery_prefetch_options), check_hook: Some(&hooks::check_recovery_prefetch), assign_hook: Some(&hooks::assign_recovery_prefetch), show_hook: None },
    GucEnumSetting { name: "debug_parallel_query", context: PGC_USERSET, group: DEVELOPER_OPTIONS, short_desc: Some("Forces the planner's use parallel query nodes."), long_desc: Some("This can be useful for testing the parallel query infrastructure by forcing the planner to generate plans that contain nodes that perform tuple communication between workers and the main process."), flags: GUC_NOT_IN_SAMPLE | GUC_EXPLAIN, variable: &vars::debug_parallel_query, boot_val: GucDefaultValue::Enum(DEBUG_PARALLEL_OFF), options: GucEnumOptions::Inline(debug_parallel_query_options), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "password_encryption", context: PGC_USERSET, group: CONN_AUTH_AUTH, short_desc: Some("Chooses the algorithm for encrypting passwords."), long_desc: None, flags: 0, variable: &vars::Password_encryption, boot_val: GucDefaultValue::Enum(PASSWORD_TYPE_SCRAM_SHA_256), options: GucEnumOptions::Inline(password_encryption_options), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "plan_cache_mode", context: PGC_USERSET, group: QUERY_TUNING_OTHER, short_desc: Some("Controls the planner's selection of custom or generic plan."), long_desc: Some("Prepared statements can have custom and generic plans, and the planner will attempt to choose which is better.  This can be set to override the default behavior."), flags: GUC_EXPLAIN, variable: &vars::plan_cache_mode, boot_val: GucDefaultValue::Enum(PLAN_CACHE_MODE_AUTO), options: GucEnumOptions::Inline(plan_cache_mode_options), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "ssl_min_protocol_version", context: PGC_SIGHUP, group: CONN_AUTH_SSL, short_desc: Some("Sets the minimum SSL/TLS protocol version to use."), long_desc: None, flags: GUC_SUPERUSER_ONLY, variable: &vars::ssl_min_protocol_version, boot_val: GucDefaultValue::Enum(PG_TLS1_2_VERSION), options: GucEnumOptions::Inline(ssl_protocol_versions_info_without_any), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "ssl_max_protocol_version", context: PGC_SIGHUP, group: CONN_AUTH_SSL, short_desc: Some("Sets the maximum SSL/TLS protocol version to use."), long_desc: None, flags: GUC_SUPERUSER_ONLY, variable: &vars::ssl_max_protocol_version, boot_val: GucDefaultValue::Enum(PG_TLS_ANY), options: GucEnumOptions::Inline(ssl_protocol_versions_info), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "recovery_init_sync_method", context: PGC_SIGHUP, group: ERROR_HANDLING_OPTIONS, short_desc: Some("Sets the method for synchronizing the data directory before crash recovery."), long_desc: None, flags: 0, variable: &vars::recovery_init_sync_method, boot_val: GucDefaultValue::Enum(DATA_DIR_SYNC_METHOD_FSYNC), options: GucEnumOptions::Inline(recovery_init_sync_method_options), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "debug_logical_replication_streaming", context: PGC_USERSET, group: DEVELOPER_OPTIONS, short_desc: Some("Forces immediate streaming or serialization of changes in large transactions."), long_desc: Some("On the publisher, it allows streaming or serializing each change in logical decoding. On the subscriber, it allows serialization of all changes to files and notifies the parallel apply workers to read and apply them at the end of the transaction."), flags: GUC_NOT_IN_SAMPLE, variable: &vars::debug_logical_replication_streaming, boot_val: GucDefaultValue::Enum(DEBUG_LOGICAL_REP_STREAMING_BUFFERED), options: GucEnumOptions::Inline(debug_logical_replication_streaming_options), check_hook: None, assign_hook: None, show_hook: None },
    GucEnumSetting { name: "regex_engine", context: PGC_USERSET, group: DEVELOPER_OPTIONS, short_desc: Some("Selects the regexp engine: auto dispatches compatible patterns to RE2, the rest to Spencer."), long_desc: None, flags: GUC_NOT_IN_SAMPLE | GUC_NO_SHOW_ALL, variable: &vars::regex_engine, boot_val: GucDefaultValue::Enum(REGEX_ENGINE_AUTO), options: GucEnumOptions::Inline(regex_engine_options), check_hook: None, assign_hook: None, show_hook: None },
    // pgrust.parallel_engine (M5-0, docs/design/m5-planner.md §2.2): the
    // product parallel-engine selector. legacy (default) = today's ported
    // Gather machinery byte-for-byte, runtime arms only via the per-arm bench
    // pool GUCs (which layer BENEATH this switch and are never affected by
    // it); runtime = the M5 unified admission router owns plan-shape routing.
    // Visible row (the condition_cache precedent): a product surface, not a
    // debug toggle.
    GucEnumSetting { name: "pgrust.parallel_engine", context: PGC_USERSET, group: CUSTOM_OPTIONS, short_desc: Some("Selects the parallel query engine: legacy Gather machinery or the morsel runtime router."), long_desc: None, flags: 0, variable: &vars::pgrust_parallel_engine, boot_val: GucDefaultValue::Enum(PARALLEL_ENGINE_RUNTIME), options: GucEnumOptions::Inline(pgrust_parallel_engine_options), check_hook: None, assign_hook: None, show_hook: None },
    // boot_val diverges from C (DEFAULT_IO_METHOD = worker): sync stays the
    // default until the worker flip letter; check_io_method refuses unported
    // methods cleanly (owner: aio_core).
    GucEnumSetting { name: "io_method", context: PGC_POSTMASTER, group: RESOURCES_IO, short_desc: Some("Selects the method for executing asynchronous I/O."), long_desc: None, flags: 0, variable: &vars::io_method, boot_val: GucDefaultValue::Enum(IOMETHOD_SYNC), options: GucEnumOptions::External(&option_sets::io_method_options), check_hook: Some(&hooks::check_io_method), assign_hook: Some(&hooks::assign_io_method), show_hook: None },
    GucEnumSetting { name: "hnsw.iterative_scan", context: PGC_USERSET, group: CUSTOM_OPTIONS, short_desc: Some("Sets the mode for iterative scans"), long_desc: None, flags: 0, variable: &vars::hnsw_iterative_scan, boot_val: GucDefaultValue::Enum(0), options: GucEnumOptions::Inline(hnsw_iterative_scan_options), check_hook: None, assign_hook: None, show_hook: None },
];
