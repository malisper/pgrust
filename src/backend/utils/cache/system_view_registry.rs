use crate::include::nodes::parsenodes::{SqlType, SqlTypeKind};
use crate::include::nodes::primnodes::QueryColumn;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyntheticSystemViewKind {
    PgEnum,
    PgType,
    PgRange,
    PgViews,
    PgMatviews,
    PgIndexes,
    PgPolicies,
    PgRules,
    PgStats,
    PgSettings,
    PgStatActivity,
    PgStatAllTables,
    PgStatUserTables,
    PgStatioUserTables,
    PgStatUserFunctions,
    PgStatIo,
    PgStatProgressCopy,
    PgLocks,
    InformationSchemaTables,
    InformationSchemaViews,
    InformationSchemaColumns,
    InformationSchemaColumnColumnUsage,
    InformationSchemaTriggers,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyntheticSystemViewColumn {
    pub name: &'static str,
    pub sql_type: SqlType,
}

impl SyntheticSystemViewColumn {
    pub const fn new(name: &'static str, sql_type: SqlType) -> Self {
        Self { name, sql_type }
    }

    pub const fn text(name: &'static str) -> Self {
        Self::new(name, SqlType::new(SqlTypeKind::Text))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyntheticSystemView {
    pub kind: SyntheticSystemViewKind,
    pub canonical_name: &'static str,
    pub aliases: &'static [&'static str],
    pub columns: &'static [SyntheticSystemViewColumn],
    pub view_definition_sql: &'static str,
}

impl SyntheticSystemView {
    pub fn matches_name(&self, name: &str) -> bool {
        self.canonical_name.eq_ignore_ascii_case(name)
            || self
                .aliases
                .iter()
                .any(|alias| alias.eq_ignore_ascii_case(name))
    }

    pub fn output_columns(&self) -> Vec<QueryColumn> {
        self.columns
            .iter()
            .map(|column| QueryColumn {
                name: column.name.into(),
                sql_type: column.sql_type,
                wire_type_oid: None,
            })
            .collect()
    }

    pub fn unqualified_name(&self) -> &'static str {
        self.canonical_name
            .rsplit('.')
            .next()
            .unwrap_or(self.canonical_name)
    }

    pub fn has_metadata_definition(&self) -> bool {
        !self.view_definition_sql.is_empty()
    }
}

pub fn synthetic_system_view(name: &str) -> Option<&'static SyntheticSystemView> {
    SYNTHETIC_SYSTEM_VIEWS
        .iter()
        .find(|view| view.matches_name(name))
}

pub fn synthetic_system_views() -> &'static [SyntheticSystemView] {
    &SYNTHETIC_SYSTEM_VIEWS
}

const PG_VIEW_ALIASES: &[&str] = &["pg_views", "pg_catalog.pg_views"];
const PG_ENUM_ALIASES: &[&str] = &["pg_enum", "pg_catalog.pg_enum"];
const PG_TYPE_ALIASES: &[&str] = &["pg_type", "pg_catalog.pg_type"];
const PG_RANGE_ALIASES: &[&str] = &["pg_range", "pg_catalog.pg_range"];
const PG_MATVIEWS_ALIASES: &[&str] = &["pg_matviews", "pg_catalog.pg_matviews"];
const PG_INDEXES_ALIASES: &[&str] = &["pg_indexes", "pg_catalog.pg_indexes"];
const PG_POLICIES_ALIASES: &[&str] = &["pg_policies", "pg_catalog.pg_policies"];
const PG_RULES_ALIASES: &[&str] = &["pg_rules", "pg_catalog.pg_rules"];
const PG_STATS_ALIASES: &[&str] = &["pg_stats", "pg_catalog.pg_stats"];
const PG_SETTINGS_ALIASES: &[&str] = &["pg_settings", "pg_catalog.pg_settings"];
const PG_STAT_ACTIVITY_ALIASES: &[&str] = &["pg_stat_activity", "pg_catalog.pg_stat_activity"];
const PG_STAT_ALL_TABLES_ALIASES: &[&str] =
    &["pg_stat_all_tables", "pg_catalog.pg_stat_all_tables"];
const PG_STAT_USER_TABLES_ALIASES: &[&str] =
    &["pg_stat_user_tables", "pg_catalog.pg_stat_user_tables"];
const PG_STATIO_USER_TABLES_ALIASES: &[&str] =
    &["pg_statio_user_tables", "pg_catalog.pg_statio_user_tables"];
const PG_STAT_USER_FUNCTIONS_ALIASES: &[&str] = &[
    "pg_stat_user_functions",
    "pg_catalog.pg_stat_user_functions",
];
const PG_STAT_IO_ALIASES: &[&str] = &["pg_stat_io", "pg_catalog.pg_stat_io"];
const PG_STAT_PROGRESS_COPY_ALIASES: &[&str] =
    &["pg_stat_progress_copy", "pg_catalog.pg_stat_progress_copy"];
const PG_LOCKS_ALIASES: &[&str] = &["pg_locks", "pg_catalog.pg_locks"];
const INFORMATION_SCHEMA_TABLES_ALIASES: &[&str] = &["information_schema.tables"];
const INFORMATION_SCHEMA_VIEWS_ALIASES: &[&str] = &["information_schema.views"];
const INFORMATION_SCHEMA_COLUMNS_ALIASES: &[&str] = &["information_schema.columns"];
const INFORMATION_SCHEMA_COLUMN_COLUMN_USAGE_ALIASES: &[&str] =
    &["information_schema.column_column_usage"];
const INFORMATION_SCHEMA_TRIGGERS_ALIASES: &[&str] = &["information_schema.triggers"];

const PG_ENUM_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::new("oid", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::new("enumtypid", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::new("enumsortorder", SqlType::new(SqlTypeKind::Float4)),
    SyntheticSystemViewColumn::new("enumlabel", SqlType::new(SqlTypeKind::Name)),
];

const PG_TYPE_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::new("oid", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::new("typname", SqlType::new(SqlTypeKind::Name)),
    SyntheticSystemViewColumn::new("typnamespace", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::new("typowner", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::new("typlen", SqlType::new(SqlTypeKind::Int2)),
    SyntheticSystemViewColumn::new("typbyval", SqlType::new(SqlTypeKind::Bool)),
    SyntheticSystemViewColumn::new("typtype", SqlType::new(SqlTypeKind::InternalChar)),
    SyntheticSystemViewColumn::new("typisdefined", SqlType::new(SqlTypeKind::Bool)),
    SyntheticSystemViewColumn::new("typalign", SqlType::new(SqlTypeKind::InternalChar)),
    SyntheticSystemViewColumn::new("typstorage", SqlType::new(SqlTypeKind::InternalChar)),
    SyntheticSystemViewColumn::new("typrelid", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::new("typsubscript", SqlType::new(SqlTypeKind::RegProc)),
    SyntheticSystemViewColumn::new("typelem", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::new("typarray", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::new("typinput", SqlType::new(SqlTypeKind::RegProc)),
    SyntheticSystemViewColumn::new("typoutput", SqlType::new(SqlTypeKind::RegProc)),
    SyntheticSystemViewColumn::new("typreceive", SqlType::new(SqlTypeKind::RegProc)),
    SyntheticSystemViewColumn::new("typsend", SqlType::new(SqlTypeKind::RegProc)),
    SyntheticSystemViewColumn::new("typmodin", SqlType::new(SqlTypeKind::RegProc)),
    SyntheticSystemViewColumn::new("typmodout", SqlType::new(SqlTypeKind::RegProc)),
    SyntheticSystemViewColumn::new("typdelim", SqlType::new(SqlTypeKind::InternalChar)),
    SyntheticSystemViewColumn::new("typanalyze", SqlType::new(SqlTypeKind::RegProc)),
    SyntheticSystemViewColumn::new("typbasetype", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::new("typcollation", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::new("typacl", SqlType::array_of(SqlType::new(SqlTypeKind::Text))),
];

const PG_RANGE_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::new("rngtypid", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::new("rngsubtype", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::new("rngmultitypid", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::new("rngcollation", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::new("rngsubopc", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::new("rngcanonical", SqlType::new(SqlTypeKind::RegProc)),
    SyntheticSystemViewColumn::new("rngsubdiff", SqlType::new(SqlTypeKind::RegProc)),
];

const PG_VIEWS_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("schemaname"),
    SyntheticSystemViewColumn::text("viewname"),
    SyntheticSystemViewColumn::text("viewowner"),
    SyntheticSystemViewColumn::text("definition"),
];

const PG_MATVIEWS_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("schemaname"),
    SyntheticSystemViewColumn::text("matviewname"),
    SyntheticSystemViewColumn::text("matviewowner"),
    SyntheticSystemViewColumn::text("tablespace"),
    SyntheticSystemViewColumn::new("hasindexes", SqlType::new(SqlTypeKind::Bool)),
    SyntheticSystemViewColumn::new("ispopulated", SqlType::new(SqlTypeKind::Bool)),
    SyntheticSystemViewColumn::text("definition"),
];

const PG_INDEXES_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("schemaname"),
    SyntheticSystemViewColumn::text("tablename"),
    SyntheticSystemViewColumn::text("indexname"),
    SyntheticSystemViewColumn::text("tablespace"),
    SyntheticSystemViewColumn::text("indexdef"),
];

const PG_POLICIES_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::new("schemaname", SqlType::new(SqlTypeKind::Name)),
    SyntheticSystemViewColumn::new("tablename", SqlType::new(SqlTypeKind::Name)),
    SyntheticSystemViewColumn::new("policyname", SqlType::new(SqlTypeKind::Name)),
    SyntheticSystemViewColumn::text("permissive"),
    SyntheticSystemViewColumn::new("roles", SqlType::array_of(SqlType::new(SqlTypeKind::Name))),
    SyntheticSystemViewColumn::text("cmd"),
    SyntheticSystemViewColumn::text("qual"),
    SyntheticSystemViewColumn::text("with_check"),
];

const PG_POLICIES_DEFINITION_SQL: &str = r#"SELECT
    n.nspname AS schemaname,
    c.relname AS tablename,
    pol.polname AS policyname,
    CASE
        WHEN pol.polpermissive THEN 'PERMISSIVE'
        ELSE 'RESTRICTIVE'
    END AS permissive,
    CASE
        WHEN pol.polroles = '{0}' THEN string_to_array('public', '')
        ELSE ARRAY(
            SELECT rolname
            FROM pg_catalog.pg_authid
            WHERE oid = ANY (pol.polroles)
            ORDER BY 1
        )
    END AS roles,
    CASE pol.polcmd
        WHEN 'r' THEN 'SELECT'
        WHEN 'a' THEN 'INSERT'
        WHEN 'w' THEN 'UPDATE'
        WHEN 'd' THEN 'DELETE'
        WHEN '*' THEN 'ALL'
    END AS cmd,
    pg_catalog.pg_get_expr(pol.polqual, pol.polrelid) AS qual,
    pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid) AS with_check
FROM pg_catalog.pg_policy pol
JOIN pg_catalog.pg_class c ON (c.oid = pol.polrelid)
LEFT JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)"#;

const PG_RULES_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("schemaname"),
    SyntheticSystemViewColumn::text("tablename"),
    SyntheticSystemViewColumn::text("rulename"),
    SyntheticSystemViewColumn::text("definition"),
];

const PG_STATS_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("schemaname"),
    SyntheticSystemViewColumn::text("tablename"),
    SyntheticSystemViewColumn::text("attname"),
    SyntheticSystemViewColumn::new("inherited", SqlType::new(SqlTypeKind::Bool)),
    SyntheticSystemViewColumn::new("null_frac", SqlType::new(SqlTypeKind::Float4)),
    SyntheticSystemViewColumn::new("avg_width", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::new("n_distinct", SqlType::new(SqlTypeKind::Float4)),
    SyntheticSystemViewColumn::new("most_common_vals", SqlType::new(SqlTypeKind::AnyArray)),
    SyntheticSystemViewColumn::new(
        "most_common_freqs",
        SqlType::array_of(SqlType::new(SqlTypeKind::Float4)),
    ),
    SyntheticSystemViewColumn::new("histogram_bounds", SqlType::new(SqlTypeKind::AnyArray)),
    SyntheticSystemViewColumn::new("correlation", SqlType::new(SqlTypeKind::Float4)),
    SyntheticSystemViewColumn::new("most_common_elems", SqlType::new(SqlTypeKind::AnyArray)),
    SyntheticSystemViewColumn::new(
        "most_common_elem_freqs",
        SqlType::array_of(SqlType::new(SqlTypeKind::Float4)),
    ),
    SyntheticSystemViewColumn::new(
        "elem_count_histogram",
        SqlType::array_of(SqlType::new(SqlTypeKind::Float4)),
    ),
    SyntheticSystemViewColumn::new(
        "range_length_histogram",
        SqlType::new(SqlTypeKind::AnyArray),
    ),
    SyntheticSystemViewColumn::new("range_empty_frac", SqlType::new(SqlTypeKind::Float4)),
    SyntheticSystemViewColumn::new(
        "range_bounds_histogram",
        SqlType::new(SqlTypeKind::AnyArray),
    ),
];

const PG_STAT_ACTIVITY_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::new("pid", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::text("datname"),
    SyntheticSystemViewColumn::text("usename"),
    SyntheticSystemViewColumn::text("state"),
    SyntheticSystemViewColumn::text("query"),
    SyntheticSystemViewColumn::text("backend_type"),
];

const PG_SETTINGS_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("name"),
    SyntheticSystemViewColumn::text("setting"),
];

const PG_LOCKS_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("locktype"),
    SyntheticSystemViewColumn::new("database", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::new("relation", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::new("page", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::new("tuple", SqlType::new(SqlTypeKind::Int2)),
    SyntheticSystemViewColumn::text("virtualxid"),
    SyntheticSystemViewColumn::new("transactionid", SqlType::new(SqlTypeKind::Xid)),
    SyntheticSystemViewColumn::new("classid", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::new("objid", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::new("objsubid", SqlType::new(SqlTypeKind::Int2)),
    SyntheticSystemViewColumn::text("virtualtransaction"),
    SyntheticSystemViewColumn::new("pid", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::text("mode"),
    SyntheticSystemViewColumn::new("granted", SqlType::new(SqlTypeKind::Bool)),
    SyntheticSystemViewColumn::new("fastpath", SqlType::new(SqlTypeKind::Bool)),
    SyntheticSystemViewColumn::new("waitstart", SqlType::new(SqlTypeKind::TimestampTz)),
];

const PG_LOCKS_DEFINITION_SQL: &str = "SELECT * FROM pg_catalog.pg_lock_status() AS L";

const INFORMATION_SCHEMA_TRIGGERS_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("trigger_catalog"),
    SyntheticSystemViewColumn::text("trigger_schema"),
    SyntheticSystemViewColumn::text("trigger_name"),
    SyntheticSystemViewColumn::text("event_manipulation"),
    SyntheticSystemViewColumn::text("event_object_catalog"),
    SyntheticSystemViewColumn::text("event_object_schema"),
    SyntheticSystemViewColumn::text("event_object_table"),
    SyntheticSystemViewColumn::new("action_order", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::text("action_condition"),
    SyntheticSystemViewColumn::text("action_statement"),
    SyntheticSystemViewColumn::text("action_orientation"),
    SyntheticSystemViewColumn::text("action_timing"),
    SyntheticSystemViewColumn::text("action_reference_old_table"),
    SyntheticSystemViewColumn::text("action_reference_new_table"),
    SyntheticSystemViewColumn::text("action_reference_old_row"),
    SyntheticSystemViewColumn::text("action_reference_new_row"),
    SyntheticSystemViewColumn::new("created", SqlType::new(SqlTypeKind::Timestamp)),
];

const PG_STAT_USER_TABLES_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::new("relid", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::text("schemaname"),
    SyntheticSystemViewColumn::text("relname"),
    SyntheticSystemViewColumn::new("seq_scan", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("last_seq_scan", SqlType::new(SqlTypeKind::TimestampTz)),
    SyntheticSystemViewColumn::new("seq_tup_read", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("idx_scan", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("last_idx_scan", SqlType::new(SqlTypeKind::TimestampTz)),
    SyntheticSystemViewColumn::new("idx_tup_fetch", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("n_tup_ins", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("n_tup_upd", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("n_tup_del", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("n_tup_hot_upd", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("n_tup_newpage_upd", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("n_live_tup", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("n_dead_tup", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("n_mod_since_analyze", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("n_ins_since_vacuum", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("last_vacuum", SqlType::new(SqlTypeKind::TimestampTz)),
    SyntheticSystemViewColumn::new("last_autovacuum", SqlType::new(SqlTypeKind::TimestampTz)),
    SyntheticSystemViewColumn::new("last_analyze", SqlType::new(SqlTypeKind::TimestampTz)),
    SyntheticSystemViewColumn::new("last_autoanalyze", SqlType::new(SqlTypeKind::TimestampTz)),
    SyntheticSystemViewColumn::new("vacuum_count", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("autovacuum_count", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("analyze_count", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("autoanalyze_count", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("total_vacuum_time", SqlType::new(SqlTypeKind::Float8)),
    SyntheticSystemViewColumn::new("total_autovacuum_time", SqlType::new(SqlTypeKind::Float8)),
    SyntheticSystemViewColumn::new("total_analyze_time", SqlType::new(SqlTypeKind::Float8)),
    SyntheticSystemViewColumn::new("total_autoanalyze_time", SqlType::new(SqlTypeKind::Float8)),
];

const PG_STATIO_USER_TABLES_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::new("relid", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::text("schemaname"),
    SyntheticSystemViewColumn::text("relname"),
    SyntheticSystemViewColumn::new("heap_blks_read", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("heap_blks_hit", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("idx_blks_read", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("idx_blks_hit", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("toast_blks_read", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("toast_blks_hit", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("tidx_blks_read", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("tidx_blks_hit", SqlType::new(SqlTypeKind::Int8)),
];

const PG_STAT_USER_FUNCTIONS_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::new("funcid", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::text("schemaname"),
    SyntheticSystemViewColumn::text("funcname"),
    SyntheticSystemViewColumn::new("calls", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("total_time", SqlType::new(SqlTypeKind::Float8)),
    SyntheticSystemViewColumn::new("self_time", SqlType::new(SqlTypeKind::Float8)),
];

const PG_STAT_IO_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("backend_type"),
    SyntheticSystemViewColumn::text("object"),
    SyntheticSystemViewColumn::text("context"),
    SyntheticSystemViewColumn::new("reads", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("read_bytes", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("read_time", SqlType::new(SqlTypeKind::Float8)),
    SyntheticSystemViewColumn::new("writes", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("write_bytes", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("write_time", SqlType::new(SqlTypeKind::Float8)),
    SyntheticSystemViewColumn::new("writebacks", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("writeback_time", SqlType::new(SqlTypeKind::Float8)),
    SyntheticSystemViewColumn::new("extends", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("extend_bytes", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("extend_time", SqlType::new(SqlTypeKind::Float8)),
    SyntheticSystemViewColumn::new("hits", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("evictions", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("reuses", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("fsyncs", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("fsync_time", SqlType::new(SqlTypeKind::Float8)),
    SyntheticSystemViewColumn::new("stats_reset", SqlType::new(SqlTypeKind::TimestampTz)),
];

const PG_STAT_PROGRESS_COPY_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::new("pid", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::new("datid", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::new("datname", SqlType::new(SqlTypeKind::Name)),
    SyntheticSystemViewColumn::new("relid", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::text("command"),
    SyntheticSystemViewColumn::text("type"),
    SyntheticSystemViewColumn::new("bytes_processed", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("bytes_total", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("tuples_processed", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("tuples_excluded", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("tuples_skipped", SqlType::new(SqlTypeKind::Int8)),
];

const INFORMATION_SCHEMA_TABLES_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("table_name"),
    SyntheticSystemViewColumn::text("is_insertable_into"),
];

const INFORMATION_SCHEMA_VIEWS_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("table_catalog"),
    SyntheticSystemViewColumn::text("table_schema"),
    SyntheticSystemViewColumn::text("table_name"),
    SyntheticSystemViewColumn::text("view_definition"),
    SyntheticSystemViewColumn::text("check_option"),
    SyntheticSystemViewColumn::text("is_updatable"),
    SyntheticSystemViewColumn::text("is_insertable_into"),
    SyntheticSystemViewColumn::text("is_trigger_updatable"),
    SyntheticSystemViewColumn::text("is_trigger_deletable"),
    SyntheticSystemViewColumn::text("is_trigger_insertable_into"),
];

const INFORMATION_SCHEMA_COLUMNS_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("table_catalog"),
    SyntheticSystemViewColumn::text("table_schema"),
    SyntheticSystemViewColumn::text("table_name"),
    SyntheticSystemViewColumn::text("column_name"),
    SyntheticSystemViewColumn::new("ordinal_position", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::text("column_default"),
    SyntheticSystemViewColumn::text("is_nullable"),
    SyntheticSystemViewColumn::text("data_type"),
    SyntheticSystemViewColumn::new("character_maximum_length", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::new("character_octet_length", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::new("numeric_precision", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::new("numeric_precision_radix", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::new("numeric_scale", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::new("datetime_precision", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::text("interval_type"),
    SyntheticSystemViewColumn::new("interval_precision", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::text("character_set_catalog"),
    SyntheticSystemViewColumn::text("character_set_schema"),
    SyntheticSystemViewColumn::text("character_set_name"),
    SyntheticSystemViewColumn::text("collation_catalog"),
    SyntheticSystemViewColumn::text("collation_schema"),
    SyntheticSystemViewColumn::text("collation_name"),
    SyntheticSystemViewColumn::text("domain_catalog"),
    SyntheticSystemViewColumn::text("domain_schema"),
    SyntheticSystemViewColumn::text("domain_name"),
    SyntheticSystemViewColumn::text("udt_catalog"),
    SyntheticSystemViewColumn::text("udt_schema"),
    SyntheticSystemViewColumn::text("udt_name"),
    SyntheticSystemViewColumn::text("scope_catalog"),
    SyntheticSystemViewColumn::text("scope_schema"),
    SyntheticSystemViewColumn::text("scope_name"),
    SyntheticSystemViewColumn::new("maximum_cardinality", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::text("dtd_identifier"),
    SyntheticSystemViewColumn::text("is_self_referencing"),
    SyntheticSystemViewColumn::text("is_identity"),
    SyntheticSystemViewColumn::text("identity_generation"),
    SyntheticSystemViewColumn::text("identity_start"),
    SyntheticSystemViewColumn::text("identity_increment"),
    SyntheticSystemViewColumn::text("identity_maximum"),
    SyntheticSystemViewColumn::text("identity_minimum"),
    SyntheticSystemViewColumn::text("identity_cycle"),
    SyntheticSystemViewColumn::text("is_generated"),
    SyntheticSystemViewColumn::text("generation_expression"),
    SyntheticSystemViewColumn::text("is_updatable"),
];

const INFORMATION_SCHEMA_COLUMN_COLUMN_USAGE_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("table_catalog"),
    SyntheticSystemViewColumn::text("table_schema"),
    SyntheticSystemViewColumn::text("table_name"),
    SyntheticSystemViewColumn::text("column_name"),
    SyntheticSystemViewColumn::text("dependent_column"),
];

const SYNTHETIC_SYSTEM_VIEWS: [SyntheticSystemView; 23] = [
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgEnum,
        canonical_name: "pg_catalog.pg_enum",
        aliases: PG_ENUM_ALIASES,
        columns: PG_ENUM_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgType,
        canonical_name: "pg_catalog.pg_type",
        aliases: PG_TYPE_ALIASES,
        columns: PG_TYPE_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgRange,
        canonical_name: "pg_catalog.pg_range",
        aliases: PG_RANGE_ALIASES,
        columns: PG_RANGE_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgViews,
        canonical_name: "pg_catalog.pg_views",
        aliases: PG_VIEW_ALIASES,
        columns: PG_VIEWS_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgMatviews,
        canonical_name: "pg_catalog.pg_matviews",
        aliases: PG_MATVIEWS_ALIASES,
        columns: PG_MATVIEWS_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgIndexes,
        canonical_name: "pg_catalog.pg_indexes",
        aliases: PG_INDEXES_ALIASES,
        columns: PG_INDEXES_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgPolicies,
        canonical_name: "pg_catalog.pg_policies",
        aliases: PG_POLICIES_ALIASES,
        columns: PG_POLICIES_COLUMNS,
        view_definition_sql: PG_POLICIES_DEFINITION_SQL,
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgRules,
        canonical_name: "pg_catalog.pg_rules",
        aliases: PG_RULES_ALIASES,
        columns: PG_RULES_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgStats,
        canonical_name: "pg_catalog.pg_stats",
        aliases: PG_STATS_ALIASES,
        columns: PG_STATS_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgSettings,
        canonical_name: "pg_catalog.pg_settings",
        aliases: PG_SETTINGS_ALIASES,
        columns: PG_SETTINGS_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgStatActivity,
        canonical_name: "pg_catalog.pg_stat_activity",
        aliases: PG_STAT_ACTIVITY_ALIASES,
        columns: PG_STAT_ACTIVITY_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgStatAllTables,
        canonical_name: "pg_catalog.pg_stat_all_tables",
        aliases: PG_STAT_ALL_TABLES_ALIASES,
        columns: PG_STAT_USER_TABLES_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgStatUserTables,
        canonical_name: "pg_catalog.pg_stat_user_tables",
        aliases: PG_STAT_USER_TABLES_ALIASES,
        columns: PG_STAT_USER_TABLES_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgStatioUserTables,
        canonical_name: "pg_catalog.pg_statio_user_tables",
        aliases: PG_STATIO_USER_TABLES_ALIASES,
        columns: PG_STATIO_USER_TABLES_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgStatUserFunctions,
        canonical_name: "pg_catalog.pg_stat_user_functions",
        aliases: PG_STAT_USER_FUNCTIONS_ALIASES,
        columns: PG_STAT_USER_FUNCTIONS_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgStatIo,
        canonical_name: "pg_catalog.pg_stat_io",
        aliases: PG_STAT_IO_ALIASES,
        columns: PG_STAT_IO_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgStatProgressCopy,
        canonical_name: "pg_catalog.pg_stat_progress_copy",
        aliases: PG_STAT_PROGRESS_COPY_ALIASES,
        columns: PG_STAT_PROGRESS_COPY_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgLocks,
        canonical_name: "pg_catalog.pg_locks",
        aliases: PG_LOCKS_ALIASES,
        columns: PG_LOCKS_COLUMNS,
        view_definition_sql: PG_LOCKS_DEFINITION_SQL,
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::InformationSchemaTables,
        canonical_name: "information_schema.tables",
        aliases: INFORMATION_SCHEMA_TABLES_ALIASES,
        columns: INFORMATION_SCHEMA_TABLES_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::InformationSchemaViews,
        canonical_name: "information_schema.views",
        aliases: INFORMATION_SCHEMA_VIEWS_ALIASES,
        columns: INFORMATION_SCHEMA_VIEWS_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::InformationSchemaColumns,
        canonical_name: "information_schema.columns",
        aliases: INFORMATION_SCHEMA_COLUMNS_ALIASES,
        columns: INFORMATION_SCHEMA_COLUMNS_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::InformationSchemaColumnColumnUsage,
        canonical_name: "information_schema.column_column_usage",
        aliases: INFORMATION_SCHEMA_COLUMN_COLUMN_USAGE_ALIASES,
        columns: INFORMATION_SCHEMA_COLUMN_COLUMN_USAGE_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::InformationSchemaTriggers,
        canonical_name: "information_schema.triggers",
        aliases: INFORMATION_SCHEMA_TRIGGERS_ALIASES,
        columns: INFORMATION_SCHEMA_TRIGGERS_COLUMNS,
        view_definition_sql: "",
    },
];
