use crate::include::nodes::parsenodes::{SqlType, SqlTypeKind};
use crate::include::nodes::primnodes::QueryColumn;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyntheticSystemViewKind {
    PgEnum,
    PgType,
    PgConstraint,
    PgInitPrivs,
    PgRange,
    PgTables,
    PgViews,
    PgMatviews,
    PgIndexes,
    PgPolicies,
    PgPublicationTables,
    PgSequences,
    PgRules,
    PgStats,
    PgStatsExt,
    PgStatsExtExprs,
    PgSettings,
    PgAvailableExtensions,
    PgAvailableExtensionVersions,
    PgBackendMemoryContexts,
    PgShmemAllocationsNuma,
    PgConfig,
    PgCursors,
    PgFileSettings,
    PgHbaFileRules,
    PgIdentFileMappings,
    PgPreparedXacts,
    PgPreparedStatements,
    PgStatWalReceiver,
    PgWaitEvents,
    PgTimezoneNames,
    PgTimezoneAbbrevs,
    PgUserMappings,
    PgRoles,
    PgStatActivity,
    PgStatDatabase,
    PgStatCheckpointer,
    PgStatWal,
    PgStatSlru,
    PgStatArchiver,
    PgStatBgwriter,
    PgStatRecoveryPrefetch,
    PgStatSubscriptionStats,
    PgStatAllTables,
    PgStatUserTables,
    PgStatioUserTables,
    PgStatUserFunctions,
    PgStatIo,
    PgStatProgressCopy,
    PgLocks,
    InformationSchemaTables,
    InformationSchemaViews,
    InformationSchemaSequences,
    InformationSchemaColumns,
    InformationSchemaRoutines,
    InformationSchemaParameters,
    InformationSchemaRoutineRoutineUsage,
    InformationSchemaRoutineSequenceUsage,
    InformationSchemaRoutineColumnUsage,
    InformationSchemaRoutineTableUsage,
    InformationSchemaColumnColumnUsage,
    InformationSchemaColumnDomainUsage,
    InformationSchemaDomainConstraints,
    InformationSchemaDomains,
    InformationSchemaCheckConstraints,
    InformationSchemaTriggers,
    InformationSchemaForeignDataWrappers,
    InformationSchemaForeignDataWrapperOptions,
    InformationSchemaForeignServers,
    InformationSchemaForeignServerOptions,
    InformationSchemaUserMappings,
    InformationSchemaUserMappingOptions,
    InformationSchemaUsagePrivileges,
    InformationSchemaRoleUsageGrants,
    InformationSchemaForeignTables,
    InformationSchemaForeignTableOptions,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyntheticSystemViewFunction {
    pub proc_oid: u32,
    pub function_name: &'static str,
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

    pub fn set_returning_function(&self) -> Option<SyntheticSystemViewFunction> {
        use SyntheticSystemViewKind::*;
        let (proc_oid, function_name) = match self.kind {
            PgAvailableExtensions => (3082, "pg_available_extensions"),
            PgAvailableExtensionVersions => (3083, "pg_available_extension_versions"),
            PgBackendMemoryContexts => (2282, "pg_get_backend_memory_contexts"),
            PgShmemAllocationsNuma => (4100, "pg_get_shmem_allocations_numa"),
            PgConfig => (3400, "pg_config"),
            PgCursors => (2511, "pg_cursor"),
            PgFileSettings => (3329, "pg_show_all_file_settings"),
            PgHbaFileRules => (3401, "pg_hba_file_rules"),
            PgIdentFileMappings => (6250, "pg_ident_file_mappings"),
            PgPreparedXacts => (1065, "pg_prepared_xact"),
            PgPreparedStatements => (2510, "pg_prepared_statement"),
            PgSettings => (2084, "pg_show_all_settings"),
            PgStatWalReceiver => (3317, "pg_stat_get_wal_receiver"),
            PgWaitEvents => (6318, "pg_get_wait_events"),
            PgTimezoneNames => (2856, "pg_timezone_names"),
            PgTimezoneAbbrevs => (2599, "pg_timezone_abbrevs_abbrevs"),
            _ => return None,
        };
        Some(SyntheticSystemViewFunction {
            proc_oid,
            function_name,
        })
    }
}

pub fn synthetic_system_view(name: &str) -> Option<&'static SyntheticSystemView> {
    SYNTHETIC_SYSTEM_VIEWS
        .iter()
        .find(|view| view.matches_name(name))
}

pub fn synthetic_system_views() -> &'static [SyntheticSystemView] {
    SYNTHETIC_SYSTEM_VIEWS
}

const PG_VIEW_ALIASES: &[&str] = &["pg_views", "pg_catalog.pg_views"];
const PG_TABLES_ALIASES: &[&str] = &["pg_tables", "pg_catalog.pg_tables"];
const PG_ENUM_ALIASES: &[&str] = &["pg_enum", "pg_catalog.pg_enum"];
const PG_TYPE_ALIASES: &[&str] = &["pg_type", "pg_catalog.pg_type"];
const PG_CONSTRAINT_ALIASES: &[&str] = &["pg_constraint", "pg_catalog.pg_constraint"];
const PG_RANGE_ALIASES: &[&str] = &["pg_range", "pg_catalog.pg_range"];
const PG_MATVIEWS_ALIASES: &[&str] = &["pg_matviews", "pg_catalog.pg_matviews"];
const PG_INDEXES_ALIASES: &[&str] = &["pg_indexes", "pg_catalog.pg_indexes"];
const PG_POLICIES_ALIASES: &[&str] = &["pg_policies", "pg_catalog.pg_policies"];
const PG_PUBLICATION_TABLES_ALIASES: &[&str] =
    &["pg_publication_tables", "pg_catalog.pg_publication_tables"];
const PG_SEQUENCES_ALIASES: &[&str] = &["pg_sequences", "pg_catalog.pg_sequences"];
const PG_RULES_ALIASES: &[&str] = &["pg_rules", "pg_catalog.pg_rules"];
const PG_STATS_ALIASES: &[&str] = &["pg_stats", "pg_catalog.pg_stats"];
const PG_STATS_EXT_ALIASES: &[&str] = &["pg_stats_ext", "pg_catalog.pg_stats_ext"];
const PG_STATS_EXT_EXPRS_ALIASES: &[&str] =
    &["pg_stats_ext_exprs", "pg_catalog.pg_stats_ext_exprs"];
const PG_SETTINGS_ALIASES: &[&str] = &["pg_settings", "pg_catalog.pg_settings"];
const PG_AVAILABLE_EXTENSIONS_ALIASES: &[&str] = &[
    "pg_available_extensions",
    "pg_catalog.pg_available_extensions",
];
const PG_AVAILABLE_EXTENSION_VERSIONS_ALIASES: &[&str] = &[
    "pg_available_extension_versions",
    "pg_catalog.pg_available_extension_versions",
];
const PG_BACKEND_MEMORY_CONTEXTS_ALIASES: &[&str] = &[
    "pg_backend_memory_contexts",
    "pg_catalog.pg_backend_memory_contexts",
];
const PG_SHMEM_ALLOCATIONS_NUMA_ALIASES: &[&str] = &[
    "pg_shmem_allocations_numa",
    "pg_catalog.pg_shmem_allocations_numa",
];
const PG_CONFIG_ALIASES: &[&str] = &["pg_config", "pg_catalog.pg_config"];
const PG_CURSORS_ALIASES: &[&str] = &["pg_cursors", "pg_catalog.pg_cursors"];
const PG_FILE_SETTINGS_ALIASES: &[&str] = &["pg_file_settings", "pg_catalog.pg_file_settings"];
const PG_HBA_FILE_RULES_ALIASES: &[&str] = &["pg_hba_file_rules", "pg_catalog.pg_hba_file_rules"];
const PG_IDENT_FILE_MAPPINGS_ALIASES: &[&str] = &[
    "pg_ident_file_mappings",
    "pg_catalog.pg_ident_file_mappings",
];
const PG_PREPARED_XACTS_ALIASES: &[&str] = &["pg_prepared_xacts", "pg_catalog.pg_prepared_xacts"];
const PG_PREPARED_STATEMENTS_ALIASES: &[&str] = &[
    "pg_prepared_statements",
    "pg_catalog.pg_prepared_statements",
];
const PG_STAT_WAL_RECEIVER_ALIASES: &[&str] =
    &["pg_stat_wal_receiver", "pg_catalog.pg_stat_wal_receiver"];
const PG_WAIT_EVENTS_ALIASES: &[&str] = &["pg_wait_events", "pg_catalog.pg_wait_events"];
const PG_TIMEZONE_NAMES_ALIASES: &[&str] = &["pg_timezone_names", "pg_catalog.pg_timezone_names"];
const PG_TIMEZONE_ABBREVS_ALIASES: &[&str] =
    &["pg_timezone_abbrevs", "pg_catalog.pg_timezone_abbrevs"];
const PG_USER_MAPPINGS_ALIASES: &[&str] = &["pg_user_mappings", "pg_catalog.pg_user_mappings"];
const PG_ROLES_ALIASES: &[&str] = &["pg_roles", "pg_catalog.pg_roles"];
const PG_STAT_ACTIVITY_ALIASES: &[&str] = &["pg_stat_activity", "pg_catalog.pg_stat_activity"];
const PG_STAT_DATABASE_ALIASES: &[&str] = &["pg_stat_database", "pg_catalog.pg_stat_database"];
const PG_STAT_CHECKPOINTER_ALIASES: &[&str] =
    &["pg_stat_checkpointer", "pg_catalog.pg_stat_checkpointer"];
const PG_STAT_WAL_ALIASES: &[&str] = &["pg_stat_wal", "pg_catalog.pg_stat_wal"];
const PG_STAT_SLRU_ALIASES: &[&str] = &["pg_stat_slru", "pg_catalog.pg_stat_slru"];
const PG_STAT_ARCHIVER_ALIASES: &[&str] = &["pg_stat_archiver", "pg_catalog.pg_stat_archiver"];
const PG_STAT_BGWRITER_ALIASES: &[&str] = &["pg_stat_bgwriter", "pg_catalog.pg_stat_bgwriter"];
const PG_STAT_RECOVERY_PREFETCH_ALIASES: &[&str] = &[
    "pg_stat_recovery_prefetch",
    "pg_catalog.pg_stat_recovery_prefetch",
];
const PG_STAT_SUBSCRIPTION_STATS_ALIASES: &[&str] = &[
    "pg_stat_subscription_stats",
    "pg_catalog.pg_stat_subscription_stats",
];
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
const INFORMATION_SCHEMA_SEQUENCES_ALIASES: &[&str] = &["information_schema.sequences"];
const INFORMATION_SCHEMA_COLUMNS_ALIASES: &[&str] = &["information_schema.columns"];
const INFORMATION_SCHEMA_ROUTINES_ALIASES: &[&str] = &["information_schema.routines"];
const INFORMATION_SCHEMA_PARAMETERS_ALIASES: &[&str] = &["information_schema.parameters"];
const INFORMATION_SCHEMA_ROUTINE_ROUTINE_USAGE_ALIASES: &[&str] =
    &["information_schema.routine_routine_usage"];
const INFORMATION_SCHEMA_ROUTINE_SEQUENCE_USAGE_ALIASES: &[&str] =
    &["information_schema.routine_sequence_usage"];
const INFORMATION_SCHEMA_ROUTINE_COLUMN_USAGE_ALIASES: &[&str] =
    &["information_schema.routine_column_usage"];
const INFORMATION_SCHEMA_ROUTINE_TABLE_USAGE_ALIASES: &[&str] =
    &["information_schema.routine_table_usage"];
const INFORMATION_SCHEMA_COLUMN_COLUMN_USAGE_ALIASES: &[&str] =
    &["information_schema.column_column_usage"];
const INFORMATION_SCHEMA_COLUMN_DOMAIN_USAGE_ALIASES: &[&str] =
    &["information_schema.column_domain_usage"];
const INFORMATION_SCHEMA_DOMAIN_CONSTRAINTS_ALIASES: &[&str] =
    &["information_schema.domain_constraints"];
const INFORMATION_SCHEMA_DOMAINS_ALIASES: &[&str] = &["information_schema.domains"];
const INFORMATION_SCHEMA_CHECK_CONSTRAINTS_ALIASES: &[&str] =
    &["information_schema.check_constraints"];
const INFORMATION_SCHEMA_TRIGGERS_ALIASES: &[&str] = &["information_schema.triggers"];
const INFORMATION_SCHEMA_FOREIGN_DATA_WRAPPERS_ALIASES: &[&str] =
    &["information_schema.foreign_data_wrappers"];
const INFORMATION_SCHEMA_FOREIGN_DATA_WRAPPER_OPTIONS_ALIASES: &[&str] =
    &["information_schema.foreign_data_wrapper_options"];
const INFORMATION_SCHEMA_FOREIGN_SERVERS_ALIASES: &[&str] = &["information_schema.foreign_servers"];
const INFORMATION_SCHEMA_FOREIGN_SERVER_OPTIONS_ALIASES: &[&str] =
    &["information_schema.foreign_server_options"];
const INFORMATION_SCHEMA_USER_MAPPINGS_ALIASES: &[&str] = &["information_schema.user_mappings"];
const INFORMATION_SCHEMA_USER_MAPPING_OPTIONS_ALIASES: &[&str] =
    &["information_schema.user_mapping_options"];
const INFORMATION_SCHEMA_USAGE_PRIVILEGES_ALIASES: &[&str] =
    &["information_schema.usage_privileges"];
const INFORMATION_SCHEMA_ROLE_USAGE_GRANTS_ALIASES: &[&str] =
    &["information_schema.role_usage_grants"];
const INFORMATION_SCHEMA_FOREIGN_TABLES_ALIASES: &[&str] = &["information_schema.foreign_tables"];
const INFORMATION_SCHEMA_FOREIGN_TABLE_OPTIONS_ALIASES: &[&str] =
    &["information_schema.foreign_table_options"];

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
    SyntheticSystemViewColumn::new("typtypmod", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::new("typcollation", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::new("typnotnull", SqlType::new(SqlTypeKind::Bool)),
    SyntheticSystemViewColumn::new("typdefault", SqlType::new(SqlTypeKind::Text)),
    SyntheticSystemViewColumn::new("typacl", SqlType::array_of(SqlType::new(SqlTypeKind::Text))),
];

const PG_CONSTRAINT_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::new("oid", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::new("conname", SqlType::new(SqlTypeKind::Name)),
    SyntheticSystemViewColumn::new("connamespace", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::new("contype", SqlType::new(SqlTypeKind::InternalChar)),
    SyntheticSystemViewColumn::new("condeferrable", SqlType::new(SqlTypeKind::Bool)),
    SyntheticSystemViewColumn::new("condeferred", SqlType::new(SqlTypeKind::Bool)),
    SyntheticSystemViewColumn::new("conenforced", SqlType::new(SqlTypeKind::Bool)),
    SyntheticSystemViewColumn::new("convalidated", SqlType::new(SqlTypeKind::Bool)),
    SyntheticSystemViewColumn::new("conrelid", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::new("contypid", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::new("conindid", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::new("conparentid", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::new("confrelid", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::new("confupdtype", SqlType::new(SqlTypeKind::InternalChar)),
    SyntheticSystemViewColumn::new("confdeltype", SqlType::new(SqlTypeKind::InternalChar)),
    SyntheticSystemViewColumn::new("confmatchtype", SqlType::new(SqlTypeKind::InternalChar)),
    SyntheticSystemViewColumn::new("conkey", SqlType::array_of(SqlType::new(SqlTypeKind::Int2))),
    SyntheticSystemViewColumn::new(
        "confkey",
        SqlType::array_of(SqlType::new(SqlTypeKind::Int2)),
    ),
    SyntheticSystemViewColumn::new(
        "conpfeqop",
        SqlType::array_of(SqlType::new(SqlTypeKind::Oid)),
    ),
    SyntheticSystemViewColumn::new(
        "conppeqop",
        SqlType::array_of(SqlType::new(SqlTypeKind::Oid)),
    ),
    SyntheticSystemViewColumn::new(
        "conffeqop",
        SqlType::array_of(SqlType::new(SqlTypeKind::Oid)),
    ),
    SyntheticSystemViewColumn::new(
        "confdelsetcols",
        SqlType::array_of(SqlType::new(SqlTypeKind::Int2)),
    ),
    SyntheticSystemViewColumn::new(
        "conexclop",
        SqlType::array_of(SqlType::new(SqlTypeKind::Oid)),
    ),
    SyntheticSystemViewColumn::new("conbin", SqlType::new(SqlTypeKind::PgNodeTree)),
    SyntheticSystemViewColumn::new("conislocal", SqlType::new(SqlTypeKind::Bool)),
    SyntheticSystemViewColumn::new("coninhcount", SqlType::new(SqlTypeKind::Int2)),
    SyntheticSystemViewColumn::new("connoinherit", SqlType::new(SqlTypeKind::Bool)),
    SyntheticSystemViewColumn::new("conperiod", SqlType::new(SqlTypeKind::Bool)),
];

const PG_INIT_PRIVS_ALIASES: &[&str] = &["pg_init_privs", "pg_catalog.pg_init_privs"];
const PG_INIT_PRIVS_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::new("objoid", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::new("classoid", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::new("objsubid", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::new("privtype", SqlType::new(SqlTypeKind::InternalChar)),
    SyntheticSystemViewColumn::new(
        "initprivs",
        SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
    ),
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

const PG_TABLES_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("schemaname"),
    SyntheticSystemViewColumn::text("tablename"),
    SyntheticSystemViewColumn::text("tableowner"),
    SyntheticSystemViewColumn::text("tablespace"),
    SyntheticSystemViewColumn::new("hasindexes", SqlType::new(SqlTypeKind::Bool)),
    SyntheticSystemViewColumn::new("hasrules", SqlType::new(SqlTypeKind::Bool)),
    SyntheticSystemViewColumn::new("hastriggers", SqlType::new(SqlTypeKind::Bool)),
    SyntheticSystemViewColumn::new("rowsecurity", SqlType::new(SqlTypeKind::Bool)),
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

const PG_PUBLICATION_TABLES_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::new("pubname", SqlType::new(SqlTypeKind::Name)),
    SyntheticSystemViewColumn::new("schemaname", SqlType::new(SqlTypeKind::Name)),
    SyntheticSystemViewColumn::new("tablename", SqlType::new(SqlTypeKind::Name)),
    SyntheticSystemViewColumn::new(
        "attnames",
        SqlType::array_of(SqlType::new(SqlTypeKind::Name)),
    ),
    SyntheticSystemViewColumn::text("rowfilter"),
];

const PG_PUBLICATION_TABLES_DEFINITION_SQL: &str = r#"SELECT
    p.pubname,
    n.nspname AS schemaname,
    c.relname AS tablename,
    (SELECT array_agg(a.attname ORDER BY a.attnum)
       FROM pg_catalog.pg_attribute a
      WHERE a.attrelid = gpt.relid AND a.attnum = ANY(gpt.attrs)) AS attnames,
    pg_catalog.pg_get_expr(gpt.qual, gpt.relid) AS rowfilter
FROM pg_catalog.pg_publication p,
     LATERAL pg_catalog.pg_get_publication_tables(p.pubname) gpt(pubid, relid, attrs, qual),
     pg_catalog.pg_class c
     JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)
WHERE c.oid = gpt.relid"#;

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

const PG_STATS_EXT_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("schemaname"),
    SyntheticSystemViewColumn::text("tablename"),
    SyntheticSystemViewColumn::text("statistics_schemaname"),
    SyntheticSystemViewColumn::text("statistics_name"),
    SyntheticSystemViewColumn::text("statistics_owner"),
    SyntheticSystemViewColumn::new(
        "attnames",
        SqlType::array_of(SqlType::new(SqlTypeKind::Name)),
    ),
    SyntheticSystemViewColumn::new("exprs", SqlType::array_of(SqlType::new(SqlTypeKind::Text))),
    SyntheticSystemViewColumn::new(
        "kinds",
        SqlType::array_of(SqlType::new(SqlTypeKind::InternalChar)),
    ),
    SyntheticSystemViewColumn::new("inherited", SqlType::new(SqlTypeKind::Bool)),
    SyntheticSystemViewColumn::new(
        "n_distinct",
        SqlType::new(SqlTypeKind::Bytea)
            .with_identity(crate::include::catalog::PG_NDISTINCT_TYPE_OID, 0),
    ),
    SyntheticSystemViewColumn::new(
        "dependencies",
        SqlType::new(SqlTypeKind::Bytea)
            .with_identity(crate::include::catalog::PG_DEPENDENCIES_TYPE_OID, 0),
    ),
    SyntheticSystemViewColumn::new("most_common_vals", SqlType::new(SqlTypeKind::AnyArray)),
    SyntheticSystemViewColumn::new(
        "most_common_val_nulls",
        SqlType::array_of(SqlType::new(SqlTypeKind::Bool)),
    ),
    SyntheticSystemViewColumn::new(
        "most_common_freqs",
        SqlType::array_of(SqlType::new(SqlTypeKind::Float8)),
    ),
    SyntheticSystemViewColumn::new(
        "most_common_base_freqs",
        SqlType::array_of(SqlType::new(SqlTypeKind::Float8)),
    ),
];

const PG_STATS_EXT_EXPRS_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("schemaname"),
    SyntheticSystemViewColumn::text("tablename"),
    SyntheticSystemViewColumn::text("statistics_schemaname"),
    SyntheticSystemViewColumn::text("statistics_name"),
    SyntheticSystemViewColumn::text("statistics_owner"),
    SyntheticSystemViewColumn::text("expr"),
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
];

const PG_USER_MAPPINGS_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::new("umid", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::new("srvid", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::new("srvname", SqlType::new(SqlTypeKind::Name)),
    SyntheticSystemViewColumn::new("umuser", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::new("usename", SqlType::new(SqlTypeKind::Name)),
    SyntheticSystemViewColumn::new(
        "umoptions",
        SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
    ),
];

const PG_ROLES_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::new("rolname", SqlType::new(SqlTypeKind::Name)),
    SyntheticSystemViewColumn::new("oid", SqlType::new(SqlTypeKind::Oid)),
];

const PG_STAT_ACTIVITY_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::new("pid", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::text("datname"),
    SyntheticSystemViewColumn::text("usename"),
    SyntheticSystemViewColumn::text("state"),
    SyntheticSystemViewColumn::new("query_id", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::text("query"),
    SyntheticSystemViewColumn::text("backend_type"),
];

const PG_SETTINGS_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("name"),
    SyntheticSystemViewColumn::text("setting"),
    SyntheticSystemViewColumn::text("unit"),
    SyntheticSystemViewColumn::text("category"),
    SyntheticSystemViewColumn::text("short_desc"),
    SyntheticSystemViewColumn::text("extra_desc"),
    SyntheticSystemViewColumn::text("context"),
    SyntheticSystemViewColumn::text("vartype"),
    SyntheticSystemViewColumn::text("source"),
    SyntheticSystemViewColumn::text("min_val"),
    SyntheticSystemViewColumn::text("max_val"),
    SyntheticSystemViewColumn::new(
        "enumvals",
        SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
    ),
    SyntheticSystemViewColumn::text("boot_val"),
    SyntheticSystemViewColumn::text("reset_val"),
    SyntheticSystemViewColumn::text("sourcefile"),
    SyntheticSystemViewColumn::new("sourceline", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::new("pending_restart", SqlType::new(SqlTypeKind::Bool)),
];

const PG_AVAILABLE_EXTENSIONS_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::new("name", SqlType::new(SqlTypeKind::Name)),
    SyntheticSystemViewColumn::text("default_version"),
    SyntheticSystemViewColumn::text("installed_version"),
    SyntheticSystemViewColumn::text("comment"),
];

const PG_AVAILABLE_EXTENSION_VERSIONS_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::new("name", SqlType::new(SqlTypeKind::Name)),
    SyntheticSystemViewColumn::text("version"),
    SyntheticSystemViewColumn::new("installed", SqlType::new(SqlTypeKind::Bool)),
    SyntheticSystemViewColumn::new("superuser", SqlType::new(SqlTypeKind::Bool)),
    SyntheticSystemViewColumn::new("trusted", SqlType::new(SqlTypeKind::Bool)),
    SyntheticSystemViewColumn::new("relocatable", SqlType::new(SqlTypeKind::Bool)),
    SyntheticSystemViewColumn::new("schema", SqlType::new(SqlTypeKind::Name)),
    SyntheticSystemViewColumn::new(
        "requires",
        SqlType::array_of(SqlType::new(SqlTypeKind::Name)),
    ),
    SyntheticSystemViewColumn::text("comment"),
];

const PG_BACKEND_MEMORY_CONTEXTS_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("name"),
    SyntheticSystemViewColumn::text("ident"),
    SyntheticSystemViewColumn::text("type"),
    SyntheticSystemViewColumn::new("level", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::new("path", SqlType::array_of(SqlType::new(SqlTypeKind::Int4))),
    SyntheticSystemViewColumn::new("total_bytes", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("total_nblocks", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("free_bytes", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("free_chunks", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("used_bytes", SqlType::new(SqlTypeKind::Int8)),
];

const PG_SHMEM_ALLOCATIONS_NUMA_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("name"),
    SyntheticSystemViewColumn::new("numa_node", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::new("size", SqlType::new(SqlTypeKind::Int8)),
];

const PG_CONFIG_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("name"),
    SyntheticSystemViewColumn::text("setting"),
];

const PG_CURSORS_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("name"),
    SyntheticSystemViewColumn::text("statement"),
    SyntheticSystemViewColumn::new("is_holdable", SqlType::new(SqlTypeKind::Bool)),
    SyntheticSystemViewColumn::new("is_binary", SqlType::new(SqlTypeKind::Bool)),
    SyntheticSystemViewColumn::new("is_scrollable", SqlType::new(SqlTypeKind::Bool)),
    SyntheticSystemViewColumn::new("creation_time", SqlType::new(SqlTypeKind::TimestampTz)),
];

const PG_FILE_SETTINGS_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("sourcefile"),
    SyntheticSystemViewColumn::new("sourceline", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::new("seqno", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::text("name"),
    SyntheticSystemViewColumn::text("setting"),
    SyntheticSystemViewColumn::new("applied", SqlType::new(SqlTypeKind::Bool)),
    SyntheticSystemViewColumn::text("error"),
];

const PG_HBA_FILE_RULES_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::new("rule_number", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::text("file_name"),
    SyntheticSystemViewColumn::new("line_number", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::text("type"),
    SyntheticSystemViewColumn::new(
        "database",
        SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
    ),
    SyntheticSystemViewColumn::new(
        "user_name",
        SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
    ),
    SyntheticSystemViewColumn::text("address"),
    SyntheticSystemViewColumn::text("netmask"),
    SyntheticSystemViewColumn::text("auth_method"),
    SyntheticSystemViewColumn::new(
        "options",
        SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
    ),
    SyntheticSystemViewColumn::text("error"),
];

const PG_IDENT_FILE_MAPPINGS_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::new("map_number", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::text("file_name"),
    SyntheticSystemViewColumn::new("line_number", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::text("map_name"),
    SyntheticSystemViewColumn::text("sys_name"),
    SyntheticSystemViewColumn::text("pg_username"),
    SyntheticSystemViewColumn::text("error"),
];

const PG_PREPARED_XACTS_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::new("transaction", SqlType::new(SqlTypeKind::Xid)),
    SyntheticSystemViewColumn::text("gid"),
    SyntheticSystemViewColumn::new("prepared", SqlType::new(SqlTypeKind::TimestampTz)),
    SyntheticSystemViewColumn::new("owner", SqlType::new(SqlTypeKind::Name)),
    SyntheticSystemViewColumn::new("database", SqlType::new(SqlTypeKind::Name)),
];

const PG_PREPARED_STATEMENTS_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("name"),
    SyntheticSystemViewColumn::text("statement"),
    SyntheticSystemViewColumn::new("prepare_time", SqlType::new(SqlTypeKind::TimestampTz)),
    SyntheticSystemViewColumn::new(
        "parameter_types",
        SqlType::array_of(SqlType::new(SqlTypeKind::RegType)),
    ),
    SyntheticSystemViewColumn::new(
        "result_types",
        SqlType::array_of(SqlType::new(SqlTypeKind::RegType)),
    ),
    SyntheticSystemViewColumn::new("from_sql", SqlType::new(SqlTypeKind::Bool)),
    SyntheticSystemViewColumn::new("generic_plans", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("custom_plans", SqlType::new(SqlTypeKind::Int8)),
];

const PG_STAT_WAL_RECEIVER_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::new("pid", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::text("status"),
    SyntheticSystemViewColumn::new("receive_start_lsn", SqlType::new(SqlTypeKind::PgLsn)),
    SyntheticSystemViewColumn::new("receive_start_tli", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::new("written_lsn", SqlType::new(SqlTypeKind::PgLsn)),
    SyntheticSystemViewColumn::new("flushed_lsn", SqlType::new(SqlTypeKind::PgLsn)),
    SyntheticSystemViewColumn::new("received_tli", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::new("last_msg_send_time", SqlType::new(SqlTypeKind::TimestampTz)),
    SyntheticSystemViewColumn::new(
        "last_msg_receipt_time",
        SqlType::new(SqlTypeKind::TimestampTz),
    ),
    SyntheticSystemViewColumn::new("latest_end_lsn", SqlType::new(SqlTypeKind::PgLsn)),
    SyntheticSystemViewColumn::new("latest_end_time", SqlType::new(SqlTypeKind::TimestampTz)),
    SyntheticSystemViewColumn::text("slot_name"),
    SyntheticSystemViewColumn::text("sender_host"),
    SyntheticSystemViewColumn::new("sender_port", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::text("conninfo"),
];

const PG_WAIT_EVENTS_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("type"),
    SyntheticSystemViewColumn::text("name"),
    SyntheticSystemViewColumn::text("description"),
];

const PG_TIMEZONE_NAMES_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("name"),
    SyntheticSystemViewColumn::text("abbrev"),
    SyntheticSystemViewColumn::new("utc_offset", SqlType::new(SqlTypeKind::Interval)),
    SyntheticSystemViewColumn::new("is_dst", SqlType::new(SqlTypeKind::Bool)),
];

const PG_TIMEZONE_ABBREVS_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("abbrev"),
    SyntheticSystemViewColumn::new("utc_offset", SqlType::new(SqlTypeKind::Interval)),
    SyntheticSystemViewColumn::new("is_dst", SqlType::new(SqlTypeKind::Bool)),
];

const PG_STAT_DATABASE_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::new("datid", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::text("datname"),
    SyntheticSystemViewColumn::new("numbackends", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::new("xact_commit", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("xact_rollback", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("blks_read", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("blks_hit", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("tup_returned", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("tup_fetched", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("tup_inserted", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("tup_updated", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("tup_deleted", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("conflicts", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("temp_files", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("temp_bytes", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("deadlocks", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("checksum_failures", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new(
        "checksum_last_failure",
        SqlType::new(SqlTypeKind::TimestampTz),
    ),
    SyntheticSystemViewColumn::new("blk_read_time", SqlType::new(SqlTypeKind::Float8)),
    SyntheticSystemViewColumn::new("blk_write_time", SqlType::new(SqlTypeKind::Float8)),
    SyntheticSystemViewColumn::new("session_time", SqlType::new(SqlTypeKind::Float8)),
    SyntheticSystemViewColumn::new("active_time", SqlType::new(SqlTypeKind::Float8)),
    SyntheticSystemViewColumn::new(
        "idle_in_transaction_time",
        SqlType::new(SqlTypeKind::Float8),
    ),
    SyntheticSystemViewColumn::new("sessions", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("sessions_abandoned", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("sessions_fatal", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("sessions_killed", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new(
        "parallel_workers_to_launch",
        SqlType::new(SqlTypeKind::Int8),
    ),
    SyntheticSystemViewColumn::new("parallel_workers_launched", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("stats_reset", SqlType::new(SqlTypeKind::TimestampTz)),
];

const PG_STAT_CHECKPOINTER_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::new("num_timed", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("num_requested", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("num_done", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("restartpoints_timed", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("restartpoints_req", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("restartpoints_done", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("write_time", SqlType::new(SqlTypeKind::Float8)),
    SyntheticSystemViewColumn::new("sync_time", SqlType::new(SqlTypeKind::Float8)),
    SyntheticSystemViewColumn::new("buffers_written", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("slru_written", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("stats_reset", SqlType::new(SqlTypeKind::TimestampTz)),
];

const PG_STAT_WAL_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::new("wal_records", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("wal_fpi", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("wal_bytes", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("wal_buffers_full", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("stats_reset", SqlType::new(SqlTypeKind::TimestampTz)),
];

const PG_STAT_SLRU_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("name"),
    SyntheticSystemViewColumn::new("blks_zeroed", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("blks_hit", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("blks_read", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("blks_written", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("blks_exists", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("flushes", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("truncates", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("stats_reset", SqlType::new(SqlTypeKind::TimestampTz)),
];

const PG_STAT_ARCHIVER_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::new("archived_count", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::text("last_archived_wal"),
    SyntheticSystemViewColumn::new("last_archived_time", SqlType::new(SqlTypeKind::TimestampTz)),
    SyntheticSystemViewColumn::new("failed_count", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::text("last_failed_wal"),
    SyntheticSystemViewColumn::new("last_failed_time", SqlType::new(SqlTypeKind::TimestampTz)),
    SyntheticSystemViewColumn::new("stats_reset", SqlType::new(SqlTypeKind::TimestampTz)),
];

const PG_STAT_BGWRITER_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::new("buffers_clean", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("maxwritten_clean", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("buffers_alloc", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("stats_reset", SqlType::new(SqlTypeKind::TimestampTz)),
];

const PG_STAT_RECOVERY_PREFETCH_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::new("stats_reset", SqlType::new(SqlTypeKind::TimestampTz)),
    SyntheticSystemViewColumn::new("prefetch", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("hit", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("skip_init", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("skip_new", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("skip_fpw", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("skip_rep", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("wal_distance", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::new("block_distance", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::new("io_depth", SqlType::new(SqlTypeKind::Int4)),
];

const PG_STAT_SUBSCRIPTION_STATS_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::new("subid", SqlType::new(SqlTypeKind::Oid)),
    SyntheticSystemViewColumn::new("subname", SqlType::new(SqlTypeKind::Name)),
    SyntheticSystemViewColumn::new("apply_error_count", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("sync_error_count", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("confl_insert_exists", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new(
        "confl_update_origin_differs",
        SqlType::new(SqlTypeKind::Int8),
    ),
    SyntheticSystemViewColumn::new("confl_update_exists", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("confl_update_missing", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new(
        "confl_delete_origin_differs",
        SqlType::new(SqlTypeKind::Int8),
    ),
    SyntheticSystemViewColumn::new("confl_delete_missing", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new(
        "confl_multiple_unique_conflicts",
        SqlType::new(SqlTypeKind::Int8),
    ),
    SyntheticSystemViewColumn::new("stats_reset", SqlType::new(SqlTypeKind::TimestampTz)),
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

const INFORMATION_SCHEMA_ROUTINES_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("specific_schema"),
    SyntheticSystemViewColumn::text("specific_name"),
    SyntheticSystemViewColumn::text("routine_schema"),
    SyntheticSystemViewColumn::text("routine_name"),
];

const INFORMATION_SCHEMA_PARAMETERS_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("specific_schema"),
    SyntheticSystemViewColumn::text("specific_name"),
    SyntheticSystemViewColumn::new("ordinal_position", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::text("parameter_name"),
    SyntheticSystemViewColumn::text("parameter_default"),
];

const INFORMATION_SCHEMA_ROUTINE_ROUTINE_USAGE_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("specific_name"),
    SyntheticSystemViewColumn::text("routine_name"),
];

const INFORMATION_SCHEMA_ROUTINE_SEQUENCE_USAGE_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("routine_schema"),
    SyntheticSystemViewColumn::text("routine_name"),
    SyntheticSystemViewColumn::text("sequence_name"),
];

const INFORMATION_SCHEMA_ROUTINE_COLUMN_USAGE_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("routine_schema"),
    SyntheticSystemViewColumn::text("routine_name"),
    SyntheticSystemViewColumn::text("table_name"),
    SyntheticSystemViewColumn::text("column_name"),
];

const INFORMATION_SCHEMA_ROUTINE_TABLE_USAGE_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("routine_schema"),
    SyntheticSystemViewColumn::text("routine_name"),
    SyntheticSystemViewColumn::text("table_name"),
];

const INFORMATION_SCHEMA_COLUMN_COLUMN_USAGE_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("table_catalog"),
    SyntheticSystemViewColumn::text("table_schema"),
    SyntheticSystemViewColumn::text("table_name"),
    SyntheticSystemViewColumn::text("column_name"),
    SyntheticSystemViewColumn::text("dependent_column"),
];

const INFORMATION_SCHEMA_COLUMN_DOMAIN_USAGE_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("domain_catalog"),
    SyntheticSystemViewColumn::text("domain_schema"),
    SyntheticSystemViewColumn::text("domain_name"),
    SyntheticSystemViewColumn::text("table_catalog"),
    SyntheticSystemViewColumn::text("table_schema"),
    SyntheticSystemViewColumn::text("table_name"),
    SyntheticSystemViewColumn::text("column_name"),
];

const INFORMATION_SCHEMA_DOMAIN_CONSTRAINTS_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("constraint_catalog"),
    SyntheticSystemViewColumn::text("constraint_schema"),
    SyntheticSystemViewColumn::text("constraint_name"),
    SyntheticSystemViewColumn::text("domain_catalog"),
    SyntheticSystemViewColumn::text("domain_schema"),
    SyntheticSystemViewColumn::text("domain_name"),
    SyntheticSystemViewColumn::text("is_deferrable"),
    SyntheticSystemViewColumn::text("initially_deferred"),
];

const INFORMATION_SCHEMA_DOMAINS_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("domain_catalog"),
    SyntheticSystemViewColumn::text("domain_schema"),
    SyntheticSystemViewColumn::text("domain_name"),
    SyntheticSystemViewColumn::text("data_type"),
    SyntheticSystemViewColumn::new("character_maximum_length", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::new("character_octet_length", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::text("character_set_catalog"),
    SyntheticSystemViewColumn::text("character_set_schema"),
    SyntheticSystemViewColumn::text("character_set_name"),
    SyntheticSystemViewColumn::text("collation_catalog"),
    SyntheticSystemViewColumn::text("collation_schema"),
    SyntheticSystemViewColumn::text("collation_name"),
    SyntheticSystemViewColumn::new("numeric_precision", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::new("numeric_precision_radix", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::new("numeric_scale", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::new("datetime_precision", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::text("interval_type"),
    SyntheticSystemViewColumn::new("interval_precision", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::text("domain_default"),
    SyntheticSystemViewColumn::text("udt_catalog"),
    SyntheticSystemViewColumn::text("udt_schema"),
    SyntheticSystemViewColumn::text("udt_name"),
    SyntheticSystemViewColumn::text("scope_catalog"),
    SyntheticSystemViewColumn::text("scope_schema"),
    SyntheticSystemViewColumn::text("scope_name"),
    SyntheticSystemViewColumn::new("maximum_cardinality", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::text("dtd_identifier"),
];

const INFORMATION_SCHEMA_CHECK_CONSTRAINTS_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("constraint_catalog"),
    SyntheticSystemViewColumn::text("constraint_schema"),
    SyntheticSystemViewColumn::text("constraint_name"),
    SyntheticSystemViewColumn::text("check_clause"),
];

const INFORMATION_SCHEMA_FOREIGN_DATA_WRAPPERS_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("foreign_data_wrapper_catalog"),
    SyntheticSystemViewColumn::text("foreign_data_wrapper_name"),
    SyntheticSystemViewColumn::text("authorization_identifier"),
    SyntheticSystemViewColumn::text("library_name"),
    SyntheticSystemViewColumn::text("foreign_data_wrapper_language"),
];

const INFORMATION_SCHEMA_FOREIGN_DATA_WRAPPER_OPTIONS_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("foreign_data_wrapper_catalog"),
    SyntheticSystemViewColumn::text("foreign_data_wrapper_name"),
    SyntheticSystemViewColumn::text("option_name"),
    SyntheticSystemViewColumn::text("option_value"),
];

const INFORMATION_SCHEMA_FOREIGN_SERVERS_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("foreign_server_catalog"),
    SyntheticSystemViewColumn::text("foreign_server_name"),
    SyntheticSystemViewColumn::text("foreign_data_wrapper_catalog"),
    SyntheticSystemViewColumn::text("foreign_data_wrapper_name"),
    SyntheticSystemViewColumn::text("foreign_server_type"),
    SyntheticSystemViewColumn::text("foreign_server_version"),
    SyntheticSystemViewColumn::text("authorization_identifier"),
];

const INFORMATION_SCHEMA_FOREIGN_SERVER_OPTIONS_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("foreign_server_catalog"),
    SyntheticSystemViewColumn::text("foreign_server_name"),
    SyntheticSystemViewColumn::text("option_name"),
    SyntheticSystemViewColumn::text("option_value"),
];

const INFORMATION_SCHEMA_USER_MAPPINGS_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("authorization_identifier"),
    SyntheticSystemViewColumn::text("foreign_server_catalog"),
    SyntheticSystemViewColumn::text("foreign_server_name"),
];

const INFORMATION_SCHEMA_USER_MAPPING_OPTIONS_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("authorization_identifier"),
    SyntheticSystemViewColumn::text("foreign_server_catalog"),
    SyntheticSystemViewColumn::text("foreign_server_name"),
    SyntheticSystemViewColumn::text("option_name"),
    SyntheticSystemViewColumn::text("option_value"),
];

const INFORMATION_SCHEMA_USAGE_PRIVILEGES_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("grantor"),
    SyntheticSystemViewColumn::text("grantee"),
    SyntheticSystemViewColumn::text("object_catalog"),
    SyntheticSystemViewColumn::text("object_schema"),
    SyntheticSystemViewColumn::text("object_name"),
    SyntheticSystemViewColumn::text("object_type"),
    SyntheticSystemViewColumn::text("privilege_type"),
    SyntheticSystemViewColumn::text("is_grantable"),
];

const INFORMATION_SCHEMA_FOREIGN_TABLES_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("foreign_table_catalog"),
    SyntheticSystemViewColumn::text("foreign_table_schema"),
    SyntheticSystemViewColumn::text("foreign_table_name"),
    SyntheticSystemViewColumn::text("foreign_server_catalog"),
    SyntheticSystemViewColumn::text("foreign_server_name"),
];

const INFORMATION_SCHEMA_FOREIGN_TABLE_OPTIONS_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("foreign_table_catalog"),
    SyntheticSystemViewColumn::text("foreign_table_schema"),
    SyntheticSystemViewColumn::text("foreign_table_name"),
    SyntheticSystemViewColumn::text("option_name"),
    SyntheticSystemViewColumn::text("option_value"),
];

const PG_SEQUENCES_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("schemaname"),
    SyntheticSystemViewColumn::text("sequencename"),
    SyntheticSystemViewColumn::text("sequenceowner"),
    SyntheticSystemViewColumn::text("data_type"),
    SyntheticSystemViewColumn::new("start_value", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("min_value", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("max_value", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("increment_by", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("cycle", SqlType::new(SqlTypeKind::Bool)),
    SyntheticSystemViewColumn::new("cache_size", SqlType::new(SqlTypeKind::Int8)),
    SyntheticSystemViewColumn::new("last_value", SqlType::new(SqlTypeKind::Int8)),
];

const INFORMATION_SCHEMA_SEQUENCES_COLUMNS: &[SyntheticSystemViewColumn] = &[
    SyntheticSystemViewColumn::text("sequence_catalog"),
    SyntheticSystemViewColumn::text("sequence_schema"),
    SyntheticSystemViewColumn::text("sequence_name"),
    SyntheticSystemViewColumn::text("data_type"),
    SyntheticSystemViewColumn::new("numeric_precision", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::new("numeric_precision_radix", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::new("numeric_scale", SqlType::new(SqlTypeKind::Int4)),
    SyntheticSystemViewColumn::text("start_value"),
    SyntheticSystemViewColumn::text("minimum_value"),
    SyntheticSystemViewColumn::text("maximum_value"),
    SyntheticSystemViewColumn::text("increment"),
    SyntheticSystemViewColumn::text("cycle_option"),
];

const SYNTHETIC_SYSTEM_VIEWS: &[SyntheticSystemView] = &[
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
        kind: SyntheticSystemViewKind::PgConstraint,
        canonical_name: "pg_catalog.pg_constraint",
        aliases: PG_CONSTRAINT_ALIASES,
        columns: PG_CONSTRAINT_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgInitPrivs,
        canonical_name: "pg_catalog.pg_init_privs",
        aliases: PG_INIT_PRIVS_ALIASES,
        columns: PG_INIT_PRIVS_COLUMNS,
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
        kind: SyntheticSystemViewKind::PgTables,
        canonical_name: "pg_catalog.pg_tables",
        aliases: PG_TABLES_ALIASES,
        columns: PG_TABLES_COLUMNS,
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
        kind: SyntheticSystemViewKind::PgPublicationTables,
        canonical_name: "pg_catalog.pg_publication_tables",
        aliases: PG_PUBLICATION_TABLES_ALIASES,
        columns: PG_PUBLICATION_TABLES_COLUMNS,
        view_definition_sql: PG_PUBLICATION_TABLES_DEFINITION_SQL,
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgSequences,
        canonical_name: "pg_catalog.pg_sequences",
        aliases: PG_SEQUENCES_ALIASES,
        columns: PG_SEQUENCES_COLUMNS,
        view_definition_sql: "",
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
        kind: SyntheticSystemViewKind::PgStatsExt,
        canonical_name: "pg_catalog.pg_stats_ext",
        aliases: PG_STATS_EXT_ALIASES,
        columns: PG_STATS_EXT_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgStatsExtExprs,
        canonical_name: "pg_catalog.pg_stats_ext_exprs",
        aliases: PG_STATS_EXT_EXPRS_ALIASES,
        columns: PG_STATS_EXT_EXPRS_COLUMNS,
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
        kind: SyntheticSystemViewKind::PgAvailableExtensions,
        canonical_name: "pg_catalog.pg_available_extensions",
        aliases: PG_AVAILABLE_EXTENSIONS_ALIASES,
        columns: PG_AVAILABLE_EXTENSIONS_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgAvailableExtensionVersions,
        canonical_name: "pg_catalog.pg_available_extension_versions",
        aliases: PG_AVAILABLE_EXTENSION_VERSIONS_ALIASES,
        columns: PG_AVAILABLE_EXTENSION_VERSIONS_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgBackendMemoryContexts,
        canonical_name: "pg_catalog.pg_backend_memory_contexts",
        aliases: PG_BACKEND_MEMORY_CONTEXTS_ALIASES,
        columns: PG_BACKEND_MEMORY_CONTEXTS_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgShmemAllocationsNuma,
        canonical_name: "pg_catalog.pg_shmem_allocations_numa",
        aliases: PG_SHMEM_ALLOCATIONS_NUMA_ALIASES,
        columns: PG_SHMEM_ALLOCATIONS_NUMA_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgConfig,
        canonical_name: "pg_catalog.pg_config",
        aliases: PG_CONFIG_ALIASES,
        columns: PG_CONFIG_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgCursors,
        canonical_name: "pg_catalog.pg_cursors",
        aliases: PG_CURSORS_ALIASES,
        columns: PG_CURSORS_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgFileSettings,
        canonical_name: "pg_catalog.pg_file_settings",
        aliases: PG_FILE_SETTINGS_ALIASES,
        columns: PG_FILE_SETTINGS_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgHbaFileRules,
        canonical_name: "pg_catalog.pg_hba_file_rules",
        aliases: PG_HBA_FILE_RULES_ALIASES,
        columns: PG_HBA_FILE_RULES_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgIdentFileMappings,
        canonical_name: "pg_catalog.pg_ident_file_mappings",
        aliases: PG_IDENT_FILE_MAPPINGS_ALIASES,
        columns: PG_IDENT_FILE_MAPPINGS_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgPreparedXacts,
        canonical_name: "pg_catalog.pg_prepared_xacts",
        aliases: PG_PREPARED_XACTS_ALIASES,
        columns: PG_PREPARED_XACTS_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgPreparedStatements,
        canonical_name: "pg_catalog.pg_prepared_statements",
        aliases: PG_PREPARED_STATEMENTS_ALIASES,
        columns: PG_PREPARED_STATEMENTS_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgStatWalReceiver,
        canonical_name: "pg_catalog.pg_stat_wal_receiver",
        aliases: PG_STAT_WAL_RECEIVER_ALIASES,
        columns: PG_STAT_WAL_RECEIVER_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgWaitEvents,
        canonical_name: "pg_catalog.pg_wait_events",
        aliases: PG_WAIT_EVENTS_ALIASES,
        columns: PG_WAIT_EVENTS_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgTimezoneNames,
        canonical_name: "pg_catalog.pg_timezone_names",
        aliases: PG_TIMEZONE_NAMES_ALIASES,
        columns: PG_TIMEZONE_NAMES_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgTimezoneAbbrevs,
        canonical_name: "pg_catalog.pg_timezone_abbrevs",
        aliases: PG_TIMEZONE_ABBREVS_ALIASES,
        columns: PG_TIMEZONE_ABBREVS_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgUserMappings,
        canonical_name: "pg_catalog.pg_user_mappings",
        aliases: PG_USER_MAPPINGS_ALIASES,
        columns: PG_USER_MAPPINGS_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgRoles,
        canonical_name: "pg_catalog.pg_roles",
        aliases: PG_ROLES_ALIASES,
        columns: PG_ROLES_COLUMNS,
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
        kind: SyntheticSystemViewKind::PgStatDatabase,
        canonical_name: "pg_catalog.pg_stat_database",
        aliases: PG_STAT_DATABASE_ALIASES,
        columns: PG_STAT_DATABASE_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgStatCheckpointer,
        canonical_name: "pg_catalog.pg_stat_checkpointer",
        aliases: PG_STAT_CHECKPOINTER_ALIASES,
        columns: PG_STAT_CHECKPOINTER_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgStatWal,
        canonical_name: "pg_catalog.pg_stat_wal",
        aliases: PG_STAT_WAL_ALIASES,
        columns: PG_STAT_WAL_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgStatSlru,
        canonical_name: "pg_catalog.pg_stat_slru",
        aliases: PG_STAT_SLRU_ALIASES,
        columns: PG_STAT_SLRU_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgStatArchiver,
        canonical_name: "pg_catalog.pg_stat_archiver",
        aliases: PG_STAT_ARCHIVER_ALIASES,
        columns: PG_STAT_ARCHIVER_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgStatBgwriter,
        canonical_name: "pg_catalog.pg_stat_bgwriter",
        aliases: PG_STAT_BGWRITER_ALIASES,
        columns: PG_STAT_BGWRITER_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgStatRecoveryPrefetch,
        canonical_name: "pg_catalog.pg_stat_recovery_prefetch",
        aliases: PG_STAT_RECOVERY_PREFETCH_ALIASES,
        columns: PG_STAT_RECOVERY_PREFETCH_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::PgStatSubscriptionStats,
        canonical_name: "pg_catalog.pg_stat_subscription_stats",
        aliases: PG_STAT_SUBSCRIPTION_STATS_ALIASES,
        columns: PG_STAT_SUBSCRIPTION_STATS_COLUMNS,
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
        kind: SyntheticSystemViewKind::InformationSchemaSequences,
        canonical_name: "information_schema.sequences",
        aliases: INFORMATION_SCHEMA_SEQUENCES_ALIASES,
        columns: INFORMATION_SCHEMA_SEQUENCES_COLUMNS,
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
        kind: SyntheticSystemViewKind::InformationSchemaRoutines,
        canonical_name: "information_schema.routines",
        aliases: INFORMATION_SCHEMA_ROUTINES_ALIASES,
        columns: INFORMATION_SCHEMA_ROUTINES_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::InformationSchemaParameters,
        canonical_name: "information_schema.parameters",
        aliases: INFORMATION_SCHEMA_PARAMETERS_ALIASES,
        columns: INFORMATION_SCHEMA_PARAMETERS_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::InformationSchemaRoutineRoutineUsage,
        canonical_name: "information_schema.routine_routine_usage",
        aliases: INFORMATION_SCHEMA_ROUTINE_ROUTINE_USAGE_ALIASES,
        columns: INFORMATION_SCHEMA_ROUTINE_ROUTINE_USAGE_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::InformationSchemaRoutineSequenceUsage,
        canonical_name: "information_schema.routine_sequence_usage",
        aliases: INFORMATION_SCHEMA_ROUTINE_SEQUENCE_USAGE_ALIASES,
        columns: INFORMATION_SCHEMA_ROUTINE_SEQUENCE_USAGE_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::InformationSchemaRoutineColumnUsage,
        canonical_name: "information_schema.routine_column_usage",
        aliases: INFORMATION_SCHEMA_ROUTINE_COLUMN_USAGE_ALIASES,
        columns: INFORMATION_SCHEMA_ROUTINE_COLUMN_USAGE_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::InformationSchemaRoutineTableUsage,
        canonical_name: "information_schema.routine_table_usage",
        aliases: INFORMATION_SCHEMA_ROUTINE_TABLE_USAGE_ALIASES,
        columns: INFORMATION_SCHEMA_ROUTINE_TABLE_USAGE_COLUMNS,
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
        kind: SyntheticSystemViewKind::InformationSchemaColumnDomainUsage,
        canonical_name: "information_schema.column_domain_usage",
        aliases: INFORMATION_SCHEMA_COLUMN_DOMAIN_USAGE_ALIASES,
        columns: INFORMATION_SCHEMA_COLUMN_DOMAIN_USAGE_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::InformationSchemaDomainConstraints,
        canonical_name: "information_schema.domain_constraints",
        aliases: INFORMATION_SCHEMA_DOMAIN_CONSTRAINTS_ALIASES,
        columns: INFORMATION_SCHEMA_DOMAIN_CONSTRAINTS_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::InformationSchemaDomains,
        canonical_name: "information_schema.domains",
        aliases: INFORMATION_SCHEMA_DOMAINS_ALIASES,
        columns: INFORMATION_SCHEMA_DOMAINS_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::InformationSchemaCheckConstraints,
        canonical_name: "information_schema.check_constraints",
        aliases: INFORMATION_SCHEMA_CHECK_CONSTRAINTS_ALIASES,
        columns: INFORMATION_SCHEMA_CHECK_CONSTRAINTS_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::InformationSchemaTriggers,
        canonical_name: "information_schema.triggers",
        aliases: INFORMATION_SCHEMA_TRIGGERS_ALIASES,
        columns: INFORMATION_SCHEMA_TRIGGERS_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::InformationSchemaForeignDataWrappers,
        canonical_name: "information_schema.foreign_data_wrappers",
        aliases: INFORMATION_SCHEMA_FOREIGN_DATA_WRAPPERS_ALIASES,
        columns: INFORMATION_SCHEMA_FOREIGN_DATA_WRAPPERS_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::InformationSchemaForeignDataWrapperOptions,
        canonical_name: "information_schema.foreign_data_wrapper_options",
        aliases: INFORMATION_SCHEMA_FOREIGN_DATA_WRAPPER_OPTIONS_ALIASES,
        columns: INFORMATION_SCHEMA_FOREIGN_DATA_WRAPPER_OPTIONS_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::InformationSchemaForeignServers,
        canonical_name: "information_schema.foreign_servers",
        aliases: INFORMATION_SCHEMA_FOREIGN_SERVERS_ALIASES,
        columns: INFORMATION_SCHEMA_FOREIGN_SERVERS_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::InformationSchemaForeignServerOptions,
        canonical_name: "information_schema.foreign_server_options",
        aliases: INFORMATION_SCHEMA_FOREIGN_SERVER_OPTIONS_ALIASES,
        columns: INFORMATION_SCHEMA_FOREIGN_SERVER_OPTIONS_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::InformationSchemaUserMappings,
        canonical_name: "information_schema.user_mappings",
        aliases: INFORMATION_SCHEMA_USER_MAPPINGS_ALIASES,
        columns: INFORMATION_SCHEMA_USER_MAPPINGS_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::InformationSchemaUserMappingOptions,
        canonical_name: "information_schema.user_mapping_options",
        aliases: INFORMATION_SCHEMA_USER_MAPPING_OPTIONS_ALIASES,
        columns: INFORMATION_SCHEMA_USER_MAPPING_OPTIONS_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::InformationSchemaUsagePrivileges,
        canonical_name: "information_schema.usage_privileges",
        aliases: INFORMATION_SCHEMA_USAGE_PRIVILEGES_ALIASES,
        columns: INFORMATION_SCHEMA_USAGE_PRIVILEGES_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::InformationSchemaRoleUsageGrants,
        canonical_name: "information_schema.role_usage_grants",
        aliases: INFORMATION_SCHEMA_ROLE_USAGE_GRANTS_ALIASES,
        columns: INFORMATION_SCHEMA_USAGE_PRIVILEGES_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::InformationSchemaForeignTables,
        canonical_name: "information_schema.foreign_tables",
        aliases: INFORMATION_SCHEMA_FOREIGN_TABLES_ALIASES,
        columns: INFORMATION_SCHEMA_FOREIGN_TABLES_COLUMNS,
        view_definition_sql: "",
    },
    SyntheticSystemView {
        kind: SyntheticSystemViewKind::InformationSchemaForeignTableOptions,
        canonical_name: "information_schema.foreign_table_options",
        aliases: INFORMATION_SCHEMA_FOREIGN_TABLE_OPTIONS_ALIASES,
        columns: INFORMATION_SCHEMA_FOREIGN_TABLE_OPTIONS_COLUMNS,
        view_definition_sql: "",
    },
];
