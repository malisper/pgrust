use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;

mod async_notify;
mod catalog_access;
pub(crate) mod commands;
pub(crate) mod ddl;
pub(crate) mod foreign_keys;
mod large_objects;
mod relation_refs;
mod sequences;
mod stats_import;
mod temp;
mod toast;
mod txn;

use crate::backend::access::transam::xact::{
    CommandId, MvccError, TransactionId, TransactionManager,
};
use crate::backend::access::transam::xlog::{WalBgWriter, WalError, WalWriter};
use crate::backend::access::transam::{
    CheckpointCommitBarrier, CheckpointCommitGuard, CheckpointRequestFlags, Checkpointer,
    ControlFileError,
};
use crate::backend::catalog::catalog::{CatalogIndexBuildOptions, column_desc};
use crate::backend::catalog::indexing::rebuild_system_catalog_indexes_in_pool;
use crate::backend::catalog::namespace::effective_search_path as namespace_effective_search_path;
use crate::backend::catalog::rows::physical_catalog_rows_from_catcache;
use crate::backend::catalog::store::{CatalogMutationEffect, CatalogWriteContext};
use crate::backend::catalog::toasting::ToastCatalogChanges;
use crate::backend::catalog::{CatalogError, CatalogStore};
use crate::backend::catalog::{
    bootstrap::bootstrap_catalog_kinds, persistence::sync_catalog_rows_subset_in_pool,
};
use crate::backend::commands::analyze::collect_analyze_stats;
use crate::backend::executor::{
    ExecError, ExecutorContext, LockStatusProvider, SessionReplicationRole, StatementResult, Value,
    execute_readonly_statement,
};
use crate::backend::parser::Statement;
use crate::backend::parser::{
    AlterSequenceStatement, AlterTableAddColumnStatement, AlterTableDropColumnStatement,
    AlterTableRenameColumnStatement, AlterTableRenameStatement, AnalyzeStatement, CatalogLookup,
    CommentOnColumnStatement, CommentOnConstraintStatement, CommentOnDomainStatement,
    CommentOnTableStatement, CommentOnTypeStatement, CreateCompositeTypeStatement,
    CreateDomainStatement, CreateIndexStatement, CreateSchemaStatement, CreateSequenceStatement,
    CreateTableAsStatement, CreateTableStatement, CreateViewStatement, DropDomainStatement,
    DropSequenceStatement, DropViewStatement, OnCommitAction, ParseError, SqlType, SqlTypeKind,
    TablePersistence, bind_delete, bind_insert, bind_update, create_relation_desc,
    lower_create_table_with_catalog, normalize_create_table_as_name, normalize_create_table_name,
    normalize_create_view_name,
};
use crate::backend::storage::lmgr::{
    AdvisoryLockKey, AdvisoryLockManager, AdvisoryLockSnapshotRow, RowLockManager,
    RowLockSnapshotRow, TableLockManager, TableLockMode, TableLockSnapshotRow,
    TransactionLockSnapshotRow, lock_relations_interruptible, lock_tables_interruptible,
    unlock_relations,
};
use crate::backend::storage::smgr::{RelFileLocator, StorageManager};
pub use crate::backend::utils::activity::{DatabaseStatsStore, SessionStatsState};
#[allow(unused_imports)]
pub(crate) use crate::backend::utils::activity::{
    FunctionStatsDelta, FunctionStatsEntry, IoStatsEntry, IoStatsKey, RelationStatsDelta,
    RelationStatsEntry, StatsDelta, StatsFetchConsistency, StatsMutationEffect,
    TrackFunctionsSetting, default_pg_stat_io_keys, now_timestamptz,
};
use crate::backend::utils::cache::catcache::CatCache;
use crate::backend::utils::cache::inval::{
    CatalogInvalidation, accept_invalidation_messages, catalog_invalidation_from_effect,
    finalize_aborted_local_catalog_invalidations, finalize_command_end_local_catalog_invalidations,
    finalize_committed_catalog_effects,
};
use crate::backend::utils::cache::lsyscache::{
    LazyCatalogLookup, access_method_name_for_relation, constraint_rows_for_relation,
    describe_relation_by_oid, has_index_on_relation, relation_display_name,
    relation_namespace_name,
};
use crate::backend::utils::cache::plancache::PlanCache;
use crate::backend::utils::cache::relcache::RelCacheEntry;
use crate::backend::utils::cache::syscache::{
    BackendCacheState, backend_catcache as syscache_backend_catcache,
    invalidate_backend_cache_state,
};
use crate::backend::utils::misc::checkpoint::{CheckpointConfig, CheckpointStatsSnapshot};
use crate::backend::utils::misc::interrupts::InterruptState;
use crate::include::access::htup::{AttributeAlign, AttributeStorage};
use crate::include::catalog::{
    BOOTSTRAP_SUPERUSER_OID, CURRENT_DATABASE_NAME, PUBLIC_NAMESPACE_OID, PgConstraintRow,
    PgEnumRow, PgRangeRow, PgTypeRow, RangeCanonicalization, annotate_catalog_type_io_procs,
    builtin_type_row_by_oid, synthetic_type_output_proc_oid, system_catalog_indexes,
};
use crate::pgrust::auth::{AuthCatalog, AuthState};
pub use crate::pgrust::autovacuum::AutovacuumConfig;
use crate::pgrust::cluster::{Cluster, ClusterShared, SessionActivityEntry, SessionActivityState};
use crate::pl::plpgsql::PlpgsqlFunctionCache;
use crate::{BufferPool, ClientId, SmgrStorageBackend};
use ddl::{
    ensure_can_set_role, ensure_relation_owner, map_catalog_error,
    reject_column_with_foreign_key_dependencies, reject_index_with_referencing_foreign_keys,
    reject_inheritance_tree_ddl, reject_relation_with_dependent_views,
    validate_alter_table_add_column,
};
pub(crate) use large_objects::LargeObjectRuntime;
use relation_refs::{collect_direct_relation_oids_from_select, collect_rels_from_planned_stmt};
pub(crate) use sequences::{
    SequenceData, SequenceMutationEffect, SequenceOwnedByRef, SequenceRuntime,
    default_sequence_name_base, default_sequence_oid_from_default_expr, format_nextval_default_oid,
    initial_sequence_state, resolve_sequence_options_spec, sequence_type_oid_for_serial_kind,
};
use toast::{toast_bindings_from_create_result, toast_bindings_from_temp_relation};
use txn::AutoCommitGuard;

#[derive(Debug)]
pub enum DatabaseError {
    Catalog(CatalogError),
    Control(ControlFileError),
    Mvcc(MvccError),
    Wal(WalError),
}

impl From<CatalogError> for DatabaseError {
    fn from(e: CatalogError) -> Self {
        Self::Catalog(e)
    }
}

impl From<MvccError> for DatabaseError {
    fn from(e: MvccError) -> Self {
        Self::Mvcc(e)
    }
}

impl From<ControlFileError> for DatabaseError {
    fn from(e: ControlFileError) -> Self {
        Self::Control(e)
    }
}

pub use crate::backend::storage::lmgr::TransactionWaiter;
pub use crate::pgrust::session::{SelectGuard, Session};
pub(crate) use async_notify::{
    AsyncListenAction, AsyncListenOp, AsyncNotifyRuntime, PendingNotification,
    merge_pending_notifications, queue_pending_notification,
};
pub(crate) use ddl::reject_relation_with_referencing_foreign_keys;
pub(crate) use foreign_keys::{
    alter_table_add_constraint_lock_requests, alter_table_validate_constraint_lock_requests,
    delete_foreign_key_lock_requests, execute_set_constraints, insert_foreign_key_lock_requests,
    merge_table_lock_requests, prepared_insert_foreign_key_lock_requests,
    relation_foreign_key_lock_requests, table_lock_relations, update_foreign_key_lock_requests,
    validate_deferred_constraints, validate_immediate_constraints,
};

pub(crate) const LOGICAL_RELATION_LOCK_SPC_OID: u32 = u32::MAX;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DatabaseOpenOptions {
    pub pool_size: usize,
    pub durable_shutdown: bool,
    pub autovacuum: AutovacuumConfig,
}

impl DatabaseOpenOptions {
    pub fn new(pool_size: usize) -> Self {
        Self {
            pool_size,
            durable_shutdown: true,
            autovacuum: if cfg!(test) {
                AutovacuumConfig::test_default()
            } else {
                AutovacuumConfig::production_default()
            },
        }
    }

    pub fn for_tests(pool_size: usize) -> Self {
        Self {
            pool_size,
            durable_shutdown: false,
            autovacuum: AutovacuumConfig::test_default(),
        }
    }

    pub fn with_autovacuum_config(mut self, autovacuum: AutovacuumConfig) -> Self {
        self.autovacuum = autovacuum;
        self
    }
}

#[derive(Clone)]
pub struct Database {
    pub(crate) cluster: Arc<ClusterShared>,
    pub database_oid: u32,
    pub pool: Arc<BufferPool<SmgrStorageBackend>>,
    pub wal: Option<Arc<WalWriter>>,
    pub autovacuum_config: AutovacuumConfig,
    pub checkpoint_config: Arc<CheckpointConfig>,
    pub checkpoint_stats: Arc<RwLock<CheckpointStatsSnapshot>>,
    pub checkpoint_commit_barrier: Arc<CheckpointCommitBarrier>,
    pub checkpointer: Option<Arc<Checkpointer>>,
    pub txns: Arc<RwLock<TransactionManager>>,
    pub shared_catalog: Arc<RwLock<CatalogStore>>,
    pub catalog: Arc<RwLock<CatalogStore>>,
    pub txn_waiter: Arc<TransactionWaiter>,
    pub table_locks: Arc<TableLockManager>,
    pub plan_cache: Arc<PlanCache>,
    pub(crate) backend_cache_states: Arc<RwLock<HashMap<ClientId, BackendCacheState>>>,
    pub(crate) session_interrupt_states: Arc<RwLock<HashMap<ClientId, Arc<InterruptState>>>>,
    pub(crate) session_auth_states: Arc<RwLock<HashMap<ClientId, AuthState>>>,
    pub(crate) session_row_security_states: Arc<RwLock<HashMap<ClientId, bool>>>,
    pub(crate) session_replication_role_states:
        Arc<RwLock<HashMap<ClientId, SessionReplicationRole>>>,
    pub(crate) session_stats_states: Arc<RwLock<HashMap<ClientId, Arc<RwLock<SessionStatsState>>>>>,
    pub(crate) session_plpgsql_function_caches:
        Arc<RwLock<HashMap<ClientId, Arc<RwLock<PlpgsqlFunctionCache>>>>>,
    pub(crate) session_temp_backend_ids: Arc<RwLock<HashMap<ClientId, TempBackendId>>>,
    pub(crate) database_create_grants: Arc<RwLock<Vec<DatabaseCreateGrant>>>,
    pub(crate) temp_relations: Arc<RwLock<HashMap<TempBackendId, TempNamespace>>>,
    pub(crate) domains: Arc<RwLock<BTreeMap<String, DomainEntry>>>,
    pub(crate) enum_types: Arc<RwLock<BTreeMap<String, EnumTypeEntry>>>,
    pub(crate) range_types: Arc<RwLock<BTreeMap<String, RangeTypeEntry>>>,
    pub(crate) base_types: Arc<RwLock<BTreeMap<u32, BaseTypeEntry>>>,
    pub(crate) conversions: Arc<RwLock<BTreeMap<String, ConversionEntry>>>,
    pub(crate) statistics_objects: Arc<RwLock<BTreeMap<String, StatisticsObjectEntry>>>,
    pub(crate) sequences: Arc<SequenceRuntime>,
    pub(crate) advisory_locks: Arc<AdvisoryLockManager>,
    pub(crate) row_locks: Arc<RowLockManager>,
    pub(crate) async_notify_runtime: Arc<AsyncNotifyRuntime>,
    pub(crate) stats: Arc<RwLock<DatabaseStatsStore>>,
    pub(crate) large_objects: Arc<LargeObjectRuntime>,
    pub(crate) _wal_bg_writer: Option<Arc<WalBgWriter>>,
}

const TEMP_DB_OID_BASE: u32 = 0x7000_0000;
const TEMP_TOAST_NAMESPACE_OID_BASE: u32 = 0x7800_0000;
pub(crate) type TempBackendId = u32;
type CatalogTxnContext = Option<(TransactionId, CommandId)>;

#[derive(Debug, Clone)]
pub(crate) struct TempCatalogEntry {
    pub entry: RelCacheEntry,
    pub on_commit: OnCommitAction,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct TempNamespace {
    pub oid: u32,
    pub name: String,
    pub owner_oid: u32,
    pub toast_oid: u32,
    pub toast_name: String,
    pub tables: BTreeMap<String, TempCatalogEntry>,
    pub generation: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct CreatedTempRelation {
    entry: RelCacheEntry,
    toast: Option<ToastCatalogChanges>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DomainEntry {
    pub oid: u32,
    pub array_oid: u32,
    pub name: String,
    pub namespace_oid: u32,
    pub sql_type: SqlType,
    pub default: Option<String>,
    pub check: Option<String>,
    pub not_null: bool,
    pub enum_check: Option<DomainCheckEntry>,
    pub typacl: Option<Vec<String>>,
    pub comment: Option<String>,
}

const DOMAIN_IN_PROC_OID: u32 = 2597;
const DOMAIN_RECV_PROC_OID: u32 = 2598;
const ARRAY_IN_PROC_OID: u32 = 750;
const ARRAY_OUT_PROC_OID: u32 = 751;
const ARRAY_RECV_PROC_OID: u32 = 2400;
const ARRAY_SEND_PROC_OID: u32 = 2401;
const ARRAY_TYPANALYZE_PROC_OID: u32 = 3816;
const ARRAY_SUBSCRIPT_HANDLER_PROC_OID: u32 = 6179;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DomainCheckEntry {
    pub name: String,
    pub allowed_enum_label_oids: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BaseTypeEntry {
    pub oid: u32,
    pub array_oid: u32,
    pub input_proc_oid: u32,
    pub output_proc_oid: u32,
    pub receive_proc_oid: u32,
    pub send_proc_oid: u32,
    pub typmodin_proc_oid: u32,
    pub typmodout_proc_oid: u32,
    pub analyze_proc_oid: u32,
    pub subscript_proc_oid: u32,
    pub typstorage: AttributeStorage,
    pub default: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EnumLabelEntry {
    pub oid: u32,
    pub label: String,
    pub sort_order: f64,
    pub committed: bool,
    pub creating_xid: Option<TransactionId>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EnumTypeEntry {
    pub oid: u32,
    pub array_oid: u32,
    pub name: String,
    pub namespace_oid: u32,
    pub labels: Vec<EnumLabelEntry>,
    pub creating_xid: Option<TransactionId>,
    pub typacl: Option<Vec<String>>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RangeTypeEntry {
    pub oid: u32,
    pub array_oid: u32,
    pub multirange_oid: u32,
    pub multirange_array_oid: u32,
    pub name: String,
    pub multirange_name: String,
    pub namespace_oid: u32,
    pub owner_oid: u32,
    pub public_usage: bool,
    pub owner_usage: bool,
    pub subtype: SqlType,
    #[serde(default)]
    pub subtype_dependency_oid: Option<u32>,
    // :HACK: Stored for catalog compatibility; range comparison still uses the subtype's
    // default ordering until opclass-specific range support is wired through execution.
    pub subtype_opclass: Option<String>,
    pub subtype_diff: Option<String>,
    pub collation: Option<String>,
    pub typacl: Option<Vec<String>>,
    pub comment: Option<String>,
}

pub(crate) fn load_range_type_entries(
    base_dir: &Path,
    database_oid: u32,
) -> Result<BTreeMap<String, RangeTypeEntry>, DatabaseError> {
    let path = range_types_file_path(base_dir, database_oid);
    if !path.exists() {
        return Ok(BTreeMap::new());
    }
    let text = std::fs::read_to_string(&path)
        .map_err(|err| DatabaseError::Catalog(CatalogError::Io(err.to_string())))?;
    serde_json::from_str(&text).map_err(|_| {
        DatabaseError::Catalog(CatalogError::Corrupt("invalid range type metadata file"))
    })
}

pub(crate) fn save_range_type_entries(
    base_dir: &Path,
    database_oid: u32,
    range_types: &BTreeMap<String, RangeTypeEntry>,
) -> Result<(), ExecError> {
    let path = range_types_file_path(base_dir, database_oid);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(range_type_metadata_io_error)?;
    }
    let text = serde_json::to_string_pretty(range_types).map_err(|err| {
        ExecError::Parse(ParseError::UnexpectedToken {
            expected: "range type metadata serialization",
            actual: err.to_string(),
        })
    })?;
    std::fs::write(path, text).map_err(range_type_metadata_io_error)
}

fn range_types_file_path(base_dir: &Path, database_oid: u32) -> PathBuf {
    base_dir
        .join("base")
        .join(database_oid.to_string())
        .join("pg_pgrust_range_types.json")
}

fn range_type_metadata_io_error(error: std::io::Error) -> ExecError {
    ExecError::Parse(ParseError::UnexpectedToken {
        expected: "range type metadata persistence",
        actual: error.to_string(),
    })
}

#[derive(Debug, Clone)]
pub(crate) struct DynamicTypeSnapshot {
    pub domains: BTreeMap<String, DomainEntry>,
    pub enum_types: BTreeMap<String, EnumTypeEntry>,
    pub range_types: BTreeMap<String, RangeTypeEntry>,
    pub base_types: BTreeMap<u32, BaseTypeEntry>,
}

fn domain_sql_type(domain: &DomainEntry) -> SqlType {
    if domain.enum_check.is_some() && matches!(domain.sql_type.kind, SqlTypeKind::Enum) {
        return domain
            .sql_type
            .with_identity(domain.oid, domain.sql_type.type_oid);
    }
    domain
        .sql_type
        .with_identity(domain.oid, domain.sql_type.type_oid)
}

fn dynamic_range_array_type_names(
    range_types: &BTreeMap<String, RangeTypeEntry>,
) -> BTreeMap<u32, String> {
    let non_array_type_names = range_types
        .values()
        .flat_map(|entry| {
            [
                (entry.namespace_oid, entry.name.to_ascii_lowercase()),
                (
                    entry.namespace_oid,
                    entry.multirange_name.to_ascii_lowercase(),
                ),
            ]
        })
        .collect::<BTreeSet<_>>();
    let mut used_array_type_names = BTreeSet::new();
    let mut names = BTreeMap::new();

    for entry in range_types.values() {
        let array_name = reserve_dynamic_array_type_name(
            &entry.name,
            entry.namespace_oid,
            &non_array_type_names,
            &mut used_array_type_names,
        );
        names.insert(entry.array_oid, array_name);
        let multirange_array_name = reserve_dynamic_array_type_name(
            &entry.multirange_name,
            entry.namespace_oid,
            &non_array_type_names,
            &mut used_array_type_names,
        );
        names.insert(entry.multirange_array_oid, multirange_array_name);
    }

    names
}

fn reserve_dynamic_array_type_name(
    type_name: &str,
    namespace_oid: u32,
    non_array_type_names: &BTreeSet<(u32, String)>,
    used_array_type_names: &mut BTreeSet<(u32, String)>,
) -> String {
    let mut candidate = format!("_{type_name}");
    while non_array_type_names.contains(&(namespace_oid, candidate.to_ascii_lowercase()))
        || used_array_type_names.contains(&(namespace_oid, candidate.to_ascii_lowercase()))
    {
        candidate.insert(0, '_');
    }
    used_array_type_names.insert((namespace_oid, candidate.to_ascii_lowercase()));
    candidate
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StatisticsObjectEntry {
    pub oid: u32,
    pub name: String,
    pub namespace_oid: u32,
    pub relation_name: String,
    pub relation_oid: u32,
    pub statistics_target: i16,
    pub kinds: Vec<String>,
    pub targets: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConversionEntry {
    pub oid: u32,
    pub name: String,
    pub namespace_oid: u32,
    pub for_encoding: String,
    pub to_encoding: String,
    pub function_name: String,
    pub is_default: bool,
    pub owner_oid: u32,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DatabaseCreateGrant {
    pub grantee_oid: u32,
    pub grantor_oid: u32,
    pub grant_option: bool,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) enum TempMutationEffect {
    Create {
        name: String,
        entry: RelCacheEntry,
        on_commit: OnCommitAction,
        namespace_created: bool,
    },
    Drop {
        name: String,
        entry: RelCacheEntry,
        on_commit: OnCommitAction,
    },
    Rename {
        old_name: String,
        new_name: String,
    },
}

impl Database {
    #[cfg(test)]
    pub fn open(base_dir: impl Into<PathBuf>, pool_size: usize) -> Result<Self, DatabaseError> {
        Self::open_with_options(base_dir, DatabaseOpenOptions::for_tests(pool_size))
    }

    #[cfg(not(test))]
    pub fn open(base_dir: impl Into<PathBuf>, pool_size: usize) -> Result<Self, DatabaseError> {
        Self::open_with_options(base_dir, DatabaseOpenOptions::new(pool_size))
    }

    pub fn open_with_options(
        base_dir: impl Into<PathBuf>,
        options: DatabaseOpenOptions,
    ) -> Result<Self, DatabaseError> {
        Cluster::open_with_options(base_dir.into(), options)?
            .connect_database("postgres")
            .map_err(|e| match e {
                ExecError::DetailedError { message, .. } => {
                    DatabaseError::Catalog(CatalogError::Io(message))
                }
                other => DatabaseError::Catalog(CatalogError::Io(format!("{other:?}"))),
            })
    }

    pub fn open_ephemeral(pool_size: usize) -> Result<Self, DatabaseError> {
        Cluster::open_ephemeral(pool_size)?
            .connect_database("postgres")
            .map_err(|e| match e {
                ExecError::DetailedError { message, .. } => {
                    DatabaseError::Catalog(CatalogError::Io(message))
                }
                other => DatabaseError::Catalog(CatalogError::Io(format!("{other:?}"))),
            })
    }

    pub(crate) fn install_interrupt_state(
        &self,
        client_id: ClientId,
        interrupts: Arc<InterruptState>,
    ) {
        self.session_interrupt_states
            .write()
            .insert(client_id, interrupts);
    }

    pub(crate) fn interrupt_state(&self, client_id: ClientId) -> Arc<InterruptState> {
        self.session_interrupt_states
            .read()
            .get(&client_id)
            .cloned()
            .unwrap_or_else(|| Arc::new(InterruptState::new()))
    }

    pub(crate) fn allocate_statement_lock_scope_id(&self) -> u64 {
        self.cluster
            .open_databases
            .read()
            .get(&self.database_oid)
            .map(|state| {
                state
                    .next_statement_lock_scope_id
                    .fetch_add(1, Ordering::Relaxed)
            })
            .unwrap_or(1)
    }

    pub(crate) fn install_auth_state(&self, client_id: ClientId, auth: AuthState) {
        self.session_auth_states.write().insert(client_id, auth);
    }

    pub(crate) fn install_row_security_enabled(&self, client_id: ClientId, enabled: bool) {
        self.session_row_security_states
            .write()
            .insert(client_id, enabled);
    }

    pub(crate) fn row_security_enabled(&self, client_id: ClientId) -> bool {
        self.session_row_security_states
            .read()
            .get(&client_id)
            .copied()
            .unwrap_or(true)
    }

    pub(crate) fn install_session_replication_role(
        &self,
        client_id: ClientId,
        role: SessionReplicationRole,
    ) {
        self.session_replication_role_states
            .write()
            .insert(client_id, role);
    }

    pub(crate) fn session_replication_role(&self, client_id: ClientId) -> SessionReplicationRole {
        self.session_replication_role_states
            .read()
            .get(&client_id)
            .copied()
            .unwrap_or_default()
    }

    pub(crate) fn install_stats_state(
        &self,
        client_id: ClientId,
        stats_state: Arc<RwLock<SessionStatsState>>,
    ) {
        self.session_stats_states
            .write()
            .insert(client_id, stats_state);
    }

    pub(crate) fn install_plpgsql_function_cache(
        &self,
        client_id: ClientId,
        cache: Arc<RwLock<PlpgsqlFunctionCache>>,
    ) {
        self.session_plpgsql_function_caches
            .write()
            .insert(client_id, cache);
    }

    pub(crate) fn install_temp_backend_id(
        &self,
        client_id: ClientId,
        temp_backend_id: TempBackendId,
    ) {
        self.session_temp_backend_ids
            .write()
            .insert(client_id, temp_backend_id);
    }

    pub(crate) fn temp_backend_id(&self, client_id: ClientId) -> TempBackendId {
        self.session_temp_backend_ids
            .read()
            .get(&client_id)
            .copied()
            .unwrap_or(client_id)
    }

    pub(crate) fn auth_state(&self, client_id: ClientId) -> AuthState {
        self.session_auth_states
            .read()
            .get(&client_id)
            .cloned()
            .unwrap_or_default()
    }

    pub(crate) fn clear_auth_state(&self, client_id: ClientId) {
        self.session_auth_states.write().remove(&client_id);
    }

    pub(crate) fn clear_row_security_enabled(&self, client_id: ClientId) {
        self.session_row_security_states.write().remove(&client_id);
    }

    pub(crate) fn clear_session_replication_role(&self, client_id: ClientId) {
        self.session_replication_role_states
            .write()
            .remove(&client_id);
    }

    pub(crate) fn session_stats_state(
        &self,
        client_id: ClientId,
    ) -> Arc<RwLock<SessionStatsState>> {
        self.session_stats_states
            .read()
            .get(&client_id)
            .cloned()
            .unwrap_or_else(|| Arc::new(RwLock::new(SessionStatsState::default())))
    }

    pub(crate) fn plpgsql_function_cache(
        &self,
        client_id: ClientId,
    ) -> Arc<RwLock<PlpgsqlFunctionCache>> {
        self.session_plpgsql_function_caches
            .read()
            .get(&client_id)
            .cloned()
            .unwrap_or_else(|| Arc::new(RwLock::new(PlpgsqlFunctionCache::default())))
    }

    pub(crate) fn clear_stats_state(&self, client_id: ClientId) {
        self.session_stats_states.write().remove(&client_id);
    }

    pub(crate) fn clear_plpgsql_function_cache(&self, client_id: ClientId) {
        self.session_plpgsql_function_caches
            .write()
            .remove(&client_id);
    }

    pub(crate) fn clear_temp_backend_id(&self, client_id: ClientId) {
        self.session_temp_backend_ids.write().remove(&client_id);
    }

    pub(crate) fn auth_catalog(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
    ) -> Result<AuthCatalog, CatalogError> {
        let cache = self.backend_catcache(client_id, txn_ctx)?;
        Ok(AuthCatalog::new(
            cache.authid_rows(),
            cache.auth_members_rows(),
        ))
    }

    pub(crate) fn backend_catcache(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
    ) -> Result<CatCache, CatalogError> {
        syscache_backend_catcache(self, client_id, txn_ctx)
    }

    pub(crate) fn txn_auth_catalog(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
    ) -> Result<AuthCatalog, CatalogError> {
        self.auth_catalog(client_id, Some((xid, cid)))
    }

    pub(crate) fn txn_backend_catcache(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
    ) -> Result<CatCache, CatalogError> {
        self.backend_catcache(client_id, Some((xid, cid)))
    }

    pub(crate) fn normalize_domain_name_for_create(
        &self,
        name: &str,
        configured_search_path: Option<&[String]>,
    ) -> Result<(String, String, u32), ParseError> {
        match name.split_once('.') {
            Some((schema, object)) if !object.is_empty() => {
                let namespace_oid = match schema.to_ascii_lowercase().as_str() {
                    "public" => PUBLIC_NAMESPACE_OID,
                    "pg_catalog" => {
                        return Err(ParseError::UnsupportedQualifiedName(name.to_string()));
                    }
                    _ => PUBLIC_NAMESPACE_OID,
                };
                Ok((
                    name.to_ascii_lowercase(),
                    object.to_ascii_lowercase(),
                    namespace_oid,
                ))
            }
            Some(_) => Err(ParseError::UnsupportedQualifiedName(name.to_string())),
            None => Ok((
                format!("public.{}", name.to_ascii_lowercase()),
                name.to_ascii_lowercase(),
                match self
                    .effective_search_path(0, configured_search_path)
                    .into_iter()
                    .find(|schema| schema == "public")
                {
                    Some(_) => PUBLIC_NAMESPACE_OID,
                    None => PUBLIC_NAMESPACE_OID,
                },
            )),
        }
    }

    pub(crate) fn normalize_conversion_name_for_create(
        &self,
        name: &str,
        configured_search_path: Option<&[String]>,
    ) -> Result<(String, String, u32), ParseError> {
        match name.split_once('.') {
            Some((schema, object)) if !object.is_empty() => {
                let namespace_oid = match schema.to_ascii_lowercase().as_str() {
                    "public" => PUBLIC_NAMESPACE_OID,
                    "pg_catalog" => 11,
                    _ => PUBLIC_NAMESPACE_OID,
                };
                Ok((
                    format!(
                        "{}.{}",
                        schema.to_ascii_lowercase(),
                        object.to_ascii_lowercase()
                    ),
                    object.to_ascii_lowercase(),
                    namespace_oid,
                ))
            }
            Some(_) => Err(ParseError::UnsupportedQualifiedName(name.to_string())),
            None => Ok((
                name.to_ascii_lowercase(),
                name.to_ascii_lowercase(),
                match self
                    .effective_search_path(0, configured_search_path)
                    .into_iter()
                    .find(|schema| schema == "public")
                {
                    Some(_) => PUBLIC_NAMESPACE_OID,
                    None => PUBLIC_NAMESPACE_OID,
                },
            )),
        }
    }

    pub(crate) fn domain_type_rows_for_search_path(
        &self,
        search_path: &[String],
    ) -> Vec<PgTypeRow> {
        let domains = self.domains.read();
        let base_types = self.base_types.read();
        let mut rows = domains
            .values()
            .flat_map(|domain| {
                let base_catalog = builtin_type_row_by_oid(domain.sql_type.type_oid);
                let base_entry = base_types.get(&domain.sql_type.type_oid);
                let domain_type = domain_sql_type(domain);
                [
                    PgTypeRow {
                        oid: domain.oid,
                        typname: domain.name.clone(),
                        typnamespace: domain.namespace_oid,
                        typowner: BOOTSTRAP_SUPERUSER_OID,
                        typacl: domain.typacl.clone(),
                        typlen: base_catalog
                            .as_ref()
                            .map(|row| row.typlen)
                            .unwrap_or_else(|| if domain.sql_type.is_array { -1 } else { 0 }),
                        typbyval: base_catalog.as_ref().is_some_and(|row| row.typbyval),
                        typtype: 'd',
                        typisdefined: true,
                        typalign: base_catalog
                            .as_ref()
                            .map(|row| row.typalign)
                            .unwrap_or(AttributeAlign::Int),
                        typstorage: base_catalog
                            .as_ref()
                            .map(|row| row.typstorage)
                            .or_else(|| base_entry.map(|entry| entry.typstorage))
                            .unwrap_or(AttributeStorage::Extended),
                        typrelid: 0,
                        typsubscript: 0,
                        typelem: 0,
                        typarray: domain.array_oid,
                        typinput: DOMAIN_IN_PROC_OID,
                        typoutput: base_catalog
                            .as_ref()
                            .map(|row| row.typoutput)
                            .or_else(|| base_entry.map(|entry| entry.output_proc_oid))
                            .filter(|oid| *oid != 0)
                            .unwrap_or_else(|| {
                                synthetic_type_output_proc_oid(domain.sql_type.type_oid)
                            }),
                        typreceive: DOMAIN_RECV_PROC_OID,
                        typsend: base_catalog
                            .as_ref()
                            .map(|row| row.typsend)
                            .or_else(|| base_entry.map(|entry| entry.send_proc_oid))
                            .unwrap_or(0),
                        typmodin: base_catalog
                            .as_ref()
                            .map(|row| row.typmodin)
                            .or_else(|| base_entry.map(|entry| entry.typmodin_proc_oid))
                            .unwrap_or(0),
                        typmodout: base_catalog
                            .as_ref()
                            .map(|row| row.typmodout)
                            .or_else(|| base_entry.map(|entry| entry.typmodout_proc_oid))
                            .unwrap_or(0),
                        typdelim: base_catalog.as_ref().map(|row| row.typdelim).unwrap_or(','),
                        typanalyze: base_catalog
                            .as_ref()
                            .map(|row| row.typanalyze)
                            .or_else(|| base_entry.map(|entry| entry.analyze_proc_oid))
                            .unwrap_or(0),
                        typbasetype: domain.sql_type.type_oid,
                        typcollation: base_catalog
                            .as_ref()
                            .map(|row| row.typcollation)
                            .unwrap_or(0),
                        sql_type: domain_type,
                    },
                    PgTypeRow {
                        oid: domain.array_oid,
                        typname: format!("_{}", domain.name),
                        typnamespace: domain.namespace_oid,
                        typowner: BOOTSTRAP_SUPERUSER_OID,
                        typacl: None,
                        typlen: -1,
                        typbyval: false,
                        typtype: 'b',
                        typisdefined: true,
                        typalign: AttributeAlign::Int,
                        typstorage: AttributeStorage::Extended,
                        typrelid: 0,
                        typsubscript: ARRAY_SUBSCRIPT_HANDLER_PROC_OID,
                        typelem: domain.oid,
                        typarray: 0,
                        typinput: ARRAY_IN_PROC_OID,
                        typoutput: ARRAY_OUT_PROC_OID,
                        typreceive: ARRAY_RECV_PROC_OID,
                        typsend: ARRAY_SEND_PROC_OID,
                        typmodin: 0,
                        typmodout: 0,
                        typdelim: ',',
                        typanalyze: ARRAY_TYPANALYZE_PROC_OID,
                        typbasetype: 0,
                        typcollation: 0,
                        sql_type: SqlType::array_of(domain_type),
                    },
                ]
            })
            .collect::<Vec<_>>();
        annotate_catalog_type_io_procs(&mut rows);
        rows.sort_by_key(|row| {
            let schema_rank = search_path
                .iter()
                .position(|schema| {
                    (schema == "public" && row.typnamespace == PUBLIC_NAMESPACE_OID)
                        || (schema == "pg_catalog" && row.typnamespace == 11)
                })
                .unwrap_or(usize::MAX);
            (schema_rank, row.typname.clone())
        });
        rows
    }

    pub(crate) fn enum_type_rows_for_search_path(&self, search_path: &[String]) -> Vec<PgTypeRow> {
        let enum_types = self.enum_types.read();
        let mut rows = enum_types
            .values()
            .flat_map(|entry| {
                let base_sql_type = SqlType::new(SqlTypeKind::Enum).with_identity(entry.oid, 0);
                [
                    PgTypeRow {
                        oid: entry.oid,
                        typname: entry.name.clone(),
                        typnamespace: entry.namespace_oid,
                        typowner: BOOTSTRAP_SUPERUSER_OID,
                        typacl: entry.typacl.clone(),
                        typlen: 4,
                        typbyval: true,
                        typtype: 'e',
                        typisdefined: true,
                        typalign: AttributeAlign::Int,
                        typstorage: AttributeStorage::Plain,
                        typrelid: 0,
                        typsubscript: 0,
                        typelem: 0,
                        typarray: entry.array_oid,
                        typinput: 0,
                        typoutput: 0,
                        typreceive: 0,
                        typsend: 0,
                        typmodin: 0,
                        typmodout: 0,
                        typdelim: ',',
                        typanalyze: 0,
                        typbasetype: 0,
                        typcollation: 0,
                        // :HACK: User-defined enums are text-backed for now. This unlocks
                        // catalog/type resolution and basic storage flow, but does not yet
                        // enforce label membership or enum ordering semantics.
                        sql_type: base_sql_type,
                    },
                    PgTypeRow {
                        oid: entry.array_oid,
                        typname: format!("_{}", entry.name),
                        typnamespace: entry.namespace_oid,
                        typowner: BOOTSTRAP_SUPERUSER_OID,
                        typacl: entry.typacl.clone(),
                        typlen: -1,
                        typbyval: false,
                        typtype: 'b',
                        typisdefined: true,
                        typalign: AttributeAlign::Int,
                        typstorage: AttributeStorage::Extended,
                        typrelid: 0,
                        typsubscript: 0,
                        typelem: entry.oid,
                        typarray: 0,
                        typinput: 0,
                        typoutput: 0,
                        typreceive: 0,
                        typsend: 0,
                        typmodin: 0,
                        typmodout: 0,
                        typdelim: ',',
                        typanalyze: 0,
                        typbasetype: 0,
                        typcollation: 0,
                        sql_type: SqlType::array_of(base_sql_type)
                            .with_identity(entry.array_oid, 0),
                    },
                ]
            })
            .collect::<Vec<_>>();
        annotate_catalog_type_io_procs(&mut rows);
        rows.sort_by_key(|row| {
            let schema_rank = search_path
                .iter()
                .position(|schema| {
                    (schema == "public" && row.typnamespace == PUBLIC_NAMESPACE_OID)
                        || (schema == "pg_catalog" && row.typnamespace == 11)
                })
                .unwrap_or(usize::MAX);
            (schema_rank, row.typname.clone())
        });
        rows
    }

    pub(crate) fn enum_label_oid(&self, type_oid: u32, label: &str) -> Option<u32> {
        self.enum_types
            .read()
            .values()
            .find(|entry| entry.oid == type_oid)?
            .labels
            .iter()
            .find(|entry| entry.label == label)
            .map(|entry| entry.oid)
    }

    pub(crate) fn enum_label_is_committed(&self, type_oid: u32, label_oid: u32) -> bool {
        self.enum_types
            .read()
            .values()
            .find(|entry| entry.oid == type_oid)
            .and_then(|entry| entry.labels.iter().find(|label| label.oid == label_oid))
            .is_none_or(|label| label.committed)
    }

    pub(crate) fn uncommitted_enum_label_oids(&self) -> Vec<u32> {
        self.enum_types
            .read()
            .values()
            .flat_map(|entry| {
                entry
                    .labels
                    .iter()
                    .filter(|label| !label.committed)
                    .map(|label| label.oid)
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    pub(crate) fn domain_allowed_enum_label_oids(&self, domain_oid: u32) -> Option<Vec<u32>> {
        self.domains
            .read()
            .values()
            .find(|domain| domain.oid == domain_oid)?
            .enum_check
            .as_ref()
            .map(|check| check.allowed_enum_label_oids.clone())
    }

    pub(crate) fn domain_check_name(&self, domain_oid: u32) -> Option<String> {
        self.domains
            .read()
            .values()
            .find(|domain| domain.oid == domain_oid)?
            .enum_check
            .as_ref()
            .map(|check| check.name.clone())
    }

    pub(crate) fn domain_check_by_type_oid(&self, domain_oid: u32) -> Option<String> {
        self.domains
            .read()
            .values()
            .find(|domain| domain.oid == domain_oid)?
            .check
            .clone()
    }

    pub(crate) fn domain_checks_for_catalog(&self) -> BTreeMap<u32, (String, Vec<u32>)> {
        self.domains
            .read()
            .values()
            .filter_map(|domain| {
                domain.enum_check.as_ref().map(|check| {
                    (
                        domain.oid,
                        (check.name.clone(), check.allowed_enum_label_oids.clone()),
                    )
                })
            })
            .collect()
    }

    pub(crate) fn dynamic_type_snapshot(&self) -> DynamicTypeSnapshot {
        DynamicTypeSnapshot {
            domains: self.domains.read().clone(),
            enum_types: self.enum_types.read().clone(),
            range_types: self.range_types.read().clone(),
            base_types: self.base_types.read().clone(),
        }
    }

    pub(crate) fn restore_dynamic_type_snapshot(&self, snapshot: &DynamicTypeSnapshot) {
        *self.domains.write() = snapshot.domains.clone();
        *self.enum_types.write() = snapshot.enum_types.clone();
        *self.range_types.write() = snapshot.range_types.clone();
        *self.base_types.write() = snapshot.base_types.clone();
        self.plan_cache.invalidate_all();
    }

    pub(crate) fn base_type_default(&self, type_oid: u32) -> Option<String> {
        self.base_types
            .read()
            .get(&type_oid)
            .and_then(|entry| entry.default.clone())
    }

    pub(crate) fn commit_enum_labels_created_by(&self, xid: TransactionId) {
        let mut changed = false;
        for entry in self.enum_types.write().values_mut() {
            if entry.creating_xid == Some(xid) {
                entry.creating_xid = None;
                changed = true;
            }
            for label in &mut entry.labels {
                if label.creating_xid == Some(xid) {
                    label.committed = true;
                    label.creating_xid = None;
                    changed = true;
                }
            }
        }
        if changed {
            self.plan_cache.invalidate_all();
        }
    }

    pub(crate) fn enum_label(&self, type_oid: u32, label_oid: u32) -> Option<String> {
        self.enum_types
            .read()
            .values()
            .find(|entry| entry.oid == type_oid)?
            .labels
            .iter()
            .find(|entry| entry.oid == label_oid)
            .map(|entry| entry.label.clone())
    }

    pub(crate) fn enum_rows_for_catalog(&self) -> Vec<PgEnumRow> {
        let mut rows = self
            .enum_types
            .read()
            .values()
            .flat_map(|entry| {
                entry
                    .labels
                    .iter()
                    .map(|label| PgEnumRow {
                        oid: label.oid,
                        enumtypid: entry.oid,
                        enumsortorder: label.sort_order,
                        enumlabel: label.label.clone(),
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            left.enumtypid
                .cmp(&right.enumtypid)
                .then_with(|| left.enumsortorder.total_cmp(&right.enumsortorder))
                .then_with(|| left.enumlabel.cmp(&right.enumlabel))
        });
        rows
    }

    pub(crate) fn range_type_rows_for_search_path(&self, search_path: &[String]) -> Vec<PgTypeRow> {
        let range_types = self.range_types.read();
        let array_type_names = dynamic_range_array_type_names(&range_types);
        let mut rows = range_types
            .values()
            .flat_map(|entry| {
                let discrete = matches!(
                    entry.subtype.kind,
                    SqlTypeKind::Int4 | SqlTypeKind::Int8 | SqlTypeKind::Date
                );
                let base_sql_type = SqlType::range(entry.oid, entry.subtype.type_oid)
                    .with_identity(entry.oid, entry.subtype.typrelid)
                    .with_range_metadata(entry.subtype.type_oid, entry.multirange_oid, discrete);
                let multirange_sql_type = SqlType::multirange(entry.multirange_oid, entry.oid)
                    .with_identity(entry.multirange_oid, entry.subtype.typrelid)
                    .with_range_metadata(entry.subtype.type_oid, entry.multirange_oid, discrete)
                    .with_multirange_range_oid(entry.oid);
                let range_align = builtin_type_row_by_oid(entry.subtype.type_oid)
                    .map(|row| {
                        if row.typalign == AttributeAlign::Double {
                            AttributeAlign::Double
                        } else {
                            AttributeAlign::Int
                        }
                    })
                    .unwrap_or(AttributeAlign::Int);
                let array_name = array_type_names
                    .get(&entry.array_oid)
                    .cloned()
                    .unwrap_or_else(|| format!("_{}", entry.name));
                let multirange_array_name = array_type_names
                    .get(&entry.multirange_array_oid)
                    .cloned()
                    .unwrap_or_else(|| format!("_{}", entry.multirange_name));
                [
                    PgTypeRow {
                        oid: entry.oid,
                        typname: entry.name.clone(),
                        typnamespace: entry.namespace_oid,
                        typowner: entry.owner_oid,
                        typacl: entry.typacl.clone(),
                        typlen: -1,
                        typbyval: false,
                        typtype: 'r',
                        typisdefined: true,
                        typalign: range_align,
                        typstorage: AttributeStorage::Extended,
                        typrelid: 0,
                        typsubscript: 0,
                        typelem: 0,
                        typarray: entry.array_oid,
                        typinput: 0,
                        typoutput: 0,
                        typreceive: 0,
                        typsend: 0,
                        typmodin: 0,
                        typmodout: 0,
                        typdelim: ',',
                        typanalyze: 0,
                        typbasetype: 0,
                        typcollation: 0,
                        sql_type: base_sql_type,
                    },
                    PgTypeRow {
                        oid: entry.array_oid,
                        typname: array_name,
                        typnamespace: entry.namespace_oid,
                        typowner: entry.owner_oid,
                        typacl: entry.typacl.clone(),
                        typlen: -1,
                        typbyval: false,
                        typtype: 'b',
                        typisdefined: true,
                        typalign: range_align,
                        typstorage: AttributeStorage::Extended,
                        typrelid: 0,
                        typsubscript: 0,
                        typelem: entry.oid,
                        typarray: 0,
                        typinput: 0,
                        typoutput: 0,
                        typreceive: 0,
                        typsend: 0,
                        typmodin: 0,
                        typmodout: 0,
                        typdelim: ',',
                        typanalyze: 0,
                        typbasetype: 0,
                        typcollation: 0,
                        sql_type: SqlType::array_of(base_sql_type),
                    },
                    PgTypeRow {
                        oid: entry.multirange_oid,
                        typname: entry.multirange_name.clone(),
                        typnamespace: entry.namespace_oid,
                        typowner: entry.owner_oid,
                        typacl: entry.typacl.clone(),
                        typlen: -1,
                        typbyval: false,
                        typtype: 'm',
                        typisdefined: true,
                        typalign: range_align,
                        typstorage: AttributeStorage::Extended,
                        typrelid: 0,
                        typsubscript: 0,
                        typelem: 0,
                        typarray: entry.multirange_array_oid,
                        typinput: 0,
                        typoutput: 0,
                        typreceive: 0,
                        typsend: 0,
                        typmodin: 0,
                        typmodout: 0,
                        typdelim: ',',
                        typanalyze: 0,
                        typbasetype: 0,
                        typcollation: 0,
                        sql_type: multirange_sql_type,
                    },
                    PgTypeRow {
                        oid: entry.multirange_array_oid,
                        typname: multirange_array_name,
                        typnamespace: entry.namespace_oid,
                        typowner: entry.owner_oid,
                        typacl: entry.typacl.clone(),
                        typlen: -1,
                        typbyval: false,
                        typtype: 'b',
                        typisdefined: true,
                        typalign: range_align,
                        typstorage: AttributeStorage::Extended,
                        typrelid: 0,
                        typsubscript: 0,
                        typelem: entry.multirange_oid,
                        typarray: 0,
                        typinput: 0,
                        typoutput: 0,
                        typreceive: 0,
                        typsend: 0,
                        typmodin: 0,
                        typmodout: 0,
                        typdelim: ',',
                        typanalyze: 0,
                        typbasetype: 0,
                        typcollation: 0,
                        sql_type: SqlType::array_of(multirange_sql_type),
                    },
                ]
            })
            .collect::<Vec<_>>();
        annotate_catalog_type_io_procs(&mut rows);
        rows.sort_by_key(|row| {
            let schema_rank = search_path
                .iter()
                .position(|schema| {
                    (schema == "public" && row.typnamespace == PUBLIC_NAMESPACE_OID)
                        || (schema == "pg_catalog" && row.typnamespace == 11)
                })
                .unwrap_or(usize::MAX);
            (schema_rank, row.typname.clone())
        });
        rows
    }

    pub(crate) fn dynamic_type_rows_for_search_path(
        &self,
        search_path: &[String],
    ) -> Vec<PgTypeRow> {
        let mut rows = self.domain_type_rows_for_search_path(search_path);
        rows.extend(self.enum_type_rows_for_search_path(search_path));
        rows.extend(self.range_type_rows_for_search_path(search_path));
        rows
    }

    pub(crate) fn refresh_catalog_store_dynamic_type_rows(
        &self,
        client_id: ClientId,
        configured_search_path: Option<&[String]>,
    ) {
        let search_path = self.effective_search_path(client_id, configured_search_path);
        let rows = self.dynamic_type_rows_for_search_path(&search_path);
        self.catalog.write().set_extra_type_rows(rows);
    }

    pub(crate) fn range_rows(&self) -> Vec<PgRangeRow> {
        self.range_types
            .read()
            .values()
            .map(|entry| PgRangeRow {
                rngtypid: entry.oid,
                rngsubtype: entry.subtype.type_oid,
                rngmultitypid: entry.multirange_oid,
                rngcollation: crate::backend::catalog::catalog::default_column_collation_oid(
                    entry.subtype,
                ),
                rngsubopc: crate::include::catalog::default_btree_opclass_oid(
                    entry.subtype.type_oid,
                )
                .unwrap_or_else(|| {
                    if entry.subtype.is_array {
                        crate::include::catalog::ARRAY_BTREE_OPCLASS_OID
                    } else {
                        0
                    }
                }),
                rngcanonical: None,
                rngsubdiff: entry.subtype_diff.clone(),
                canonicalization: match entry.subtype.kind {
                    SqlTypeKind::Int4 | SqlTypeKind::Int8 | SqlTypeKind::Date => {
                        RangeCanonicalization::Discrete
                    }
                    _ => RangeCanonicalization::Continuous,
                },
            })
            .collect()
    }

    pub(crate) fn clear_interrupt_state(&self, client_id: ClientId) {
        self.session_interrupt_states.write().remove(&client_id);
        self.clear_auth_state(client_id);
        self.clear_row_security_enabled(client_id);
        self.clear_session_replication_role(client_id);
        self.clear_stats_state(client_id);
        self.clear_plpgsql_function_cache(client_id);
        self.sequences.clear_currvals_for_client(client_id);
    }

    pub(crate) fn register_session_activity(&self, client_id: ClientId) {
        self.cluster.session_activity.write().insert(
            client_id,
            SessionActivityEntry {
                client_id,
                database_oid: self.database_oid,
                state: SessionActivityState::Idle,
                query: String::new(),
            },
        );
    }

    pub(crate) fn set_session_query_active(&self, client_id: ClientId, query: &str) {
        let mut activity = self.cluster.session_activity.write();
        let entry = activity
            .entry(client_id)
            .or_insert_with(|| SessionActivityEntry {
                client_id,
                database_oid: self.database_oid,
                state: SessionActivityState::Idle,
                query: String::new(),
            });
        entry.database_oid = self.database_oid;
        entry.state = SessionActivityState::Active;
        entry.query = query.to_string();
    }

    pub(crate) fn set_session_query_idle(&self, client_id: ClientId) {
        if let Some(entry) = self.cluster.session_activity.write().get_mut(&client_id) {
            entry.database_oid = self.database_oid;
            entry.state = SessionActivityState::Idle;
            entry.query.clear();
        }
    }

    pub(crate) fn clear_session_activity(&self, client_id: ClientId) {
        self.cluster.session_activity.write().remove(&client_id);
    }

    pub(crate) fn pg_locks_rows(&self) -> Vec<Vec<Value>> {
        self.pg_lock_status_rows_for_client(0)
    }

    fn pg_lock_status_rows_for_client(&self, current_client_id: ClientId) -> Vec<Vec<Value>> {
        let mut rows = Vec::new();

        for row in self.table_locks.snapshot() {
            let database_oid = if row.rel.db_oid == 0 {
                0
            } else {
                row.rel.db_oid
            };
            rows.push((
                format!(
                    "relation/{database_oid}/{}/{}/{}/{}/{}",
                    row.rel.rel_number,
                    row.client_id,
                    row.mode.pg_mode_name(),
                    row.waitstart.is_some(),
                    row.granted
                ),
                relation_lock_row_to_values(
                    database_oid,
                    self.resolve_relation_oid_for_lock(row.rel),
                    row,
                ),
            ));
        }

        let states = self
            .cluster
            .open_databases
            .read()
            .iter()
            .map(|(db_oid, state)| (*db_oid, Arc::clone(state)))
            .collect::<Vec<_>>();
        for (db_oid, state) in states {
            for row in state.advisory_locks.snapshot() {
                rows.push((
                    format!(
                        "advisory/{db_oid}/{}/{}/{}/{}/{}",
                        advisory_key_sort_fragment(row.key),
                        row.owner.virtualtransaction(),
                        row.mode.pg_mode_name(),
                        row.waitstart.is_some(),
                        row.granted
                    ),
                    advisory_lock_row_to_values(db_oid, row),
                ));
            }
            for row in state.row_locks.snapshot() {
                rows.push((
                    format!(
                        "tuple/{db_oid}/{}/{}/{}/{}/{}",
                        row.tag.relation_oid,
                        row.tag.tid.block_number,
                        row.tag.tid.offset_number,
                        row.owner.client_id,
                        row.granted
                    ),
                    row_lock_row_to_values(db_oid, row),
                ));
            }
        }

        for row in self.txn_waiter.snapshot() {
            rows.push((
                format!(
                    "transactionid/{}/{}/{}",
                    row.xid, row.client_id, row.granted
                ),
                transaction_lock_row_to_values(row),
            ));
        }

        let mut virtual_clients = self
            .cluster
            .open_databases
            .read()
            .values()
            .flat_map(|state| {
                let mut client_ids = state
                    .session_auth_states
                    .read()
                    .keys()
                    .copied()
                    .collect::<Vec<_>>();
                client_ids.extend(state.session_interrupt_states.read().keys().copied());
                client_ids
            })
            .collect::<Vec<_>>();
        if current_client_id != 0 {
            virtual_clients.push(current_client_id);
        }
        virtual_clients.sort_unstable();
        virtual_clients.dedup();
        for client_id in virtual_clients {
            rows.push((
                format!("virtualxid/{client_id}"),
                virtualxid_lock_row_to_values(client_id),
            ));
        }

        rows.sort_by(|left, right| left.0.cmp(&right.0));
        rows.into_iter().map(|(_, row)| row).collect()
    }

    pub(crate) fn pg_stat_activity_rows(&self) -> Vec<Vec<Value>> {
        let database_names = self
            .shared_catalog
            .read()
            .catcache()
            .map(|cache| {
                cache
                    .database_rows()
                    .into_iter()
                    .map(|row| (row.oid, row.datname))
                    .collect::<HashMap<_, _>>()
            })
            .unwrap_or_default();
        let role_names = self
            .shared_catalog
            .read()
            .catcache()
            .map(|cache| {
                cache
                    .authid_rows()
                    .into_iter()
                    .map(|row| (row.oid, row.rolname))
                    .collect::<HashMap<_, _>>()
            })
            .unwrap_or_default();
        let sessions_by_db = self
            .cluster
            .open_databases
            .read()
            .iter()
            .map(|(db_oid, state)| (*db_oid, Arc::clone(state)))
            .collect::<Vec<_>>();
        let activity = self
            .cluster
            .session_activity
            .read()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut rows = activity
            .into_iter()
            .map(|entry| {
                let usename = sessions_by_db
                    .iter()
                    .find(|(db_oid, _)| *db_oid == entry.database_oid)
                    .and_then(|(_, state)| {
                        state
                            .session_auth_states
                            .read()
                            .get(&entry.client_id)
                            .map(|auth| auth.current_user_oid())
                    })
                    .and_then(|role_oid| role_names.get(&role_oid).cloned())
                    .unwrap_or_else(|| "unknown".to_string());
                vec![
                    Value::Int32(entry.client_id as i32),
                    Value::Text(
                        database_names
                            .get(&entry.database_oid)
                            .cloned()
                            .unwrap_or_else(|| "unknown".to_string())
                            .into(),
                    ),
                    Value::Text(usename.into()),
                    Value::Text(entry.state.as_str().into()),
                    Value::Text(entry.query.into()),
                ]
            })
            .collect::<Vec<_>>();
        rows.sort_by_key(|row| match row.first() {
            Some(Value::Int32(pid)) => *pid,
            _ => i32::MAX,
        });
        rows
    }

    pub(crate) fn accept_invalidation_messages(&self, client_id: ClientId) {
        accept_invalidation_messages(self, client_id);
    }

    pub(crate) fn current_database_name(&self) -> String {
        self.shared_catalog
            .read()
            .catcache()
            .ok()
            .and_then(|cache| {
                cache
                    .database_rows()
                    .into_iter()
                    .find(|row| row.oid == self.database_oid)
                    .map(|row| row.datname)
            })
            .unwrap_or_else(|| CURRENT_DATABASE_NAME.to_string())
    }

    pub(crate) fn checkpoint_config_value(&self, name: &str) -> Option<String> {
        self.checkpoint_config.value_for_show(name)
    }

    pub(crate) fn autovacuum_config_value(&self, name: &str) -> Option<String> {
        self.autovacuum_config.value_for_show(name)
    }

    pub(crate) fn checkpoint_stats_snapshot(&self) -> CheckpointStatsSnapshot {
        self.checkpoint_stats.read().clone()
    }

    pub(crate) fn request_checkpoint(
        &self,
        flags: CheckpointRequestFlags,
    ) -> Result<(), ExecError> {
        let Some(checkpointer) = self.checkpointer.as_ref() else {
            return Ok(());
        };
        checkpointer
            .request(flags)
            .map_err(|message| ExecError::DetailedError {
                message: "checkpoint failed".into(),
                detail: Some(message),
                hint: None,
                sqlstate: "58000",
            })
    }

    pub(crate) fn checkpoint_commit_guard(&self) -> CheckpointCommitGuard {
        self.checkpoint_commit_barrier.enter()
    }

    fn resolve_relation_oid_for_lock(&self, rel: RelFileLocator) -> Option<u32> {
        if rel.spc_oid == LOGICAL_RELATION_LOCK_SPC_OID {
            if rel.db_oid == 0 {
                return self
                    .shared_catalog
                    .read()
                    .catcache()
                    .ok()?
                    .class_by_oid(rel.rel_number)
                    .map(|row| row.oid);
            }

            let state = self
                .cluster
                .open_databases
                .read()
                .get(&rel.db_oid)
                .cloned()?;

            return state
                .catalog
                .read()
                .catcache()
                .ok()
                .and_then(|catcache| catcache.class_by_oid(rel.rel_number).map(|row| row.oid));
        }

        if rel.db_oid == 0 {
            return self
                .shared_catalog
                .read()
                .catcache()
                .ok()?
                .class_rows()
                .into_iter()
                .find(|row| row.relfilenode == rel.rel_number && row.reltablespace == rel.spc_oid)
                .map(|row| row.oid);
        }

        let state = self
            .cluster
            .open_databases
            .read()
            .get(&rel.db_oid)
            .cloned()?;

        if let Some(oid) = state.catalog.read().catcache().ok().and_then(|catcache| {
            catcache
                .class_rows()
                .into_iter()
                .find(|row| row.relfilenode == rel.rel_number && row.reltablespace == rel.spc_oid)
                .map(|row| row.oid)
        }) {
            return Some(oid);
        }

        state
            .temp_relations
            .read()
            .values()
            .flat_map(|namespace| namespace.tables.values())
            .find(|entry| entry.entry.rel == rel)
            .map(|entry| entry.entry.relation_oid)
    }
}

impl LockStatusProvider for Database {
    fn pg_lock_status_rows(&self, current_client_id: ClientId) -> Vec<Vec<Value>> {
        self.pg_lock_status_rows_for_client(current_client_id)
    }
}

fn relation_lock_row_to_values(
    database_oid: u32,
    relation_oid: Option<u32>,
    row: TableLockSnapshotRow,
) -> Vec<Value> {
    vec![
        Value::Text("relation".into()),
        oid_value(database_oid),
        relation_oid.map(oid_value).unwrap_or(Value::Null),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Text(format!("{}/session", row.client_id).into()),
        Value::Int32(row.client_id as i32),
        Value::Text(row.mode.pg_mode_name().into()),
        Value::Bool(row.granted),
        Value::Bool(false),
        row.waitstart.map(Value::TimestampTz).unwrap_or(Value::Null),
    ]
}

fn advisory_lock_row_to_values(database_oid: u32, row: AdvisoryLockSnapshotRow) -> Vec<Value> {
    let (classid, objid, objsubid) = advisory_lock_key_fields(row.key);
    vec![
        Value::Text("advisory".into()),
        oid_value(database_oid),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        oid_value(classid),
        oid_value(objid),
        Value::Int16(objsubid),
        Value::Text(row.owner.virtualtransaction().into()),
        Value::Int32(row.owner.client_id as i32),
        Value::Text(row.mode.pg_mode_name().into()),
        Value::Bool(row.granted),
        Value::Bool(false),
        row.waitstart.map(Value::TimestampTz).unwrap_or(Value::Null),
    ]
}

fn row_lock_row_to_values(database_oid: u32, row: RowLockSnapshotRow) -> Vec<Value> {
    vec![
        Value::Text("tuple".into()),
        oid_value(database_oid),
        oid_value(row.tag.relation_oid),
        Value::Int32(row.tag.tid.block_number as i32),
        Value::Int16(row.tag.tid.offset_number as i16),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Text(row_lock_virtualtransaction(row.owner).into()),
        Value::Int32(row.owner.client_id as i32),
        Value::Text(row.mode.pg_lock_mode_name().into()),
        Value::Bool(row.granted),
        Value::Bool(false),
        row.waitstart.map(Value::TimestampTz).unwrap_or(Value::Null),
    ]
}

fn transaction_lock_row_to_values(row: TransactionLockSnapshotRow) -> Vec<Value> {
    vec![
        Value::Text("transactionid".into()),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        oid_value(row.xid),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Text(format!("{}/xact:{}", row.client_id, row.xid).into()),
        Value::Int32(row.client_id as i32),
        Value::Text(
            if row.granted {
                "ExclusiveLock"
            } else {
                "ShareLock"
            }
            .into(),
        ),
        Value::Bool(row.granted),
        Value::Bool(false),
        row.waitstart.map(Value::TimestampTz).unwrap_or(Value::Null),
    ]
}

fn virtualxid_lock_row_to_values(client_id: ClientId) -> Vec<Value> {
    let virtualtransaction = format!("{client_id}/session");
    vec![
        Value::Text("virtualxid".into()),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Text(virtualtransaction.clone().into()),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Text(virtualtransaction.into()),
        Value::Int32(client_id as i32),
        Value::Text("ExclusiveLock".into()),
        Value::Bool(true),
        Value::Bool(false),
        Value::Null,
    ]
}

fn row_lock_virtualtransaction(owner: crate::backend::storage::lmgr::RowLockOwner) -> String {
    match owner.scope {
        crate::backend::storage::lmgr::RowLockScope::Session => {
            format!("{}/session", owner.client_id)
        }
        crate::backend::storage::lmgr::RowLockScope::Transaction(scope_id) => {
            format!("{}/xact:{scope_id}", owner.client_id)
        }
        crate::backend::storage::lmgr::RowLockScope::Statement(scope_id) => {
            format!("{}/stmt:{scope_id}", owner.client_id)
        }
    }
}

fn advisory_lock_key_fields(key: AdvisoryLockKey) -> (u32, u32, i16) {
    match key {
        AdvisoryLockKey::BigInt(value) => {
            let bits = value as u64;
            ((bits >> 32) as u32, bits as u32, 1)
        }
        AdvisoryLockKey::TwoInt(first, second) => (first as u32, second as u32, 2),
    }
}

fn advisory_key_sort_fragment(key: AdvisoryLockKey) -> String {
    match key {
        AdvisoryLockKey::BigInt(value) => format!("b:{value}"),
        AdvisoryLockKey::TwoInt(first, second) => format!("i:{first}:{second}"),
    }
}

fn oid_value(oid: u32) -> Value {
    Value::Int64(i64::from(oid))
}

pub(crate) fn bootstrap_ephemeral_catalog(
    pool: &Arc<BufferPool<SmgrStorageBackend>>,
    txns: &Arc<RwLock<TransactionManager>>,
) -> Result<(), DatabaseError> {
    use crate::backend::catalog::bootstrap::bootstrap_catalog_rel;
    use crate::backend::catalog::indexing::system_catalog_index_rel_for_db;

    pool.with_storage_mut(|storage| {
        for kind in bootstrap_catalog_kinds() {
            let rel = bootstrap_catalog_rel(kind, 1);
            let _ = storage.smgr.open(rel);
            let _ =
                storage
                    .smgr
                    .create(rel, crate::backend::storage::smgr::ForkNumber::Main, false);
        }
        for descriptor in system_catalog_indexes() {
            let rel = system_catalog_index_rel_for_db(*descriptor, 1);
            let _ = storage.smgr.open(rel);
            let _ =
                storage
                    .smgr
                    .create(rel, crate::backend::storage::smgr::ForkNumber::Main, false);
        }
    });

    let catalog = crate::backend::catalog::Catalog::default();
    let rows = physical_catalog_rows_from_catcache(&CatCache::from_catalog(&catalog));
    sync_catalog_rows_subset_in_pool(pool, &rows, 1, &bootstrap_catalog_kinds())?;
    rebuild_system_catalog_indexes_in_pool(pool, txns)?;
    Ok(())
}

impl Drop for Database {
    fn drop(&mut self) {}
}

#[cfg(test)]
#[path = "database_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "toast_tests.rs"]
mod toast_tests;
