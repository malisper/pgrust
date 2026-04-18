use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;

mod catalog_access;
mod commands;
mod ddl;
pub(crate) mod foreign_keys;
mod relation_refs;
mod sequences;
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
    ExecError, ExecutorContext, StatementResult, Value, execute_readonly_statement,
};
use crate::backend::parser::Statement;
use crate::backend::parser::{
    AlterSequenceStatement, AlterTableAddColumnStatement, AlterTableDropColumnStatement,
    AlterTableRenameColumnStatement, AlterTableRenameStatement, AnalyzeStatement, CatalogLookup,
    CommentOnDomainStatement, CommentOnTableStatement, CreateCompositeTypeStatement,
    CreateDomainStatement, CreateIndexStatement, CreateSchemaStatement, CreateSequenceStatement,
    CreateTableAsStatement, CreateTableStatement, CreateViewStatement, DropDomainStatement,
    DropSequenceStatement, DropViewStatement, OnCommitAction, ParseError, SqlType, SqlTypeKind,
    TablePersistence, bind_delete, bind_insert, bind_update, create_relation_desc,
    lower_create_table_with_catalog, normalize_create_table_as_name, normalize_create_table_name,
    normalize_create_view_name,
};
use crate::backend::storage::lmgr::{
    TableLockManager, TableLockMode, lock_relations_interruptible, lock_tables_interruptible,
    unlock_relations,
};
use crate::backend::storage::smgr::{RelFileLocator, StorageManager};
use crate::backend::utils::cache::catcache::CatCache;
use crate::backend::utils::cache::inval::{
    CatalogInvalidation, accept_invalidation_messages, catalog_invalidation_from_effect,
    finalize_aborted_local_catalog_invalidations, finalize_command_end_local_catalog_invalidations,
    finalize_committed_catalog_effects, publish_committed_catalog_invalidation,
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
    BOOTSTRAP_SUPERUSER_OID, PUBLIC_NAMESPACE_OID, PgConstraintRow, PgRangeRow, PgTypeRow,
    RangeCanonicalization, relkind_has_storage, system_catalog_indexes,
};
use crate::pgrust::auth::{AuthCatalog, AuthState};
use crate::pgrust::cluster::{Cluster, ClusterShared, SessionActivityEntry, SessionActivityState};
use crate::pl::plpgsql::execute_do;
use crate::{BufferPool, ClientId, SmgrStorageBackend};
use ddl::{
    ensure_can_set_role, ensure_relation_owner, lookup_heap_relation_for_ddl, map_catalog_error,
    reject_column_with_foreign_key_dependencies, reject_index_with_referencing_foreign_keys,
    reject_inheritance_tree_ddl, reject_relation_with_dependent_views,
    validate_alter_table_add_column,
};
use relation_refs::{collect_direct_relation_oids_from_select, collect_rels_from_planned_stmt};
pub(crate) use sequences::{
    SequenceData, SequenceMutationEffect, SequenceOptions, SequenceOwnedByRef, SequenceRuntime,
    SequenceState, apply_sequence_option_patch, default_sequence_name_base,
    default_sequence_oid_from_default_expr, format_nextval_default_oid, initial_sequence_state,
    resolve_sequence_options_spec, sequence_type_oid_for_serial_kind,
    sequence_type_oid_for_sql_type,
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
pub(crate) use ddl::reject_relation_with_referencing_foreign_keys;
pub(crate) use foreign_keys::{
    alter_table_add_constraint_lock_requests, alter_table_validate_constraint_lock_requests,
    delete_foreign_key_lock_requests, insert_foreign_key_lock_requests,
    prepared_insert_foreign_key_lock_requests, relation_foreign_key_lock_requests,
    table_lock_relations, update_foreign_key_lock_requests,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatabaseOpenOptions {
    pub pool_size: usize,
}

impl DatabaseOpenOptions {
    pub const fn new(pool_size: usize) -> Self {
        Self { pool_size }
    }
}

#[derive(Clone)]
pub struct Database {
    pub(crate) cluster: Arc<ClusterShared>,
    pub database_oid: u32,
    pub pool: Arc<BufferPool<SmgrStorageBackend>>,
    pub wal: Option<Arc<WalWriter>>,
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
    pub(crate) database_create_grants: Arc<RwLock<Vec<DatabaseCreateGrant>>>,
    pub(crate) temp_relations: Arc<RwLock<HashMap<ClientId, TempNamespace>>>,
    pub(crate) domains: Arc<RwLock<BTreeMap<String, DomainEntry>>>,
    pub(crate) enum_types: Arc<RwLock<BTreeMap<String, EnumTypeEntry>>>,
    pub(crate) range_types: Arc<RwLock<BTreeMap<String, RangeTypeEntry>>>,
    pub(crate) conversions: Arc<RwLock<BTreeMap<String, ConversionEntry>>>,
    pub(crate) sequences: Arc<SequenceRuntime>,
    pub(crate) _wal_bg_writer: Option<Arc<WalBgWriter>>,
}

const TEMP_DB_OID_BASE: u32 = 0x7000_0000;
const TEMP_TOAST_NAMESPACE_OID_BASE: u32 = 0x7800_0000;
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
    pub name: String,
    pub namespace_oid: u32,
    pub sql_type: SqlType,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EnumTypeEntry {
    pub oid: u32,
    pub array_oid: u32,
    pub name: String,
    pub namespace_oid: u32,
    pub labels: Vec<String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RangeTypeEntry {
    pub oid: u32,
    pub array_oid: u32,
    pub name: String,
    pub namespace_oid: u32,
    pub subtype: SqlType,
    pub subtype_diff: Option<String>,
    pub collation: Option<String>,
    pub comment: Option<String>,
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

    pub(crate) fn install_auth_state(&self, client_id: ClientId, auth: AuthState) {
        self.session_auth_states.write().insert(client_id, auth);
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
                    format!("{}.{}", schema.to_ascii_lowercase(), object.to_ascii_lowercase()),
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
        let mut rows = domains
            .values()
            .map(|domain| PgTypeRow {
                oid: domain.oid,
                typname: domain.name.clone(),
                typnamespace: domain.namespace_oid,
                typowner: BOOTSTRAP_SUPERUSER_OID,
                typlen: if domain.sql_type.is_array { -1 } else { 0 },
                typalign: AttributeAlign::Int,
                typstorage: AttributeStorage::Extended,
                typrelid: 0,
                typelem: 0,
                typarray: 0,
                sql_type: domain.sql_type,
            })
            .collect::<Vec<_>>();
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
                let base_sql_type = SqlType::new(SqlTypeKind::Text).with_identity(entry.oid, 0);
                [
                    PgTypeRow {
                        oid: entry.oid,
                        typname: entry.name.clone(),
                        typnamespace: entry.namespace_oid,
                        typowner: BOOTSTRAP_SUPERUSER_OID,
                        typlen: -1,
                        typalign: AttributeAlign::Int,
                        typstorage: AttributeStorage::Extended,
                        typrelid: 0,
                        typelem: 0,
                        typarray: entry.array_oid,
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
                        typlen: -1,
                        typalign: AttributeAlign::Int,
                        typstorage: AttributeStorage::Extended,
                        typrelid: 0,
                        typelem: entry.oid,
                        typarray: 0,
                        sql_type: SqlType::array_of(base_sql_type),
                    },
                ]
            })
            .collect::<Vec<_>>();
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

    pub(crate) fn range_type_rows_for_search_path(&self, search_path: &[String]) -> Vec<PgTypeRow> {
        let range_types = self.range_types.read();
        let mut rows = range_types
            .values()
            .flat_map(|entry| {
                let base_sql_type = SqlType::new(SqlTypeKind::Text).with_identity(entry.oid, 0);
                [
                    PgTypeRow {
                        oid: entry.oid,
                        typname: entry.name.clone(),
                        typnamespace: entry.namespace_oid,
                        typowner: BOOTSTRAP_SUPERUSER_OID,
                        typlen: -1,
                        typalign: AttributeAlign::Int,
                        typstorage: AttributeStorage::Extended,
                        typrelid: 0,
                        typelem: 0,
                        typarray: entry.array_oid,
                        // :HACK: User-defined ranges are currently text-backed. This is enough
                        // for parser/catalog visibility and later DDL type resolution, but it
                        // does not implement PostgreSQL's arbitrary-subtype range semantics yet.
                        sql_type: base_sql_type,
                    },
                    PgTypeRow {
                        oid: entry.array_oid,
                        typname: format!("_{}", entry.name),
                        typnamespace: entry.namespace_oid,
                        typowner: BOOTSTRAP_SUPERUSER_OID,
                        typlen: -1,
                        typalign: AttributeAlign::Int,
                        typstorage: AttributeStorage::Extended,
                        typrelid: 0,
                        typelem: entry.oid,
                        typarray: 0,
                        sql_type: SqlType::array_of(base_sql_type),
                    },
                ]
            })
            .collect::<Vec<_>>();
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

    pub(crate) fn range_rows(&self) -> Vec<PgRangeRow> {
        self.range_types
            .read()
            .values()
            .map(|entry| PgRangeRow {
                rngtypid: entry.oid,
                rngsubtype: entry.subtype.type_oid,
                rngcollation: 0,
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

    pub(crate) fn checkpoint_config_value(&self, name: &str) -> Option<String> {
        self.checkpoint_config.value_for_show(name)
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
}

fn bootstrap_ephemeral_catalog(
    pool: &Arc<BufferPool<SmgrStorageBackend>>,
    txns: &Arc<RwLock<TransactionManager>>,
) -> Result<(), DatabaseError> {
    pool.with_storage_mut(|storage| {
        for kind in bootstrap_catalog_kinds() {
            let rel = RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: kind.relation_oid(),
            };
            let _ = storage.smgr.open(rel);
            let _ =
                storage
                    .smgr
                    .create(rel, crate::backend::storage::smgr::ForkNumber::Main, false);
        }
        for descriptor in system_catalog_indexes() {
            let rel = RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: descriptor.relation_oid,
            };
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
