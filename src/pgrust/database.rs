use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

mod catalog_access;
mod commands;
mod ddl;
pub(crate) mod foreign_keys;
mod relation_refs;
mod temp;
mod toast;
mod txn;

use crate::backend::access::transam::xact::{
    CommandId, MvccError, TransactionId, TransactionManager,
};
use crate::backend::access::transam::xlog::{WalBgWriter, WalError, WalWriter};
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
    ExecError, ExecutorContext, StatementResult, execute_readonly_statement,
};
use crate::backend::parser::Statement;
use crate::backend::parser::{
    AlterTableAddColumnStatement, AlterTableDropColumnStatement, AlterTableRenameColumnStatement,
    AlterTableRenameStatement, AnalyzeStatement, CatalogLookup, CommentOnDomainStatement,
    CommentOnTableStatement, CreateDomainStatement, CreateIndexStatement, CreateSchemaStatement,
    CreateTableAsStatement, CreateTableStatement, CreateViewStatement, DropDomainStatement,
    DropViewStatement, OnCommitAction, ParseError, SqlType, TablePersistence, bind_delete,
    bind_insert, bind_update, create_relation_desc, lower_create_table_with_catalog,
    normalize_create_table_as_name, normalize_create_table_name, normalize_create_view_name,
};
use crate::backend::storage::lmgr::{
    TableLockManager, TableLockMode, lock_relations_interruptible, lock_tables_interruptible,
    unlock_relations,
};
use crate::backend::storage::smgr::{MdStorageManager, RelFileLocator, StorageManager};
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
use crate::backend::utils::misc::interrupts::InterruptState;
use crate::include::access::htup::{AttributeAlign, AttributeStorage};
use crate::include::catalog::{
    BOOTSTRAP_SUPERUSER_OID, PUBLIC_NAMESPACE_OID, PgConstraintRow, PgTypeRow,
    system_catalog_indexes,
};
use crate::pgrust::auth::{AuthCatalog, AuthState};
use crate::pl::plpgsql::execute_do;
use crate::{BufferPool, ClientId, SmgrStorageBackend};
use ddl::{
    ensure_can_set_role, ensure_relation_owner, lookup_heap_relation_for_ddl, map_catalog_error,
    reject_column_with_foreign_key_dependencies, reject_index_with_referencing_foreign_keys,
    reject_inheritance_tree_ddl, reject_relation_with_dependent_views,
    validate_alter_table_add_column,
};
use relation_refs::{collect_direct_relation_oids_from_select, collect_rels_from_planned_stmt};
use toast::{toast_bindings_from_create_result, toast_bindings_from_temp_relation};
use txn::AutoCommitGuard;

#[derive(Debug)]
pub enum DatabaseError {
    Catalog(CatalogError),
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

pub use crate::backend::storage::lmgr::TransactionWaiter;
pub use crate::pgrust::session::{SelectGuard, Session};
pub(crate) use ddl::reject_relation_with_referencing_foreign_keys;
pub(crate) use foreign_keys::{
    alter_table_add_constraint_lock_requests, alter_table_validate_constraint_lock_requests,
    delete_foreign_key_lock_requests, insert_foreign_key_lock_requests,
    prepared_insert_foreign_key_lock_requests, relation_foreign_key_lock_requests,
    table_lock_relations, update_foreign_key_lock_requests,
};

#[derive(Clone)]
pub struct Database {
    pub pool: Arc<BufferPool<SmgrStorageBackend>>,
    pub wal: Option<Arc<WalWriter>>,
    pub txns: Arc<RwLock<TransactionManager>>,
    pub catalog: Arc<RwLock<CatalogStore>>,
    pub txn_waiter: Arc<TransactionWaiter>,
    pub table_locks: Arc<TableLockManager>,
    pub plan_cache: Arc<PlanCache>,
    pub(crate) backend_cache_states: Arc<RwLock<HashMap<ClientId, BackendCacheState>>>,
    pub(crate) session_interrupt_states: Arc<RwLock<HashMap<ClientId, Arc<InterruptState>>>>,
    pub(crate) session_auth_states: Arc<RwLock<HashMap<ClientId, AuthState>>>,
    pub(crate) temp_relations: Arc<RwLock<HashMap<ClientId, TempNamespace>>>,
    pub(crate) domains: Arc<RwLock<BTreeMap<String, DomainEntry>>>,
    _wal_bg_writer: Option<Arc<WalBgWriter>>,
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
        Self::open_with_options(base_dir, pool_size, false)
    }

    pub fn open_with_options(
        base_dir: impl Into<PathBuf>,
        pool_size: usize,
        wal_replay: bool,
    ) -> Result<Self, DatabaseError> {
        let base_dir = base_dir.into();
        std::fs::create_dir_all(&base_dir)
            .map_err(|e| DatabaseError::Catalog(CatalogError::Io(e.to_string())))?;

        let mut txns = TransactionManager::new_durable(&base_dir)?;
        let catalog = CatalogStore::load(&base_dir)?;

        let wal_dir = base_dir.join("pg_wal");
        if wal_replay && wal_dir.join("wal.log").exists() {
            let mut recovery_smgr = MdStorageManager::new_in_recovery(&base_dir);
            {
                use crate::backend::storage::smgr::{ForkNumber, StorageManager};
                let relcache = catalog.relcache()?;
                for (_, entry) in relcache.entries() {
                    let _ = recovery_smgr.open(entry.rel);
                    let _ = recovery_smgr.create(entry.rel, ForkNumber::Main, false);
                }
            }
            let stats = crate::backend::access::transam::xlog::replay::perform_wal_recovery(
                &wal_dir,
                &mut recovery_smgr,
                &mut txns,
            )
            .map_err(DatabaseError::Wal)?;
            if stats.records_replayed > 0 {
                eprintln!(
                    "WAL recovery: {} records ({} FPIs, {} inserts, {} commits, {} aborted)",
                    stats.records_replayed, stats.fpis, stats.inserts, stats.commits, stats.aborted
                );
            }
        }

        let smgr = MdStorageManager::new(&base_dir);
        let wal = Arc::new(WalWriter::new(&wal_dir).map_err(DatabaseError::Wal)?);

        let pool =
            BufferPool::new_with_wal(SmgrStorageBackend::new(smgr), pool_size, Arc::clone(&wal));

        {
            use crate::backend::storage::smgr::{ForkNumber, StorageManager};
            let relcache = catalog.relcache()?;
            for (_, entry) in relcache.entries() {
                let rel = entry.rel;
                pool.with_storage_mut(|s| {
                    let _ = s.smgr.open(rel);
                    let _ = s.smgr.create(rel, ForkNumber::Main, true);
                });
            }
        }

        let wal_bg_writer = WalBgWriter::start(Arc::clone(&wal), Duration::from_millis(200));

        Ok(Self {
            pool: Arc::new(pool),
            wal: Some(wal),
            txns: Arc::new(RwLock::new(txns)),
            catalog: Arc::new(RwLock::new(catalog)),
            txn_waiter: Arc::new(TransactionWaiter::new()),
            table_locks: Arc::new(TableLockManager::new()),
            plan_cache: Arc::new(PlanCache::new()),
            backend_cache_states: Arc::new(RwLock::new(HashMap::new())),
            session_interrupt_states: Arc::new(RwLock::new(HashMap::new())),
            session_auth_states: Arc::new(RwLock::new(HashMap::new())),
            temp_relations: Arc::new(RwLock::new(HashMap::new())),
            domains: Arc::new(RwLock::new(BTreeMap::new())),
            _wal_bg_writer: Some(Arc::new(wal_bg_writer)),
        })
    }

    pub fn open_ephemeral(pool_size: usize) -> Result<Self, DatabaseError> {
        let pool = Arc::new(BufferPool::new(SmgrStorageBackend::new_mem(), pool_size));
        let txns = Arc::new(RwLock::new(TransactionManager::new_ephemeral()));
        bootstrap_ephemeral_catalog(&pool, &txns)?;
        let catalog = CatalogStore::new_ephemeral();

        Ok(Self {
            pool,
            wal: None,
            txns,
            catalog: Arc::new(RwLock::new(catalog)),
            txn_waiter: Arc::new(TransactionWaiter::new()),
            table_locks: Arc::new(TableLockManager::new()),
            plan_cache: Arc::new(PlanCache::new()),
            backend_cache_states: Arc::new(RwLock::new(HashMap::new())),
            session_interrupt_states: Arc::new(RwLock::new(HashMap::new())),
            session_auth_states: Arc::new(RwLock::new(HashMap::new())),
            temp_relations: Arc::new(RwLock::new(HashMap::new())),
            domains: Arc::new(RwLock::new(BTreeMap::new())),
            _wal_bg_writer: None,
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

    pub(crate) fn clear_interrupt_state(&self, client_id: ClientId) {
        self.session_interrupt_states.write().remove(&client_id);
        self.clear_auth_state(client_id);
    }

    pub(crate) fn accept_invalidation_messages(&self, client_id: ClientId) {
        accept_invalidation_messages(self, client_id);
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
    fn drop(&mut self) {
        self.txns.write().flush_clog();
    }
}

#[cfg(test)]
#[path = "database_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "toast_tests.rs"]
mod toast_tests;
