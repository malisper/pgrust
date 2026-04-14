use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

mod catalog_access;
mod commands;
mod ddl;
mod relation_refs;
mod temp;
mod toast;
mod txn;

use crate::backend::access::transam::xact::{
    CommandId, MvccError, TransactionId, TransactionManager,
};
use crate::backend::access::transam::xlog::{WalBgWriter, WalError, WalWriter};
use crate::backend::catalog::catalog::{CatalogIndexBuildOptions, column_desc};
use crate::backend::catalog::namespace::{
    effective_search_path as namespace_effective_search_path,
    normalize_create_table_as_stmt_with_search_path as namespace_normalize_create_table_as_stmt_with_search_path,
    normalize_create_table_stmt_with_search_path as namespace_normalize_create_table_stmt_with_search_path,
    normalize_create_view_stmt_with_search_path as namespace_normalize_create_view_stmt_with_search_path,
};
use crate::backend::catalog::store::{CatalogMutationEffect, CatalogWriteContext};
use crate::backend::catalog::toasting::ToastCatalogChanges;
use crate::backend::catalog::{CatalogError, CatalogStore};
use crate::backend::commands::analyze::collect_analyze_stats;
use crate::backend::executor::{
    ExecError, ExecutorContext, StatementResult, execute_readonly_statement,
};
use crate::backend::parser::Statement;
use crate::backend::parser::{
    AlterTableAddColumnStatement, AlterTableDropColumnStatement, AlterTableRenameColumnStatement,
    AlterTableRenameStatement, AnalyzeStatement, CatalogLookup, CommentOnTableStatement,
    CreateIndexStatement, CreateTableAsStatement, CreateTableStatement, CreateViewStatement,
    DropViewStatement, OnCommitAction, ParseError, TablePersistence, bind_delete, bind_insert,
    bind_update, create_relation_desc, lower_create_table,
};
use crate::backend::storage::lmgr::{
    TableLockManager, TableLockMode, lock_relations_interruptible, lock_tables_interruptible,
    unlock_relations,
};
use crate::backend::storage::smgr::{MdStorageManager, RelFileLocator, StorageManager};
use crate::backend::utils::cache::inval::{
    CatalogInvalidation, catalog_invalidation_from_effect,
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
    SessionCatalogState, invalidate_session_catalog_state,
};
use crate::backend::utils::misc::interrupts::InterruptState;
use crate::include::catalog::PgConstraintRow;
use crate::pl::plpgsql::execute_do;
use crate::{BufferPool, ClientId, SmgrStorageBackend};
use ddl::{
    lookup_heap_relation_for_ddl, map_catalog_error, namespace_oid_for_relation_name,
    reject_relation_with_dependent_views, validate_alter_table_add_column,
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

#[derive(Clone)]
pub struct Database {
    pub pool: Arc<BufferPool<SmgrStorageBackend>>,
    pub wal: Arc<WalWriter>,
    pub txns: Arc<RwLock<TransactionManager>>,
    pub catalog: Arc<RwLock<CatalogStore>>,
    pub txn_waiter: Arc<TransactionWaiter>,
    pub table_locks: Arc<TableLockManager>,
    pub plan_cache: Arc<PlanCache>,
    pub(crate) session_catalog_states: Arc<RwLock<HashMap<ClientId, SessionCatalogState>>>,
    pub(crate) session_interrupt_states: Arc<RwLock<HashMap<ClientId, Arc<InterruptState>>>>,
    pub(crate) temp_relations: Arc<RwLock<HashMap<ClientId, TempNamespace>>>,
    _wal_bg_writer: Arc<WalBgWriter>,
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
            wal,
            txns: Arc::new(RwLock::new(txns)),
            catalog: Arc::new(RwLock::new(catalog)),
            txn_waiter: Arc::new(TransactionWaiter::new()),
            table_locks: Arc::new(TableLockManager::new()),
            plan_cache: Arc::new(PlanCache::new()),
            session_catalog_states: Arc::new(RwLock::new(HashMap::new())),
            session_interrupt_states: Arc::new(RwLock::new(HashMap::new())),
            temp_relations: Arc::new(RwLock::new(HashMap::new())),
            _wal_bg_writer: Arc::new(wal_bg_writer),
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

    pub(crate) fn clear_interrupt_state(&self, client_id: ClientId) {
        self.session_interrupt_states.write().remove(&client_id);
    }
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
