use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::backend::access::transam::xact::{
    CommandId, MvccError, TransactionId, TransactionManager,
};
use crate::backend::access::transam::xlog::{WalBgWriter, WalError, WalWriter};
use crate::backend::catalog::catalog::{CatalogIndexBuildOptions, column_desc};
use crate::backend::catalog::namespace::{
    effective_search_path as namespace_effective_search_path,
    normalize_create_table_as_stmt_with_search_path as namespace_normalize_create_table_as_stmt_with_search_path,
    normalize_create_table_stmt_with_search_path as namespace_normalize_create_table_stmt_with_search_path,
};
use crate::backend::catalog::store::{CatalogMutationEffect, CatalogWriteContext};
use crate::backend::catalog::{CatalogError, CatalogStore};
use crate::backend::executor::{
    ExecError, ExecutorContext, StatementResult, execute_readonly_statement,
};
use crate::backend::parser::Statement;
use crate::backend::parser::{
    AlterTableAddColumnStatement, CatalogLookup, CommentOnTableStatement, CreateIndexStatement,
    CreateTableAsStatement, CreateTableStatement, OnCommitAction, ParseError, TablePersistence,
    bind_delete, bind_insert, bind_update, build_plan, create_relation_desc,
    derive_literal_default_value,
};
use crate::backend::storage::lmgr::{
    TableLockManager, TableLockMode, lock_relations, unlock_relations,
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
use crate::include::catalog::PgConstraintRow;
use crate::pl::plpgsql::execute_do;
use crate::{BufferPool, ClientId, SmgrStorageBackend};

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

/// A thread-safe handle to a pgrust database instance.
///
/// All shared state is behind `Arc` so cloning the handle is cheap and gives
/// each thread independent access to the same pool, transaction manager, and
/// catalog.
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
    pub(crate) temp_relations: Arc<RwLock<HashMap<ClientId, TempNamespace>>>,
    /// Background WAL writer — flushes BufWriter to kernel periodically.
    _wal_bg_writer: Arc<WalBgWriter>,
}

const TEMP_DB_OID_BASE: u32 = 0x7000_0000;
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
    pub tables: BTreeMap<String, TempCatalogEntry>,
    pub generation: u64,
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

        // --- WAL Recovery ---
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

        // Open storage files for all existing relations so inserts don't need to.
        {
            use crate::backend::storage::smgr::{ForkNumber, StorageManager};
            let relcache = catalog.relcache()?;
            for (_, entry) in relcache.entries() {
                let rel = entry.rel;
                pool.with_storage_mut(|s| {
                    let _ = s.smgr.open(rel);
                    // Use is_redo=true so create tolerates existing fork files on restart.
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
            temp_relations: Arc::new(RwLock::new(HashMap::new())),
            _wal_bg_writer: Arc::new(wal_bg_writer),
        })
    }

    pub(crate) fn temp_db_oid(client_id: ClientId) -> u32 {
        TEMP_DB_OID_BASE.saturating_add(client_id)
    }

    pub(crate) fn temp_namespace_name(client_id: ClientId) -> String {
        format!("pg_temp_{client_id}")
    }

    pub(crate) fn temp_namespace_oid(client_id: ClientId) -> u32 {
        Self::temp_db_oid(client_id)
    }

    #[cfg(test)]
    fn has_active_temp_namespace(&self, client_id: ClientId) -> bool {
        self.temp_relations.read().contains_key(&client_id)
    }

    fn owned_temp_namespace(&self, client_id: ClientId) -> Option<TempNamespace> {
        self.temp_relations.read().get(&client_id).cloned()
    }

    pub(crate) fn other_session_temp_namespace_oid(
        &self,
        client_id: ClientId,
        namespace_oid: u32,
    ) -> bool {
        namespace_oid >= TEMP_DB_OID_BASE && namespace_oid != Self::temp_namespace_oid(client_id)
    }

    fn invalidate_session_catalog_state(&self, client_id: ClientId) {
        invalidate_session_catalog_state(self, client_id);
    }

    pub(crate) fn effective_search_path(
        &self,
        client_id: ClientId,
        configured_search_path: Option<&[String]>,
    ) -> Vec<String> {
        namespace_effective_search_path(
            self.owned_temp_namespace(client_id)
                .as_ref()
                .map(|ns| ns.name.as_str()),
            configured_search_path,
        )
    }

    fn normalize_create_table_stmt_with_search_path(
        &self,
        stmt: &CreateTableStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<(String, TablePersistence), ParseError> {
        namespace_normalize_create_table_stmt_with_search_path(stmt, configured_search_path)
    }

    fn normalize_create_table_as_stmt_with_search_path(
        &self,
        stmt: &CreateTableAsStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<(String, TablePersistence), ParseError> {
        namespace_normalize_create_table_as_stmt_with_search_path(stmt, configured_search_path)
    }

    pub(crate) fn lazy_catalog_lookup(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        configured_search_path: Option<&[String]>,
    ) -> LazyCatalogLookup<'_> {
        let search_path = self.effective_search_path(client_id, configured_search_path);
        LazyCatalogLookup {
            db: self,
            client_id,
            txn_ctx,
            search_path,
        }
    }

    pub(crate) fn describe_relation_by_oid(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        relation_oid: u32,
    ) -> Option<RelCacheEntry> {
        describe_relation_by_oid(self, client_id, txn_ctx, relation_oid)
    }

    pub(crate) fn relation_namespace_name(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        relation_oid: u32,
    ) -> Option<String> {
        relation_namespace_name(self, client_id, txn_ctx, relation_oid)
    }

    pub(crate) fn relation_display_name(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        configured_search_path: Option<&[String]>,
        relation_oid: u32,
    ) -> Option<String> {
        relation_display_name(
            self,
            client_id,
            txn_ctx,
            configured_search_path,
            relation_oid,
        )
    }

    pub(crate) fn has_index_on_relation(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        relation_oid: u32,
    ) -> bool {
        has_index_on_relation(self, client_id, txn_ctx, relation_oid)
    }

    pub(crate) fn access_method_name_for_relation(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        relation_oid: u32,
    ) -> Option<String> {
        access_method_name_for_relation(self, client_id, txn_ctx, relation_oid)
    }

    pub(crate) fn constraint_rows_for_relation(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        relation_oid: u32,
    ) -> Vec<PgConstraintRow> {
        constraint_rows_for_relation(self, client_id, txn_ctx, relation_oid)
    }

    #[cfg(test)]
    pub(crate) fn temp_entry(
        &self,
        client_id: ClientId,
        table_name: &str,
    ) -> Option<RelCacheEntry> {
        let normalized = normalize_temp_lookup_name(table_name);
        self.temp_relations
            .read()
            .get(&client_id)
            .and_then(|ns| ns.tables.get(&normalized).map(|entry| entry.entry.clone()))
    }

    fn ensure_temp_namespace(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<TempNamespace, ExecError> {
        if let Some(namespace) = self.owned_temp_namespace(client_id) {
            return Ok(namespace);
        }

        let namespace = TempNamespace {
            oid: Self::temp_namespace_oid(client_id),
            name: Self::temp_namespace_name(client_id),
            tables: BTreeMap::new(),
            generation: 0,
        };
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
        };
        let effect = self
            .catalog
            .write()
            .create_namespace_mvcc(namespace.oid, &namespace.name, &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        {
            let mut namespaces = self.temp_relations.write();
            namespaces.insert(client_id, namespace.clone());
        }
        temp_effects.push(TempMutationEffect::Create {
            name: namespace.name.clone(),
            entry: RelCacheEntry {
                rel: RelFileLocator {
                    spc_oid: 0,
                    db_oid: 1,
                    rel_number: crate::include::catalog::BootstrapCatalogKind::PgNamespace
                        .relation_oid(),
                },
                relation_oid: namespace.oid,
                namespace_oid: namespace.oid,
                row_type_oid: 0,
                relpersistence: 't',
                relkind: 'n',
                desc: crate::backend::executor::RelationDesc {
                    columns: Vec::new(),
                },
                index: None,
            },
            on_commit: OnCommitAction::PreserveRows,
            namespace_created: true,
        });
        self.invalidate_session_catalog_state(client_id);
        Ok(namespace)
    }

    fn create_temp_relation_in_transaction(
        &self,
        client_id: ClientId,
        table_name: String,
        desc: crate::backend::executor::RelationDesc,
        on_commit: OnCommitAction,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<RelCacheEntry, ExecError> {
        let normalized = normalize_temp_lookup_name(&table_name);
        let namespace =
            self.ensure_temp_namespace(client_id, xid, cid, catalog_effects, temp_effects)?;
        if namespace.tables.contains_key(&normalized) {
            return Err(ExecError::Parse(ParseError::TableAlreadyExists(normalized)));
        }

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
        };
        let (entry, effect) = self
            .catalog
            .write()
            .create_table_mvcc_with_options(
                format!("{}.{}", namespace.name, normalized),
                desc,
                namespace.oid,
                Self::temp_db_oid(client_id),
                't',
                &ctx,
            )
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        let rel_entry = RelCacheEntry {
            rel: entry.rel,
            relation_oid: entry.relation_oid,
            namespace_oid: entry.namespace_oid,
            row_type_oid: entry.row_type_oid,
            relpersistence: entry.relpersistence,
            relkind: entry.relkind,
            desc: entry.desc,
            index: None,
        };
        {
            let mut namespaces = self.temp_relations.write();
            let namespace = namespaces.get_mut(&client_id).ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(normalized.clone()))
            })?;
            namespace.tables.insert(
                normalized.clone(),
                TempCatalogEntry {
                    entry: rel_entry.clone(),
                    on_commit,
                },
            );
            namespace.generation = namespace.generation.saturating_add(1);
        }
        temp_effects.push(TempMutationEffect::Create {
            name: normalized,
            entry: rel_entry.clone(),
            on_commit,
            namespace_created: false,
        });
        self.invalidate_session_catalog_state(client_id);
        Ok(rel_entry)
    }

    pub(crate) fn drop_temp_relation_in_transaction(
        &self,
        client_id: ClientId,
        table_name: &str,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<RelCacheEntry, ExecError> {
        let normalized = normalize_temp_lookup_name(table_name);
        let removed = {
            let mut namespaces = self.temp_relations.write();
            let namespace = namespaces.get_mut(&client_id).ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(normalized.clone()))
            })?;
            let removed = namespace.tables.remove(&normalized).ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(normalized.clone()))
            })?;
            namespace.generation = namespace.generation.saturating_add(1);
            removed
        };
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
        };
        let effect = self
            .catalog
            .write()
            .drop_relation_by_oid_mvcc(removed.entry.relation_oid, &ctx)
            .map_err(map_catalog_error)?
            .1;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        temp_effects.push(TempMutationEffect::Drop {
            name: normalized,
            entry: removed.entry.clone(),
            on_commit: removed.on_commit,
        });
        self.invalidate_session_catalog_state(client_id);
        Ok(removed.entry)
    }

    pub(crate) fn apply_temp_on_commit(&self, client_id: ClientId) -> Result<(), ExecError> {
        let mut to_delete = Vec::new();
        let mut to_drop = Vec::new();
        {
            let namespaces = self.temp_relations.read();
            if let Some(namespace) = namespaces.get(&client_id) {
                for (name, entry) in &namespace.tables {
                    match entry.on_commit {
                        OnCommitAction::PreserveRows => {}
                        OnCommitAction::DeleteRows => to_delete.push(entry.entry.rel),
                        OnCommitAction::Drop => to_drop.push(name.clone()),
                    }
                }
            }
        }

        for rel in to_delete {
            let _ = self.pool.invalidate_relation(rel);
            self.pool
                .with_storage_mut(|s| {
                    s.smgr
                        .truncate(rel, crate::backend::storage::smgr::ForkNumber::Main, 0)
                })
                .map_err(crate::backend::access::heap::heapam::HeapError::Storage)?;
        }

        for name in to_drop {
            let xid = self.txns.write().begin();
            let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
            let mut catalog_effects = Vec::new();
            let mut temp_effects = Vec::new();
            let result = self.drop_temp_relation_in_transaction(
                client_id,
                &name,
                xid,
                0,
                &mut catalog_effects,
                &mut temp_effects,
            );
            let result = self.finish_txn(
                client_id,
                xid,
                result.map(|_| StatementResult::AffectedRows(0)),
                &catalog_effects,
                &temp_effects,
            );
            guard.disarm();
            let _ = result?;
        }
        Ok(())
    }

    pub(crate) fn cleanup_client_temp_relations(&self, client_id: ClientId) {
        let Some(namespace) = self.owned_temp_namespace(client_id) else {
            return;
        };
        for name in namespace.tables.keys().cloned().collect::<Vec<_>>() {
            let xid = self.txns.write().begin();
            let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
            let mut catalog_effects = Vec::new();
            let mut temp_effects = Vec::new();
            let result = self.drop_temp_relation_in_transaction(
                client_id,
                &name,
                xid,
                0,
                &mut catalog_effects,
                &mut temp_effects,
            );
            let _ = self.finish_txn(
                client_id,
                xid,
                result.map(|_| StatementResult::AffectedRows(0)),
                &catalog_effects,
                &temp_effects,
            );
            guard.disarm();
        }
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid: 0,
            client_id,
            waiter: None,
        };
        if let Ok(effect) =
            self.catalog
                .write()
                .drop_namespace_mvcc(namespace.oid, &namespace.name, &ctx)
        {
            let _ = self.finish_txn(
                client_id,
                xid,
                Ok(StatementResult::AffectedRows(0)),
                &[effect],
                &[],
            );
            guard.disarm();
        }
    }

    pub(crate) fn execute_create_table_stmt_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateTableStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let mut temp_effects = Vec::new();
        let result = self.execute_create_table_stmt_in_transaction_with_search_path(
            client_id,
            create_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
            &mut temp_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &temp_effects);
        guard.disarm();
        result
    }

    pub(crate) fn execute_comment_on_table_stmt_with_search_path(
        &self,
        client_id: ClientId,
        comment_stmt: &CommentOnTableStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let relation = self
            .lazy_catalog_lookup(client_id, None, configured_search_path)
            .lookup_relation(&comment_stmt.table_name)
            .ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(comment_stmt.table_name.clone()))
            })?;
        self.table_locks
            .lock_table(relation.rel, TableLockMode::AccessExclusive, client_id);
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_comment_on_table_stmt_in_transaction_with_search_path(
            client_id,
            comment_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[]);
        guard.disarm();
        self.table_locks.unlock_table(relation.rel, client_id);
        result
    }

    pub(crate) fn execute_alter_table_add_column_stmt_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterTableAddColumnStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let relation = self
            .lazy_catalog_lookup(client_id, None, configured_search_path)
            .lookup_relation(&alter_stmt.table_name)
            .ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(alter_stmt.table_name.clone()))
            })?;
        self.table_locks
            .lock_table(relation.rel, TableLockMode::AccessExclusive, client_id);
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_table_add_column_stmt_in_transaction_with_search_path(
            client_id,
            alter_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[]);
        guard.disarm();
        self.table_locks.unlock_table(relation.rel, client_id);
        result
    }

    pub(crate) fn execute_comment_on_table_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        comment_stmt: &CommentOnTableStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = catalog.lookup_relation(&comment_stmt.table_name).ok_or_else(|| {
            ExecError::Parse(ParseError::TableDoesNotExist(comment_stmt.table_name.clone()))
        })?;
        if relation.relpersistence == 't' {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "permanent table for COMMENT ON TABLE",
                actual: "temporary table".into(),
            }));
        }

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
        };
        let effect = self
            .catalog
            .write()
            .comment_relation_mvcc(relation.relation_oid, comment_stmt.comment.as_deref(), &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_table_add_column_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        alter_stmt: &AlterTableAddColumnStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let relation = catalog.lookup_relation(&alter_stmt.table_name).ok_or_else(|| {
            ExecError::Parse(ParseError::TableDoesNotExist(alter_stmt.table_name.clone()))
        })?;
        if relation.relpersistence == 't' {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "permanent table for ALTER TABLE ADD COLUMN",
                actual: "temporary table".into(),
            }));
        }
        let column = validate_alter_table_add_column(&relation.desc, &alter_stmt.column)?;
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
        };
        let effect = self
            .catalog
            .write()
            .alter_table_add_column_mvcc(relation.relation_oid, column, &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_create_table_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateTableStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let (table_name, persistence) =
            self.normalize_create_table_stmt_with_search_path(create_stmt, configured_search_path)?;
        let desc = create_relation_desc(create_stmt);
        match persistence {
            TablePersistence::Permanent => {
                let mut catalog_guard = self.catalog.write();
                let ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid,
                    client_id,
                    waiter: None,
                };
                let result =
                    catalog_guard.create_table_mvcc(table_name.clone(), desc.clone(), &ctx);
                match result {
                    Err(CatalogError::TableAlreadyExists(name)) if create_stmt.if_not_exists => {
                        Ok(StatementResult::AffectedRows(0))
                    }
                    Err(err) => Err(match err {
                        CatalogError::TableAlreadyExists(name) => {
                            ExecError::Parse(ParseError::TableAlreadyExists(name))
                        }
                        CatalogError::UnknownTable(name) => {
                            ExecError::Parse(ParseError::TableDoesNotExist(name))
                        }
                        CatalogError::UnknownColumn(name) => {
                            ExecError::Parse(ParseError::UnknownColumn(name))
                        }
                        CatalogError::UnknownType(name) => {
                            ExecError::Parse(ParseError::UnsupportedType(name))
                        }
                        CatalogError::UniqueViolation(constraint) => {
                            ExecError::UniqueViolation { constraint }
                        }
                        CatalogError::Io(_) | CatalogError::Corrupt(_) => {
                            ExecError::Parse(ParseError::UnexpectedToken {
                                expected: "valid catalog state",
                                actual: "catalog error".into(),
                            })
                        }
                    }),
                    Ok((entry, effect)) => {
                        drop(catalog_guard);
                        self.apply_catalog_mutation_effect_immediate(&effect)?;
                        catalog_effects.push(effect);
                        let _ = entry;
                        Ok(StatementResult::AffectedRows(0))
                    }
                }
            }
            TablePersistence::Temporary => {
                let _ = self.create_temp_relation_in_transaction(
                    client_id,
                    table_name,
                    desc,
                    create_stmt.on_commit,
                    xid,
                    cid,
                    catalog_effects,
                    temp_effects,
                )?;
                Ok(StatementResult::AffectedRows(0))
            }
        }
    }

    pub(crate) fn execute_create_index_stmt_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateIndexStatement,
        configured_search_path: Option<&[String]>,
        maintenance_work_mem_kb: usize,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_create_index_stmt_in_transaction_with_search_path(
            client_id,
            create_stmt,
            xid,
            0,
            configured_search_path,
            maintenance_work_mem_kb,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_create_index_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateIndexStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        maintenance_work_mem_kb: usize,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let entry = catalog
            .lookup_any_relation(&create_stmt.table_name)
            .ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(
                    create_stmt.table_name.clone(),
                ))
            })?;

        if entry.relpersistence == 't' {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "permanent table for CREATE INDEX",
                actual: "temporary table".into(),
            }));
        }
        if entry.relkind != 'r' {
            return Err(ExecError::Parse(ParseError::TableDoesNotExist(
                create_stmt.table_name.clone(),
            )));
        }
        let access_method = crate::backend::utils::cache::lsyscache::access_method_row_by_name(
            self,
            client_id,
            Some((xid, cid)),
            create_stmt.using_method.as_deref().unwrap_or("btree"),
        )
        .filter(|row| row.amtype == 'i')
        .ok_or_else(|| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "USING btree",
                actual: "unsupported index access method".into(),
            })
        })?;
        if !access_method.amname.eq_ignore_ascii_case("btree") {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "USING btree",
                actual: "unsupported index access method".into(),
            }));
        }
        if !create_stmt.include_columns.is_empty()
            || !create_stmt.options.is_empty()
            || create_stmt.predicate.is_some()
            || create_stmt
                .columns
                .iter()
                .any(|column| column.descending || column.nulls_first.is_some())
        {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "simple btree column index",
                actual: "unsupported CREATE INDEX feature".into(),
            }));
        }
        let type_rows = crate::backend::utils::cache::syscache::ensure_type_rows(
            self,
            client_id,
            Some((xid, cid)),
        );
        let mut indclass = Vec::with_capacity(create_stmt.columns.len());
        let mut indcollation = Vec::with_capacity(create_stmt.columns.len());
        let mut indoption = Vec::with_capacity(create_stmt.columns.len());
        for column in &create_stmt.columns {
            let bound_column = entry
                .desc
                .columns
                .iter()
                .find(|desc| desc.name.eq_ignore_ascii_case(&column.name))
                .ok_or_else(|| ExecError::Parse(ParseError::UnknownColumn(column.name.clone())))?;
            let type_oid = type_rows
                .iter()
                .find(|row| row.sql_type == bound_column.sql_type)
                .map(|row| row.oid)
                .ok_or_else(|| {
                    ExecError::Parse(ParseError::UnsupportedType(column.name.clone()))
                })?;
            let opclass = crate::backend::utils::cache::lsyscache::default_opclass_for_am_and_type(
                self,
                client_id,
                Some((xid, cid)),
                access_method.oid,
                type_oid,
            )
            .ok_or_else(|| ExecError::Parse(ParseError::UnsupportedType(column.name.clone())))?;
            indclass.push(opclass.oid);
            indcollation.push(0);
            let mut option = 0i16;
            if column.descending {
                option |= 0x0001;
            }
            if column.nulls_first.unwrap_or(false) {
                option |= 0x0002;
            }
            indoption.push(option);
        }
        let build_options = CatalogIndexBuildOptions {
            am_oid: access_method.oid,
            indclass,
            indcollation,
            indoption,
        };

        let mut catalog_guard = self.catalog.write();
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
        };
        let result = catalog_guard.create_index_for_relation_mvcc_with_options(
            create_stmt.index_name.clone(),
            entry.relation_oid,
            create_stmt.unique,
            &create_stmt.columns,
            &build_options,
            &ctx,
        );
        match result {
            Ok((index_entry, effect)) => {
                drop(catalog_guard);
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                let snapshot = self
                    .txns
                    .read()
                    .snapshot_for_command(xid, cid)
                    .map_err(|_| ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "index build snapshot",
                        actual: "snapshot creation failed".into(),
                    }))?;
                let index_meta = index_entry
                    .index_meta
                    .clone()
                    .ok_or_else(|| ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "index metadata",
                        actual: "missing index metadata".into(),
                    }))?;
                let build_ctx = crate::include::access::amapi::IndexBuildContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    client_id,
                    snapshot,
                    heap_relation: entry.rel,
                    heap_desc: entry.desc.clone(),
                    index_relation: index_entry.rel,
                    index_name: create_stmt.index_name.clone(),
                    index_desc: index_entry.desc.clone(),
                    index_meta: crate::backend::utils::cache::relcache::IndexRelCacheEntry {
                        indrelid: index_meta.indrelid,
                        indnatts: index_meta.indkey.len() as i16,
                        indnkeyatts: index_meta.indkey.len() as i16,
                        indisunique: index_meta.indisunique,
                        indnullsnotdistinct: false,
                        indisprimary: false,
                        indisexclusion: false,
                        indimmediate: false,
                        indisclustered: false,
                        indisvalid: index_meta.indisvalid,
                        indcheckxmin: false,
                        indisready: index_meta.indisready,
                        indislive: index_meta.indislive,
                        indisreplident: false,
                        am_oid: access_method.oid,
                        am_handler_oid: Some(access_method.amhandler),
                        indkey: index_meta.indkey.clone(),
                        indclass: index_meta.indclass.clone(),
                        indcollation: index_meta.indcollation.clone(),
                        indoption: index_meta.indoption.clone(),
                        opfamily_oids: Vec::new(),
                        opcintype_oids: Vec::new(),
                        indexprs: index_meta.indexprs.clone(),
                        indpred: index_meta.indpred.clone(),
                    },
                    maintenance_work_mem_kb,
                };
                crate::backend::access::index::indexam::index_build_stub(
                    &build_ctx,
                    access_method.oid,
                )
                .map_err(|err| match err {
                    CatalogError::UniqueViolation(constraint) => {
                        ExecError::UniqueViolation { constraint }
                    }
                    _ => ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "index access method build",
                        actual: "index build failed".into(),
                    }),
                })?;
                let mut catalog_guard = self.catalog.write();
                let readiness_ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid: cid.saturating_add(1),
                    client_id,
                    waiter: None,
                };
                let ready_effect = catalog_guard.set_index_ready_valid_mvcc(
                    index_entry.relation_oid,
                    true,
                    true,
                    &readiness_ctx,
                );
                drop(catalog_guard);
                catalog_effects.push(effect.clone());
                catalog_effects.push(ready_effect.map_err(|_| {
                    ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "index catalog readiness update",
                        actual: "index readiness update failed".into(),
                    })
                })?);
                return Ok(StatementResult::AffectedRows(0));
            }
            Err(err) => Err(match err {
                CatalogError::TableAlreadyExists(name) => {
                    ExecError::Parse(ParseError::TableAlreadyExists(name))
                }
                CatalogError::UnknownTable(name) => {
                    ExecError::Parse(ParseError::TableDoesNotExist(name))
                }
                CatalogError::UnknownColumn(name) => {
                    ExecError::Parse(ParseError::UnknownColumn(name))
                }
                CatalogError::UnknownType(name) => {
                    ExecError::Parse(ParseError::UnsupportedType(name))
                }
                CatalogError::UniqueViolation(constraint) => {
                    ExecError::UniqueViolation { constraint }
                }
                CatalogError::Io(_) | CatalogError::Corrupt(_) => {
                    ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "valid catalog state",
                        actual: "catalog error".into(),
                    })
                }
            }),
        }
    }

    pub(crate) fn execute_drop_table_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &crate::backend::parser::DropTableStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let rels = drop_stmt
            .table_names
            .iter()
            .filter_map(|name| catalog.lookup_any_relation(name).map(|e| e.rel))
            .collect::<Vec<_>>();
        for rel in &rels {
            self.table_locks
                .lock_table(*rel, TableLockMode::AccessExclusive, client_id);
        }

        let mut dropped = 0usize;
        let mut result = Ok(StatementResult::AffectedRows(0));
        for table_name in &drop_stmt.table_names {
            let maybe_entry = catalog.lookup_any_relation(table_name);
            if maybe_entry
                .as_ref()
                .is_some_and(|entry| entry.relpersistence == 't')
            {
                match self.drop_temp_relation_in_transaction(
                    client_id,
                    table_name,
                    xid,
                    cid,
                    catalog_effects,
                    temp_effects,
                ) {
                    Ok(_) => dropped += 1,
                    Err(_) if drop_stmt.if_exists => {}
                    Err(err) => {
                        result = Err(err);
                        break;
                    }
                }
                continue;
            }

            let relation_oid = match maybe_entry.as_ref() {
                Some(entry) if entry.relkind == 'r' => entry.relation_oid,
                Some(_) | None if drop_stmt.if_exists => continue,
                Some(_) | None => {
                    result = Err(ExecError::Parse(ParseError::TableDoesNotExist(
                        table_name.clone(),
                    )));
                    break;
                }
            };
            let mut catalog_guard = self.catalog.write();
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid,
                client_id,
                waiter: Some(self.txn_waiter.clone()),
            };
            match catalog_guard.drop_relation_by_oid_mvcc(relation_oid, &ctx) {
                Ok((entries, effect)) => {
                    drop(catalog_guard);
                    self.apply_catalog_mutation_effect_immediate(&effect)?;
                    catalog_effects.push(effect);
                    let _ = entries;
                    dropped += 1;
                }
                Err(CatalogError::UnknownTable(_)) if drop_stmt.if_exists => {}
                Err(CatalogError::UnknownTable(_)) => {
                    result = Err(ExecError::Parse(ParseError::TableDoesNotExist(
                        table_name.clone(),
                    )));
                    break;
                }
                Err(other) => {
                    result = Err(ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "droppable table",
                        actual: format!("{other:?}"),
                    }));
                    break;
                }
            }
        }

        for rel in rels {
            self.table_locks.unlock_table(rel, client_id);
        }

        if result.is_ok() {
            Ok(StatementResult::AffectedRows(dropped))
        } else {
            result
        }
    }

    pub(crate) fn execute_create_table_as_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateTableAsStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let (table_name, persistence) = self
            .normalize_create_table_as_stmt_with_search_path(create_stmt, configured_search_path)?;
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let plan = build_plan(&create_stmt.query, &catalog)?;
        let mut rels = std::collections::BTreeSet::new();
        collect_rels_from_plan(&plan, &mut rels);

        let snapshot = self.txns.read().snapshot_for_command(xid, cid)?;
        let mut ctx = ExecutorContext {
            pool: Arc::clone(&self.pool),
            txns: self.txns.clone(),
            txn_waiter: Some(self.txn_waiter.clone()),
            snapshot,
            client_id,
            next_command_id: cid,
            outer_rows: Vec::new(),
            timed: false,
        };
        let query_result = execute_readonly_statement(
            Statement::Select(create_stmt.query.clone()),
            &catalog,
            &mut ctx,
        );
        let StatementResult::Query {
            columns,
            column_names,
            rows,
        } = query_result?
        else {
            unreachable!("ctas query should return rows");
        };

        let desc = crate::backend::executor::RelationDesc {
            columns: columns
                .iter()
                .enumerate()
                .map(|(index, column)| {
                    let name = create_stmt
                        .column_names
                        .get(index)
                        .cloned()
                        .unwrap_or_else(|| column_names[index].clone());
                    column_desc(name, column.sql_type, true)
                })
                .collect(),
        };

        let rel = match persistence {
            TablePersistence::Permanent => {
                let stmt = CreateTableStatement {
                    schema_name: None,
                    table_name: table_name.clone(),
                    persistence,
                    on_commit: create_stmt.on_commit,
                    columns: desc
                        .columns
                        .iter()
                        .map(|column| crate::backend::parser::ColumnDef {
                            name: column.name.clone(),
                            ty: column.sql_type,
                            nullable: true,
                            default_expr: None,
                        })
                        .collect(),
                    if_not_exists: create_stmt.if_not_exists,
                };
                let mut catalog_guard = self.catalog.write();
                let write_ctx = CatalogWriteContext {
                    pool: self.pool.clone(),
                    txns: self.txns.clone(),
                    xid,
                    cid,
                    client_id,
                    waiter: None,
                };
                let (entry, effect) = catalog_guard
                    .create_table_mvcc(table_name.clone(), create_relation_desc(&stmt), &write_ctx)
                    .map_err(|err| match err {
                        CatalogError::TableAlreadyExists(name) => {
                            ExecError::Parse(ParseError::TableAlreadyExists(name))
                        }
                        CatalogError::UnknownTable(name) => {
                            ExecError::Parse(ParseError::TableDoesNotExist(name))
                        }
                        CatalogError::UnknownColumn(name) => {
                            ExecError::Parse(ParseError::UnknownColumn(name))
                        }
                        CatalogError::UnknownType(name) => {
                            ExecError::Parse(ParseError::UnsupportedType(name))
                        }
                        CatalogError::UniqueViolation(constraint) => {
                            ExecError::UniqueViolation { constraint }
                        }
                        CatalogError::Io(_) | CatalogError::Corrupt(_) => {
                            ExecError::Parse(ParseError::UnexpectedToken {
                                expected: "valid catalog state",
                                actual: "catalog error".into(),
                            })
                        }
                    })?;
                drop(catalog_guard);
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
                entry.rel
            }
            TablePersistence::Temporary => {
                self.create_temp_relation_in_transaction(
                    client_id,
                    table_name.clone(),
                    desc.clone(),
                    create_stmt.on_commit,
                    xid,
                    cid,
                    catalog_effects,
                    temp_effects,
                )?
                .rel
            }
        };

        if rows.is_empty() {
            return Ok(StatementResult::AffectedRows(0));
        }

        let snapshot = self.txns.read().snapshot_for_command(xid, cid)?;
        let mut insert_ctx = ExecutorContext {
            pool: Arc::clone(&self.pool),
            txns: self.txns.clone(),
            txn_waiter: Some(self.txn_waiter.clone()),
            snapshot,
            client_id,
            next_command_id: cid,
            outer_rows: Vec::new(),
            timed: false,
        };
        let inserted = crate::backend::commands::tablecmds::execute_insert_values(
            rel,
            &desc,
            &[],
            &rows,
            &mut insert_ctx,
            xid,
            cid,
        )?;
        Ok(StatementResult::AffectedRows(inserted))
    }

    pub(crate) fn execute_create_table_as_stmt_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateTableAsStatement,
        xid: Option<TransactionId>,
        cid: u32,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        if let Some(xid) = xid {
            let mut catalog_effects = Vec::new();
            let mut temp_effects = Vec::new();
            return self.execute_create_table_as_stmt_in_transaction_with_search_path(
                client_id,
                create_stmt,
                xid,
                cid,
                configured_search_path,
                &mut catalog_effects,
                &mut temp_effects,
            );
        }
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let mut temp_effects = Vec::new();
        let result = self.execute_create_table_as_stmt_in_transaction_with_search_path(
            client_id,
            create_stmt,
            xid,
            cid,
            configured_search_path,
            &mut catalog_effects,
            &mut temp_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &temp_effects);
        guard.disarm();
        result
    }

    /// Execute a single SQL statement inside an auto-commit transaction
    /// (for DML) or without a transaction (for queries/DDL).
    pub fn execute(&self, client_id: ClientId, sql: &str) -> Result<StatementResult, ExecError> {
        self.execute_with_search_path(client_id, sql, None)
    }

    pub(crate) fn execute_with_search_path(
        &self,
        client_id: ClientId,
        sql: &str,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        use crate::backend::access::transam::xact::INVALID_TRANSACTION_ID;
        use crate::backend::commands::tablecmds::{
            execute_analyze, execute_delete_with_waiter, execute_insert, execute_truncate_table,
            execute_update_with_waiter, execute_vacuum,
        };
        use crate::backend::executor::execute_readonly_statement;

        let stmt = self.plan_cache.get_statement(sql)?;

        match stmt {
            Statement::Do(ref do_stmt) => execute_do(do_stmt),
            Statement::Analyze(ref analyze_stmt) => {
                let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                execute_analyze(analyze_stmt.clone(), &catalog)
            }
            Statement::CreateIndex(ref create_stmt) => {
                self.execute_create_index_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                    65_536,
                )
            }
            Statement::AlterTableAddColumn(ref alter_stmt) => self
                .execute_alter_table_add_column_stmt_with_search_path(
                    client_id,
                    alter_stmt,
                    configured_search_path,
                ),
            Statement::Set(_)
            | Statement::Reset(_)
            // :HACK: numeric.sql also sets parallel_workers reloptions. Accept and ignore that
            // narrow ALTER TABLE form until table reloptions are represented properly.
            | Statement::AlterTableSet(_) => Ok(StatementResult::AffectedRows(0)),
            Statement::CommentOnTable(ref comment_stmt) => self
                .execute_comment_on_table_stmt_with_search_path(
                    client_id,
                    comment_stmt,
                    configured_search_path,
                ),
            Statement::Select(_) | Statement::Values(_) | Statement::Explain(_) => {
                let visible_catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                let (plan_or_stmt, rels) = {
                    let mut rels = std::collections::BTreeSet::new();
                    match &stmt {
                        Statement::Select(select) => {
                            let plan =
                                crate::backend::parser::build_plan(select, &visible_catalog)?;
                            collect_rels_from_plan(&plan, &mut rels);
                        }
                        Statement::Values(_) => {}
                        Statement::Explain(explain) => {
                            if let Statement::Select(select) = explain.statement.as_ref() {
                                let plan =
                                    crate::backend::parser::build_plan(select, &visible_catalog)?;
                                collect_rels_from_plan(&plan, &mut rels);
                            }
                        }
                        _ => unreachable!(),
                    }
                    (stmt, rels.into_iter().collect::<Vec<_>>())
                };

                lock_relations(&self.table_locks, client_id, &rels);

                let snapshot = self.txns.read().snapshot(INVALID_TRANSACTION_ID)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    snapshot,
                    client_id,
                    next_command_id: 0,
                    outer_rows: Vec::new(),
                    timed: false,
                };
                let result = execute_readonly_statement(plan_or_stmt, &visible_catalog, &mut ctx);
                drop(ctx);

                unlock_relations(&self.table_locks, client_id, &rels);
                result
            }

            Statement::Insert(ref insert_stmt) => {
                let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                let bound = bind_insert(insert_stmt, &catalog)?;
                let rel = bound.rel;
                self.table_locks
                    .lock_table(rel, TableLockMode::RowExclusive, client_id);

                let xid = self.txns.write().begin();
                let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
                let snapshot = self.txns.read().snapshot_for_command(xid, 0)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    snapshot,
                    client_id,
                    next_command_id: 0,
                    outer_rows: Vec::new(),
                    timed: false,
                };
                let result = execute_insert(bound, &mut ctx, xid, 0);
                drop(ctx);
                let result = self.finish_txn(client_id, xid, result, &[], &[]);
                guard.disarm();
                self.table_locks.unlock_table(rel, client_id);
                result
            }

            Statement::Update(ref update_stmt) => {
                let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                let bound = bind_update(update_stmt, &catalog)?;
                let rel = bound.rel;
                self.table_locks
                    .lock_table(rel, TableLockMode::RowExclusive, client_id);

                let xid = self.txns.write().begin();
                let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
                let snapshot = self.txns.read().snapshot_for_command(xid, 0)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    snapshot,
                    client_id,
                    next_command_id: 0,
                    outer_rows: Vec::new(),
                    timed: false,
                };
                let result = execute_update_with_waiter(
                    bound,
                    &mut ctx,
                    xid,
                    0,
                    Some((&self.txns, &self.txn_waiter)),
                );
                drop(ctx);
                let result = self.finish_txn(client_id, xid, result, &[], &[]);
                guard.disarm();
                self.table_locks.unlock_table(rel, client_id);
                result
            }

            Statement::Delete(ref delete_stmt) => {
                let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                let bound = bind_delete(delete_stmt, &catalog)?;
                let rel = bound.rel;
                self.table_locks
                    .lock_table(rel, TableLockMode::RowExclusive, client_id);

                let xid = self.txns.write().begin();
                let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
                let snapshot = self.txns.read().snapshot_for_command(xid, 0)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    snapshot,
                    client_id,
                    next_command_id: 0,
                    outer_rows: Vec::new(),
                    timed: false,
                };
                let result = execute_delete_with_waiter(
                    bound,
                    &mut ctx,
                    xid,
                    Some((&self.txns, &self.txn_waiter)),
                );
                drop(ctx);
                let result = self.finish_txn(client_id, xid, result, &[], &[]);
                guard.disarm();
                self.table_locks.unlock_table(rel, client_id);
                result
            }

            Statement::CreateTable(ref create_stmt) => {
                self.execute_create_table_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                )
            }

            Statement::CreateTableAs(ref create_stmt) => {
                self.execute_create_table_as_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    None,
                    0,
                    configured_search_path,
                )
            }

            Statement::DropTable(ref drop_stmt) => {
                let xid = self.txns.write().begin();
                let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
                let mut catalog_effects = Vec::new();
                let mut temp_effects = Vec::new();
                let result = self.execute_drop_table_stmt_in_transaction_with_search_path(
                    client_id,
                    drop_stmt,
                    xid,
                    0,
                    configured_search_path,
                    &mut catalog_effects,
                    &mut temp_effects,
                );
                let result = self.finish_txn(client_id, xid, result, &catalog_effects, &temp_effects);
                guard.disarm();
                result
            }

            Statement::TruncateTable(ref truncate_stmt) => {
                let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                let rels = {
                    truncate_stmt
                        .table_names
                        .iter()
                        .filter_map(|name| catalog.lookup_any_relation(name).map(|e| e.rel))
                        .collect::<Vec<_>>()
                };
                for rel in &rels {
                    self.table_locks
                        .lock_table(*rel, TableLockMode::AccessExclusive, client_id);
                }

                let snapshot = self.txns.read().snapshot(INVALID_TRANSACTION_ID)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    txns: self.txns.clone(),
                    txn_waiter: Some(self.txn_waiter.clone()),
                    snapshot,
                    client_id,
                    next_command_id: 0,
                    outer_rows: Vec::new(),
                    timed: false,
                };
                let result = execute_truncate_table(
                    truncate_stmt.clone(),
                    &catalog,
                    &mut ctx,
                    INVALID_TRANSACTION_ID,
                );
                drop(ctx);
                for rel in rels {
                    self.table_locks.unlock_table(rel, client_id);
                }
                result
            }

            Statement::Vacuum(ref vacuum_stmt) => {
                let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
                execute_vacuum(vacuum_stmt.clone(), &catalog)
            }

            Statement::Begin | Statement::Commit | Statement::Rollback => {
                Ok(StatementResult::AffectedRows(0))
            }
        }
    }

    /// Set up a SELECT query for streaming: lock, snapshot, plan, but do NOT
    /// execute.  Returns a `SelectGuard` that the caller can drive with
    /// `exec_next` one row at a time.  The guard releases the table lock on
    /// drop.
    ///
    /// If `txn_ctx` is provided, the snapshot is taken within that
    /// transaction (Read Committed: fresh snapshot per statement, but
    /// aware of the transaction's own writes via xid/cid).
    pub fn execute_streaming(
        &self,
        client_id: ClientId,
        select_stmt: &crate::backend::parser::SelectStatement,
        txn_ctx: Option<(TransactionId, CommandId)>,
    ) -> Result<SelectGuard<'_>, ExecError> {
        self.execute_streaming_with_search_path(client_id, select_stmt, txn_ctx, None)
    }

    pub(crate) fn execute_streaming_with_search_path(
        &self,
        client_id: ClientId,
        select_stmt: &crate::backend::parser::SelectStatement,
        txn_ctx: Option<(TransactionId, CommandId)>,
        configured_search_path: Option<&[String]>,
    ) -> Result<SelectGuard<'_>, ExecError> {
        use crate::backend::access::transam::xact::INVALID_TRANSACTION_ID;
        use crate::backend::executor::executor_start;
        use crate::backend::parser::build_plan;

        let (plan, rels) = {
            let visible_catalog =
                self.lazy_catalog_lookup(client_id, txn_ctx, configured_search_path);
            let plan = build_plan(select_stmt, &visible_catalog)?;
            let mut rels = std::collections::BTreeSet::new();
            collect_rels_from_plan(&plan, &mut rels);
            (plan, rels.into_iter().collect::<Vec<_>>())
        };

        lock_relations(&self.table_locks, client_id, &rels);

        let (snapshot, command_id) = match txn_ctx {
            Some((xid, cid)) => (self.txns.read().snapshot_for_command(xid, cid)?, cid),
            None => (self.txns.read().snapshot(INVALID_TRANSACTION_ID)?, 0),
        };
        let columns = plan.columns();
        let column_names = plan.column_names();
        let state = executor_start(plan);
        let ctx = ExecutorContext {
            pool: std::sync::Arc::clone(&self.pool),
            txns: self.txns.clone(),
            txn_waiter: Some(self.txn_waiter.clone()),
            snapshot,
            client_id,
            next_command_id: command_id,
            outer_rows: Vec::new(),
            timed: false,
        };

        Ok(SelectGuard {
            state,
            ctx,
            columns,
            column_names,
            rels,
            table_locks: &self.table_locks,
            client_id,
        })
    }
}

fn normalize_temp_lookup_name(table_name: &str) -> String {
    table_name
        .strip_prefix("pg_temp.")
        .unwrap_or(table_name)
        .to_ascii_lowercase()
}

fn collect_rels_from_expr(
    expr: &crate::backend::executor::Expr,
    rels: &mut std::collections::BTreeSet<RelFileLocator>,
) {
    use crate::backend::executor::Expr;

    match expr {
        Expr::Column(_)
        | Expr::OuterColumn { .. }
        | Expr::Const(_)
        | Expr::Random
        | Expr::CurrentTimestamp => {}
        Expr::UnaryPlus(inner)
        | Expr::Negate(inner)
        | Expr::BitNot(inner)
        | Expr::Cast(inner, _)
        | Expr::Not(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => collect_rels_from_expr(inner, rels),
        Expr::Add(left, right)
        | Expr::Sub(left, right)
        | Expr::BitAnd(left, right)
        | Expr::BitOr(left, right)
        | Expr::BitXor(left, right)
        | Expr::Shl(left, right)
        | Expr::Shr(left, right)
        | Expr::Mul(left, right)
        | Expr::Div(left, right)
        | Expr::Mod(left, right)
        | Expr::Concat(left, right)
        | Expr::Eq(left, right)
        | Expr::NotEq(left, right)
        | Expr::Lt(left, right)
        | Expr::LtEq(left, right)
        | Expr::Gt(left, right)
        | Expr::GtEq(left, right)
        | Expr::RegexMatch(left, right)
        | Expr::And(left, right)
        | Expr::Or(left, right)
        | Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::JsonGet(left, right)
        | Expr::JsonGetText(left, right)
        | Expr::JsonPath(left, right)
        | Expr::JsonPathText(left, right)
        | Expr::JsonbContains(left, right)
        | Expr::JsonbContained(left, right)
        | Expr::JsonbExists(left, right)
        | Expr::JsonbExistsAny(left, right)
        | Expr::JsonbExistsAll(left, right)
        | Expr::JsonbPathExists(left, right)
        | Expr::JsonbPathMatch(left, right) => {
            collect_rels_from_expr(left, rels);
            collect_rels_from_expr(right, rels);
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            collect_rels_from_expr(expr, rels);
            collect_rels_from_expr(pattern, rels);
            if let Some(escape) = escape {
                collect_rels_from_expr(escape, rels);
            }
        }
        Expr::FuncCall { args, .. } => {
            for arg in args {
                collect_rels_from_expr(arg, rels);
            }
        }
        Expr::ArrayLiteral { elements, .. } => {
            for element in elements {
                collect_rels_from_expr(element, rels);
            }
        }
        Expr::ArrayOverlap(left, right) => {
            collect_rels_from_expr(left, rels);
            collect_rels_from_expr(right, rels);
        }
        Expr::ScalarSubquery(plan) | Expr::ExistsSubquery(plan) => {
            collect_rels_from_plan(plan, rels);
        }
        Expr::AnySubquery { left, subquery, .. } | Expr::AllSubquery { left, subquery, .. } => {
            collect_rels_from_expr(left, rels);
            collect_rels_from_plan(subquery, rels);
        }
        Expr::AnyArray { left, right, .. } | Expr::AllArray { left, right, .. } => {
            collect_rels_from_expr(left, rels);
            collect_rels_from_expr(right, rels);
        }
    }
}

fn collect_rels_from_plan(
    plan: &crate::backend::executor::Plan,
    rels: &mut std::collections::BTreeSet<RelFileLocator>,
) {
    use crate::backend::executor::Plan;

    match plan {
        Plan::Result => {}
        Plan::SeqScan { rel, .. } => {
            rels.insert(*rel);
        }
        Plan::IndexScan { rel, .. } => {
            rels.insert(*rel);
        }
        Plan::NestedLoopJoin { left, right, on } => {
            collect_rels_from_plan(left, rels);
            collect_rels_from_plan(right, rels);
            collect_rels_from_expr(on, rels);
        }
        Plan::Filter { input, predicate } => {
            collect_rels_from_plan(input, rels);
            collect_rels_from_expr(predicate, rels);
        }
        Plan::OrderBy { input, items } => {
            collect_rels_from_plan(input, rels);
            for item in items {
                collect_rels_from_expr(&item.expr, rels);
            }
        }
        Plan::Limit { input, .. } => collect_rels_from_plan(input, rels),
        Plan::Projection { input, targets } => {
            collect_rels_from_plan(input, rels);
            for target in targets {
                collect_rels_from_expr(&target.expr, rels);
            }
        }
        Plan::Aggregate {
            input,
            group_by,
            accumulators,
            having,
            ..
        } => {
            collect_rels_from_plan(input, rels);
            for expr in group_by {
                collect_rels_from_expr(expr, rels);
            }
            for accum in accumulators {
                for arg in &accum.args {
                    collect_rels_from_expr(arg, rels);
                }
            }
            if let Some(expr) = having {
                collect_rels_from_expr(expr, rels);
            }
        }
        Plan::FunctionScan { call } => match call {
            crate::include::nodes::plannodes::SetReturningCall::GenerateSeries {
                start,
                stop,
                step,
                ..
            } => {
                collect_rels_from_expr(start, rels);
                collect_rels_from_expr(stop, rels);
                collect_rels_from_expr(step, rels);
            }
            crate::include::nodes::plannodes::SetReturningCall::Unnest { args, .. } => {
                for arg in args {
                    collect_rels_from_expr(arg, rels);
                }
            }
            crate::include::nodes::plannodes::SetReturningCall::JsonTableFunction { args, .. } => {
                for arg in args {
                    collect_rels_from_expr(arg, rels);
                }
            }
            crate::include::nodes::plannodes::SetReturningCall::RegexTableFunction {
                args, ..
            } => {
                for arg in args {
                    collect_rels_from_expr(arg, rels);
                }
            }
        },
        Plan::Values { rows, .. } => {
            for row in rows {
                for expr in row {
                    collect_rels_from_expr(expr, rels);
                }
            }
        }
        Plan::ProjectSet { input, targets } => {
            collect_rels_from_plan(input, rels);
            for target in targets {
                match target {
                    crate::include::nodes::plannodes::ProjectSetTarget::Scalar(entry) => {
                        collect_rels_from_expr(&entry.expr, rels);
                    }
                    crate::include::nodes::plannodes::ProjectSetTarget::Set { call, .. } => {
                        match call {
                            crate::include::nodes::plannodes::SetReturningCall::GenerateSeries {
                                start,
                                stop,
                                step,
                                ..
                            } => {
                                collect_rels_from_expr(start, rels);
                                collect_rels_from_expr(stop, rels);
                                collect_rels_from_expr(step, rels);
                            }
                            crate::include::nodes::plannodes::SetReturningCall::Unnest {
                                args,
                                ..
                            } => {
                                for arg in args {
                                    collect_rels_from_expr(arg, rels);
                                }
                            }
                            crate::include::nodes::plannodes::SetReturningCall::JsonTableFunction {
                                args,
                                ..
                            } => {
                                for arg in args {
                                    collect_rels_from_expr(arg, rels);
                                }
                            }
                            crate::include::nodes::plannodes::SetReturningCall::RegexTableFunction {
                                args,
                                ..
                            } => {
                                for arg in args {
                                    collect_rels_from_expr(arg, rels);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

impl Database {
    pub(crate) fn catalog_invalidation_from_effect(
        effect: &CatalogMutationEffect,
    ) -> CatalogInvalidation {
        catalog_invalidation_from_effect(effect)
    }

    pub(crate) fn finalize_command_end_local_catalog_invalidations(
        &self,
        client_id: ClientId,
        invalidations: &[CatalogInvalidation],
    ) {
        finalize_command_end_local_catalog_invalidations(self, client_id, invalidations);
    }

    fn apply_catalog_mutation_effect_immediate(
        &self,
        effect: &CatalogMutationEffect,
    ) -> Result<(), ExecError> {
        for rel in &effect.created_rels {
            self.pool
                .with_storage_mut(|s| {
                    let _ = s.smgr.open(*rel);
                    s.smgr
                        .create(*rel, crate::backend::storage::smgr::ForkNumber::Main, true)
                })
                .map_err(|e| {
                    ExecError::Heap(crate::backend::access::heap::heapam::HeapError::Storage(e))
                })?;
        }
        Ok(())
    }

    pub(crate) fn finalize_committed_catalog_effects(
        &self,
        source_client_id: ClientId,
        effects: &[CatalogMutationEffect],
        invalidations: &[CatalogInvalidation],
    ) {
        finalize_committed_catalog_effects(self, source_client_id, effects, invalidations);
    }

    pub(crate) fn finalize_aborted_catalog_effects(&self, effects: &[CatalogMutationEffect]) {
        for effect in effects {
            for rel in &effect.created_rels {
                let _ = self.pool.invalidate_relation(*rel);
                self.pool
                    .with_storage_mut(|s| s.smgr.unlink(*rel, None, false));
            }
        }
    }

    pub(crate) fn finalize_aborted_local_catalog_invalidations(
        &self,
        client_id: ClientId,
        prior_invalidations: &[CatalogInvalidation],
        current_invalidations: &[CatalogInvalidation],
    ) {
        finalize_aborted_local_catalog_invalidations(
            self,
            client_id,
            prior_invalidations,
            current_invalidations,
        );
    }

    pub(crate) fn finalize_committed_temp_effects(
        &self,
        _client_id: ClientId,
        _effects: &[TempMutationEffect],
    ) {
    }

    pub(crate) fn finalize_aborted_temp_effects(
        &self,
        client_id: ClientId,
        effects: &[TempMutationEffect],
    ) {
        let mut namespaces = self.temp_relations.write();
        for effect in effects.iter().rev() {
            match effect {
                TempMutationEffect::Create {
                    name,
                    namespace_created,
                    ..
                } => {
                    if *namespace_created {
                        namespaces.remove(&client_id);
                        continue;
                    }
                    if let Some(namespace) = namespaces.get_mut(&client_id) {
                        namespace.tables.remove(name);
                        namespace.generation = namespace.generation.saturating_add(1);
                    }
                }
                TempMutationEffect::Drop {
                    name,
                    entry,
                    on_commit,
                } => {
                    if let Some(namespace) = namespaces.get_mut(&client_id) {
                        namespace.tables.insert(
                            name.clone(),
                            TempCatalogEntry {
                                entry: entry.clone(),
                                on_commit: *on_commit,
                            },
                        );
                        namespace.generation = namespace.generation.saturating_add(1);
                    }
                }
            }
        }
        drop(namespaces);
        self.invalidate_session_catalog_state(client_id);
    }

    fn finish_txn(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        result: Result<StatementResult, ExecError>,
        catalog_effects: &[CatalogMutationEffect],
        temp_effects: &[TempMutationEffect],
    ) -> Result<StatementResult, ExecError> {
        match result {
            Ok(r) => {
                // Write commit record to WAL, then flush. The commit record
                // ensures recovery can mark this transaction committed in the
                // CLOG even if we crash before updating it on disk.
                self.pool.write_wal_commit(xid).map_err(|e| {
                    ExecError::Heap(crate::backend::access::heap::heapam::HeapError::Storage(
                        crate::backend::storage::smgr::SmgrError::Io(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            e,
                        )),
                    ))
                })?;
                self.pool.flush_wal().map_err(|e| {
                    ExecError::Heap(crate::backend::access::heap::heapam::HeapError::Storage(
                        crate::backend::storage::smgr::SmgrError::Io(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            e,
                        )),
                    ))
                })?;
                self.txns.write().commit(xid).map_err(|e| {
                    ExecError::Heap(crate::backend::access::heap::heapam::HeapError::Mvcc(e))
                })?;
                let invalidations = catalog_effects
                    .iter()
                    .map(Self::catalog_invalidation_from_effect)
                    .filter(|invalidation| !invalidation.is_empty())
                    .collect::<Vec<_>>();
                self.finalize_command_end_local_catalog_invalidations(client_id, &invalidations);
                self.finalize_committed_catalog_effects(client_id, catalog_effects, &invalidations);
                self.finalize_committed_temp_effects(client_id, temp_effects);
                self.apply_temp_on_commit(client_id)?;
                self.txn_waiter.notify();
                Ok(r)
            }
            Err(e) => {
                let _ = self.txns.write().abort(xid);
                self.finalize_aborted_catalog_effects(catalog_effects);
                self.finalize_aborted_temp_effects(client_id, temp_effects);
                self.txn_waiter.notify();
                Err(e)
            }
        }
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        self.txns.write().flush_clog();
    }
}

fn validate_alter_table_add_column(
    desc: &crate::backend::executor::RelationDesc,
    column: &crate::backend::parser::ColumnDef,
) -> Result<crate::backend::executor::ColumnDesc, ExecError> {
    if !column.nullable {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ADD COLUMN without NOT NULL",
            actual: "NOT NULL".into(),
        }));
    }
    if matches!(
        column.name.to_ascii_lowercase().as_str(),
        "tableoid" | "ctid" | "xmin" | "xmax" | "cmin" | "cmax"
    ) {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "non-system column name",
            actual: column.name.clone(),
        }));
    }
    if desc
        .columns
        .iter()
        .any(|existing| existing.name.eq_ignore_ascii_case(&column.name))
    {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "new column name",
            actual: format!("column already exists: {}", column.name),
        }));
    }

    let mut desc = column_desc(column.name.clone(), column.ty, true);
    desc.default_expr = column.default_expr.clone();
    if let Some(sql) = desc.default_expr.as_deref() {
        desc.missing_default_value = Some(derive_literal_default_value(sql, desc.sql_type)?);
    }
    Ok(desc)
}

fn map_catalog_error(err: CatalogError) -> ExecError {
    match err {
        CatalogError::TableAlreadyExists(name) => {
            ExecError::Parse(ParseError::TableAlreadyExists(name))
        }
        CatalogError::UnknownTable(name) => ExecError::Parse(ParseError::TableDoesNotExist(name)),
        CatalogError::UnknownColumn(name) => ExecError::Parse(ParseError::UnknownColumn(name)),
        CatalogError::UnknownType(name) => ExecError::Parse(ParseError::UnsupportedType(name)),
        CatalogError::UniqueViolation(constraint) => ExecError::UniqueViolation { constraint },
        CatalogError::Io(_) | CatalogError::Corrupt(_) => {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "valid catalog state",
                actual: "catalog error".into(),
            })
        }
    }
}

/// RAII guard that aborts a transaction if dropped without being disarmed.
/// Prevents leaked in-progress transactions when a thread panics during
/// auto-commit execution.
struct AutoCommitGuard<'a> {
    txns: &'a Arc<RwLock<TransactionManager>>,
    txn_waiter: &'a TransactionWaiter,
    xid: TransactionId,
    committed: bool,
}

impl<'a> AutoCommitGuard<'a> {
    fn new(
        txns: &'a Arc<RwLock<TransactionManager>>,
        txn_waiter: &'a TransactionWaiter,
        xid: TransactionId,
    ) -> Self {
        Self {
            txns,
            txn_waiter,
            xid,
            committed: false,
        }
    }

    fn disarm(mut self) {
        self.committed = true;
    }
}

impl Drop for AutoCommitGuard<'_> {
    fn drop(&mut self) {
        if !self.committed {
            let _ = self.txns.write().abort(self.xid);
            self.txn_waiter.notify();
        }
    }
}

#[cfg(test)]
#[path = "database_tests.rs"]
mod tests;
