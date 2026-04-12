use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::backend::access::transam::xact::{
    CommandId, MvccError, TransactionId, TransactionManager,
};
use crate::backend::access::transam::xlog::{WalBgWriter, WalError, WalWriter};
use crate::backend::catalog::bootstrap::{bootstrap_catalog_entry, bootstrap_catalog_kinds};
use crate::backend::catalog::catalog::{allocate_relation_object_oids, column_desc};
use crate::backend::catalog::pg_constraint::derived_pg_constraint_rows;
use crate::backend::catalog::pg_depend::derived_relation_depend_rows;
use crate::backend::catalog::store::{
    CatalogMutationEffect, CatalogWriteContext, load_physical_catalog_rows,
    sync_catalog_rows_subset,
};
use crate::backend::catalog::{CatalogError, CatalogStore};
use crate::backend::executor::{
    ExecError, ExecutorContext, StatementResult, execute_readonly_statement,
};
use crate::backend::parser::Statement;
use crate::backend::parser::{
    CreateIndexStatement, CreateTableAsStatement, CreateTableStatement, OnCommitAction, ParseError,
    TablePersistence, bind_delete, bind_insert, bind_update, build_plan, create_relation_desc,
    normalize_create_table_as_name, normalize_create_table_name,
};
use crate::backend::storage::lmgr::{
    TableLockManager, TableLockMode, lock_relations, unlock_relations,
};
use crate::backend::storage::smgr::{MdStorageManager, RelFileLocator, StorageManager};
use crate::backend::utils::cache::catcache::CatCache;
use crate::backend::utils::cache::plancache::PlanCache;
use crate::backend::utils::cache::relcache::{RelCache, RelCacheEntry};
use crate::backend::utils::cache::visible_catalog::VisibleCatalog;
use crate::include::catalog::{
    BOOTSTRAP_SUPERUSER_OID, BootstrapCatalogKind, PgAttrdefRow, PgAttributeRow, PgClassRow,
    PgNamespaceRow, PgTypeRow,
};
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
    catalog_cache_generation: Arc<AtomicU64>,
    client_visible_caches: Arc<RwLock<HashMap<ClientId, ClientVisibleCache>>>,
    temp_relations: Arc<RwLock<HashMap<ClientId, TempNamespace>>>,
    /// Background WAL writer — flushes BufWriter to kernel periodically.
    _wal_bg_writer: Arc<WalBgWriter>,
}

const TEMP_DB_OID_BASE: u32 = 0x7000_0000;
type CatalogTxnContext = Option<(TransactionId, CommandId)>;

#[derive(Debug, Clone)]
struct TempCatalogEntry {
    entry: RelCacheEntry,
    on_commit: OnCommitAction,
}

#[derive(Debug, Default, Clone)]
struct TempNamespace {
    tables: BTreeMap<String, TempCatalogEntry>,
    next_rel_number: u32,
    next_oid: u32,
    generation: u64,
    synced_generation: u64,
}

#[derive(Debug, Clone)]
struct ClientVisibleCache {
    generation: u64,
    relcache: RelCache,
    catcache: CatCache,
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
            catalog_cache_generation: Arc::new(AtomicU64::new(0)),
            client_visible_caches: Arc::new(RwLock::new(HashMap::new())),
            temp_relations: Arc::new(RwLock::new(HashMap::new())),
            _wal_bg_writer: Arc::new(wal_bg_writer),
        })
    }

    fn temp_db_oid(client_id: ClientId) -> u32 {
        TEMP_DB_OID_BASE.saturating_add(client_id)
    }

    fn temp_catalog_entry(client_id: ClientId, kind: BootstrapCatalogKind) -> RelCacheEntry {
        let entry = bootstrap_catalog_entry(kind);
        let rel = RelFileLocator {
            spc_oid: 0,
            db_oid: Self::temp_db_oid(client_id),
            rel_number: kind.relation_oid(),
        };
        RelCacheEntry {
            rel,
            relation_oid: entry.relation_oid,
            namespace_oid: entry.namespace_oid,
            row_type_oid: entry.row_type_oid,
            relpersistence: 'p',
            relkind: entry.relkind,
            desc: entry.desc,
        }
    }

    pub(crate) fn refresh_catalog_storage(&self) {
        for kind in bootstrap_catalog_kinds() {
            let rel = bootstrap_catalog_entry(kind).rel;
            let _ = self.pool.invalidate_relation(rel);
            let _ = self.pool.with_storage_mut(|s| {
                use crate::backend::storage::smgr::{ForkNumber, StorageManager};
                let _ = s.smgr.open(rel);
                let _ = s.smgr.create(rel, ForkNumber::Main, true);
            });
        }
    }

    fn has_active_temp_namespace(&self, client_id: ClientId) -> bool {
        self.temp_relations
            .read()
            .get(&client_id)
            .is_some_and(|namespace| !namespace.tables.is_empty())
    }

    fn catalog_cache_generation(&self) -> u64 {
        self.catalog_cache_generation.load(Ordering::Acquire)
    }

    fn invalidate_visible_caches(&self) {
        self.catalog_cache_generation.fetch_add(1, Ordering::AcqRel);
        self.client_visible_caches.write().clear();
    }

    fn invalidate_client_visible_cache(&self, client_id: ClientId) {
        self.client_visible_caches.write().remove(&client_id);
    }

    fn client_visible_cache_snapshot(&self, client_id: ClientId) -> Option<ClientVisibleCache> {
        let generation = self.catalog_cache_generation();
        if let Some(cache) = self.client_visible_caches.read().get(&client_id).cloned()
            && cache.generation == generation
        {
            return Some(cache);
        }

        let rebuilt = {
            let catalog_guard = self.catalog.read();
            let txns = self.txns.read();
            let snapshot = txns
                .snapshot(crate::backend::access::transam::xact::INVALID_TRANSACTION_ID)
                .ok();

            snapshot
                .and_then(|snapshot| {
                    catalog_guard
                        .catcache_with_snapshot(&self.pool, &txns, &snapshot, client_id)
                        .ok()
                })
                .and_then(|catcache| {
                    RelCache::from_catcache(&catcache)
                        .ok()
                        .map(|relcache| ClientVisibleCache {
                            generation,
                            relcache,
                            catcache,
                        })
                })
                .or_else(|| {
                    let relcache = catalog_guard.relcache().ok()?;
                    let catcache = catalog_guard.catcache().ok()?;
                    Some(ClientVisibleCache {
                        generation,
                        relcache,
                        catcache,
                    })
                })
        }?;

        if self.catalog_cache_generation() == generation {
            self.client_visible_caches
                .write()
                .insert(client_id, rebuilt.clone());
        }

        Some(rebuilt)
    }

    fn snapshot_visible_state(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
    ) -> (RelCache, Option<CatCache>) {
        {
            let catalog_guard = self.catalog.read();
            let txns = self.txns.read();
            let snapshot = match txn_ctx {
                Some((xid, cid)) => txns.snapshot_for_command(xid, cid).ok(),
                None => txns
                    .snapshot(crate::backend::access::transam::xact::INVALID_TRANSACTION_ID)
                    .ok(),
            };
            if let Some(snapshot) = snapshot
                && let Ok(catcache) = catalog_guard
                    .catcache_with_snapshot(&self.pool, &txns, &snapshot, client_id)
            {
                let relcache = RelCache::from_catcache(&catcache).unwrap_or_default();
                return (relcache, Some(catcache));
            }
            (catalog_guard.relcache().unwrap_or_default(), catalog_guard.catcache().ok())
        }
    }

    fn raw_visible_relcache(&self, client_id: ClientId, txn_ctx: CatalogTxnContext) -> RelCache {
        let (mut relcache, _) = self.snapshot_visible_state(client_id, txn_ctx);
        if let Some(namespace) = self.temp_relations.read().get(&client_id) {
            for (name, temp) in &namespace.tables {
                relcache.insert(name.clone(), temp.entry.clone());
                relcache.insert(format!("pg_temp.{name}"), temp.entry.clone());
            }
            for kind in temp_catalog_sync_kinds(namespace) {
                let entry = Self::temp_catalog_entry(client_id, kind);
                relcache.insert(kind.relation_name(), entry.clone());
                relcache.insert(format!("pg_catalog.{}", kind.relation_name()), entry);
            }
        }
        relcache
    }

    fn effective_search_path(
        &self,
        client_id: ClientId,
        configured_search_path: Option<&[String]>,
    ) -> Vec<String> {
        let mut path = Vec::new();
        let has_temp_namespace = self.has_active_temp_namespace(client_id);
        let explicit = configured_search_path
            .map(|search_path| {
                search_path
                    .iter()
                    .map(|schema| schema.trim().to_ascii_lowercase())
                    .filter(|schema| !schema.is_empty())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|| vec!["public".into()]);

        if has_temp_namespace && !explicit.iter().any(|schema| schema == "pg_temp") {
            path.push("pg_temp".into());
        }
        if !explicit.iter().any(|schema| schema == "pg_catalog") {
            path.push("pg_catalog".into());
        }
        for schema in explicit {
            if schema == "pg_temp" && !has_temp_namespace {
                continue;
            }
            if !path.iter().any(|existing| existing == &schema) {
                path.push(schema);
            }
        }
        path
    }

    fn resolve_unqualified_create_persistence(
        &self,
        table_name: &str,
        persistence: TablePersistence,
        configured_search_path: Option<&[String]>,
    ) -> Result<TablePersistence, ParseError> {
        if persistence == TablePersistence::Temporary {
            return Ok(TablePersistence::Temporary);
        }

        let Some(search_path) = configured_search_path else {
            return Ok(TablePersistence::Permanent);
        };

        for schema in search_path {
            let schema = schema.trim().to_ascii_lowercase();
            match schema.as_str() {
                "" | "$user" => continue,
                "public" => return Ok(TablePersistence::Permanent),
                "pg_temp" => return Ok(TablePersistence::Temporary),
                "pg_catalog" => {
                    return Err(ParseError::UnsupportedQualifiedName(format!(
                        "pg_catalog.{table_name}"
                    )));
                }
                _ => continue,
            }
        }

        Err(ParseError::NoSchemaSelectedForCreate)
    }

    fn normalize_create_table_stmt_with_search_path(
        &self,
        stmt: &CreateTableStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<(String, TablePersistence), ParseError> {
        let (table_name, persistence) = normalize_create_table_name(stmt)?;
        if stmt.schema_name.is_some() {
            return Ok((table_name, persistence));
        }
        Ok((
            table_name.clone(),
            self.resolve_unqualified_create_persistence(
                &table_name,
                persistence,
                configured_search_path,
            )?,
        ))
    }

    fn normalize_create_table_as_stmt_with_search_path(
        &self,
        stmt: &CreateTableAsStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<(String, TablePersistence), ParseError> {
        let (table_name, persistence) = normalize_create_table_as_name(stmt)?;
        if stmt.schema_name.is_some() {
            return Ok((table_name, persistence));
        }
        Ok((
            table_name.clone(),
            self.resolve_unqualified_create_persistence(
                &table_name,
                persistence,
                configured_search_path,
            )?,
        ))
    }

    pub(crate) fn visible_relcache(&self, client_id: ClientId) -> RelCache {
        self.visible_relcache_with_txn_search_path(client_id, None, None)
    }

    pub(crate) fn visible_relcache_with_search_path(
        &self,
        client_id: ClientId,
        configured_search_path: Option<&[String]>,
    ) -> RelCache {
        self.visible_relcache_with_txn_search_path(client_id, None, configured_search_path)
    }

    pub(crate) fn visible_relcache_with_txn_search_path(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        configured_search_path: Option<&[String]>,
    ) -> RelCache {
        if txn_ctx.is_none() && !self.has_active_temp_namespace(client_id) {
            if let Some(cache) = self.client_visible_cache_snapshot(client_id) {
                let search_path = self.effective_search_path(client_id, configured_search_path);
                return cache.relcache.with_search_path(&search_path);
            }
        }
        let relcache = self.raw_visible_relcache(client_id, txn_ctx);
        let search_path = self.effective_search_path(client_id, configured_search_path);
        relcache.with_search_path(&search_path)
    }

    pub(crate) fn visible_catalog_with_search_path(
        &self,
        client_id: ClientId,
        configured_search_path: Option<&[String]>,
    ) -> VisibleCatalog {
        self.visible_catalog_with_txn_search_path(client_id, None, configured_search_path)
    }

    pub(crate) fn visible_catalog_with_txn_search_path(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        configured_search_path: Option<&[String]>,
    ) -> VisibleCatalog {
        if txn_ctx.is_none() && !self.has_active_temp_namespace(client_id) {
            if let Some(cache) = self.client_visible_cache_snapshot(client_id) {
                let search_path = self.effective_search_path(client_id, configured_search_path);
                return VisibleCatalog::new(
                    cache.relcache.with_search_path(&search_path),
                    Some(cache.catcache),
                );
            }
        }

        let relcache = self.raw_visible_relcache(client_id, txn_ctx);
        let search_path = self.effective_search_path(client_id, configured_search_path);
        let (_, catcache) = self.snapshot_visible_state(client_id, txn_ctx);
        VisibleCatalog::new(relcache.with_search_path(&search_path), catcache)
    }

    pub(crate) fn sync_visible_catalog_heaps(&self, client_id: ClientId) {
        if self.has_active_temp_namespace(client_id) {
            {
                let namespaces = self.temp_relations.read();
                if let Some(namespace) = namespaces.get(&client_id)
                    && namespace.synced_generation == namespace.generation
                {
                    return;
                }
            }
            let catalog_guard = self.catalog.read();
            let base_dir = catalog_guard.base_dir().to_path_buf();
            drop(catalog_guard);
            if let Ok(mut rows) = load_physical_catalog_rows(&base_dir) {
                let temp_namespace_oid = Self::temp_db_oid(client_id);
                rows.namespaces.push(PgNamespaceRow {
                    oid: temp_namespace_oid,
                    nspname: "pg_temp".into(),
                    nspowner: BOOTSTRAP_SUPERUSER_OID,
                });

                if let Some(namespace) = self.temp_relations.read().get(&client_id) {
                    let sync_kinds = temp_catalog_sync_kinds(namespace);
                    for (name, temp) in &namespace.tables {
                        rows.classes.push(PgClassRow {
                            oid: temp.entry.relation_oid,
                            relname: name.clone(),
                            relnamespace: temp.entry.namespace_oid,
                            reltype: temp.entry.row_type_oid,
                            relowner: BOOTSTRAP_SUPERUSER_OID,
                            relam: crate::include::catalog::relam_for_relkind(temp.entry.relkind),
                            relfilenode: temp.entry.rel.rel_number,
                            relpersistence: temp.entry.relpersistence,
                            relkind: temp.entry.relkind,
                        });
                        rows.types.push(PgTypeRow {
                            oid: temp.entry.row_type_oid,
                            typname: name.clone(),
                            typnamespace: temp.entry.namespace_oid,
                            typowner: BOOTSTRAP_SUPERUSER_OID,
                            typrelid: temp.entry.relation_oid,
                            sql_type: crate::backend::parser::SqlType::new(
                                crate::backend::parser::SqlTypeKind::Text,
                            ),
                        });
                        rows.attributes
                            .extend(temp.entry.desc.columns.iter().enumerate().map(
                                |(idx, column)| PgAttributeRow {
                                    attrelid: temp.entry.relation_oid,
                                    attname: column.name.clone(),
                                    atttypid: crate::backend::utils::cache::catcache::sql_type_oid(
                                        column.sql_type,
                                    ),
                                    attnum: idx.saturating_add(1) as i16,
                                    attnotnull: !column.storage.nullable,
                                    atttypmod: column.sql_type.typmod,
                                    sql_type: column.sql_type,
                                },
                            ));
                        rows.attrdefs.extend(
                            temp.entry.desc.columns.iter().enumerate().filter_map(
                                |(idx, column)| {
                                    Some(PgAttrdefRow {
                                        oid: column.attrdef_oid?,
                                        adrelid: temp.entry.relation_oid,
                                        adnum: idx.saturating_add(1) as i16,
                                        adbin: column.default_expr.clone()?,
                                    })
                                },
                            ),
                        );
                        rows.constraints.extend(derived_pg_constraint_rows(
                            temp.entry.relation_oid,
                            name,
                            temp.entry.namespace_oid,
                            &temp.entry.desc,
                        ));
                        rows.depends.extend(derived_relation_depend_rows(
                            temp.entry.relation_oid,
                            temp.entry.namespace_oid,
                            temp.entry.row_type_oid,
                            &temp.entry.desc,
                        ));
                    }
                    let _ = sync_catalog_rows_subset(
                        &base_dir,
                        &rows,
                        temp_namespace_oid,
                        &sync_kinds,
                    );
                }
                if let Some(namespace) = self.temp_relations.write().get_mut(&client_id) {
                    namespace.synced_generation = namespace.generation;
                }
                self.refresh_catalog_storage();
            }
        }
    }

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

    fn create_temp_relation(
        &self,
        client_id: ClientId,
        table_name: String,
        mut desc: crate::backend::executor::RelationDesc,
        on_commit: OnCommitAction,
    ) -> Result<RelCacheEntry, ExecError> {
        let normalized = normalize_temp_lookup_name(&table_name);
        let entry = {
            let mut namespaces = self.temp_relations.write();
            let namespace = namespaces.entry(client_id).or_default();
            if namespace.tables.contains_key(&normalized) {
                return Err(ExecError::Parse(ParseError::TableAlreadyExists(normalized)));
            }
            let rel_number = namespace.next_rel_number.max(1);
            namespace.next_rel_number = rel_number.saturating_add(1);
            let base_oid = Self::temp_db_oid(client_id);
            let relation_oid = namespace.next_oid.max(base_oid.saturating_add(1));
            let row_type_oid = relation_oid.saturating_add(1);
            let mut next_oid = row_type_oid.saturating_add(1);
            allocate_relation_object_oids(&mut desc, &mut next_oid);
            namespace.next_oid = next_oid;
            let entry = RelCacheEntry {
                rel: RelFileLocator {
                    spc_oid: 0,
                    db_oid: Self::temp_db_oid(client_id),
                    rel_number,
                },
                relation_oid,
                namespace_oid: Self::temp_db_oid(client_id),
                row_type_oid,
                relpersistence: 't',
                relkind: 'r',
                desc,
            };
            namespace.tables.insert(
                normalized,
                TempCatalogEntry {
                    entry: entry.clone(),
                    on_commit,
                },
            );
            namespace.generation = namespace.generation.saturating_add(1);
            entry
        };
        self.invalidate_client_visible_cache(client_id);

        let _ = self.pool.with_storage_mut(|s| {
            use crate::backend::storage::smgr::StorageManager;
            let _ = s.smgr.open(entry.rel);
            let _ = s.smgr.create(
                entry.rel,
                crate::backend::storage::smgr::ForkNumber::Main,
                false,
            );
        });
        Ok(entry)
    }

    pub(crate) fn drop_temp_relation(
        &self,
        client_id: ClientId,
        table_name: &str,
    ) -> Result<RelCacheEntry, ExecError> {
        let normalized = normalize_temp_lookup_name(table_name);
        let entry = {
            let mut namespaces = self.temp_relations.write();
            let namespace = namespaces.get_mut(&client_id).ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(normalized.clone()))
            })?;
            let entry = namespace
                .tables
                .remove(&normalized)
                .map(|entry| entry.entry)
                .ok_or_else(|| {
                    ExecError::Parse(ParseError::TableDoesNotExist(normalized.clone()))
                })?;
            namespace.generation = namespace.generation.saturating_add(1);
            if namespace.tables.is_empty() {
                namespaces.remove(&client_id);
            }
            entry
        };
        self.invalidate_client_visible_cache(client_id);
        let _ = self.pool.invalidate_relation(entry.rel);
        self.pool
            .with_storage_mut(|s| s.smgr.unlink(entry.rel, None, false));
        Ok(entry)
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
            let _ = self.drop_temp_relation(client_id, &name)?;
        }
        Ok(())
    }

    pub(crate) fn cleanup_client_temp_relations(&self, client_id: ClientId) {
        self.invalidate_client_visible_cache(client_id);
        let entries = {
            let mut namespaces = self.temp_relations.write();
            namespaces
                .remove(&client_id)
                .map(|ns| {
                    ns.tables
                        .into_values()
                        .map(|entry| entry.entry)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        };
        for entry in entries {
            let _ = self.pool.invalidate_relation(entry.rel);
            self.pool
                .with_storage_mut(|s| s.smgr.unlink(entry.rel, None, false));
        }
    }

    pub(crate) fn execute_create_table_stmt(
        &self,
        client_id: ClientId,
        create_stmt: &CreateTableStatement,
    ) -> Result<StatementResult, ExecError> {
        self.execute_create_table_stmt_with_search_path(client_id, create_stmt, None)
    }

    pub(crate) fn execute_create_table_stmt_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateTableStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let persistence = self
            .normalize_create_table_stmt_with_search_path(create_stmt, configured_search_path)?
            .1;
        if persistence == TablePersistence::Temporary {
            let (table_name, _) = self
                .normalize_create_table_stmt_with_search_path(create_stmt, configured_search_path)?;
            let _ = self.create_temp_relation(
                client_id,
                table_name,
                create_relation_desc(create_stmt),
                create_stmt.on_commit,
            )?;
            return Ok(StatementResult::AffectedRows(0));
        }

        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_create_table_stmt_in_transaction_with_search_path(
            client_id,
            create_stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects);
        guard.disarm();
        result
    }

    pub(crate) fn execute_create_table_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateTableStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let (table_name, persistence) =
            self.normalize_create_table_stmt_with_search_path(create_stmt, configured_search_path)?;
        let desc = create_relation_desc(create_stmt);
        match persistence {
            TablePersistence::Permanent => {
                let mut catalog_guard = self.catalog.write();
                let ctx = CatalogWriteContext {
                    pool: &self.pool,
                    txns: &self.txns,
                    xid,
                    cid,
                    client_id,
                    waiter: None,
                };
                let result = catalog_guard.create_table_mvcc(table_name.clone(), desc.clone(), &ctx);
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
                let _ =
                    self.create_temp_relation(client_id, table_name, desc, create_stmt.on_commit)?;
                Ok(StatementResult::AffectedRows(0))
            }
        }
    }

    pub(crate) fn execute_create_index_stmt(
        &self,
        client_id: ClientId,
        create_stmt: &CreateIndexStatement,
    ) -> Result<StatementResult, ExecError> {
        self.execute_create_index_stmt_with_search_path(client_id, create_stmt, None)
    }

    pub(crate) fn execute_create_index_stmt_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateIndexStatement,
        configured_search_path: Option<&[String]>,
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
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects);
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
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let visible_relcache =
            self.visible_relcache_with_txn_search_path(client_id, Some((xid, cid)), configured_search_path);
        let entry = visible_relcache
            .get_by_name(&create_stmt.table_name)
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

        let mut catalog_guard = self.catalog.write();
        let ctx = CatalogWriteContext {
            pool: &self.pool,
            txns: &self.txns,
            xid,
            cid,
            client_id,
            waiter: None,
        };
        let result = catalog_guard.create_index_for_relation_mvcc(
            create_stmt.index_name.clone(),
            entry.relation_oid,
            create_stmt.unique,
            &create_stmt.columns,
            &ctx,
        );
        match result {
            Ok((entry, effect)) => {
                drop(catalog_guard);
                self.apply_catalog_mutation_effect_immediate(&effect)?;
                catalog_effects.push(effect);
                let _ = entry;
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
    ) -> Result<StatementResult, ExecError> {
        let relcache =
            self.visible_relcache_with_txn_search_path(client_id, Some((xid, cid)), configured_search_path);
        let rels = drop_stmt
            .table_names
            .iter()
            .filter_map(|name| relcache.get_by_name(name).map(|e| e.rel))
            .collect::<Vec<_>>();
        for rel in &rels {
            self.table_locks
                .lock_table(*rel, TableLockMode::AccessExclusive, client_id);
        }

        let mut dropped = 0usize;
        let mut result = Ok(StatementResult::AffectedRows(0));
        for table_name in &drop_stmt.table_names {
            if self.temp_entry(client_id, table_name).is_some() {
                match self.drop_temp_relation(client_id, table_name) {
                    Ok(_) => dropped += 1,
                    Err(_) if drop_stmt.if_exists => {}
                    Err(err) => {
                        result = Err(err);
                        break;
                    }
                }
                continue;
            }

            let relation_oid = match self
                .visible_relcache_with_txn_search_path(
                    client_id,
                    Some((xid, cid)),
                    configured_search_path,
                )
                .get_by_name(table_name)
            {
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
                pool: &self.pool,
                txns: &self.txns,
                xid,
                cid,
                client_id,
                waiter: Some(&self.txn_waiter),
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

    pub(crate) fn execute_create_table_as_stmt(
        &self,
        client_id: ClientId,
        create_stmt: &CreateTableAsStatement,
        xid: Option<TransactionId>,
        cid: u32,
    ) -> Result<StatementResult, ExecError> {
        self.execute_create_table_as_stmt_with_search_path(client_id, create_stmt, xid, cid, None)
    }

    pub(crate) fn execute_create_table_as_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        create_stmt: &CreateTableAsStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let (table_name, persistence) = self
            .normalize_create_table_as_stmt_with_search_path(create_stmt, configured_search_path)?;
        if select_statement_needs_temp_catalog_sync(&create_stmt.query) {
            self.sync_visible_catalog_heaps(client_id);
        }
        let visible_catalog = self.visible_catalog_with_txn_search_path(
            client_id,
            Some((xid, cid)),
            configured_search_path,
        );
        let plan = build_plan(&create_stmt.query, &visible_catalog)?;
        let mut rels = std::collections::BTreeSet::new();
        collect_rels_from_plan(&plan, &mut rels);

        let snapshot = self.txns.read().snapshot_for_command(xid, cid)?;
        let mut ctx = ExecutorContext {
            pool: Arc::clone(&self.pool),
            txns: self.txns.clone(),
            snapshot,
            client_id,
            next_command_id: cid,
            outer_rows: Vec::new(),
            timed: false,
        };
        let query_result = execute_readonly_statement(
            Statement::Select(create_stmt.query.clone()),
            &visible_catalog,
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
                    pool: &self.pool,
                    txns: &self.txns,
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
                self.create_temp_relation(
                    client_id,
                    table_name.clone(),
                    desc.clone(),
                    create_stmt.on_commit,
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
            snapshot,
            client_id,
            next_command_id: cid,
            outer_rows: Vec::new(),
            timed: false,
        };
        let inserted = crate::backend::commands::tablecmds::execute_insert_values(
            rel, &desc, &rows, &mut insert_ctx, xid, cid,
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
            return self.execute_create_table_as_stmt_in_transaction_with_search_path(
                client_id,
                create_stmt,
                xid,
                cid,
                configured_search_path,
                &mut catalog_effects,
            );
        }
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_create_table_as_stmt_in_transaction_with_search_path(
            client_id,
            create_stmt,
            xid,
            cid,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects);
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
                let visible_relcache =
                    self.visible_relcache_with_search_path(client_id, configured_search_path);
                execute_analyze(analyze_stmt.clone(), &visible_relcache)
            }
            Statement::CreateIndex(ref create_stmt) => {
                self.execute_create_index_stmt_with_search_path(
                    client_id,
                    create_stmt,
                    configured_search_path,
                )
            }
            Statement::Set(_)
            | Statement::Reset(_)
            // :HACK: numeric.sql also sets parallel_workers reloptions. Accept and ignore that
            // narrow ALTER TABLE form until table reloptions are represented properly.
            | Statement::AlterTableSet(_) => Ok(StatementResult::AffectedRows(0)),
            Statement::Select(_) | Statement::Values(_) | Statement::Explain(_) | Statement::ShowTables => {
                if statement_needs_temp_catalog_sync(&stmt) {
                    self.sync_visible_catalog_heaps(client_id);
                }
                let visible_catalog =
                    self.visible_catalog_with_search_path(client_id, configured_search_path);
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
                        Statement::ShowTables => {}
                        _ => unreachable!(),
                    }
                    (stmt, rels.into_iter().collect::<Vec<_>>())
                };

                lock_relations(&self.table_locks, client_id, &rels);

                let snapshot = self.txns.read().snapshot(INVALID_TRANSACTION_ID)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    txns: self.txns.clone(),
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
                let visible_relcache =
                    self.visible_relcache_with_search_path(client_id, configured_search_path);
                let bound = bind_insert(insert_stmt, &visible_relcache)?;
                let rel = bound.rel;
                self.table_locks
                    .lock_table(rel, TableLockMode::RowExclusive, client_id);

                let xid = self.txns.write().begin();
                let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
                let snapshot = self.txns.read().snapshot_for_command(xid, 0)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    txns: self.txns.clone(),
                    snapshot,
                    client_id,
                    next_command_id: 0,
                    outer_rows: Vec::new(),
                    timed: false,
                };
                let result = execute_insert(bound, &mut ctx, xid, 0);
                drop(ctx);
                let result = self.finish_txn(client_id, xid, result, &[]);
                guard.disarm();
                self.table_locks.unlock_table(rel, client_id);
                result
            }

            Statement::Update(ref update_stmt) => {
                let visible_relcache =
                    self.visible_relcache_with_search_path(client_id, configured_search_path);
                let bound = bind_update(update_stmt, &visible_relcache)?;
                let rel = bound.rel;
                self.table_locks
                    .lock_table(rel, TableLockMode::RowExclusive, client_id);

                let xid = self.txns.write().begin();
                let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
                let snapshot = self.txns.read().snapshot_for_command(xid, 0)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    txns: self.txns.clone(),
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
                let result = self.finish_txn(client_id, xid, result, &[]);
                guard.disarm();
                self.table_locks.unlock_table(rel, client_id);
                result
            }

            Statement::Delete(ref delete_stmt) => {
                let visible_relcache =
                    self.visible_relcache_with_search_path(client_id, configured_search_path);
                let bound = bind_delete(delete_stmt, &visible_relcache)?;
                let rel = bound.rel;
                self.table_locks
                    .lock_table(rel, TableLockMode::RowExclusive, client_id);

                let xid = self.txns.write().begin();
                let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
                let snapshot = self.txns.read().snapshot_for_command(xid, 0)?;
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    txns: self.txns.clone(),
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
                let result = self.finish_txn(client_id, xid, result, &[]);
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
                let result = self.execute_drop_table_stmt_in_transaction_with_search_path(
                    client_id,
                    drop_stmt,
                    xid,
                    0,
                    configured_search_path,
                    &mut catalog_effects,
                );
                let result = self.finish_txn(client_id, xid, result, &catalog_effects);
                guard.disarm();
                result
            }

            Statement::TruncateTable(ref truncate_stmt) => {
                let relcache =
                    self.visible_relcache_with_search_path(client_id, configured_search_path);
                let rels = {
                    truncate_stmt
                        .table_names
                        .iter()
                        .filter_map(|name| relcache.get_by_name(name).map(|e| e.rel))
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
                    snapshot,
                    client_id,
                    next_command_id: 0,
                    outer_rows: Vec::new(),
                    timed: false,
                };
                let result =
                    execute_truncate_table(truncate_stmt.clone(), &relcache, &mut ctx);
                drop(ctx);
                for rel in rels {
                    self.table_locks.unlock_table(rel, client_id);
                }
                result
            }

            Statement::Vacuum(ref vacuum_stmt) => {
                let visible_relcache =
                    self.visible_relcache_with_search_path(client_id, configured_search_path);
                execute_vacuum(vacuum_stmt.clone(), &visible_relcache)
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
            if statement_needs_temp_catalog_sync(&Statement::Select(select_stmt.clone())) {
                self.sync_visible_catalog_heaps(client_id);
            }
            let visible_catalog = self.visible_catalog_with_txn_search_path(
                client_id,
                txn_ctx,
                configured_search_path,
            );
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

fn temp_catalog_sync_kinds(namespace: &TempNamespace) -> Vec<BootstrapCatalogKind> {
    let mut kinds = vec![
        BootstrapCatalogKind::PgNamespace,
        BootstrapCatalogKind::PgClass,
        BootstrapCatalogKind::PgType,
        BootstrapCatalogKind::PgAttribute,
        BootstrapCatalogKind::PgDepend,
    ];

    if namespace.tables.values().any(|temp| {
        temp.entry
            .desc
            .columns
            .iter()
            .any(|column| column.default_expr.is_some())
    }) {
        kinds.push(BootstrapCatalogKind::PgAttrdef);
    }

    if namespace.tables.values().any(|temp| {
        temp.entry
            .desc
            .columns
            .iter()
            .any(|column| !column.storage.nullable)
    }) {
        kinds.push(BootstrapCatalogKind::PgConstraint);
    }

    kinds
}

fn normalize_temp_lookup_name(table_name: &str) -> String {
    table_name
        .strip_prefix("pg_temp.")
        .unwrap_or(table_name)
        .to_ascii_lowercase()
}

fn table_name_may_need_temp_catalog_sync(name: &str) -> bool {
    let normalized = name.to_ascii_lowercase();
    normalized.starts_with("pg_catalog.")
        || normalized.starts_with("pg_temp.")
        || normalized.starts_with("pg_")
}

fn from_item_needs_temp_catalog_sync(from: &crate::backend::parser::FromItem) -> bool {
    use crate::backend::parser::FromItem;

    match from {
        FromItem::Table { name } => table_name_may_need_temp_catalog_sync(name),
        FromItem::Values { .. } | FromItem::FunctionCall { .. } => false,
        FromItem::DerivedTable(select) => select_statement_needs_temp_catalog_sync(select),
        FromItem::Join { left, right, .. } => {
            from_item_needs_temp_catalog_sync(left) || from_item_needs_temp_catalog_sync(right)
        }
        FromItem::Alias { source, .. } => from_item_needs_temp_catalog_sync(source),
    }
}

fn select_statement_needs_temp_catalog_sync(stmt: &crate::backend::parser::SelectStatement) -> bool {
    stmt.with.iter().any(|cte| match &cte.body {
        crate::backend::parser::CteBody::Select(select) => {
            select_statement_needs_temp_catalog_sync(select)
        }
        crate::backend::parser::CteBody::Values(_) => false,
    }) || stmt
        .from
        .as_ref()
        .is_some_and(from_item_needs_temp_catalog_sync)
}

pub(crate) fn statement_needs_temp_catalog_sync(stmt: &Statement) -> bool {
    match stmt {
        Statement::Select(select) => select_statement_needs_temp_catalog_sync(select),
        Statement::Explain(explain) => statement_needs_temp_catalog_sync(explain.statement.as_ref()),
        Statement::Values(_) | Statement::ShowTables => false,
        _ => false,
    }
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
        Plan::GenerateSeries {
            start, stop, step, ..
        } => {
            collect_rels_from_expr(start, rels);
            collect_rels_from_expr(stop, rels);
            collect_rels_from_expr(step, rels);
        }
        Plan::Values { rows, .. } => {
            for row in rows {
                for expr in row {
                    collect_rels_from_expr(expr, rels);
                }
            }
        }
        Plan::Unnest { args, .. } => {
            for arg in args {
                collect_rels_from_expr(arg, rels);
            }
        }
        Plan::JsonTableFunction { arg, .. } => collect_rels_from_expr(arg, rels),
    }
}

impl Database {
    fn apply_catalog_mutation_effect_immediate(
        &self,
        effect: &CatalogMutationEffect,
    ) -> Result<(), ExecError> {
        for rel in &effect.created_rels {
            self.pool.with_storage_mut(|s| {
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

    pub(crate) fn finalize_committed_catalog_effects(&self, effects: &[CatalogMutationEffect]) {
        let catalog_changed = effects.iter().any(|effect| {
            !effect.touched_catalogs.is_empty()
                || !effect.created_rels.is_empty()
                || !effect.dropped_rels.is_empty()
        });
        let mut touched_catalogs = Vec::new();
        for effect in effects {
            for &kind in &effect.touched_catalogs {
                if !touched_catalogs.contains(&kind) {
                    touched_catalogs.push(kind);
                }
            }
        }
        for kind in touched_catalogs {
            let rel = bootstrap_catalog_entry(kind).rel;
            let nblocks = self
                .pool
                .with_storage_mut(|s| s.smgr.nblocks(rel, crate::backend::storage::smgr::ForkNumber::Main))
                .unwrap_or(0);
            for block in 0..nblocks {
                let _ = crate::backend::access::heap::heapam::heap_flush(&self.pool, 0, rel, block);
            }
        }
        for effect in effects {
            for rel in &effect.dropped_rels {
                let _ = self.pool.invalidate_relation(*rel);
                self.pool
                    .with_storage_mut(|s| s.smgr.unlink(*rel, None, false));
            }
        }
        if catalog_changed {
            self.invalidate_visible_caches();
        }
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

    fn finish_txn(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        result: Result<StatementResult, ExecError>,
        catalog_effects: &[CatalogMutationEffect],
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
                self.finalize_committed_catalog_effects(catalog_effects);
                self.apply_temp_on_commit(client_id)?;
                self.txn_waiter.notify();
                Ok(r)
            }
            Err(e) => {
                let _ = self.txns.write().abort(xid);
                self.finalize_aborted_catalog_effects(catalog_effects);
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
