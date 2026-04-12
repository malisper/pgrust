use parking_lot::RwLock;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::backend::access::transam::xact::{
    CommandId, MvccError, TransactionId, TransactionManager,
};
use crate::backend::access::transam::xlog::{WalBgWriter, WalError, WalWriter};
use crate::backend::catalog::bootstrap::{bootstrap_catalog_entry, bootstrap_catalog_kinds};
use crate::backend::catalog::catalog::column_desc;
use crate::backend::catalog::store::{
    CatalogMutationEffect, CatalogWriteContext,
    load_visible_attrdef_rows, load_visible_attribute_rows, load_visible_class_rows,
    load_visible_namespace_rows, load_visible_type_rows,
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
use crate::backend::utils::cache::catcache::{CatCache, normalize_catalog_name};
use crate::backend::utils::cache::plancache::PlanCache;
use crate::backend::utils::cache::relcache::{RelCache, RelCacheEntry};
use crate::backend::utils::cache::visible_catalog::VisibleCatalog;
use crate::include::catalog::{
    BootstrapCatalogKind, PgAttrdefRow, PgAttributeRow, PgClassRow, PgNamespaceRow, PgTypeRow,
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
    session_catalog_states: Arc<RwLock<HashMap<ClientId, SessionCatalogState>>>,
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
    oid: u32,
    name: String,
    tables: BTreeMap<String, TempCatalogEntry>,
    generation: u64,
}

#[derive(Debug, Clone)]
struct ClientVisibleCache {
    generation: u64,
    relcache: RelCache,
    catcache: CatCache,
}

#[derive(Debug, Default, Clone)]
struct SessionCatalogState {
    catalog_snapshot: Option<crate::backend::access::transam::xact::Snapshot>,
    namespace_rows: Option<Vec<PgNamespaceRow>>,
    class_rows: Option<Vec<PgClassRow>>,
    attribute_rows: Option<Vec<PgAttributeRow>>,
    attrdef_rows: Option<Vec<PgAttrdefRow>>,
    type_rows: Option<Vec<PgTypeRow>>,
    relation_entries_by_oid: HashMap<u32, RelCacheEntry>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct CatalogInvalidation {
    pub touched_catalogs: BTreeSet<BootstrapCatalogKind>,
    pub relation_oids: BTreeSet<u32>,
    pub namespace_oids: BTreeSet<u32>,
    pub type_oids: BTreeSet<u32>,
    pub full_reset: bool,
}

impl CatalogInvalidation {
    pub(crate) fn is_empty(&self) -> bool {
        !self.full_reset
            && self.touched_catalogs.is_empty()
            && self.relation_oids.is_empty()
            && self.namespace_oids.is_empty()
            && self.type_oids.is_empty()
    }
}

#[derive(Debug, Clone)]
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

pub(crate) struct LazyCatalogLookup<'a> {
    db: &'a Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    search_path: Vec<String>,
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
            session_catalog_states: Arc::new(RwLock::new(HashMap::new())),
            temp_relations: Arc::new(RwLock::new(HashMap::new())),
            _wal_bg_writer: Arc::new(wal_bg_writer),
        })
    }

    fn temp_db_oid(client_id: ClientId) -> u32 {
        TEMP_DB_OID_BASE.saturating_add(client_id)
    }

    fn temp_namespace_name(client_id: ClientId) -> String {
        format!("pg_temp_{client_id}")
    }

    fn temp_namespace_oid(client_id: ClientId) -> u32 {
        Self::temp_db_oid(client_id)
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
        self.temp_relations.read().contains_key(&client_id)
    }

    fn owned_temp_namespace(&self, client_id: ClientId) -> Option<TempNamespace> {
        self.temp_relations.read().get(&client_id).cloned()
    }

    fn other_session_temp_namespace_oid(&self, client_id: ClientId, namespace_oid: u32) -> bool {
        namespace_oid >= TEMP_DB_OID_BASE && namespace_oid != Self::temp_namespace_oid(client_id)
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

    fn invalidate_session_catalog_state(&self, client_id: ClientId) {
        self.session_catalog_states.write().remove(&client_id);
    }

    fn invalidate_session_catalog_entry(
        &self,
        client_id: ClientId,
        invalidation: &CatalogInvalidation,
    ) {
        let mut states = self.session_catalog_states.write();
        let Some(state) = states.get_mut(&client_id) else {
            return;
        };

        if invalidation.full_reset {
            *state = SessionCatalogState::default();
            return;
        }

        if invalidation
            .touched_catalogs
            .iter()
            .any(|kind| matches!(kind, BootstrapCatalogKind::PgNamespace))
        {
            state.namespace_rows = None;
            state.catalog_snapshot = None;
        }
        if invalidation
            .touched_catalogs
            .iter()
            .any(|kind| matches!(kind, BootstrapCatalogKind::PgClass))
        {
            state.class_rows = None;
            state.catalog_snapshot = None;
        }
        if invalidation
            .touched_catalogs
            .iter()
            .any(|kind| matches!(kind, BootstrapCatalogKind::PgAttribute))
        {
            state.attribute_rows = None;
            state.catalog_snapshot = None;
        }
        if invalidation
            .touched_catalogs
            .iter()
            .any(|kind| matches!(kind, BootstrapCatalogKind::PgAttrdef))
        {
            state.attrdef_rows = None;
            state.catalog_snapshot = None;
        }
        if invalidation
            .touched_catalogs
            .iter()
            .any(|kind| matches!(kind, BootstrapCatalogKind::PgType))
        {
            state.type_rows = None;
            state.catalog_snapshot = None;
        }

        for oid in &invalidation.relation_oids {
            state.relation_entries_by_oid.remove(oid);
        }

        if !invalidation.namespace_oids.is_empty() {
            state.namespace_rows = None;
            state.class_rows = None;
            state.relation_entries_by_oid.clear();
            state.catalog_snapshot = None;
        }
        if !invalidation.type_oids.is_empty() {
            state.type_rows = None;
            state.relation_entries_by_oid.clear();
            state.catalog_snapshot = None;
        }
    }

    pub(crate) fn apply_session_catalog_invalidation(
        &self,
        client_id: ClientId,
        invalidation: &CatalogInvalidation,
    ) {
        if invalidation.is_empty() {
            return;
        }
        self.invalidate_session_catalog_entry(client_id, invalidation);
        self.invalidate_client_visible_cache(client_id);
    }

    fn publish_session_catalog_invalidation(
        &self,
        source_client_id: Option<ClientId>,
        invalidation: &CatalogInvalidation,
    ) {
        if invalidation.is_empty() {
            return;
        }
        if invalidation.full_reset {
            self.session_catalog_states.write().clear();
            self.invalidate_visible_caches();
            return;
        }
        let mut client_ids = self
            .session_catalog_states
            .read()
            .keys()
            .copied()
            .collect::<Vec<_>>();
        let visible_client_ids = self
            .client_visible_caches
            .read()
            .keys()
            .copied()
            .collect::<Vec<_>>();
        for client_id in visible_client_ids {
            if !client_ids.contains(&client_id) {
                client_ids.push(client_id);
            }
        }
        for client_id in client_ids {
            if Some(client_id) == source_client_id {
                continue;
            }
            self.apply_session_catalog_invalidation(client_id, invalidation);
        }
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

    fn apply_temp_relcache_overlay(&self, client_id: ClientId, relcache: &mut RelCache) {
        if let Some(namespace) = self.temp_relations.read().get(&client_id) {
            for (name, temp) in &namespace.tables {
                relcache.insert(format!("{}.{}", namespace.name, name), temp.entry.clone());
                relcache.insert(format!("pg_temp.{name}"), temp.entry.clone());
            }
        }
    }

    fn catalog_snapshot_for_lookup(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
    ) -> Option<crate::backend::access::transam::xact::Snapshot> {
        if let Some(snapshot) = self
            .session_catalog_states
            .read()
            .get(&client_id)
            .and_then(|state| state.catalog_snapshot.clone())
        {
            return Some(snapshot);
        }

        let snapshot = {
            let txns = self.txns.read();
            match txn_ctx {
                Some((xid, cid)) => txns.snapshot_for_command(xid, cid).ok(),
                None => txns
                    .snapshot(crate::backend::access::transam::xact::INVALID_TRANSACTION_ID)
                    .ok(),
            }
        }?;

        self.session_catalog_states
            .write()
            .entry(client_id)
            .or_default()
            .catalog_snapshot = Some(snapshot.clone());
        Some(snapshot)
    }

    fn ensure_namespace_rows(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
    ) -> Vec<PgNamespaceRow> {
        if let Some(rows) = self
            .session_catalog_states
            .read()
            .get(&client_id)
            .and_then(|state| state.namespace_rows.clone())
        {
            return rows;
        }
        let Some(snapshot) = self.catalog_snapshot_for_lookup(client_id, txn_ctx) else {
            return Vec::new();
        };
        let rows = {
            let catalog = self.catalog.read();
            let txns = self.txns.read();
            load_visible_namespace_rows(catalog.base_dir(), &self.pool, &txns, &snapshot, client_id)
                .unwrap_or_default()
        };
        self.session_catalog_states
            .write()
            .entry(client_id)
            .or_default()
            .namespace_rows = Some(rows.clone());
        rows
    }

    fn ensure_class_rows(&self, client_id: ClientId, txn_ctx: CatalogTxnContext) -> Vec<PgClassRow> {
        if let Some(rows) = self
            .session_catalog_states
            .read()
            .get(&client_id)
            .and_then(|state| state.class_rows.clone())
        {
            return rows;
        }
        let Some(snapshot) = self.catalog_snapshot_for_lookup(client_id, txn_ctx) else {
            return Vec::new();
        };
        let rows = {
            let catalog = self.catalog.read();
            let txns = self.txns.read();
            load_visible_class_rows(catalog.base_dir(), &self.pool, &txns, &snapshot, client_id)
                .unwrap_or_default()
        };
        self.session_catalog_states
            .write()
            .entry(client_id)
            .or_default()
            .class_rows = Some(rows.clone());
        rows
    }

    fn ensure_attribute_rows(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
    ) -> Vec<PgAttributeRow> {
        if let Some(rows) = self
            .session_catalog_states
            .read()
            .get(&client_id)
            .and_then(|state| state.attribute_rows.clone())
        {
            return rows;
        }
        let Some(snapshot) = self.catalog_snapshot_for_lookup(client_id, txn_ctx) else {
            return Vec::new();
        };
        let rows = {
            let catalog = self.catalog.read();
            let txns = self.txns.read();
            load_visible_attribute_rows(catalog.base_dir(), &self.pool, &txns, &snapshot, client_id)
                .unwrap_or_default()
        };
        self.session_catalog_states
            .write()
            .entry(client_id)
            .or_default()
            .attribute_rows = Some(rows.clone());
        rows
    }

    fn ensure_attrdef_rows(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
    ) -> Vec<PgAttrdefRow> {
        if let Some(rows) = self
            .session_catalog_states
            .read()
            .get(&client_id)
            .and_then(|state| state.attrdef_rows.clone())
        {
            return rows;
        }
        let Some(snapshot) = self.catalog_snapshot_for_lookup(client_id, txn_ctx) else {
            return Vec::new();
        };
        let rows = {
            let catalog = self.catalog.read();
            let txns = self.txns.read();
            load_visible_attrdef_rows(catalog.base_dir(), &self.pool, &txns, &snapshot, client_id)
                .unwrap_or_default()
        };
        self.session_catalog_states
            .write()
            .entry(client_id)
            .or_default()
            .attrdef_rows = Some(rows.clone());
        rows
    }

    fn ensure_type_rows(&self, client_id: ClientId, txn_ctx: CatalogTxnContext) -> Vec<PgTypeRow> {
        if let Some(rows) = self
            .session_catalog_states
            .read()
            .get(&client_id)
            .and_then(|state| state.type_rows.clone())
        {
            return rows;
        }
        let Some(snapshot) = self.catalog_snapshot_for_lookup(client_id, txn_ctx) else {
            return Vec::new();
        };
        let rows = {
            let catalog = self.catalog.read();
            let txns = self.txns.read();
            load_visible_type_rows(catalog.base_dir(), &self.pool, &txns, &snapshot, client_id)
                .unwrap_or_default()
        };
        self.session_catalog_states
            .write()
            .entry(client_id)
            .or_default()
            .type_rows = Some(rows.clone());
        rows
    }

    fn namespace_oid_for_name(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        name: &str,
    ) -> Option<u32> {
        let normalized = name.to_ascii_lowercase();
        self.ensure_namespace_rows(client_id, txn_ctx)
            .into_iter()
            .find(|row| row.nspname.eq_ignore_ascii_case(&normalized))
            .map(|row| row.oid)
    }

    fn type_for_oid(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        oid: u32,
    ) -> Option<PgTypeRow> {
        self.ensure_type_rows(client_id, txn_ctx)
            .into_iter()
            .find(|row| row.oid == oid)
    }

    fn relation_entry_by_oid(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        relation_oid: u32,
    ) -> Option<RelCacheEntry> {
        if let Some(entry) = self
            .temp_relations
            .read()
            .get(&client_id)
            .and_then(|namespace| {
                namespace
                    .tables
                    .values()
                    .find(|temp| temp.entry.relation_oid == relation_oid)
                    .map(|temp| temp.entry.clone())
            })
        {
            return Some(entry);
        }

        if let Some(entry) = self
            .session_catalog_states
            .read()
            .get(&client_id)
            .and_then(|state| state.relation_entries_by_oid.get(&relation_oid).cloned())
        {
            return Some(entry);
        }

        let class = self
            .ensure_class_rows(client_id, txn_ctx)
            .into_iter()
            .find(|row| row.oid == relation_oid)?;
        if self.other_session_temp_namespace_oid(client_id, class.relnamespace) {
            return None;
        }

        let attrdefs = self.ensure_attrdef_rows(client_id, txn_ctx);
        let columns = self
            .ensure_attribute_rows(client_id, txn_ctx)
            .into_iter()
            .filter(|attr| attr.attrelid == relation_oid)
            .map(|attr| {
                let sql_type = self.type_for_oid(client_id, txn_ctx, attr.atttypid)?.sql_type;
                let mut desc = column_desc(
                    attr.attname.clone(),
                    crate::backend::parser::SqlType {
                        typmod: attr.atttypmod,
                        ..sql_type
                    },
                    !attr.attnotnull,
                );
                if let Some(attrdef) = attrdefs
                    .iter()
                    .find(|attrdef| attrdef.adrelid == relation_oid && attrdef.adnum == attr.attnum)
                {
                    desc.attrdef_oid = Some(attrdef.oid);
                    desc.default_expr = Some(attrdef.adbin.clone());
                }
                Some(desc)
            })
            .collect::<Option<Vec<_>>>()?;

        let entry = RelCacheEntry {
            rel: RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: class.relfilenode,
            },
            relation_oid: class.oid,
            namespace_oid: class.relnamespace,
            row_type_oid: class.reltype,
            relpersistence: class.relpersistence,
            relkind: class.relkind,
            desc: crate::backend::executor::RelationDesc { columns },
        };

        self.session_catalog_states
            .write()
            .entry(client_id)
            .or_default()
            .relation_entries_by_oid
            .insert(relation_oid, entry.clone());
        Some(entry)
    }

    fn lazy_lookup_relation(
        &self,
        client_id: ClientId,
        txn_ctx: CatalogTxnContext,
        search_path: &[String],
        name: &str,
    ) -> Option<crate::backend::parser::BoundRelation> {
        let normalized = normalize_catalog_name(name).to_ascii_lowercase();
        if let Some((schema, relname)) = normalized.split_once('.') {
            let schema_name = if schema == "pg_temp" {
                self.owned_temp_namespace(client_id)?.name
            } else {
                schema.to_string()
            };
            let namespace_oid = self.namespace_oid_for_name(client_id, txn_ctx, &schema_name)?;
            let class = self
                .ensure_class_rows(client_id, txn_ctx)
                .into_iter()
                .find(|row| {
                    row.relnamespace == namespace_oid && row.relname.eq_ignore_ascii_case(relname)
                })?;
            let entry = self.relation_entry_by_oid(client_id, txn_ctx, class.oid)?;
            return (entry.relkind == 'r').then_some(crate::backend::parser::BoundRelation {
                rel: entry.rel,
                relation_oid: entry.relation_oid,
                desc: entry.desc.clone(),
            });
        }

        for schema in search_path {
            let namespace_oid = self.namespace_oid_for_name(client_id, txn_ctx, schema)?;
            let Some(class) = self
                .ensure_class_rows(client_id, txn_ctx)
                .into_iter()
                .find(|row| {
                    row.relnamespace == namespace_oid && row.relname.eq_ignore_ascii_case(&normalized)
                })
            else {
                continue;
            };
            let Some(entry) = self.relation_entry_by_oid(client_id, txn_ctx, class.oid) else {
                continue;
            };
            if entry.relkind == 'r' {
                return Some(crate::backend::parser::BoundRelation {
                    rel: entry.rel,
                    relation_oid: entry.relation_oid,
                    desc: entry.desc.clone(),
                });
            }
        }

        None
    }

    fn cached_visible_catalog_for_autocommit(
        &self,
        client_id: ClientId,
        configured_search_path: Option<&[String]>,
    ) -> Option<VisibleCatalog> {
        let cache = self.client_visible_cache_snapshot(client_id)?;
        let mut relcache = filter_temp_relcache_for_client(client_id, cache.relcache);
        self.apply_temp_relcache_overlay(client_id, &mut relcache);
        let search_path = self.effective_search_path(client_id, configured_search_path);
        Some(VisibleCatalog::new(
            relcache.with_search_path(&search_path),
            Some(cache.catcache),
        ))
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
        relcache = filter_temp_relcache_for_client(client_id, relcache);
        self.apply_temp_relcache_overlay(client_id, &mut relcache);
        relcache
    }

    fn effective_search_path(
        &self,
        client_id: ClientId,
        configured_search_path: Option<&[String]>,
    ) -> Vec<String> {
        let mut path = Vec::new();
        let temp_namespace = self.owned_temp_namespace(client_id);
        let explicit = configured_search_path
            .map(|search_path| {
                search_path
                    .iter()
                    .map(|schema| schema.trim().to_ascii_lowercase())
                    .filter(|schema| !schema.is_empty())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|| vec!["public".into()]);

        if let Some(namespace) = &temp_namespace
            && !explicit
                .iter()
                .any(|schema| schema == "pg_temp" || schema == &namespace.name)
        {
            path.push(namespace.name.clone());
        }
        if !explicit.iter().any(|schema| schema == "pg_catalog") {
            path.push("pg_catalog".into());
        }
        for schema in explicit {
            if schema == "pg_temp" {
                if let Some(namespace) = &temp_namespace {
                    if !path.iter().any(|existing| existing == &namespace.name) {
                        path.push(namespace.name.clone());
                    }
                }
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
        if txn_ctx.is_none() {
            if let Some(cache) = self.client_visible_cache_snapshot(client_id) {
                let mut relcache = filter_temp_relcache_for_client(client_id, cache.relcache);
                self.apply_temp_relcache_overlay(client_id, &mut relcache);
                let search_path = self.effective_search_path(client_id, configured_search_path);
                return relcache.with_search_path(&search_path);
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
                    filter_temp_relcache_for_client(client_id, cache.relcache).with_search_path(&search_path),
                    Some(cache.catcache),
                );
            }
        }

        let relcache = self.raw_visible_relcache(client_id, txn_ctx);
        let search_path = self.effective_search_path(client_id, configured_search_path);
        let (_, catcache) = self.snapshot_visible_state(client_id, txn_ctx);
        VisibleCatalog::new(relcache.with_search_path(&search_path), catcache)
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

    pub(crate) fn sync_visible_catalog_heaps(&self, client_id: ClientId) {
        let _ = client_id;
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
            pool: &self.pool,
            txns: &self.txns,
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
                    rel_number: crate::include::catalog::BootstrapCatalogKind::PgNamespace.relation_oid(),
                },
                relation_oid: namespace.oid,
                namespace_oid: namespace.oid,
                row_type_oid: 0,
                relpersistence: 't',
                relkind: 'n',
                desc: crate::backend::executor::RelationDesc { columns: Vec::new() },
            },
            on_commit: OnCommitAction::PreserveRows,
            namespace_created: true,
        });
        self.invalidate_client_visible_cache(client_id);
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
            pool: &self.pool,
            txns: &self.txns,
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
        self.invalidate_client_visible_cache(client_id);
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
            pool: &self.pool,
            txns: &self.txns,
            xid,
            cid,
            client_id,
            waiter: Some(&self.txn_waiter),
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
        self.invalidate_client_visible_cache(client_id);
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
            let result = self.finish_txn(client_id, xid, result.map(|_| StatementResult::AffectedRows(0)), &catalog_effects, &temp_effects);
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
            pool: &self.pool,
            txns: &self.txns,
            xid,
            cid: 0,
            client_id,
            waiter: None,
        };
        if let Ok(effect) = self
            .catalog
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
        temp_effects: &mut Vec<TempMutationEffect>,
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
            let maybe_entry = self
                .visible_relcache_with_txn_search_path(
                    client_id,
                    Some((xid, cid)),
                    configured_search_path,
                )
                .get_by_name(table_name)
                .cloned();
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
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let (table_name, persistence) = self
            .normalize_create_table_as_stmt_with_search_path(create_stmt, configured_search_path)?;
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
                let result = self.finish_txn(client_id, xid, result, &[], &[]);
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
                let result = self.finish_txn(client_id, xid, result, &[], &[]);
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
            let visible_catalog = self.lazy_catalog_lookup(client_id, txn_ctx, configured_search_path);
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

impl crate::backend::parser::CatalogLookup for LazyCatalogLookup<'_> {
    fn lookup_relation(&self, name: &str) -> Option<crate::backend::parser::BoundRelation> {
        self.db
            .lazy_lookup_relation(self.client_id, self.txn_ctx, &self.search_path, name)
    }

    fn type_rows(&self) -> Vec<PgTypeRow> {
        self.db.ensure_type_rows(self.client_id, self.txn_ctx)
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
        Statement::Values(_) => false,
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
    pub(crate) fn catalog_invalidation_from_effect(effect: &CatalogMutationEffect) -> CatalogInvalidation {
        CatalogInvalidation {
            touched_catalogs: effect.touched_catalogs.iter().copied().collect(),
            relation_oids: effect.relation_oids.iter().copied().collect(),
            namespace_oids: effect.namespace_oids.iter().copied().collect(),
            type_oids: effect.type_oids.iter().copied().collect(),
            full_reset: effect.full_reset,
        }
    }

    pub(crate) fn finalize_command_end_local_catalog_invalidations(
        &self,
        client_id: ClientId,
        invalidations: &[CatalogInvalidation],
    ) {
        for invalidation in invalidations {
            self.apply_session_catalog_invalidation(client_id, invalidation);
        }
    }

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

    pub(crate) fn finalize_committed_catalog_effects(
        &self,
        source_client_id: ClientId,
        effects: &[CatalogMutationEffect],
        invalidations: &[CatalogInvalidation],
    ) {
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
        for invalidation in invalidations {
            self.publish_session_catalog_invalidation(Some(source_client_id), invalidation);
        }
        if catalog_changed && invalidations.iter().any(|invalidation| invalidation.full_reset) {
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

    pub(crate) fn finalize_aborted_local_catalog_invalidations(
        &self,
        client_id: ClientId,
        prior_invalidations: &[CatalogInvalidation],
        current_invalidations: &[CatalogInvalidation],
    ) {
        for invalidation in prior_invalidations {
            self.apply_session_catalog_invalidation(client_id, invalidation);
        }
        for invalidation in current_invalidations {
            self.apply_session_catalog_invalidation(client_id, invalidation);
        }
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
        self.invalidate_client_visible_cache(client_id);
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

fn filter_temp_relcache_for_client(client_id: ClientId, relcache: RelCache) -> RelCache {
    let mut filtered = RelCache::default();
    for (name, entry) in relcache.entries() {
        if entry.relpersistence == 't'
            && entry.namespace_oid >= TEMP_DB_OID_BASE
            && entry.namespace_oid != Database::temp_namespace_oid(client_id)
        {
            continue;
        }
        filtered.insert(name.to_string(), entry.clone());
    }
    filtered
}

fn map_catalog_error(err: CatalogError) -> ExecError {
    match err {
        CatalogError::TableAlreadyExists(name) => ExecError::Parse(ParseError::TableAlreadyExists(name)),
        CatalogError::UnknownTable(name) => ExecError::Parse(ParseError::TableDoesNotExist(name)),
        CatalogError::UnknownColumn(name) => ExecError::Parse(ParseError::UnknownColumn(name)),
        CatalogError::UnknownType(name) => ExecError::Parse(ParseError::UnsupportedType(name)),
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
