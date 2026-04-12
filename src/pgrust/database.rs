use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::backend::access::transam::xact::{
    CommandId, MvccError, TransactionId, TransactionManager,
};
use crate::backend::access::transam::xlog::{WalBgWriter, WalError, WalWriter};
use crate::backend::catalog::catalog::column_desc;
use crate::backend::catalog::bootstrap::{bootstrap_catalog_entry, bootstrap_catalog_kinds};
use crate::backend::catalog::pg_depend::derived_relation_depend_rows;
use crate::backend::catalog::store::{load_physical_catalog_rows, sync_catalog_rows};
use crate::backend::catalog::{CatalogError, CatalogStore};
use crate::backend::executor::{
    ExecError, ExecutorContext, StatementResult, execute_readonly_statement,
};
use crate::backend::parser::Statement;
use crate::backend::parser::{
    CreateIndexStatement, CreateTableAsStatement, CreateTableStatement, OnCommitAction,
    ParseError, TablePersistence, bind_delete, bind_insert, bind_update, build_plan,
    create_relation_desc, normalize_create_table_as_name, normalize_create_table_name,
};
use crate::backend::storage::lmgr::{
    TableLockManager, TableLockMode, lock_relations, unlock_relations,
};
use crate::backend::storage::smgr::{MdStorageManager, RelFileLocator, StorageManager};
use crate::backend::utils::cache::plancache::PlanCache;
use crate::backend::utils::cache::relcache::{RelCache, RelCacheEntry};
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
    temp_relations: Arc<RwLock<HashMap<ClientId, TempNamespace>>>,
    /// Background WAL writer — flushes BufWriter to kernel periodically.
    _wal_bg_writer: Arc<WalBgWriter>,
}

const TEMP_DB_OID_BASE: u32 = 0x7000_0000;

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

    pub(crate) fn visible_relcache(&self, client_id: ClientId) -> RelCache {
        let catalog_guard = self.catalog.read();
        let mut relcache = catalog_guard
            .relcache()
            .unwrap_or_default();
        drop(catalog_guard);
        if let Some(namespace) = self.temp_relations.read().get(&client_id) {
            for (name, temp) in &namespace.tables {
                relcache.insert(name.clone(), temp.entry.clone());
                relcache.insert(format!("pg_temp.{name}"), temp.entry.clone());
            }
            for kind in [
                BootstrapCatalogKind::PgNamespace,
                BootstrapCatalogKind::PgClass,
                BootstrapCatalogKind::PgAttribute,
                BootstrapCatalogKind::PgType,
                BootstrapCatalogKind::PgAttrdef,
                BootstrapCatalogKind::PgDepend,
            ] {
                let entry = Self::temp_catalog_entry(client_id, kind);
                relcache.insert(kind.relation_name(), entry.clone());
                relcache.insert(format!("pg_catalog.{}", kind.relation_name()), entry);
            }
        }
        relcache
    }

    pub(crate) fn sync_visible_catalog_heaps(&self, client_id: ClientId) {
        if self.temp_relations.read().contains_key(&client_id) {
            let catalog_guard = self.catalog.read();
            let base_dir = catalog_guard.base_dir().to_path_buf();
            drop(catalog_guard);
            if let Ok(mut rows) = load_physical_catalog_rows(&base_dir) {
                let temp_namespace_oid = Self::temp_db_oid(client_id);
                rows.namespaces.push(PgNamespaceRow {
                    oid: temp_namespace_oid,
                    nspname: "pg_temp".into(),
                });

                if let Some(namespace) = self.temp_relations.read().get(&client_id) {
                    for (name, temp) in &namespace.tables {
                        rows.classes.push(PgClassRow {
                            oid: temp.entry.relation_oid,
                            relname: name.clone(),
                            relnamespace: temp.entry.namespace_oid,
                            reltype: temp.entry.row_type_oid,
                            relfilenode: temp.entry.rel.rel_number,
                            relkind: temp.entry.relkind,
                        });
                        rows.types.push(PgTypeRow {
                            oid: temp.entry.row_type_oid,
                            typname: name.clone(),
                            typnamespace: temp.entry.namespace_oid,
                            typrelid: temp.entry.relation_oid,
                            sql_type: crate::backend::parser::SqlType::new(
                                crate::backend::parser::SqlTypeKind::Text,
                            ),
                        });
                        rows.attributes.extend(
                            temp.entry
                                .desc
                                .columns
                                .iter()
                                .enumerate()
                                .map(|(idx, column)| PgAttributeRow {
                                    attrelid: temp.entry.relation_oid,
                                    attname: column.name.clone(),
                                    atttypid: crate::backend::utils::cache::catcache::sql_type_oid(
                                        column.sql_type,
                                    ),
                                    attnum: idx.saturating_add(1) as i16,
                                    attnotnull: !column.storage.nullable,
                                    atttypmod: column.sql_type.typmod,
                                    sql_type: column.sql_type,
                                }),
                        );
                        rows.attrdefs.extend(
                            temp.entry
                                .desc
                                .columns
                                .iter()
                                .enumerate()
                                .filter_map(|(idx, column)| {
                                    Some(PgAttrdefRow {
                                        oid: column.attrdef_oid?,
                                        adrelid: temp.entry.relation_oid,
                                        adnum: idx.saturating_add(1) as i16,
                                        adbin: column.default_expr.clone()?,
                                    })
                                }),
                        );
                        rows.depends.extend(derived_relation_depend_rows(
                            temp.entry.relation_oid,
                            temp.entry.namespace_oid,
                            temp.entry.row_type_oid,
                            &temp.entry.desc,
                        ));
                    }
                }

                let _ = sync_catalog_rows(&base_dir, &rows, temp_namespace_oid);
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
            for column in &mut desc.columns {
                if column.default_expr.is_some() {
                    column.attrdef_oid = Some(next_oid);
                    next_oid = next_oid.saturating_add(1);
                }
            }
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
            entry
        };

        let _ = self.pool.with_storage_mut(|s| {
            use crate::backend::storage::smgr::StorageManager;
            let _ = s.smgr.open(entry.rel);
            let _ = s.smgr.create(
                entry.rel,
                crate::backend::storage::smgr::ForkNumber::Main,
                false,
            );
        });
        self.plan_cache.invalidate_all();
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
            namespace
                .tables
                .remove(&normalized)
                .map(|entry| entry.entry)
                .ok_or_else(|| {
                    ExecError::Parse(ParseError::TableDoesNotExist(normalized.clone()))
                })?
        };
        let _ = self.pool.invalidate_relation(entry.rel);
        self.pool
            .with_storage_mut(|s| s.smgr.unlink(entry.rel, None, false));
        self.plan_cache.invalidate_all();
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
        let had_entries = !entries.is_empty();
        for entry in entries {
            let _ = self.pool.invalidate_relation(entry.rel);
            self.pool
                .with_storage_mut(|s| s.smgr.unlink(entry.rel, None, false));
        }
        if had_entries {
            self.plan_cache.invalidate_all();
        }
    }

    pub(crate) fn execute_create_table_stmt(
        &self,
        client_id: ClientId,
        create_stmt: &CreateTableStatement,
    ) -> Result<StatementResult, ExecError> {
        let (table_name, persistence) = normalize_create_table_name(create_stmt)?;
        let desc = create_relation_desc(create_stmt);
        match persistence {
            TablePersistence::Permanent => {
                let mut catalog_guard = self.catalog.write();
                let result = catalog_guard
                    .create_table(table_name.clone(), desc.clone());
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
                    Ok(entry) => {
                        drop(catalog_guard);
                        self.refresh_catalog_storage();
                        let rel = entry.rel;
                        let _ = self.pool.with_storage_mut(|s| {
                            use crate::backend::storage::smgr::StorageManager;
                            let _ = s.smgr.open(rel);
                            let _ = s.smgr.create(
                                rel,
                                crate::backend::storage::smgr::ForkNumber::Main,
                                false,
                            );
                        });
                        self.plan_cache.invalidate_all();
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
        if self.temp_entry(client_id, &create_stmt.table_name).is_some() {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "permanent table for CREATE INDEX",
                actual: "temporary table".into(),
            }));
        }

        let mut catalog_guard = self.catalog.write();
        let result = catalog_guard.create_index(
            create_stmt.index_name.clone(),
            &create_stmt.table_name,
            create_stmt.unique,
            &create_stmt.columns,
        );
        match result {
            Ok(entry) => {
                drop(catalog_guard);
                self.refresh_catalog_storage();
                let rel = entry.rel;
                let _ = self.pool.with_storage_mut(|s| {
                    use crate::backend::storage::smgr::StorageManager;
                    let _ = s.smgr.open(rel);
                    let _ = s.smgr.create(
                        rel,
                        crate::backend::storage::smgr::ForkNumber::Main,
                        false,
                    );
                });
                self.plan_cache.invalidate_all();
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

    pub(crate) fn execute_create_table_as_stmt(
        &self,
        client_id: ClientId,
        create_stmt: &CreateTableAsStatement,
        xid: Option<TransactionId>,
        cid: u32,
    ) -> Result<StatementResult, ExecError> {
        let (table_name, persistence) = normalize_create_table_as_name(create_stmt)?;
        self.sync_visible_catalog_heaps(client_id);
        let visible_relcache = self.visible_relcache(client_id);
        let plan = build_plan(&create_stmt.query, &visible_relcache)?;
        let rels = {
            let mut rels = std::collections::BTreeSet::new();
            collect_rels_from_plan(&plan, &mut rels);
            rels.into_iter().collect::<Vec<_>>()
        };
        if xid.is_none() {
            lock_relations(&self.table_locks, client_id, &rels);
        }

        let snapshot = match xid {
            Some(xid) => self.txns.read().snapshot_for_command(xid, cid)?,
            None => self
                .txns
                .read()
                .snapshot(crate::backend::access::transam::xact::INVALID_TRANSACTION_ID)?,
        };
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
            &visible_relcache,
            &mut ctx,
        );
        if xid.is_none() {
            unlock_relations(&self.table_locks, client_id, &rels);
        }
        let StatementResult::Query {
            columns,
            column_names,
            rows,
        } = query_result?
        else {
            unreachable!("ctas query should return rows");
        };

        if !create_stmt.column_names.is_empty() && create_stmt.column_names.len() != columns.len() {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "column alias count matching query column count",
                actual: format!(
                    "{} aliases for {} columns",
                    create_stmt.column_names.len(),
                    columns.len()
                ),
            }));
        }

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
                let mut catalog_guard = self.catalog.write();
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
                let entry = catalog_guard
                    .create_table(table_name.clone(), create_relation_desc(&stmt))
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
                self.refresh_catalog_storage();
                let _ = self.pool.with_storage_mut(|s| {
                    use crate::backend::storage::smgr::StorageManager;
                    let _ = s.smgr.open(entry.rel);
                    let _ = s.smgr.create(
                        entry.rel,
                        crate::backend::storage::smgr::ForkNumber::Main,
                        false,
                    );
                });
                self.plan_cache.invalidate_all();
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

        if let Some(xid) = xid {
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
            let inserted = crate::backend::commands::tablecmds::execute_insert_values(
                rel, &desc, &rows, &mut ctx, xid, cid,
            )?;
            Ok(StatementResult::AffectedRows(inserted))
        } else {
            let xid = self.txns.write().begin();
            let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
            let snapshot = self.txns.read().snapshot_for_command(xid, 0)?;
            let mut ctx = ExecutorContext {
                pool: Arc::clone(&self.pool),
                txns: self.txns.clone(),
                snapshot,
                client_id,
                next_command_id: 0,
                outer_rows: Vec::new(),
                timed: false,
            };
            let result = crate::backend::commands::tablecmds::execute_insert_values(
                rel, &desc, &rows, &mut ctx, xid, 0,
            )
            .map(StatementResult::AffectedRows);
            let result = self.finish_txn(client_id, xid, result);
            guard.disarm();
            result
        }
    }

    /// Execute a single SQL statement inside an auto-commit transaction
    /// (for DML) or without a transaction (for queries/DDL).
    pub fn execute(&self, client_id: ClientId, sql: &str) -> Result<StatementResult, ExecError> {
        use crate::backend::access::transam::xact::INVALID_TRANSACTION_ID;
        use crate::backend::commands::tablecmds::{
            execute_analyze, execute_delete_with_waiter, execute_insert,
            execute_truncate_table, execute_update_with_waiter, execute_vacuum,
        };
        use crate::backend::executor::execute_readonly_statement;

        let stmt = self.plan_cache.get_statement(sql)?;

        match stmt {
            Statement::Do(ref do_stmt) => execute_do(do_stmt),
            Statement::Analyze(ref analyze_stmt) => {
                let visible_relcache = self.visible_relcache(client_id);
                execute_analyze(analyze_stmt.clone(), &visible_relcache)
            }
            Statement::CreateIndex(ref create_stmt) => {
                self.execute_create_index_stmt(client_id, create_stmt)
            }
            Statement::Set(_)
            | Statement::Reset(_)
            // :HACK: numeric.sql also sets parallel_workers reloptions. Accept and ignore that
            // narrow ALTER TABLE form until table reloptions are represented properly.
            | Statement::AlterTableSet(_) => Ok(StatementResult::AffectedRows(0)),
            Statement::Select(_) | Statement::Values(_) | Statement::Explain(_) | Statement::ShowTables => {
                self.sync_visible_catalog_heaps(client_id);
                let visible_relcache = self.visible_relcache(client_id);
                let (plan_or_stmt, rels) = {
                    let mut rels = std::collections::BTreeSet::new();
                    match &stmt {
                        Statement::Select(select) => {
                            let plan =
                                crate::backend::parser::build_plan(select, &visible_relcache)?;
                            collect_rels_from_plan(&plan, &mut rels);
                        }
                        Statement::Values(_) => {}
                        Statement::Explain(explain) => {
                            if let Statement::Select(select) = explain.statement.as_ref() {
                                let plan =
                                    crate::backend::parser::build_plan(select, &visible_relcache)?;
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
                let result = execute_readonly_statement(plan_or_stmt, &visible_relcache, &mut ctx);
                drop(ctx);

                unlock_relations(&self.table_locks, client_id, &rels);
                result
            }

            Statement::Insert(ref insert_stmt) => {
                let visible_relcache = self.visible_relcache(client_id);
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
                let result = self.finish_txn(client_id, xid, result);
                guard.disarm();
                self.table_locks.unlock_table(rel, client_id);
                result
            }

            Statement::Update(ref update_stmt) => {
                let visible_relcache = self.visible_relcache(client_id);
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
                let result = self.finish_txn(client_id, xid, result);
                guard.disarm();
                self.table_locks.unlock_table(rel, client_id);
                result
            }

            Statement::Delete(ref delete_stmt) => {
                let visible_relcache = self.visible_relcache(client_id);
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
                let result = self.finish_txn(client_id, xid, result);
                guard.disarm();
                self.table_locks.unlock_table(rel, client_id);
                result
            }

            Statement::CreateTable(ref create_stmt) => {
                self.execute_create_table_stmt(client_id, create_stmt)
            }

            Statement::CreateTableAs(ref create_stmt) => {
                self.execute_create_table_as_stmt(client_id, create_stmt, None, 0)
            }

            Statement::DropTable(ref drop_stmt) => {
                let relcache = self.visible_relcache(client_id);
                let rels = {
                    drop_stmt
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
                let mut dropped = 0usize;
                let mut result = Ok(StatementResult::AffectedRows(0));
                for table_name in &drop_stmt.table_names {
                    if self.temp_entry(client_id, table_name).is_some() {
                        match self.drop_temp_relation(client_id, table_name) {
                            Ok(_) => dropped += 1,
                            Err(e) if drop_stmt.if_exists => {}
                            Err(e) => {
                                result = Err(e);
                                break;
                            }
                        }
                    } else {
                        let mut catalog_guard = self.catalog.write();
                        match catalog_guard.drop_table(table_name) {
                            Ok(entries) => {
                                drop(catalog_guard);
                                self.refresh_catalog_storage();
                                for entry in entries {
                                    let _ = ctx.pool.invalidate_relation(entry.rel);
                                    ctx.pool.with_storage_mut(|s| s.smgr.unlink(entry.rel, None, false));
                                }
                                dropped += 1;
                                self.plan_cache.invalidate_all();
                            }
                            Err(CatalogError::UnknownTable(_)) if drop_stmt.if_exists => {}
                            Err(CatalogError::UnknownTable(name)) => {
                                result = Err(ExecError::Parse(ParseError::TableDoesNotExist(name)));
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
                }
                if result.is_ok() {
                    result = Ok(StatementResult::AffectedRows(dropped));
                }
                drop(ctx);
                for rel in rels {
                    self.table_locks.unlock_table(rel, client_id);
                }
                result
            }

            Statement::TruncateTable(ref truncate_stmt) => {
                let relcache = self.visible_relcache(client_id);
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
                let visible_relcache = self.visible_relcache(client_id);
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
        use crate::backend::access::transam::xact::INVALID_TRANSACTION_ID;
        use crate::backend::executor::executor_start;
        use crate::backend::parser::build_plan;

        let (plan, rels) = {
            self.sync_visible_catalog_heaps(client_id);
            let visible_relcache = self.visible_relcache(client_id);
            let plan = build_plan(select_stmt, &visible_relcache)?;
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
    fn finish_txn(
        &self,
        client_id: ClientId,
        xid: TransactionId,
        result: Result<StatementResult, ExecError>,
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
                self.apply_temp_on_commit(client_id)?;
                self.txn_waiter.notify();
                Ok(r)
            }
            Err(e) => {
                let _ = self.txns.write().abort(xid);
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
