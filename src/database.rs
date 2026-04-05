use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use parking_lot::{Condvar, Mutex, RwLock};

use crate::access::heap::mvcc::{MvccError, TransactionId, TransactionManager};
use crate::catalog::{CatalogError, DurableCatalog};
use crate::executor::{ExecError, ExecutorContext, StatementResult};
use crate::storage::smgr::{MdStorageManager, RelFileLocator};
use crate::storage::wal::{WalBgWriter, WalWriter, WalError};
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

/// Allows threads to wait until a specific transaction commits or aborts.
///
/// Lives outside `RwLock<TransactionManager>` so waiters don't hold the
/// read lock while sleeping.
pub struct TransactionWaiter {
    mu: Mutex<()>,
    cv: Condvar,
}

impl TransactionWaiter {
    pub fn new() -> Self {
        Self {
            mu: Mutex::new(()),
            cv: Condvar::new(),
        }
    }

    /// Block until transaction `xid` is no longer in-progress.
    pub fn wait_for(&self, txns: &RwLock<TransactionManager>, xid: TransactionId) {
        use crate::access::heap::mvcc::TransactionStatus;
        loop {
            {
                let txns_guard = txns.read();
                match txns_guard.status(xid) {
                    Some(TransactionStatus::InProgress) => {}
                    _ => return,
                }
            }
            let mut guard = self.mu.lock();
            self.cv.wait_for(&mut guard, std::time::Duration::from_millis(10));
        }
    }

    /// Signal all waiters that a transaction state has changed.
    pub fn notify(&self) {
        self.cv.notify_all();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableLockMode {
    AccessShare,
    RowExclusive,
    AccessExclusive,
}

impl TableLockMode {
    fn conflicts_with(self, other: TableLockMode) -> bool {
        matches!(
            (self, other),
            (TableLockMode::AccessExclusive, _)
                | (_, TableLockMode::AccessExclusive)
        )
    }
}

struct TableLockEntry {
    mode: TableLockMode,
    holder: ClientId,
}

pub struct TableLockManager {
    locks: Mutex<HashMap<RelFileLocator, Vec<TableLockEntry>>>,
    cv: Condvar,
}

impl TableLockManager {
    pub fn new() -> Self {
        Self {
            locks: Mutex::new(HashMap::new()),
            cv: Condvar::new(),
        }
    }

    pub fn lock_table(&self, rel: RelFileLocator, mode: TableLockMode, client_id: ClientId) {
        let mut locks = self.locks.lock();
        loop {
            let entries = locks.entry(rel).or_default();
            let dominated_by_self = entries.iter().any(|e| {
                e.holder == client_id && !e.mode.conflicts_with(mode)
            });
            let has_conflict = entries
                .iter()
                .any(|e| e.holder != client_id && e.mode.conflicts_with(mode));
            if !has_conflict || dominated_by_self {
                entries.push(TableLockEntry {
                    mode,
                    holder: client_id,
                });
                return;
            }
            self.cv.wait(&mut locks);
        }
    }

    pub fn unlock_table(&self, rel: RelFileLocator, client_id: ClientId) {
        let mut locks = self.locks.lock();
        if let Some(entries) = locks.get_mut(&rel) {
            if let Some(idx) = entries.iter().rposition(|e| e.holder == client_id) {
                entries.remove(idx);
            }
            if entries.is_empty() {
                locks.remove(&rel);
            }
        }
        self.cv.notify_all();
    }
}

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
    pub catalog: Arc<RwLock<DurableCatalog>>,
    pub txn_waiter: Arc<TransactionWaiter>,
    pub table_locks: Arc<TableLockManager>,
    /// Background WAL writer — flushes BufWriter to kernel periodically.
    _wal_bg_writer: Arc<WalBgWriter>,
}

impl Database {
    pub fn open(base_dir: impl Into<PathBuf>, pool_size: usize) -> Result<Self, DatabaseError> {
        let base_dir = base_dir.into();
        std::fs::create_dir_all(&base_dir)
            .map_err(|e| DatabaseError::Catalog(CatalogError::Io(e.to_string())))?;

        let txns = TransactionManager::new_durable(&base_dir)?;
        let catalog = DurableCatalog::load(&base_dir)?;
        let smgr = MdStorageManager::new(&base_dir);

        let wal_dir = base_dir.join("pg_wal");
        let wal = Arc::new(WalWriter::new(&wal_dir).map_err(DatabaseError::Wal)?);

        let pool = BufferPool::new_with_wal(SmgrStorageBackend::new(smgr), pool_size, Arc::clone(&wal));

        // Open storage files for all existing relations so inserts don't need to.
        {
            use crate::storage::smgr::{ForkNumber, StorageManager};
            let cat = catalog.catalog();
            for name in cat.table_names().collect::<Vec<_>>() {
                if let Some(entry) = cat.get(name) {
                    let rel = entry.rel;
                    pool.with_storage_mut(|s| {
                        let _ = s.smgr.open(rel);
                        let _ = s.smgr.create(rel, ForkNumber::Main, false);
                    });
                }
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
            _wal_bg_writer: Arc::new(wal_bg_writer),
        })
    }

    /// Execute a single SQL statement inside an auto-commit transaction
    /// (for DML) or without a transaction (for queries/DDL).
    pub fn execute(
        &self,
        client_id: ClientId,
        sql: &str,
    ) -> Result<StatementResult, ExecError> {
        use crate::access::heap::mvcc::INVALID_TRANSACTION_ID;
        use crate::executor::execute_readonly_statement;
        use crate::executor::commands::{
            execute_create_table, execute_delete_with_waiter, execute_drop_table,
            execute_truncate_table,
            execute_insert, execute_update_with_waiter,
        };
        use crate::parser::{
            Statement, bind_delete, bind_insert, bind_update, parse_statement,
        };

        let stmt = parse_statement(sql)?;

        match stmt {
            Statement::Select(_) | Statement::Explain(_) | Statement::ShowTables => {
                let (plan_or_stmt, table_rel) = {
                    let catalog_guard = self.catalog.read();
                    let catalog = catalog_guard.catalog();
                    let table_rel = extract_table_rel(&stmt, catalog);
                    (stmt, table_rel)
                };

                if let Some(rel) = table_rel {
                    self.table_locks.lock_table(rel, TableLockMode::AccessShare, client_id);
                }

                let snapshot = self.txns.read().snapshot(INVALID_TRANSACTION_ID)?;
                let catalog_guard = self.catalog.read();
                let mut ctx = ExecutorContext {
                    pool: &self.pool,
                    txns: self.txns.clone(),
                    snapshot,
                    client_id,
                    next_command_id: 0,
                };
                let result = execute_readonly_statement(plan_or_stmt, catalog_guard.catalog(), &mut ctx);
                drop(ctx);
                drop(catalog_guard);

                if let Some(rel) = table_rel {
                    self.table_locks.unlock_table(rel, client_id);
                }
                result
            }

            Statement::Insert(ref insert_stmt) => {
                let bound = {
                    let catalog_guard = self.catalog.read();
                    bind_insert(insert_stmt, catalog_guard.catalog())?
                };
                let rel = bound.rel;
                self.table_locks.lock_table(rel, TableLockMode::RowExclusive, client_id);

                let xid = self.txns.write().begin();
                let snapshot = self.txns.read().snapshot_for_command(xid, 0)?;
                let mut ctx = ExecutorContext {
                    pool: &self.pool,
                    txns: self.txns.clone(),
                    snapshot,
                    client_id,
                    next_command_id: 0,
                };
                let result = execute_insert(bound, &mut ctx, xid, 0);
                drop(ctx);
                let result = self.finish_txn(xid, result);
                self.table_locks.unlock_table(rel, client_id);
                result
            }

            Statement::Update(ref update_stmt) => {
                let bound = {
                    let catalog_guard = self.catalog.read();
                    bind_update(update_stmt, catalog_guard.catalog())?
                };
                let rel = bound.rel;
                self.table_locks.lock_table(rel, TableLockMode::RowExclusive, client_id);

                let xid = self.txns.write().begin();
                let snapshot = self.txns.read().snapshot_for_command(xid, 0)?;
                let mut ctx = ExecutorContext {
                    pool: &self.pool,
                    txns: self.txns.clone(),
                    snapshot,
                    client_id,
                    next_command_id: 0,
                };
                let result = execute_update_with_waiter(
                    bound,
                    &mut ctx,
                    xid,
                    0,
                    Some((&self.txns, &self.txn_waiter)),
                );
                drop(ctx);
                let result = self.finish_txn(xid, result);
                self.table_locks.unlock_table(rel, client_id);
                result
            }

            Statement::Delete(ref delete_stmt) => {
                let bound = {
                    let catalog_guard = self.catalog.read();
                    bind_delete(delete_stmt, catalog_guard.catalog())?
                };
                let rel = bound.rel;
                self.table_locks.lock_table(rel, TableLockMode::RowExclusive, client_id);

                let xid = self.txns.write().begin();
                let snapshot = self.txns.read().snapshot_for_command(xid, 0)?;
                let mut ctx = ExecutorContext {
                    pool: &self.pool,
                    txns: self.txns.clone(),
                    snapshot,
                    client_id,
                    next_command_id: 0,
                };
                let result = execute_delete_with_waiter(
                    bound,
                    &mut ctx,
                    xid,
                    Some((&self.txns, &self.txn_waiter)),
                );
                drop(ctx);
                let result = self.finish_txn(xid, result);
                self.table_locks.unlock_table(rel, client_id);
                result
            }

            Statement::CreateTable(ref create_stmt) => {
                let mut catalog_guard = self.catalog.write();
                let result = execute_create_table(create_stmt.clone(), catalog_guard.catalog_mut());
                if result.is_ok() {
                    // Create the relation's storage files so inserts don't need to.
                    if let Some(entry) = catalog_guard.catalog().get(&create_stmt.table_name) {
                        let rel = entry.rel;
                        let _ = self.pool.with_storage_mut(|s| {
                            use crate::storage::smgr::StorageManager;
                            let _ = s.smgr.open(rel);
                            let _ = s.smgr.create(rel, crate::storage::smgr::ForkNumber::Main, false);
                        });
                    }
                    let _ = catalog_guard.persist();
                }
                result
            }

            Statement::DropTable(ref drop_stmt) => {
                let rels = {
                    let catalog_guard = self.catalog.read();
                    drop_stmt
                        .table_names
                        .iter()
                        .filter_map(|name| catalog_guard.catalog().get(name).map(|e| e.rel))
                        .collect::<Vec<_>>()
                };
                for rel in &rels {
                    self.table_locks.lock_table(*rel, TableLockMode::AccessExclusive, client_id);
                }

                let mut catalog_guard = self.catalog.write();
                let snapshot = self.txns.read().snapshot(INVALID_TRANSACTION_ID)?;
                let mut ctx = ExecutorContext {
                    pool: &self.pool,
                    txns: self.txns.clone(),
                    snapshot,
                    client_id,
                    next_command_id: 0,
                };
                let result = execute_drop_table(drop_stmt.clone(), catalog_guard.catalog_mut(), &mut ctx);
                if result.is_ok() {
                    let _ = catalog_guard.persist();
                }
                drop(ctx);
                drop(catalog_guard);
                for rel in rels {
                    self.table_locks.unlock_table(rel, client_id);
                }
                result
            }

            Statement::TruncateTable(ref truncate_stmt) => {
                let rels = {
                    let catalog_guard = self.catalog.read();
                    truncate_stmt
                        .table_names
                        .iter()
                        .filter_map(|name| catalog_guard.catalog().get(name).map(|e| e.rel))
                        .collect::<Vec<_>>()
                };
                for rel in &rels {
                    self.table_locks.lock_table(*rel, TableLockMode::AccessExclusive, client_id);
                }

                let catalog_guard = self.catalog.read();
                let snapshot = self.txns.read().snapshot(INVALID_TRANSACTION_ID)?;
                let mut ctx = ExecutorContext {
                    pool: &self.pool,
                    txns: self.txns.clone(),
                    snapshot,
                    client_id,
                    next_command_id: 0,
                };
                let result =
                    execute_truncate_table(truncate_stmt.clone(), catalog_guard.catalog(), &mut ctx);
                drop(ctx);
                drop(catalog_guard);
                for rel in rels {
                    self.table_locks.unlock_table(rel, client_id);
                }
                result
            }

            Statement::Vacuum(_) => Ok(StatementResult::AffectedRows(0)),

            Statement::Begin | Statement::Commit | Statement::Rollback => {
                Ok(StatementResult::AffectedRows(0))
            }
        }
    }

}

fn extract_table_rel(
    stmt: &crate::parser::Statement,
    catalog: &crate::catalog::Catalog,
) -> Option<RelFileLocator> {
    use crate::parser::Statement;
    match stmt {
        Statement::Select(s) => {
            if let Some(crate::parser::FromItem::Table(name)) = &s.from {
                catalog.get(name).map(|e| e.rel)
            } else {
                None
            }
        }
        Statement::Explain(e) => {
            if let Statement::Select(s) = e.statement.as_ref() {
                if let Some(crate::parser::FromItem::Table(name)) = &s.from {
                    catalog.get(name).map(|e| e.rel)
                } else {
                    None
                }
            } else {
                None
            }
        }
        _ => None,
    }
}

impl Database {
    fn finish_txn(
        &self,
        xid: TransactionId,
        result: Result<StatementResult, ExecError>,
    ) -> Result<StatementResult, ExecError> {
        match result {
            Ok(r) => {
                // Write commit record to WAL, then flush. The commit record
                // ensures recovery can mark this transaction committed in the
                // CLOG even if we crash before updating it on disk.
                self.pool.write_wal_commit(xid).map_err(|e| {
                    ExecError::Heap(crate::access::heap::am::HeapError::Storage(
                        crate::storage::smgr::SmgrError::Io(
                            std::io::Error::new(std::io::ErrorKind::Other, e)
                        )
                    ))
                })?;
                self.pool.flush_wal().map_err(|e| {
                    ExecError::Heap(crate::access::heap::am::HeapError::Storage(
                        crate::storage::smgr::SmgrError::Io(
                            std::io::Error::new(std::io::ErrorKind::Other, e)
                        )
                    ))
                })?;
                self.txns.write().commit(xid).map_err(|e| {
                    ExecError::Heap(crate::access::heap::am::HeapError::Mvcc(e))
                })?;
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

struct ActiveTransaction {
    xid: TransactionId,
    failed: bool,
    held_table_locks: Vec<RelFileLocator>,
    next_command_id: u32,
}

pub struct Session {
    pub client_id: ClientId,
    active_txn: Option<ActiveTransaction>,
}

impl Session {
    pub fn new(client_id: ClientId) -> Self {
        Self {
            client_id,
            active_txn: None,
        }
    }

    pub fn in_transaction(&self) -> bool {
        self.active_txn.is_some()
    }

    pub fn transaction_failed(&self) -> bool {
        self.active_txn.as_ref().is_some_and(|t| t.failed)
    }

    /// Returns the ReadyForQuery status byte for the wire protocol.
    pub fn ready_status(&self) -> u8 {
        match &self.active_txn {
            None => b'I',
            Some(t) if t.failed => b'E',
            Some(_) => b'T',
        }
    }

    pub fn execute(
        &mut self,
        db: &Database,
        sql: &str,
    ) -> Result<StatementResult, ExecError> {
        use crate::parser::{Statement, parse_statement};

        let stmt = parse_statement(sql)?;

        match stmt {
            Statement::Begin => {
                if self.active_txn.is_some() {
                    return Err(ExecError::Parse(crate::parser::ParseError::UnexpectedToken {
                        expected: "no active transaction",
                        actual: "already in a transaction block".into(),
                    }));
                }
                let xid = db.txns.write().begin();
                self.active_txn = Some(ActiveTransaction {
                    xid,
                    failed: false,
                    held_table_locks: Vec::new(),
                    next_command_id: 0,
                });
                Ok(StatementResult::AffectedRows(0))
            }

            Statement::Commit => {
                let txn = match self.active_txn.take() {
                    Some(t) => t,
                    None => return Ok(StatementResult::AffectedRows(0)),
                };
                for rel in &txn.held_table_locks {
                    db.table_locks.unlock_table(*rel, self.client_id);
                }
                // Write commit record to WAL, then flush, then update CLOG.
                db.pool.write_wal_commit(txn.xid).map_err(|e| {
                    ExecError::Heap(crate::access::heap::am::HeapError::Storage(
                        crate::storage::smgr::SmgrError::Io(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            e,
                        )),
                    ))
                })?;
                db.pool.flush_wal().map_err(|e| {
                    ExecError::Heap(crate::access::heap::am::HeapError::Storage(
                        crate::storage::smgr::SmgrError::Io(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            e,
                        )),
                    ))
                })?;
                db.txns.write().commit(txn.xid).map_err(|e| {
                    ExecError::Heap(crate::access::heap::am::HeapError::Mvcc(e))
                })?;
                db.txn_waiter.notify();
                Ok(StatementResult::AffectedRows(0))
            }

            Statement::Rollback => {
                let txn = match self.active_txn.take() {
                    Some(t) => t,
                    None => return Ok(StatementResult::AffectedRows(0)),
                };
                for rel in &txn.held_table_locks {
                    db.table_locks.unlock_table(*rel, self.client_id);
                }
                let _ = db.txns.write().abort(txn.xid);
                db.txn_waiter.notify();
                Ok(StatementResult::AffectedRows(0))
            }

            _ => {
                if let Some(ref txn) = self.active_txn {
                    if txn.failed {
                        return Err(ExecError::Parse(crate::parser::ParseError::UnexpectedToken {
                            expected: "ROLLBACK",
                            actual: "current transaction is aborted, commands ignored until end of transaction block".into(),
                        }));
                    }
                }

                if self.active_txn.is_some() {
                    let result = self.execute_in_transaction(db, stmt);
                    if result.is_err() {
                        if let Some(ref mut txn) = self.active_txn {
                            txn.failed = true;
                        }
                    }
                    result
                } else {
                    db.execute(self.client_id, sql)
                }
            }
        }
    }

    fn execute_in_transaction(
        &mut self,
        db: &Database,
        stmt: crate::parser::Statement,
    ) -> Result<StatementResult, ExecError> {
        use crate::executor::execute_readonly_statement;
        use crate::executor::commands::{
            execute_create_table, execute_delete_with_waiter, execute_drop_table,
            execute_insert, execute_truncate_table, execute_update_with_waiter,
        };
        use crate::parser::{
            Statement, bind_delete, bind_insert, bind_update,
        };

        let txn = self.active_txn.as_mut().unwrap();
        let xid = txn.xid;
        let cid = txn.next_command_id;
        txn.next_command_id = txn.next_command_id.saturating_add(1);
        let client_id = self.client_id;

        match stmt {
            Statement::Select(_) | Statement::Explain(_) | Statement::ShowTables => {
                let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
                let catalog_guard = db.catalog.read();
                let mut ctx = ExecutorContext {
                    pool: &db.pool,
                    txns: db.txns.clone(),
                    snapshot,
                    client_id,
                    next_command_id: cid,
                };
                execute_readonly_statement(stmt, catalog_guard.catalog(), &mut ctx)
            }

            Statement::Insert(ref insert_stmt) => {
                let bound = {
                    let catalog_guard = db.catalog.read();
                    bind_insert(insert_stmt, catalog_guard.catalog())?
                };
                let rel = bound.rel;
                let txn = self.active_txn.as_mut().unwrap();
                if !txn.held_table_locks.contains(&rel) {
                    db.table_locks.lock_table(rel, TableLockMode::RowExclusive, client_id);
                    txn.held_table_locks.push(rel);
                }
                let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
                let mut ctx = ExecutorContext {
                    pool: &db.pool,
                    txns: db.txns.clone(),
                    snapshot,
                    client_id,
                    next_command_id: cid,
                };
                execute_insert(bound, &mut ctx, xid, cid)
            }

            Statement::Update(ref update_stmt) => {
                let bound = {
                    let catalog_guard = db.catalog.read();
                    bind_update(update_stmt, catalog_guard.catalog())?
                };
                let rel = bound.rel;
                let txn = self.active_txn.as_mut().unwrap();
                if !txn.held_table_locks.contains(&rel) {
                    db.table_locks.lock_table(rel, TableLockMode::RowExclusive, client_id);
                    txn.held_table_locks.push(rel);
                }
                let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
                let mut ctx = ExecutorContext {
                    pool: &db.pool,
                    txns: db.txns.clone(),
                    snapshot,
                    client_id,
                    next_command_id: cid,
                };
                execute_update_with_waiter(
                    bound, &mut ctx, xid, cid,
                    Some((&db.txns, &db.txn_waiter)),
                )
            }

            Statement::Delete(ref delete_stmt) => {
                let bound = {
                    let catalog_guard = db.catalog.read();
                    bind_delete(delete_stmt, catalog_guard.catalog())?
                };
                let rel = bound.rel;
                let txn = self.active_txn.as_mut().unwrap();
                if !txn.held_table_locks.contains(&rel) {
                    db.table_locks.lock_table(rel, TableLockMode::RowExclusive, client_id);
                    txn.held_table_locks.push(rel);
                }
                let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
                let mut ctx = ExecutorContext {
                    pool: &db.pool,
                    txns: db.txns.clone(),
                    snapshot,
                    client_id,
                    next_command_id: cid,
                };
                execute_delete_with_waiter(
                    bound, &mut ctx, xid,
                    Some((&db.txns, &db.txn_waiter)),
                )
            }

            Statement::CreateTable(ref create_stmt) => {
                let mut catalog_guard = db.catalog.write();
                let result = execute_create_table(create_stmt.clone(), catalog_guard.catalog_mut());
                if result.is_ok() {
                    if let Some(entry) = catalog_guard.catalog().get(&create_stmt.table_name) {
                        let rel = entry.rel;
                        let _ = db.pool.with_storage_mut(|s| {
                            use crate::storage::smgr::StorageManager;
                            let _ = s.smgr.open(rel);
                            let _ = s.smgr.create(rel, crate::storage::smgr::ForkNumber::Main, false);
                        });
                    }
                    let _ = catalog_guard.persist();
                }
                result
            }

            Statement::DropTable(ref drop_stmt) => {
                let rels = {
                    let catalog_guard = db.catalog.read();
                    drop_stmt
                        .table_names
                        .iter()
                        .filter_map(|name| catalog_guard.catalog().get(name).map(|e| e.rel))
                        .collect::<Vec<_>>()
                };
                for rel in rels {
                    let txn = self.active_txn.as_mut().unwrap();
                    if !txn.held_table_locks.contains(&rel) {
                        db.table_locks.lock_table(rel, TableLockMode::AccessExclusive, client_id);
                        txn.held_table_locks.push(rel);
                    }
                }
                let mut catalog_guard = db.catalog.write();
                let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
                let mut ctx = ExecutorContext {
                    pool: &db.pool,
                    txns: db.txns.clone(),
                    snapshot,
                    client_id,
                    next_command_id: cid,
                };
                let result = execute_drop_table(drop_stmt.clone(), catalog_guard.catalog_mut(), &mut ctx);
                if result.is_ok() {
                    let _ = catalog_guard.persist();
                }
                result
            }

            Statement::TruncateTable(ref truncate_stmt) => {
                let rels = {
                    let catalog_guard = db.catalog.read();
                    truncate_stmt
                        .table_names
                        .iter()
                        .filter_map(|name| catalog_guard.catalog().get(name).map(|e| e.rel))
                        .collect::<Vec<_>>()
                };
                for rel in rels {
                    let txn = self.active_txn.as_mut().unwrap();
                    if !txn.held_table_locks.contains(&rel) {
                        db.table_locks.lock_table(rel, TableLockMode::AccessExclusive, client_id);
                        txn.held_table_locks.push(rel);
                    }
                }
                let catalog_guard = db.catalog.read();
                let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
                let mut ctx = ExecutorContext {
                    pool: &db.pool,
                    txns: db.txns.clone(),
                    snapshot,
                    client_id,
                    next_command_id: cid,
                };
                execute_truncate_table(truncate_stmt.clone(), catalog_guard.catalog(), &mut ctx)
            }

            Statement::Vacuum(_) => Ok(StatementResult::AffectedRows(0)),

            Statement::Begin | Statement::Commit | Statement::Rollback => {
                unreachable!("handled in Session::execute")
            }
        }
    }

    /// Prepare an insert statement for repeated execution with different
    /// parameter values.  This parses and binds the table/column metadata once;
    /// subsequent calls to `execute_prepared_insert` skip parsing entirely.
    pub fn prepare_insert(
        &self,
        db: &Database,
        table_name: &str,
        columns: Option<&[String]>,
        num_params: usize,
    ) -> Result<crate::parser::PreparedInsert, ExecError> {
        let catalog_guard = db.catalog.read();
        Ok(crate::parser::bind_insert_prepared(
            table_name,
            columns,
            num_params,
            catalog_guard.catalog(),
        )?)
    }

    /// Execute a single row insert using a previously prepared insert plan.
    /// Must be called inside an active transaction (between BEGIN and COMMIT).
    pub fn execute_prepared_insert(
        &mut self,
        db: &Database,
        prepared: &crate::parser::PreparedInsert,
        params: &[crate::executor::Value],
    ) -> Result<(), ExecError> {
        use crate::executor::commands::execute_prepared_insert_row;

        let txn = self.active_txn.as_mut().ok_or_else(|| {
            ExecError::Parse(crate::parser::ParseError::UnexpectedToken {
                expected: "active transaction",
                actual: "no active transaction for prepared insert".into(),
            })
        })?;
        let xid = txn.xid;
        let cid = txn.next_command_id;
        txn.next_command_id = txn.next_command_id.saturating_add(1);
        let client_id = self.client_id;

        let rel = prepared.rel;
        let txn = self.active_txn.as_mut().unwrap();
        if !txn.held_table_locks.contains(&rel) {
            db.table_locks.lock_table(rel, TableLockMode::RowExclusive, client_id);
            txn.held_table_locks.push(rel);
        }

        let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
        let mut ctx = ExecutorContext {
            pool: &db.pool,
            txns: db.txns.clone(),
            snapshot,
            client_id,
            next_command_id: cid,
        };
        execute_prepared_insert_row(prepared, params, &mut ctx, xid, cid)
    }

    pub fn copy_from_rows(
        &mut self,
        db: &Database,
        table_name: &str,
        rows: &[Vec<String>],
    ) -> Result<usize, ExecError> {
        use crate::executor::nodes::{ScalarType, Value};

        let started_txn = if self.active_txn.is_none() {
            let xid = db.txns.write().begin();
            self.active_txn = Some(ActiveTransaction {
                xid,
                failed: false,
                held_table_locks: Vec::new(),
                next_command_id: 0,
            });
            true
        } else {
            false
        };

        let txn = self.active_txn.as_mut().unwrap();
        let xid = txn.xid;
        let cid = txn.next_command_id;
        txn.next_command_id = txn.next_command_id.saturating_add(1);

        let (rel, desc) = {
            let catalog_guard = db.catalog.read();
            let entry = catalog_guard
                .catalog()
                .get(table_name)
                .ok_or_else(|| {
                    ExecError::Parse(crate::parser::ParseError::UnknownTable(
                        table_name.to_string(),
                    ))
                })?;
            (entry.rel, entry.desc.clone())
        };

        if !txn.held_table_locks.contains(&rel) {
            db.table_locks
                .lock_table(rel, TableLockMode::RowExclusive, self.client_id);
            txn.held_table_locks.push(rel);
        }

        let parsed_rows = rows
            .iter()
            .map(|row| {
                if row.len() != desc.columns.len() {
                    return Err(ExecError::Parse(
                        crate::parser::ParseError::InvalidInsertTargetCount {
                            expected: desc.columns.len(),
                            actual: row.len(),
                        },
                    ));
                }

                row.iter()
                    .zip(desc.columns.iter())
                    .map(|(raw, column)| {
                        if raw == "\\N" {
                            return Ok(Value::Null);
                        }
                        match column.ty {
                            ScalarType::Int32 => raw
                                .parse::<i32>()
                                .map(Value::Int32)
                                .map_err(|_| {
                                    ExecError::Parse(crate::parser::ParseError::InvalidInteger(
                                        raw.clone(),
                                    ))
                                }),
                            ScalarType::Text => Ok(Value::Text(raw.clone().into())),
                            ScalarType::Bool => match raw.as_str() {
                                "t" | "true" | "1" => Ok(Value::Bool(true)),
                                "f" | "false" | "0" => Ok(Value::Bool(false)),
                                _ => Err(ExecError::TypeMismatch {
                                    op: "copy assignment",
                                    left: Value::Null,
                                    right: Value::Text(raw.clone().into()),
                                }),
                            },
                        }
                    })
                    .collect::<Result<Vec<_>, ExecError>>()
            })
            .collect::<Result<Vec<_>, ExecError>>();

        let result = parsed_rows.and_then(|parsed_rows| {
            let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
            let mut ctx = ExecutorContext {
                pool: &db.pool,
                txns: db.txns.clone(),
                snapshot,
                client_id: self.client_id,
                next_command_id: cid,
            };
            crate::executor::commands::execute_insert_values(
                rel,
                &desc,
                &parsed_rows,
                &mut ctx,
                xid,
                cid,
            )
        });

        if started_txn {
            let txn = self.active_txn.take().unwrap();
            for rel in &txn.held_table_locks {
                db.table_locks.unlock_table(*rel, self.client_id);
            }
            match result {
                Ok(n) => {
                    db.pool.write_wal_commit(txn.xid).map_err(|e| {
                        ExecError::Heap(crate::access::heap::am::HeapError::Storage(
                            crate::storage::smgr::SmgrError::Io(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                e,
                            )),
                        ))
                    })?;
                    db.pool.flush_wal().map_err(|e| {
                        ExecError::Heap(crate::access::heap::am::HeapError::Storage(
                            crate::storage::smgr::SmgrError::Io(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                e,
                            )),
                        ))
                    })?;
                    db.txns.write().commit(txn.xid).map_err(|e| {
                        ExecError::Heap(crate::access::heap::am::HeapError::Mvcc(e))
                    })?;
                    db.txn_waiter.notify();
                    Ok(n)
                }
                Err(e) => {
                    let _ = db.txns.write().abort(txn.xid);
                    db.txn_waiter.notify();
                    Err(e)
                }
            }
        } else {
            result
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::Value;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;

    use std::time::{Duration, Instant};

    const TEST_TIMEOUT: Duration = Duration::from_secs(5);

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

    /// Run a test body with a timeout. If it doesn't complete within the
    /// timeout, panic with a deadlock message. This catches deadlocks in
    /// setup code that `join_all_with_timeout` wouldn't detect.
    fn with_test_timeout<F: FnOnce() + Send + 'static>(f: F) {
        let (tx, rx) = std::sync::mpsc::channel();
        let handle = thread::spawn(move || {
            f();
            let _ = tx.send(());
        });
        match rx.recv_timeout(TEST_TIMEOUT) {
            Ok(()) => { handle.join().unwrap(); }
            Err(_) => {
                #[cfg(feature = "deadlock_detection")]
                log_deadlocks();
                panic!("test timed out after {TEST_TIMEOUT:?} — likely deadlock");
            }
        }
    }

    fn join_all_with_timeout(handles: Vec<thread::JoinHandle<()>>, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        for h in handles {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                #[cfg(feature = "deadlock_detection")]
                log_deadlocks();
                panic!("test timed out after {timeout:?} — likely deadlock");
            }
            let (tx, rx) = std::sync::mpsc::channel();
            let waiter = thread::spawn(move || {
                let result = h.join();
                let _ = tx.send(result);
            });
            match rx.recv_timeout(remaining) {
                Ok(Ok(())) => {}
                Ok(Err(e)) => std::panic::resume_unwind(e),
                Err(_) => {
                    #[cfg(feature = "deadlock_detection")]
                    log_deadlocks();
                    panic!("test timed out after {timeout:?} — likely deadlock");
                }
            }
            let _ = waiter.join();
        }
    }

    #[cfg(feature = "deadlock_detection")]
    fn log_deadlocks() {
        let deadlocks = parking_lot::deadlock::check_deadlock();
        if deadlocks.is_empty() {
            eprintln!("pgrust: parking_lot deadlock detector found no cycles");
            return;
        }

        eprintln!("pgrust: detected {} deadlock cycle(s)", deadlocks.len());
        for (i, threads) in deadlocks.iter().enumerate() {
            eprintln!("pgrust: deadlock cycle #{i}");
            for thread in threads {
                eprintln!("pgrust: thread id {:?}", thread.thread_id());
                eprintln!("{:#?}", thread.backtrace());
            }
        }
    }

    fn temp_dir(label: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "pgrust_database_{}_{}_{}",
            label,
            std::process::id(),
            NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn single_thread_create_insert_select() {
        let base = temp_dir("single_thread");
        let db = Database::open(&base, 16).unwrap();

        db.execute(1, "create table items (id int4 not null, name text)")
            .unwrap();
        db.execute(1, "insert into items (id, name) values (1, 'alpha')")
            .unwrap();
        db.execute(1, "insert into items (id, name) values (2, 'beta')")
            .unwrap();

        match db.execute(1, "select id, name from items order by id").unwrap() {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0], vec![Value::Int32(1), Value::Text("alpha".into())]);
                assert_eq!(rows[1], vec![Value::Int32(2), Value::Text("beta".into())]);
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }

    #[test]
    fn copy_from_rows_inserts_typed_rows() {
        let base = temp_dir("copy_from_rows");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        db.execute(
            1,
            "create table pgbench_branches (bid int not null, bbalance int not null, filler text)",
        )
        .unwrap();

        let inserted = session
            .copy_from_rows(
                &db,
                "pgbench_branches",
                &[
                    vec!["1".into(), "0".into(), "\\N".into()],
                    vec!["2".into(), "5".into(), "branch".into()],
                ],
            )
            .unwrap();
        assert_eq!(inserted, 2);

        match db
            .execute(
                1,
                "select bid, bbalance, filler from pgbench_branches order by bid",
            )
            .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(
                    rows,
                    vec![
                        vec![Value::Int32(1), Value::Int32(0), Value::Null],
                        vec![
                            Value::Int32(2),
                            Value::Int32(5),
                            Value::Text("branch".into()),
                        ],
                    ]
                );
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }

    #[test]
    fn copy_from_rows_respects_active_transaction() {
        let base = temp_dir("copy_from_rows_txn");
        let db = Database::open(&base, 16).unwrap();
        let mut writer = Session::new(1);
        let mut reader = Session::new(2);

        db.execute(
            1,
            "create table pgbench_tellers (tid int not null, bid int not null, tbalance int not null, filler text)",
        )
        .unwrap();

        writer.execute(&db, "begin").unwrap();
        let inserted = writer
            .copy_from_rows(
                &db,
                "pgbench_tellers",
                &[vec!["10".into(), "1".into(), "0".into(), "\\N".into()]],
            )
            .unwrap();
        assert_eq!(inserted, 1);

        match reader
            .execute(&db, "select count(*) from pgbench_tellers")
            .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![vec![Value::Int32(0)]]);
            }
            other => panic!("expected query result, got {:?}", other),
        }

        writer.execute(&db, "commit").unwrap();

        match reader
            .execute(
                &db,
                "select tid, bid, tbalance, filler from pgbench_tellers",
            )
            .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(
                    rows,
                    vec![vec![
                        Value::Int32(10),
                        Value::Int32(1),
                        Value::Int32(0),
                        Value::Null,
                    ]]
                );
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }

    #[test]
    fn concurrent_selects_on_shared_data() {
        let base = temp_dir("concurrent_selects");
        let db = Database::open(&base, 32).unwrap();

        db.execute(1, "create table nums (id int4 not null, val int4 not null)")
            .unwrap();
        for i in 1..=10 {
            db.execute(
                1,
                &format!("insert into nums (id, val) values ({i}, {})", i * 10),
            )
            .unwrap();
        }

        let num_threads = 4;
        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let db = db.clone();
                thread::spawn(move || {
                    for _ in 0..5 {
                        match db
                            .execute(
                                (t + 100) as ClientId,
                                "select count(*) from nums",
                            )
                            .unwrap()
                        {
                            StatementResult::Query { rows, .. } => {
                                assert_eq!(rows, vec![vec![Value::Int32(10)]]);
                            }
                            other => panic!("expected query result, got {:?}", other),
                        }
                    }
                })
            })
            .collect();

        join_all_with_timeout(handles, TEST_TIMEOUT);
    }

    #[test]
    fn concurrent_inserts_and_selects() {
        let base = temp_dir("concurrent_inserts");
        let db = Database::open(&base, 64).unwrap();

        db.execute(1, "create table log (id int4 not null, thread_id int4 not null)")
            .unwrap();

        let num_threads = 4;
        let inserts_per_thread = 5;

        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let db = db.clone();
                thread::spawn(move || {
                    for i in 0..inserts_per_thread {
                        let id = t * 100 + i;
                        db.execute(
                            (t + 200) as ClientId,
                            &format!(
                                "insert into log (id, thread_id) values ({id}, {t})"
                            ),
                        )
                        .unwrap();
                    }
                })
            })
            .collect();

        join_all_with_timeout(handles, TEST_TIMEOUT);

        let total = num_threads * inserts_per_thread;
        match db
            .execute(1, "select count(*) from log")
            .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![vec![Value::Int32(total as i32)]]);
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }

    #[test]
    fn mixed_concurrent_reads_and_writes() {
        let base = temp_dir("mixed_concurrent");
        let db = Database::open(&base, 64).unwrap();

        db.execute(1, "create table counters (id int4 not null, val int4 not null)")
            .unwrap();
        db.execute(1, "insert into counters (id, val) values (1, 0)")
            .unwrap();

        let num_readers = 3;
        let num_writers = 2;
        let ops_per_thread = 5;

        let mut handles = Vec::new();

        for t in 0..num_readers {
            let db = db.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..ops_per_thread {
                    let result = db
                        .execute(
                            (t + 300) as ClientId,
                            "select val from counters where id = 1",
                        )
                        .unwrap();
                    match result {
                        StatementResult::Query { rows, .. } => {
                            assert_eq!(rows.len(), 1);
                        }
                        other => panic!("expected query result, got {:?}", other),
                    }
                }
            }));
        }

        for t in 0..num_writers {
            let db = db.clone();
            handles.push(thread::spawn(move || {
                for i in 0..ops_per_thread {
                    let id = 1000 + t * 100 + i;
                    db.execute(
                        (t + 400) as ClientId,
                        &format!(
                            "insert into counters (id, val) values ({id}, {i})"
                        ),
                    )
                    .unwrap();
                }
            }));
        }

        join_all_with_timeout(handles, TEST_TIMEOUT);

        match db
            .execute(1, "select count(*) from counters")
            .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                let expected = 1 + num_writers * ops_per_thread;
                assert_eq!(rows, vec![vec![Value::Int32(expected as i32)]]);
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }

    #[test]
    fn concurrent_updates_same_row_no_lost_updates() {
        let base = temp_dir("concurrent_update_same_row");
        let db = Database::open(&base, 64).unwrap();

        db.execute(1, "create table counter (id int4 not null, val int4 not null)")
            .unwrap();
        db.execute(1, "insert into counter (id, val) values (1, 0)")
            .unwrap();

        let num_threads = 4;
        let updates_per_thread = 10;

        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let db = db.clone();
                thread::spawn(move || {
                    for _ in 0..updates_per_thread {
                        db.execute(
                            (t + 500) as ClientId,
                            "update counter set val = val + 1 where id = 1",
                        )
                        .unwrap();
                    }
                })
            })
            .collect();

        join_all_with_timeout(handles, TEST_TIMEOUT);

        let expected = num_threads * updates_per_thread;
        match db.execute(1, "select val from counter where id = 1").unwrap() {
            StatementResult::Query { rows, .. } => {
                assert_eq!(
                    rows,
                    vec![vec![Value::Int32(expected as i32)]],
                    "expected val={expected} after {num_threads} threads x {updates_per_thread} increments"
                );
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }

    #[test]
    fn concurrent_updates_different_rows() {
        let base = temp_dir("concurrent_update_diff_rows");
        let db = Database::open(&base, 64).unwrap();

        db.execute(1, "create table slots (id int4 not null, val int4 not null)")
            .unwrap();

        let num_threads = 4;
        for i in 0..num_threads {
            db.execute(
                1,
                &format!("insert into slots (id, val) values ({i}, 0)"),
            )
            .unwrap();
        }

        let updates_per_thread = 20;
        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let db = db.clone();
                thread::spawn(move || {
                    for _ in 0..updates_per_thread {
                        db.execute(
                            (t + 600) as ClientId,
                            &format!("update slots set val = val + 1 where id = {t}"),
                        )
                        .unwrap();
                    }
                })
            })
            .collect();

        join_all_with_timeout(handles, TEST_TIMEOUT);

        for i in 0..num_threads {
            match db
                .execute(1, &format!("select val from slots where id = {i}"))
                .unwrap()
            {
                StatementResult::Query { rows, .. } => {
                    assert_eq!(
                        rows,
                        vec![vec![Value::Int32(updates_per_thread as i32)]],
                        "row {i} should have val={updates_per_thread}"
                    );
                }
                other => panic!("expected query result, got {:?}", other),
            }
        }
    }

    #[test]
    fn epq_predicate_recheck_skips_non_matching() {
        let base = temp_dir("epq_predicate_recheck");
        let db = Database::open(&base, 64).unwrap();

        db.execute(1, "create table flag (id int4 not null, val int4 not null)")
            .unwrap();
        db.execute(1, "insert into flag (id, val) values (1, 0)")
            .unwrap();

        let num_threads = 4;
        let results: Vec<_> = (0..num_threads)
            .map(|t| {
                let db = db.clone();
                thread::spawn(move || {
                    db.execute(
                        (t + 700) as ClientId,
                        "update flag set val = 99 where val = 0",
                    )
                    .unwrap()
                })
            })
            .collect();

        let deadline = Instant::now() + TEST_TIMEOUT;
        let affected: Vec<usize> = results
            .into_iter()
            .map(|h| {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    panic!("test timed out after {TEST_TIMEOUT:?} — likely deadlock");
                }
                let (tx, rx) = std::sync::mpsc::channel();
                let waiter = thread::spawn(move || { let _ = tx.send(h.join()); });
                let result = rx.recv_timeout(remaining)
                    .unwrap_or_else(|_| panic!("test timed out after {TEST_TIMEOUT:?} — likely deadlock"));
                let _ = waiter.join();
                match result.unwrap() {
                    StatementResult::AffectedRows(n) => n,
                    other => panic!("expected affected rows, got {:?}", other),
                }
            })
            .collect();

        let total_affected: usize = affected.iter().sum();
        assert!(
            total_affected >= 1,
            "at least one thread should have updated the row, got {total_affected}"
        );

        match db.execute(1, "select val from flag where id = 1").unwrap() {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![vec![Value::Int32(99)]]);
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }

    /// Regression: heap_flush used to call complete_write unconditionally even
    /// when another thread already flushed the buffer (FlushResult::AlreadyClean).
    /// This caused a NoIoInProgress error under concurrency.
    #[test]
    fn concurrent_flush_does_not_error() {
        let base = temp_dir("concurrent_flush");
        let db = Database::open(&base, 64).unwrap();

        db.execute(1, "create table ftest (id int4 not null, val int4 not null)")
            .unwrap();

        let num_threads = 4;
        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let db = db.clone();
                thread::spawn(move || {
                    for i in 0..20 {
                        let id = t * 1000 + i;
                        db.execute(
                            (t + 800) as ClientId,
                            &format!("insert into ftest (id, val) values ({id}, {i})"),
                        )
                        .unwrap();
                    }
                })
            })
            .collect();

        join_all_with_timeout(handles, TEST_TIMEOUT);

        match db.execute(1, "select count(*) from ftest").unwrap() {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![vec![Value::Int32(80)]]);
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }

    /// Regression: heap_insert_version used to do read-modify-write on a page
    /// without the content lock. Two concurrent inserts could overwrite each
    /// other's tuples, losing rows.
    #[test]
    fn concurrent_inserts_no_lost_rows() {
        let base = temp_dir("concurrent_insert_no_loss");
        let db = Database::open(&base, 64).unwrap();

        db.execute(1, "create table itest (id int4 not null)")
            .unwrap();

        let num_threads = 4;
        let inserts_per_thread = 25;
        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let db = db.clone();
                thread::spawn(move || {
                    for i in 0..inserts_per_thread {
                        let id = t * 1000 + i;
                        db.execute(
                            (t + 900) as ClientId,
                            &format!("insert into itest (id) values ({id})"),
                        )
                        .unwrap();
                    }
                })
            })
            .collect();

        join_all_with_timeout(handles, TEST_TIMEOUT);

        let expected = num_threads * inserts_per_thread;
        match db.execute(1, "select count(*) from itest").unwrap() {
            StatementResult::Query { rows, .. } => {
                assert_eq!(
                    rows,
                    vec![vec![Value::Int32(expected as i32)]],
                    "expected {expected} rows, no lost inserts"
                );
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }

    /// Regression: pin_existing_block used to call complete_read even for
    /// WaitingOnRead, which failed with NoIoInProgress because another thread
    /// already completed the read. Now uses wait_for_io instead.
    #[test]
    fn concurrent_reads_same_page_no_io_error() {
        let base = temp_dir("concurrent_reads_same_page");
        let db = Database::open(&base, 16).unwrap();

        db.execute(1, "create table rtest (id int4 not null, val int4 not null)")
            .unwrap();
        for i in 0..5 {
            db.execute(1, &format!("insert into rtest (id, val) values ({i}, {i})"))
                .unwrap();
        }

        let num_threads = 8;
        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let db = db.clone();
                thread::spawn(move || {
                    for _ in 0..50 {
                        match db
                            .execute(
                                (t + 1000) as ClientId,
                                "select count(*) from rtest",
                            )
                            .unwrap()
                        {
                            StatementResult::Query { rows, .. } => {
                                assert_eq!(rows, vec![vec![Value::Int32(5)]]);
                            }
                            other => panic!("expected query result, got {:?}", other),
                        }
                    }
                })
            })
            .collect();

        join_all_with_timeout(handles, TEST_TIMEOUT);
    }

    /// Regression: without the content lock on heap_scan_next, a reader could
    /// see a partially written page from a concurrent writer (torn read).
    /// This test exercises concurrent reads and writes on the same table to
    /// verify no panics or corrupt data.
    #[test]
    fn concurrent_read_write_same_table_no_corruption() {
        let base = temp_dir("concurrent_rw_corruption");
        let db = Database::open(&base, 64).unwrap();

        db.execute(1, "create table rwtest (id int4 not null, val int4 not null)")
            .unwrap();
        db.execute(1, "insert into rwtest (id, val) values (1, 0)")
            .unwrap();

        let num_readers = 4;
        let num_writers = 2;
        let mut handles = Vec::new();

        for t in 0..num_writers {
            let db = db.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..20 {
                    db.execute(
                        (t + 1100) as ClientId,
                        "update rwtest set val = val + 1 where id = 1",
                    )
                    .unwrap();
                }
            }));
        }

        for t in 0..num_readers {
            let db = db.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..50 {
                    match db
                        .execute(
                            (t + 1200) as ClientId,
                            "select val from rwtest where id = 1",
                        )
                        .unwrap()
                    {
                        StatementResult::Query { rows, .. } => {
                            assert_eq!(rows.len(), 1, "should always see exactly one row");
                            match &rows[0][0] {
                                Value::Int32(v) => assert!(*v >= 0, "val should never be negative"),
                                other => panic!("expected Int32, got {:?}", other),
                            }
                        }
                        other => panic!("expected query result, got {:?}", other),
                    }
                }
            }));
        }

        join_all_with_timeout(handles, TEST_TIMEOUT);

        let expected_val = num_writers * 20;
        match db.execute(1, "select val from rwtest where id = 1").unwrap() {
            StatementResult::Query { rows, .. } => {
                assert_eq!(
                    rows,
                    vec![vec![Value::Int32(expected_val as i32)]],
                    "all writer updates should be applied"
                );
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }

    /// Regression: the write-preferring RwLock in parking_lot caused a deadlock
    /// when a thread tried to acquire a txns read lock (to check xmax status)
    /// while another thread was pending a txns write lock (to commit). The
    /// pending writer blocks new readers, creating a cycle.
    /// This test verifies the deadlock is resolved (would timeout otherwise).
    #[test]
    fn no_deadlock_under_write_preferring_rwlock() {
        let base = temp_dir("write_preferring_deadlock");
        let db = Database::open(&base, 64).unwrap();

        db.execute(1, "create table dltest (id int4 not null, val int4 not null)")
            .unwrap();
        db.execute(1, "insert into dltest (id, val) values (1, 0)")
            .unwrap();

        let num_threads = 4;
        let updates_per_thread = 20;

        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let db = db.clone();
                thread::spawn(move || {
                    for _ in 0..updates_per_thread {
                        db.execute(
                            (t + 1300) as ClientId,
                            "update dltest set val = val + 1 where id = 1",
                        )
                        .unwrap();
                    }
                })
            })
            .collect();

        join_all_with_timeout(handles, TEST_TIMEOUT);

        let expected = num_threads * updates_per_thread;
        match db.execute(1, "select val from dltest where id = 1").unwrap() {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![vec![Value::Int32(expected as i32)]]);
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }

    /// Reproduces the pgbench benchmark hang more closely than the simple
    /// counter tests: many concurrent full-table UPDATE/SELECT cycles over a
    /// relation much larger than the buffer pool.
    #[test]
    fn pgbench_style_accounts_workload_completes() {
        let base = temp_dir("pgbench_style_hang");
        let db = Database::open(&base, 128).unwrap();

        db.execute(
            1,
            "create table pgbench_accounts (aid int4 not null, bid int4 not null, abalance int4 not null, filler text)",
        )
        .unwrap();

        for aid in 1..=5000 {
            db.execute(
                1,
                &format!(
                    "insert into pgbench_accounts (aid, bid, abalance, filler) values ({aid}, 1, 0, 'x')"
                ),
            )
            .unwrap();
        }

        let num_threads = 10;
        let ops_per_thread = 10;
        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let db = db.clone();
                thread::spawn(move || {
                    for i in 0..ops_per_thread {
                        let aid = ((t * 997 + i * 389) % 5000) + 1;
                        db.execute(
                            (t + 2100) as ClientId,
                            &format!(
                                "update pgbench_accounts set abalance = abalance + -1 where aid = {aid}"
                            ),
                        )
                        .unwrap();
                        match db
                            .execute(
                                (t + 2200) as ClientId,
                                &format!(
                                    "select abalance from pgbench_accounts where aid = {aid}"
                                ),
                            )
                            .unwrap()
                        {
                            StatementResult::Query { rows, .. } => {
                                assert_eq!(rows.len(), 1);
                            }
                            other => panic!("expected query result, got {:?}", other),
                        }
                    }
                })
            })
            .collect();

        join_all_with_timeout(handles, TEST_TIMEOUT);
    }

    #[test]
    fn begin_commit_groups_statements() {
        let base = temp_dir("begin_commit");
        let db = Database::open(&base, 64).unwrap();
        let mut session = Session::new(1);

        session.execute(&db, "create table txtest (id int4 not null, val int4 not null)").unwrap();
        session.execute(&db, "begin").unwrap();
        assert!(session.in_transaction());
        assert_eq!(session.ready_status(), b'T');

        session.execute(&db, "insert into txtest (id, val) values (1, 10)").unwrap();
        session.execute(&db, "insert into txtest (id, val) values (2, 20)").unwrap();
        session.execute(&db, "commit").unwrap();
        assert!(!session.in_transaction());
        assert_eq!(session.ready_status(), b'I');

        match session.execute(&db, "select count(*) from txtest").unwrap() {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![vec![Value::Int32(2)]]);
            }
            other => panic!("expected query, got {:?}", other),
        }
    }

    #[test]
    fn rollback_discards_changes() {
        let base = temp_dir("rollback");
        let db = Database::open(&base, 64).unwrap();
        let mut session = Session::new(1);

        session.execute(&db, "create table rbtest (id int4 not null)").unwrap();
        session.execute(&db, "insert into rbtest (id) values (1)").unwrap();

        session.execute(&db, "begin").unwrap();
        session.execute(&db, "insert into rbtest (id) values (2)").unwrap();
        session.execute(&db, "insert into rbtest (id) values (3)").unwrap();
        session.execute(&db, "rollback").unwrap();

        match session.execute(&db, "select count(*) from rbtest").unwrap() {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![vec![Value::Int32(1)]],
                    "only the autocommitted row should survive rollback");
            }
            other => panic!("expected query, got {:?}", other),
        }
    }

    #[test]
    fn failed_transaction_rejects_commands() {
        let base = temp_dir("failed_txn");
        let db = Database::open(&base, 64).unwrap();
        let mut session = Session::new(1);

        session.execute(&db, "create table ftest (id int4 not null)").unwrap();
        session.execute(&db, "begin").unwrap();
        session.execute(&db, "insert into ftest (id) values (1)").unwrap();

        let err = session.execute(&db, "select * from nonexistent");
        assert!(err.is_err());
        assert!(session.transaction_failed());
        assert_eq!(session.ready_status(), b'E');

        let err = session.execute(&db, "select * from ftest");
        assert!(err.is_err(), "commands should be rejected in failed transaction");

        session.execute(&db, "rollback").unwrap();
        assert!(!session.in_transaction());
        assert_eq!(session.ready_status(), b'I');

        match session.execute(&db, "select count(*) from ftest").unwrap() {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![vec![Value::Int32(0)]],
                    "all inserts should be rolled back");
            }
            other => panic!("expected query, got {:?}", other),
        }
    }

    #[test]
    fn autocommit_still_works_without_begin() {
        let base = temp_dir("autocommit");
        let db = Database::open(&base, 64).unwrap();
        let mut session = Session::new(1);

        session.execute(&db, "create table atest (id int4 not null)").unwrap();
        session.execute(&db, "insert into atest (id) values (1)").unwrap();
        session.execute(&db, "insert into atest (id) values (2)").unwrap();

        match session.execute(&db, "select count(*) from atest").unwrap() {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![vec![Value::Int32(2)]]);
            }
            other => panic!("expected query, got {:?}", other),
        }
    }

    #[test]
    fn read_committed_isolation() {
        let base = temp_dir("read_committed");
        let db = Database::open(&base, 64).unwrap();
        let mut session_a = Session::new(1);
        let mut session_b = Session::new(2);

        session_a.execute(&db, "create table isotest (id int4 not null, val int4 not null)").unwrap();
        session_a.execute(&db, "insert into isotest (id, val) values (1, 100)").unwrap();

        session_a.execute(&db, "begin").unwrap();
        session_a.execute(&db, "insert into isotest (id, val) values (2, 200)").unwrap();

        match session_b.execute(&db, "select count(*) from isotest").unwrap() {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![vec![Value::Int32(1)]],
                    "session B should not see session A's uncommitted insert");
            }
            other => panic!("expected query, got {:?}", other),
        }

        session_a.execute(&db, "commit").unwrap();

        match session_b.execute(&db, "select count(*) from isotest").unwrap() {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![vec![Value::Int32(2)]],
                    "session B should see session A's committed insert");
            }
            other => panic!("expected query, got {:?}", other),
        }
    }

    /// Multiple threads each run a loop of BEGIN; UPDATE counter; COMMIT.
    /// The row-level lock must be held for the duration of the explicit
    /// transaction, so updates cannot interleave — the final value must equal
    /// num_threads × iterations_per_thread.
    #[test]
    fn concurrent_transactions_update_counter() {
        let base = temp_dir("txn_update_counter");
        let db = Database::open(&base, 64).unwrap();

        db.execute(1, "create table counter (id int4 not null, val int4 not null)")
            .unwrap();
        db.execute(1, "insert into counter (id, val) values (1, 0)")
            .unwrap();

        let num_threads = 4;
        let iters_per_thread = 10;

        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let db = db.clone();
                thread::spawn(move || {
                    let mut session = Session::new((t + 1500) as ClientId);
                    for _ in 0..iters_per_thread {
                        session.execute(&db, "begin").unwrap();
                        session
                            .execute(&db, "update counter set val = val + 1 where id = 1")
                            .unwrap();
                        session.execute(&db, "commit").unwrap();
                    }
                })
            })
            .collect();

        join_all_with_timeout(handles, TEST_TIMEOUT);

        let expected = num_threads * iters_per_thread;
        match db.execute(1, "select val from counter where id = 1").unwrap() {
            StatementResult::Query { rows, .. } => {
                assert_eq!(
                    rows,
                    vec![vec![Value::Int32(expected as i32)]],
                    "all transactional updates must be serialized — expected {expected}"
                );
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }

    /// Within a single BEGIN block, a row inserted earlier in the transaction
    /// must be visible to a SELECT issued later in the same transaction
    /// (read-your-own-writes / command-id visibility).
    #[test]
    fn read_your_own_writes_within_transaction() {
        let base = temp_dir("read_own_writes");
        let db = Database::open(&base, 64).unwrap();
        let mut session = Session::new(1);

        session
            .execute(&db, "create table rowtable (id int4 not null, val int4 not null)")
            .unwrap();

        session.execute(&db, "begin").unwrap();
        session
            .execute(&db, "insert into rowtable (id, val) values (1, 42)")
            .unwrap();

        // The insert is not yet committed, but the same session must see it.
        match session.execute(&db, "select val from rowtable where id = 1").unwrap() {
            StatementResult::Query { rows, .. } => {
                assert_eq!(
                    rows,
                    vec![vec![Value::Int32(42)]],
                    "own uncommitted insert must be visible within the transaction"
                );
            }
            other => panic!("expected query result, got {:?}", other),
        }

        session.execute(&db, "commit").unwrap();

        // After commit the row must still be there.
        match session.execute(&db, "select count(*) from rowtable").unwrap() {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![vec![Value::Int32(1)]]);
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }

    /// Each thread runs one explicit transaction that inserts a batch of rows.
    /// No row must be lost: the final count must equal num_threads × batch_size,
    /// even though all transactions overlap in time.
    #[test]
    fn concurrent_transactions_bulk_insert() {
        let base = temp_dir("txn_bulk_insert");
        let db = Database::open(&base, 64).unwrap();

        db.execute(1, "create table bulk (id int4 not null)").unwrap();

        let num_threads = 4;
        let batch_size = 10;

        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let db = db.clone();
                thread::spawn(move || {
                    let mut session = Session::new((t + 1600) as ClientId);
                    session.execute(&db, "begin").unwrap();
                    for i in 0..batch_size {
                        let id = t * 10_000 + i;
                        session
                            .execute(&db, &format!("insert into bulk (id) values ({id})"))
                            .unwrap();
                    }
                    session.execute(&db, "commit").unwrap();
                })
            })
            .collect();

        join_all_with_timeout(handles, TEST_TIMEOUT);

        let expected = (num_threads * batch_size) as i32;
        match db.execute(1, "select count(*) from bulk").unwrap() {
            StatementResult::Query { rows, .. } => {
                assert_eq!(
                    rows,
                    vec![vec![Value::Int32(expected)]],
                    "all bulk-inserted rows must survive — expected {expected}"
                );
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }

    /// Thread A opens a transaction and inserts a row, then waits for a signal
    /// before committing.  Thread B reads the table repeatedly while A is still
    /// in progress — it must never see A's uncommitted row (no dirty reads).
    /// After A commits, B must see the row.
    #[test]
    fn no_dirty_reads_concurrent() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let base = temp_dir("no_dirty_reads");
        let db = Database::open(&base, 64).unwrap();

        db.execute(1, "create table dirty (id int4 not null)").unwrap();

        // Shared flags: writer signals when it has inserted (but not committed),
        // and when it has committed.
        let inserted = Arc::new(AtomicBool::new(false));
        let committed = Arc::new(AtomicBool::new(false));

        let inserted_w = inserted.clone();
        let committed_w = committed.clone();
        let db_w = db.clone();

        let writer = thread::spawn(move || {
            let mut session = Session::new(1700);
            session.execute(&db_w, "begin").unwrap();
            session
                .execute(&db_w, "insert into dirty (id) values (1)")
                .unwrap();
            // Signal that the insert is done but not yet committed.
            inserted_w.store(true, Ordering::Release);
            // Busy-wait a moment to give the reader time to observe the state.
            let deadline = Instant::now() + Duration::from_millis(200);
            while Instant::now() < deadline {
                std::hint::spin_loop();
            }
            session.execute(&db_w, "commit").unwrap();
            committed_w.store(true, Ordering::Release);
        });

        // Reader: spin until writer has inserted, then verify no dirty read.
        let deadline = Instant::now() + TEST_TIMEOUT;
        while !inserted.load(Ordering::Acquire) {
            if Instant::now() > deadline {
                panic!("timed out waiting for writer to insert");
            }
            std::hint::spin_loop();
        }

        // While writer is still in progress, we must see 0 rows.
        match db.execute(1800, "select count(*) from dirty").unwrap() {
            StatementResult::Query { rows, .. } => {
                assert_eq!(
                    rows,
                    vec![vec![Value::Int32(0)]],
                    "must not see uncommitted row (dirty read)"
                );
            }
            other => panic!("expected query result, got {:?}", other),
        }

        writer
            .join()
            .unwrap_or_else(|e| std::panic::resume_unwind(e));

        assert!(committed.load(Ordering::Acquire));

        // After commit, the row must now be visible.
        match db.execute(1800, "select count(*) from dirty").unwrap() {
            StatementResult::Query { rows, .. } => {
                assert_eq!(
                    rows,
                    vec![vec![Value::Int32(1)]],
                    "committed row must be visible after commit"
                );
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }

    /// Half the threads commit their transaction; half roll it back.
    /// Only the committed threads' rows must survive.
    #[test]
    fn concurrent_mixed_commit_and_rollback() {
        let base = temp_dir("mixed_commit_rollback");
        let db = Database::open(&base, 64).unwrap();

        db.execute(1, "create table mixed (id int4 not null)").unwrap();

        let num_threads = 6; // must be even
        let rows_per_thread = 5;

        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let db = db.clone();
                thread::spawn(move || {
                    let mut session = Session::new((t + 1900) as ClientId);
                    session.execute(&db, "begin").unwrap();
                    for i in 0..rows_per_thread {
                        let id = t * 10_000 + i;
                        session
                            .execute(&db, &format!("insert into mixed (id) values ({id})"))
                            .unwrap();
                    }
                    // Even-numbered threads commit; odd-numbered threads roll back.
                    if t % 2 == 0 {
                        session.execute(&db, "commit").unwrap();
                    } else {
                        session.execute(&db, "rollback").unwrap();
                    }
                })
            })
            .collect();

        join_all_with_timeout(handles, TEST_TIMEOUT);

        let committing_threads = num_threads / 2; // threads 0, 2, 4, …
        let expected = (committing_threads * rows_per_thread) as i32;
        match db.execute(1, "select count(*) from mixed").unwrap() {
            StatementResult::Query { rows, .. } => {
                assert_eq!(
                    rows,
                    vec![vec![Value::Int32(expected)]],
                    "only committed rows should survive — expected {expected}"
                );
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }
}
