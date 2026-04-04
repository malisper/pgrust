use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use parking_lot::{Condvar, Mutex, RwLock};

use crate::access::heap::mvcc::{MvccError, TransactionId, TransactionManager};
use crate::catalog::{CatalogError, DurableCatalog};
use crate::executor::{ExecError, ExecutorContext, StatementResult};
use crate::storage::smgr::{MdStorageManager, RelFileLocator};
use crate::{BufferPool, ClientId, SmgrStorageBackend};

#[derive(Debug)]
pub enum DatabaseError {
    Catalog(CatalogError),
    Mvcc(MvccError),
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
    pub txns: Arc<RwLock<TransactionManager>>,
    pub catalog: Arc<RwLock<DurableCatalog>>,
    pub txn_waiter: Arc<TransactionWaiter>,
    pub table_locks: Arc<TableLockManager>,
}

impl Database {
    pub fn open(base_dir: impl Into<PathBuf>, pool_size: usize) -> Result<Self, DatabaseError> {
        let base_dir = base_dir.into();
        std::fs::create_dir_all(&base_dir)
            .map_err(|e| DatabaseError::Catalog(CatalogError::Io(e.to_string())))?;

        let txns = TransactionManager::new_durable(&base_dir)?;
        let catalog = DurableCatalog::load(&base_dir)?;
        let smgr = MdStorageManager::new(&base_dir);
        let pool = BufferPool::new(SmgrStorageBackend::new(smgr), pool_size);

        Ok(Self {
            pool: Arc::new(pool),
            txns: Arc::new(RwLock::new(txns)),
            catalog: Arc::new(RwLock::new(catalog)),
            txn_waiter: Arc::new(TransactionWaiter::new()),
            table_locks: Arc::new(TableLockManager::new()),
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
                    let _ = catalog_guard.persist();
                }
                result
            }

            Statement::DropTable(ref drop_stmt) => {
                let rel = {
                    let catalog_guard = self.catalog.read();
                    catalog_guard
                        .catalog()
                        .get(&drop_stmt.table_name)
                        .map(|e| e.rel)
                };
                if let Some(rel) = rel {
                    self.table_locks.lock_table(rel, TableLockMode::AccessExclusive, client_id);
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
                if let Some(rel) = rel {
                    self.table_locks.unlock_table(rel, client_id);
                }
                result
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::Value;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;

    use std::time::{Duration, Instant};

    const TEST_TIMEOUT: Duration = Duration::from_secs(10);

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

    fn join_all_with_timeout(handles: Vec<thread::JoinHandle<()>>, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        for h in handles {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
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
                Err(_) => panic!("test timed out after {timeout:?} — likely deadlock"),
            }
            let _ = waiter.join();
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
}
