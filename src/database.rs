use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use crate::access::heap::mvcc::{MvccError, TransactionManager};
use crate::catalog::{CatalogError, DurableCatalog};
use crate::executor::{ExecError, ExecutorContext, StatementResult, execute_sql};
use crate::parser::ParseError;
use crate::storage::smgr::MdStorageManager;
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
        use crate::parser::{Statement, parse_statement};

        let stmt = parse_statement(sql)?;
        let needs_txn = matches!(
            stmt,
            Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_)
        );
        let needs_catalog_persist = matches!(
            stmt,
            Statement::CreateTable(_) | Statement::DropTable(_)
        );

        if needs_txn {
            let xid = self.txns.write().unwrap().begin();
            let result = {
                let txns_guard = self.txns.read().unwrap();
                let snapshot = txns_guard.snapshot(xid)?;
                let mut catalog_guard = self.catalog.write().unwrap();
                let mut ctx = ExecutorContext {
                    pool: &self.pool,
                    txns: &txns_guard,
                    snapshot,
                    client_id,
                    next_command_id: 0,
                };
                execute_sql(sql, catalog_guard.catalog_mut(), &mut ctx, xid)
            };
            match result {
                Ok(r) => {
                    self.txns.write().unwrap().commit(xid)?;
                    Ok(r)
                }
                Err(e) => {
                    let _ = self.txns.write().unwrap().abort(xid);
                    Err(e)
                }
            }
        } else {
            let txns_guard = self.txns.read().unwrap();
            let snapshot = txns_guard.snapshot(INVALID_TRANSACTION_ID)?;
            let mut catalog_guard = self.catalog.write().unwrap();
            let mut ctx = ExecutorContext {
                pool: &self.pool,
                txns: &txns_guard,
                snapshot,
                client_id,
                next_command_id: 0,
            };
            let result = execute_sql(
                sql,
                catalog_guard.catalog_mut(),
                &mut ctx,
                INVALID_TRANSACTION_ID,
            )?;
            if needs_catalog_persist {
                catalog_guard.persist().map_err(|e| {
                    ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "catalog persistence",
                        actual: format!("{e:?}"),
                    })
                })?;
            }
            Ok(result)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::Value;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

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

        for h in handles {
            h.join().unwrap();
        }
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

        for h in handles {
            h.join().unwrap();
        }

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

        for h in handles {
            h.join().unwrap();
        }

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
}
