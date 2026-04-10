use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use parking_lot::RwLock;

use crate::backend::access::transam::xact::{CommandId, MvccError, TransactionId, TransactionManager};
use crate::backend::catalog::catalog::{CatalogError, DurableCatalog};
use crate::backend::executor::{ExecError, ExecutorContext, StatementResult};
use crate::backend::parser::Statement;
use crate::backend::storage::smgr::{MdStorageManager, RelFileLocator};
use crate::backend::storage::lmgr::{TableLockManager, TableLockMode, lock_relations, unlock_relations};
use crate::backend::utils::cache::plancache::PlanCache;
use crate::backend::access::transam::xlog::{WalBgWriter, WalWriter, WalError};
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
    pub catalog: Arc<RwLock<DurableCatalog>>,
    pub txn_waiter: Arc<TransactionWaiter>,
    pub table_locks: Arc<TableLockManager>,
    pub plan_cache: Arc<PlanCache>,
    /// Background WAL writer — flushes BufWriter to kernel periodically.
    _wal_bg_writer: Arc<WalBgWriter>,
}

impl Database {
    pub fn open(base_dir: impl Into<PathBuf>, pool_size: usize) -> Result<Self, DatabaseError> {
        Self::open_with_options(base_dir, pool_size, false)
    }

    pub fn open_with_options(base_dir: impl Into<PathBuf>, pool_size: usize, wal_replay: bool) -> Result<Self, DatabaseError> {
        let base_dir = base_dir.into();
        std::fs::create_dir_all(&base_dir)
            .map_err(|e| DatabaseError::Catalog(CatalogError::Io(e.to_string())))?;

        let mut txns = TransactionManager::new_durable(&base_dir)?;
        let catalog = DurableCatalog::load(&base_dir)?;

        // --- WAL Recovery ---
        let wal_dir = base_dir.join("pg_wal");
        if wal_replay && wal_dir.join("wal.log").exists() {
            let mut recovery_smgr = MdStorageManager::new_in_recovery(&base_dir);
            {
                use crate::backend::storage::smgr::{ForkNumber, StorageManager};
                let cat = catalog.catalog();
                for name in cat.table_names().collect::<Vec<_>>() {
                    if let Some(entry) = cat.get(name) {
                        let _ = recovery_smgr.open(entry.rel);
                        let _ = recovery_smgr.create(entry.rel, ForkNumber::Main, false);
                    }
                }
            }
            let stats = crate::backend::access::transam::xlog::replay::perform_wal_recovery(
                &wal_dir, &mut recovery_smgr, &mut txns,
            ).map_err(DatabaseError::Wal)?;
            if stats.records_replayed > 0 {
                eprintln!(
                    "WAL recovery: {} records ({} FPIs, {} inserts, {} commits, {} aborted)",
                    stats.records_replayed, stats.fpis, stats.inserts, stats.commits, stats.aborted
                );
            }
        }

        let smgr = MdStorageManager::new(&base_dir);
        let wal = Arc::new(WalWriter::new(&wal_dir).map_err(DatabaseError::Wal)?);

        let pool = BufferPool::new_with_wal(SmgrStorageBackend::new(smgr), pool_size, Arc::clone(&wal));

        // Open storage files for all existing relations so inserts don't need to.
        {
            use crate::backend::storage::smgr::{ForkNumber, StorageManager};
            let cat = catalog.catalog();
            for name in cat.table_names().collect::<Vec<_>>() {
                if let Some(entry) = cat.get(name) {
                    let rel = entry.rel;
                    pool.with_storage_mut(|s| {
                        let _ = s.smgr.open(rel);
                        // Use is_redo=true so create tolerates existing fork files on restart.
                        let _ = s.smgr.create(rel, ForkNumber::Main, true);
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
            plan_cache: Arc::new(PlanCache::new()),
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
        use crate::backend::access::transam::xact::INVALID_TRANSACTION_ID;
        use crate::backend::executor::execute_readonly_statement;
        use crate::backend::commands::tablecmds::{
            execute_create_table, execute_delete_with_waiter, execute_drop_table,
            execute_truncate_table,
            execute_insert, execute_update_with_waiter,
        };
        use crate::backend::parser::{
            bind_delete, bind_insert, bind_update,
        };

        let stmt = self.plan_cache.get_statement(sql)?;

        match stmt {
            Statement::Select(_) | Statement::Explain(_) | Statement::ShowTables => {
                let (plan_or_stmt, rels) = {
                    let catalog_guard = self.catalog.read();
                    let catalog = catalog_guard.catalog();
                    let mut rels = std::collections::BTreeSet::new();
                    match &stmt {
                        Statement::Select(select) => {
                            let plan = crate::backend::parser::build_plan(select, catalog)?;
                            collect_rels_from_plan(&plan, &mut rels);
                        }
                        Statement::Explain(explain) => {
                            if let Statement::Select(select) = explain.statement.as_ref() {
                                let plan = crate::backend::parser::build_plan(select, catalog)?;
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
                let catalog_guard = self.catalog.read();
                let mut ctx = ExecutorContext {
                    pool: std::sync::Arc::clone(&self.pool),
                    txns: self.txns.clone(),
                    snapshot,
                    client_id,
                    next_command_id: 0,
                    outer_rows: Vec::new(),
                    timed: false,
                };
                let result = execute_readonly_statement(plan_or_stmt, catalog_guard.catalog(), &mut ctx);
                drop(ctx);
                drop(catalog_guard);

                unlock_relations(&self.table_locks, client_id, &rels);
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
                let result = self.finish_txn(xid, result);
                guard.disarm();
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
                let result = self.finish_txn(xid, result);
                guard.disarm();
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
                let result = self.finish_txn(xid, result);
                guard.disarm();
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
                            use crate::backend::storage::smgr::StorageManager;
                            let _ = s.smgr.open(rel);
                            let _ = s.smgr.create(rel, crate::backend::storage::smgr::ForkNumber::Main, false);
                        });
                    }
                    let _ = catalog_guard.persist();
                    self.plan_cache.invalidate_all();
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
                    pool: std::sync::Arc::clone(&self.pool),
                    txns: self.txns.clone(),
                    snapshot,
                    client_id,
                    next_command_id: 0,
                    outer_rows: Vec::new(),
                    timed: false,
                };
                let result = execute_drop_table(drop_stmt.clone(), catalog_guard.catalog_mut(), &mut ctx);
                if result.is_ok() {
                    let _ = catalog_guard.persist();
                    self.plan_cache.invalidate_all();
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
                    pool: std::sync::Arc::clone(&self.pool),
                    txns: self.txns.clone(),
                    snapshot,
                    client_id,
                    next_command_id: 0,
                    outer_rows: Vec::new(),
                    timed: false,
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
        use crate::backend::parser::build_plan;
        use crate::backend::executor::executor_start;

        let (plan, rels) = {
            let catalog_guard = self.catalog.read();
            let catalog = catalog_guard.catalog();
            let plan = build_plan(select_stmt, catalog)?;
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

fn collect_rels_from_expr(expr: &crate::backend::executor::Expr, rels: &mut std::collections::BTreeSet<RelFileLocator>) {
    use crate::backend::executor::Expr;

    match expr {
        Expr::Column(_)
        | Expr::OuterColumn { .. }
        | Expr::Const(_)
        | Expr::Random
        | Expr::CurrentTimestamp => {}
        Expr::UnaryPlus(inner)
        | Expr::Negate(inner)
        | Expr::Cast(inner, _)
        | Expr::Not(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => collect_rels_from_expr(inner, rels),
        Expr::Add(left, right)
        | Expr::Sub(left, right)
        | Expr::Mul(left, right)
        | Expr::Div(left, right)
        | Expr::Mod(left, right)
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
        | Expr::IsNotDistinctFrom(left, right) => {
            collect_rels_from_expr(left, rels);
            collect_rels_from_expr(right, rels);
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

fn collect_rels_from_plan(plan: &crate::backend::executor::Plan, rels: &mut std::collections::BTreeSet<RelFileLocator>) {
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
                if let Some(arg) = &accum.arg {
                    collect_rels_from_expr(arg, rels);
                }
            }
            if let Some(expr) = having {
                collect_rels_from_expr(expr, rels);
            }
        }
        Plan::GenerateSeries { start, stop, step, .. } => {
            collect_rels_from_expr(start, rels);
            collect_rels_from_expr(stop, rels);
            collect_rels_from_expr(step, rels);
        }
        Plan::Unnest { args, .. } => {
            for arg in args {
                collect_rels_from_expr(arg, rels);
            }
        }
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
                    ExecError::Heap(crate::backend::access::heap::heapam::HeapError::Storage(
                        crate::backend::storage::smgr::SmgrError::Io(
                            std::io::Error::new(std::io::ErrorKind::Other, e)
                        )
                    ))
                })?;
                self.pool.flush_wal().map_err(|e| {
                    ExecError::Heap(crate::backend::access::heap::heapam::HeapError::Storage(
                        crate::backend::storage::smgr::SmgrError::Io(
                            std::io::Error::new(std::io::ErrorKind::Other, e)
                        )
                    ))
                })?;
                self.txns.write().commit(xid).map_err(|e| {
                    ExecError::Heap(crate::backend::access::heap::heapam::HeapError::Mvcc(e))
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
    fn new(txns: &'a Arc<RwLock<TransactionManager>>, txn_waiter: &'a TransactionWaiter, xid: TransactionId) -> Self {
        Self { txns, txn_waiter, xid, committed: false }
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
mod tests {
    use super::*;
    use crate::backend::executor::Value;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;

    use std::time::{Duration, Instant};

    const TEST_TIMEOUT: Duration = Duration::from_secs(5);

    /// Start a background thread that periodically checks for deadlocks
    /// using parking_lot's deadlock detector.  Called once via `Once`.
    #[cfg(feature = "deadlock_detection")]
    fn start_deadlock_checker() {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            thread::Builder::new()
                .name("deadlock-checker".into())
                .spawn(|| loop {
                    thread::sleep(Duration::from_secs(1));
                    let deadlocks = parking_lot::deadlock::check_deadlock();
                    if !deadlocks.is_empty() {
                        eprintln!("=== DEADLOCK DETECTED ({} cycle(s)) ===", deadlocks.len());
                        for (i, threads) in deadlocks.iter().enumerate() {
                            eprintln!("--- cycle {i} ---");
                            for t in threads {
                                eprintln!("thread {:?}:\n{:#?}", t.thread_id(), t.backtrace());
                            }
                        }
                        // Don't panic here — just log. The test timeout will handle it.
                    }
                })
                .unwrap();
        });
    }

    #[cfg(not(feature = "deadlock_detection"))]
    fn start_deadlock_checker() {}

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
        start_deadlock_checker();
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
                assert_eq!(rows, vec![vec![Value::Int64(0)]]);
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
    fn copy_from_rows_parses_array_literals() {
        let base = temp_dir("copy_from_rows_arrays");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        db.execute(
            1,
            "create table shipments (id int not null, tags varchar[], sizes int4[])",
        )
        .unwrap();

        let inserted = session
            .copy_from_rows(
                &db,
                "shipments",
                &[vec!["1".into(), "{\"a\",\"b\"}".into(), "{1,NULL,3}".into()]],
            )
            .unwrap();
        assert_eq!(inserted, 1);

        match db
            .execute(1, "select id, tags, sizes from shipments")
            .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(
                    rows,
                    vec![vec![
                        Value::Int32(1),
                        Value::Array(vec![Value::Text("a".into()), Value::Text("b".into())]),
                        Value::Array(vec![Value::Int32(1), Value::Null, Value::Int32(3)]),
                    ]]
                );
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }

    #[test]
    fn copy_from_rows_parses_quoted_array_text_and_empty_arrays() {
        let base = temp_dir("copy_from_rows_arrays_quoted");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        db.execute(
            1,
            "create table shipments (id int not null, tags varchar[], sizes int4[])",
        )
        .unwrap();

        session
            .copy_from_rows(
                &db,
                "shipments",
                &[
                    vec!["1".into(), "{\"a,b\",\"c\\\"d\"}".into(), "{}".into()],
                    vec!["2".into(), "{}".into(), "{NULL}".into()],
                ],
            )
            .unwrap();

        match db
            .execute(1, "select id, tags, sizes from shipments order by id")
            .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(
                    rows,
                    vec![
                        vec![
                            Value::Int32(1),
                            Value::Array(vec![Value::Text("a,b".into()), Value::Text("c\"d".into())]),
                            Value::Array(vec![]),
                        ],
                        vec![
                            Value::Int32(2),
                            Value::Array(vec![]),
                            Value::Array(vec![Value::Null]),
                        ],
                    ]
                );
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }

    #[test]
    fn copy_from_rows_parses_extended_numeric_types() {
        let base = temp_dir("copy_from_rows_extended_numeric");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        db.execute(
            1,
            "create table metrics (a int2, b int8, c float4, d float8)",
        )
        .unwrap();

        let inserted = session
            .copy_from_rows(
                &db,
                "metrics",
                &[vec![
                    "7".into(),
                    "9000000000".into(),
                    "1.5".into(),
                    "2.5".into(),
                ]],
            )
            .unwrap();
        assert_eq!(inserted, 1);

        match db.execute(1, "select a, b, c, d from metrics").unwrap() {
            StatementResult::Query { rows, .. } => {
                assert_eq!(
                    rows,
                    vec![vec![
                        Value::Int16(7),
                        Value::Int64(9_000_000_000),
                        Value::Float64(1.5),
                        Value::Float64(2.5),
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
                                assert_eq!(rows, vec![vec![Value::Int64(10)]]);
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
                assert_eq!(rows, vec![vec![Value::Int64(total as i64)]]);
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
                assert_eq!(rows, vec![vec![Value::Int64(expected as i64)]]);
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
                assert_eq!(rows, vec![vec![Value::Int64(80)]]);
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
                    vec![vec![Value::Int64(expected as i64)]],
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
                                assert_eq!(rows, vec![vec![Value::Int64(5)]]);
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
                for i in 0..20 {
                    if let Err(e) = db.execute(
                        (t + 1100) as ClientId,
                        "update rwtest set val = val + 1 where id = 1",
                    ) {
                        panic!("writer {t} iteration {i} failed: {e:?}");
                    }
                }
            }));
        }

        for t in 0..num_readers {
            let db = db.clone();
            handles.push(thread::spawn(move || {
                for i in 0..50 {
                    let result = db.execute(
                        (t + 1200) as ClientId,
                        "select val from rwtest where id = 1",
                    );
                    match result {
                        Err(e) => panic!("reader {t} iteration {i} failed: {e:?}"),
                        Ok(StatementResult::Query { rows, .. }) => {
                            assert_eq!(rows.len(), 1, "should always see exactly one row");
                            match &rows[0][0] {
                                Value::Int32(v) => assert!(*v >= 0, "val should never be negative"),
                                other => panic!("expected Int32, got {:?}", other),
                            }
                        }
                        Ok(other) => panic!("expected query result, got {:?}", other),
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

    /// Regression: try_claim_tuple reads ctid before dropping the buffer lock,
    /// then checks xmax status after dropping it. If the updater commits and
    /// sets ctid between those two points, the stale ctid (== self) makes us
    /// think the row was deleted rather than updated, losing the update.
    ///
    /// Uses a Barrier so all threads start simultaneously, maximizing the
    /// chance of hitting the race window.
    #[test]
    fn no_lost_updates_under_heavy_contention() {
        let base = temp_dir("no_lost_updates_heavy");
        let db = Database::open(&base, 64).unwrap();

        db.execute(1, "create table counter (id int4 not null, val int4 not null)")
            .unwrap();
        db.execute(1, "insert into counter (id, val) values (1, 0)")
            .unwrap();

        let num_threads = 4usize;
        let increments_per_thread = 10;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(num_threads));
        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let db = db.clone();
                let barrier = barrier.clone();
                thread::spawn(move || {
                    barrier.wait();
                    for i in 0..increments_per_thread {
                        if let Err(e) = db.execute(
                            (t + 5000) as ClientId,
                            "update counter set val = val + 1 where id = 1",
                        ) {
                            panic!("thread {t} iteration {i}: {e:?}");
                        }
                    }
                })
            })
            .collect();

        join_all_with_timeout(handles, Duration::from_secs(30));

        let expected = num_threads * increments_per_thread;
        match db.execute(1, "select val from counter where id = 1").unwrap() {
            StatementResult::Query { rows, .. } => {
                let actual = match &rows[0][0] {
                    Value::Int32(v) => *v,
                    other => panic!("expected Int32, got {:?}", other),
                };
                assert_eq!(
                    actual, expected as i32,
                    "lost {} update(s): expected {expected}, got {actual}",
                    expected as i32 - actual
                );
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }

    /// Regression test for bugs/005: try_read contention busy-loop.
    ///
    /// Before the fix, try_claim_tuple used try_read() to check xmax status.
    /// Under contention (16 threads updating the same row), all try_read
    /// attempts could fail, causing an infinite busy-loop. The fix replaced
    /// try_read() with blocking read().
    ///
    /// This test verifies no lost updates under high contention.
    #[test]
    fn poc_try_read_contention_lost_update() {
        let base = temp_dir("poc_try_read_contention");
        let db = Database::open(&base, 64).unwrap();

        db.execute(1, "create table ctr (id int4 not null, val int4 not null)")
            .unwrap();
        db.execute(1, "insert into ctr (id, val) values (1, 0)")
            .unwrap();

        let num_threads = 16;
        let updates_per_thread = 5;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(num_threads));

        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let db = db.clone();
                let barrier = barrier.clone();
                thread::spawn(move || {
                    barrier.wait();
                    for i in 0..updates_per_thread {
                        if let Err(e) = db.execute(
                            (t + 6000) as ClientId,
                            "update ctr set val = val + 1 where id = 1",
                        ) {
                            panic!("thread {t} iteration {i}: {e:?}");
                        }
                    }
                })
            })
            .collect();

        join_all_with_timeout(handles, Duration::from_secs(60));

        let expected = (num_threads * updates_per_thread) as i32;
        match db.execute(1, "select val from ctr where id = 1").unwrap() {
            StatementResult::Query { rows, .. } => {
                let actual = match &rows[0][0] {
                    Value::Int32(v) => *v,
                    other => panic!("expected Int32, got {:?}", other),
                };
                assert_eq!(
                    actual, expected,
                    "LOST {} update(s): expected {expected}, got {actual}. \
                     This demonstrates the try_read contention bug — \
                     try_read returns None under txns write-lock contention, \
                     causing committed updates to be treated as in-progress.",
                    expected - actual
                );
            }
            other => panic!("expected query result, got {:?}", other),
        }
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
                assert_eq!(rows, vec![vec![Value::Int64(2)]]);
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
                assert_eq!(rows, vec![vec![Value::Int64(1)]],
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
                assert_eq!(rows, vec![vec![Value::Int64(0)]],
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
                assert_eq!(rows, vec![vec![Value::Int64(2)]]);
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
                assert_eq!(rows, vec![vec![Value::Int64(1)]],
                    "session B should not see session A's uncommitted insert");
            }
            other => panic!("expected query, got {:?}", other),
        }

        session_a.execute(&db, "commit").unwrap();

        match session_b.execute(&db, "select count(*) from isotest").unwrap() {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![vec![Value::Int64(2)]],
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
                assert_eq!(rows, vec![vec![Value::Int64(1)]]);
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
                    vec![vec![Value::Int64(expected as i64)]],
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
                    vec![vec![Value::Int64(0)]],
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
                    vec![vec![Value::Int64(1)]],
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
                    vec![vec![Value::Int64(expected as i64)]],
                    "only committed rows should survive — expected {expected}"
                );
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }

    /// Reproduces a lock-ordering deadlock between the write-preferring
    /// `txns` RwLock and the write-preferring buffer `content_lock` RwLock.
    ///
    /// The SELECT path acquires content_lock(shared) → txns.read(),
    /// while the UPDATE scan path acquires txns.read() → content_lock(shared).
    /// With pending exclusive waiters on both locks, this creates a cycle.
    ///
    /// Many readers + writers on a single-row table maximises the chance
    /// that all four roles (R, W, R2, WW) overlap on the same page.
    #[test]
    fn lock_ordering_deadlock_repro() {
        let base = temp_dir("lock_ordering_deadlock");
        let db = Database::open(&base, 16).unwrap();

        db.execute(1, "create table locktest (id int4 not null, val int4 not null)")
            .unwrap();
        db.execute(1, "insert into locktest (id, val) values (1, 0)")
            .unwrap();

        let num_readers = 8;
        let num_writers = 4;
        let mut handles = Vec::new();

        for t in 0..num_writers {
            let db = db.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..50 {
                    db.execute(
                        (t + 2000) as ClientId,
                        "update locktest set val = val + 1 where id = 1",
                    )
                    .unwrap();
                }
            }));
        }

        for t in 0..num_readers {
            let db = db.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..200 {
                    let _ = db.execute(
                        (t + 3000) as ClientId,
                        "select val from locktest where id = 1",
                    )
                    .unwrap();
                }
            }));
        }

        join_all_with_timeout(handles, TEST_TIMEOUT);
    }

    #[test]
    fn no_pins_leaked_concurrent_contention() {
        // The cold table accumulates dead versions (no vacuum), so scans get
        // slower with bloat; a small pool adds eviction pressure on top.
        let base = temp_dir("no_pins_concurrent");
        let db = Database::open(&base, 128).unwrap();

        // Create two tables so threads contend on the same rows from
        // different directions (readers vs writers, writers vs writers).
        db.execute(1, "create table hot (id int4 not null, val int4 not null)").unwrap();
        db.execute(1, "create table cold (id int4 not null, val int4 not null)").unwrap();
        for i in 0..20 {
            db.execute(1, &format!("insert into hot (id, val) values ({i}, 0)")).unwrap();
            db.execute(1, &format!("insert into cold (id, val) values ({i}, 0)")).unwrap();
        }

        let num_threads = 8;
        let iters = 100;
        let mut handles = Vec::new();

        // Writers: all contend on the same hot rows.
        for t in 0..num_threads / 2 {
            let db = db.clone();
            handles.push(thread::spawn(move || {
                let client = (t + 10) as ClientId;
                for i in 0..iters {
                    let row = i % 5; // contend on rows 0-4
                    let _ = db.execute(client, &format!(
                        "update hot set val = val + 1 where id = {row}"
                    ));
                    // Full-table scan to pin many pages at once.
                    let _ = db.execute(client, "select * from hot");
                }
            }));
        }

        // Readers + cross-table writers: read hot, write cold.
        for t in 0..num_threads / 2 {
            let db = db.clone();
            handles.push(thread::spawn(move || {
                let client = (t + 20) as ClientId;
                for i in 0..iters {
                    let row = i % 5;
                    let _ = db.execute(client, "select count(*) from hot");
                    let _ = db.execute(client, &format!(
                        "update cold set val = val + 1 where id = {row}"
                    ));
                    // Delete + reinsert to force page layout changes.
                    let _ = db.execute(client, &format!(
                        "delete from cold where id = {}", (i % 20)
                    ));
                    let _ = db.execute(client, &format!(
                        "insert into cold (id, val) values ({}, {})", i % 20, i
                    ));
                }
            }));
        }

        join_all_with_timeout(handles, Duration::from_secs(30));

        // After all threads finish, no pins should remain.
        let capacity = db.pool.capacity();
        let mut pinned = Vec::new();
        for buffer_id in 0..capacity {
            if let Some(state) = db.pool.buffer_state(buffer_id) {
                if state.pin_count > 0 {
                    pinned.push((buffer_id, state));
                }
            }
        }
        assert!(
            pinned.is_empty(),
            "buffer pin leak: {} buffer(s) still pinned after concurrent workload:\n{:#?}",
            pinned.len(),
            pinned,
        );
    }

    #[test]
    fn concurrent_same_row_updates_do_not_deadlock() {
        let base = temp_dir("no_deadlock_same_row");
        let db = Database::open(&base, 64).unwrap();
        db.execute(1, "create table t (id int4 not null, val int4 not null)").unwrap();
        db.execute(1, "insert into t (id, val) values (1, 0)").unwrap();

        let num_threads = 4;
        let iters = 200;
        let mut handles = Vec::new();
        for t in 0..num_threads {
            let db = db.clone();
            handles.push(thread::spawn(move || {
                let client = (t + 10) as ClientId;
                for _ in 0..iters {
                    db.execute(client, "update t set val = val + 1 where id = 1").unwrap();
                }
            }));
        }
        join_all_with_timeout(handles, Duration::from_secs(10));

        let result = db.execute(1, "select val from t where id = 1").unwrap();
        let expected = num_threads * iters;
        match result {
            StatementResult::Query { rows, .. } => {
                let val = match &rows[0][0] {
                    crate::backend::executor::Value::Int32(v) => *v,
                    other => panic!("expected Int32, got {other:?}"),
                };
                assert_eq!(val, expected as i32,
                    "expected val={expected} after {num_threads} threads x {iters} increments");
            }
            other => panic!("expected query result, got {other:?}"),
        }
    }

    #[test]
    fn no_pins_leaked_after_queries() {
        let base = temp_dir("no_pins_leaked");
        let db = Database::open(&base, 64).unwrap();
        let mut session = Session::new(1);

        // Set up schema and data.
        session.execute(&db, "create table pintest (id int4 not null, val int4 not null)").unwrap();
        for i in 0..50 {
            session.execute(&db, &format!("insert into pintest (id, val) values ({i}, {i})")).unwrap();
        }

        // Run a variety of query types.
        session.execute(&db, "select * from pintest").unwrap();
        session.execute(&db, "select count(*) from pintest").unwrap();
        session.execute(&db, "select id, val from pintest where id > 10").unwrap();
        session.execute(&db, "select id + val from pintest").unwrap();
        session.execute(&db, "update pintest set val = val + 1 where id = 1").unwrap();
        session.execute(&db, "update pintest set val = 0").unwrap();
        session.execute(&db, "delete from pintest where id > 40").unwrap();

        // Explicit transaction.
        session.execute(&db, "begin").unwrap();
        session.execute(&db, "insert into pintest (id, val) values (999, 999)").unwrap();
        session.execute(&db, "select * from pintest where id = 999").unwrap();
        session.execute(&db, "commit").unwrap();

        // Rolled-back transaction.
        session.execute(&db, "begin").unwrap();
        session.execute(&db, "insert into pintest (id, val) values (1000, 1000)").unwrap();
        session.execute(&db, "rollback").unwrap();

        // Assert no pins are held anywhere in the buffer pool.
        let capacity = db.pool.capacity();
        let mut pinned = Vec::new();
        for buffer_id in 0..capacity {
            if let Some(state) = db.pool.buffer_state(buffer_id) {
                if state.pin_count > 0 {
                    pinned.push((buffer_id, state));
                }
            }
        }
        assert!(
            pinned.is_empty(),
            "buffer pin leak: {} buffer(s) still pinned after all queries completed:\n{:#?}",
            pinned.len(),
            pinned,
        );
    }

    #[test]
    fn streaming_select_supports_correlated_subqueries() {
        let base = temp_dir("streaming_correlated_subquery");
        let db = Database::open(&base, 64).unwrap();
        let mut session = Session::new(1);

        session
            .execute(&db, "create table people (id int4 not null, name text)")
            .unwrap();
        session
            .execute(&db, "create table pets (id int4 not null, owner_id int4, name text)")
            .unwrap();
        session
            .execute(&db, "insert into people (id, name) values (1, 'alice'), (2, 'bob'), (3, 'carol')")
            .unwrap();
        session
            .execute(&db, "insert into pets (id, owner_id, name) values (10, 1, 'mocha'), (11, 1, 'pixel'), (12, 2, 'otis')")
            .unwrap();

        let stmt = crate::backend::parser::parse_select(
            "select p.id, (select count(*) from pets q where q.owner_id = p.id) from people p order by p.id",
        )
        .unwrap();
        let mut guard = session.execute_streaming(&db, &stmt).unwrap();
        let mut rows = Vec::new();
        while let Some(slot) = crate::backend::executor::exec_next(&mut guard.state, &mut guard.ctx).unwrap() {
            rows.push(
                slot.values()
                    .unwrap()
                    .iter()
                    .map(|v| v.to_owned_value())
                    .collect::<Vec<_>>(),
            );
        }
        drop(guard);

        assert_eq!(
            rows,
            vec![
                vec![Value::Int32(1), Value::Int64(2)],
                vec![Value::Int32(2), Value::Int64(1)],
                vec![Value::Int32(3), Value::Int64(0)],
            ]
        );
    }

    #[test]
    fn streaming_correlated_subquery_holds_access_share_lock_on_inner_relation() {
        use std::sync::mpsc;

        let base = temp_dir("streaming_correlated_subquery_lock");
        let db = Database::open(&base, 64).unwrap();
        let mut session = Session::new(1);

        session
            .execute(&db, "create table people (id int4 not null, name text)")
            .unwrap();
        session
            .execute(&db, "create table pets (id int4 not null, owner_id int4, name text)")
            .unwrap();
        session
            .execute(&db, "insert into people (id, name) values (1, 'alice')")
            .unwrap();
        session
            .execute(&db, "insert into pets (id, owner_id, name) values (10, 1, 'mocha')")
            .unwrap();

        let stmt = crate::backend::parser::parse_select(
            "select p.id, exists (select 1 from pets q where q.owner_id = p.id) from people p",
        )
        .unwrap();
        let guard = session.execute_streaming(&db, &stmt).unwrap();

        let db2 = db.clone();
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let worker = thread::spawn(move || {
            started_tx.send(()).unwrap();
            db2.execute(2, "truncate pets").unwrap();
            done_tx.send(()).unwrap();
        });

        started_rx.recv().unwrap();
        assert!(
            done_rx.recv_timeout(Duration::from_millis(200)).is_err(),
            "truncate should block while the streaming guard holds the inner relation lock"
        );

        drop(guard);
        done_rx.recv_timeout(TEST_TIMEOUT).unwrap();
        worker.join().unwrap();
    }

}
