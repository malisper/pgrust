use super::*;
use crate::RelFileLocator;
use crate::backend::parser::{Catalog, CatalogLookup};
use crate::backend::storage::smgr::{ForkNumber, MdStorageManager, StorageManager};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

pub(crate) struct SeededSqlHarness {
    pub base: PathBuf,
    pub txns: TransactionManager,
    catalog: Catalog,
}

impl SeededSqlHarness {
    pub(crate) fn new(label: &str, catalog: Catalog) -> Self {
        let base = temp_dir(label);
        let txns = TransactionManager::new_durable(&base).unwrap();
        crate::backend::catalog::store::sync_catalog_heaps_for_tests(&base, &catalog).unwrap();
        create_relation_forks(&base, &catalog);
        Self {
            base,
            txns,
            catalog,
        }
    }

    pub(crate) fn execute(
        &mut self,
        xid: TransactionId,
        sql: &str,
    ) -> Result<StatementResult, ExecError> {
        let base = self.base.clone();
        let txns = self.txns.clone();
        let sql = sql.to_string();
        let mut catalog = std::mem::take(&mut self.catalog);

        let (catalog, result) = run_with_large_stack_result("executor-test-sql", move || {
            let smgr = MdStorageManager::new(&base);
            let pool = Arc::new(BufferPool::new(SmgrStorageBackend::new(smgr), 8));
            for (_, entry) in catalog.entries() {
                create_fork(&pool, entry.rel);
            }
            let txns_arc = Arc::new(parking_lot::RwLock::new(txns.clone()));
            let mut ctx = ExecutorContext {
                pool,
                txns: txns_arc,
                txn_waiter: None,
                sequences: Some(Arc::new(
                    crate::pgrust::database::SequenceRuntime::new_ephemeral(),
                )),
                large_objects: Some(Arc::new(
                    crate::pgrust::database::LargeObjectRuntime::new_ephemeral(),
                )),
                async_notify_runtime: None,
                advisory_locks: Arc::new(crate::backend::storage::lmgr::AdvisoryLockManager::new()),
                row_locks: Arc::new(crate::backend::storage::lmgr::RowLockManager::new()),
                checkpoint_stats:
                    crate::backend::utils::misc::checkpoint::CheckpointStatsSnapshot::default(),
                datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(
                ),
                interrupts: Arc::new(
                    crate::backend::utils::misc::interrupts::InterruptState::new(),
                ),
                stats: Arc::new(parking_lot::RwLock::new(
                    crate::pgrust::database::DatabaseStatsStore::with_default_io_rows(),
                )),
                session_stats: Arc::new(parking_lot::RwLock::new(
                    crate::pgrust::database::SessionStatsState::default(),
                )),
                snapshot: txns.snapshot(xid).unwrap(),
                transaction_state: None,
                client_id: 77,
                current_database_name: "postgres".to_string(),
                session_user_oid: crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
                current_user_oid: crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
                active_role_oid: None,
                session_replication_role: Default::default(),
                statement_lock_scope_id: None,
                transaction_lock_scope_id: None,
                next_command_id: 0,
                default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
                expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
                case_test_values: Vec::new(),
                system_bindings: Vec::new(),
                subplans: Vec::new(),
                timed: false,
                allow_side_effects: true,
                pending_async_notifications: Vec::new(),
                catalog: catalog.materialize_visible_catalog(),
                compiled_functions: std::collections::HashMap::new(),
                cte_tables: std::collections::HashMap::new(),
                cte_producers: std::collections::HashMap::new(),
                recursive_worktables: std::collections::HashMap::new(),
                deferred_foreign_keys: None,
                trigger_depth: 0,
            };
            let result = execute_sql(&sql, &mut catalog, &mut ctx, xid);
            (catalog, result)
        });

        self.catalog = catalog;
        result
    }
}

fn temp_dir(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "pgrust_executor_{}_{}_{}",
        label,
        std::process::id(),
        NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&path);
    fs::create_dir_all(&path).unwrap();
    path
}

fn run_with_large_stack_result<F, T>(name: &str, f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    thread::Builder::new()
        .name(name.into())
        .stack_size(32 * 1024 * 1024)
        .spawn(f)
        .unwrap()
        .join()
        .unwrap()
}

fn create_relation_forks(base: &Path, catalog: &Catalog) {
    let smgr = MdStorageManager::new(base);
    let pool = Arc::new(BufferPool::new(SmgrStorageBackend::new(smgr), 8));
    for (_, entry) in catalog.entries() {
        create_fork(&pool, entry.rel);
    }
}

fn create_fork(pool: &BufferPool<SmgrStorageBackend>, rel: RelFileLocator) {
    pool.with_storage_mut(|storage| {
        storage.smgr.open(rel).unwrap();
        match storage.smgr.create(rel, ForkNumber::Main, false) {
            Ok(()) => {}
            Err(crate::backend::storage::smgr::SmgrError::AlreadyExists { .. }) => {}
            Err(err) => panic!("create_fork failed: {err:?}"),
        }
    });
}
