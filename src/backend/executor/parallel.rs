use super::{ExecError, ExecutorContext, ExprEvalBindings, PgPrngState, executor_start};
use crate::include::access::itemptr::ItemPointerData;
use crate::include::nodes::datum::Value;
use crate::include::nodes::execnodes::SystemVarBinding;
use crate::include::nodes::plannodes::Plan;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::sync::mpsc::SyncSender;
use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};
use std::thread;

thread_local! {
    static PARALLEL_WORKER: Cell<Option<(usize, usize)>> = const { Cell::new(None) };
    static PARALLEL_RUNTIME: RefCell<Option<Arc<ParallelRuntime>>> = const { RefCell::new(None) };
}

#[derive(Debug, Default)]
pub(crate) struct ParallelRuntime {
    seq_scans: parking_lot::Mutex<HashMap<usize, Arc<ParallelSeqScanState>>>,
}

#[derive(Debug, Default)]
struct ParallelSeqScanState {
    next_block: AtomicU32,
}

impl ParallelRuntime {
    pub(crate) fn next_seq_scan_block(&self, source_id: usize) -> u32 {
        let state = {
            let mut seq_scans = self.seq_scans.lock();
            seq_scans
                .entry(source_id)
                .or_insert_with(|| Arc::new(ParallelSeqScanState::default()))
                .clone()
        };
        state.next_block.fetch_add(1, Ordering::Relaxed)
    }
}

#[derive(Debug)]
pub(crate) struct WorkerTuple {
    pub(crate) values: Vec<Value>,
    pub(crate) system_bindings: Vec<SystemVarBinding>,
    pub(crate) grouping_refs: Vec<usize>,
    pub(crate) tid: Option<ItemPointerData>,
    pub(crate) table_oid: Option<u32>,
}

#[derive(Debug)]
pub(crate) enum WorkerMessage {
    Row(WorkerTuple),
    Error(ExecError),
}

#[derive(Clone)]
pub(crate) struct WorkerContextSeed {
    pool: std::sync::Arc<crate::BufferPool<crate::SmgrStorageBackend>>,
    data_dir: Option<std::path::PathBuf>,
    txns: std::sync::Arc<
        parking_lot::RwLock<crate::backend::access::transam::xact::TransactionManager>,
    >,
    txn_waiter: Option<std::sync::Arc<crate::pgrust::database::TransactionWaiter>>,
    lock_status_provider: Option<std::sync::Arc<dyn super::LockStatusProvider>>,
    sequences: Option<std::sync::Arc<crate::pgrust::database::SequenceRuntime>>,
    large_objects: Option<std::sync::Arc<crate::pgrust::database::LargeObjectRuntime>>,
    stats_import_runtime: Option<std::sync::Arc<dyn super::StatsImportRuntime>>,
    async_notify_runtime: Option<std::sync::Arc<crate::pgrust::database::AsyncNotifyRuntime>>,
    advisory_locks: std::sync::Arc<crate::backend::storage::lmgr::AdvisoryLockManager>,
    row_locks: std::sync::Arc<crate::backend::storage::lmgr::RowLockManager>,
    checkpoint_stats: crate::backend::utils::misc::checkpoint::CheckpointStatsSnapshot,
    datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig,
    statement_timestamp_usecs: i64,
    gucs: HashMap<String, String>,
    interrupts: std::sync::Arc<crate::backend::utils::misc::interrupts::InterruptState>,
    stats: std::sync::Arc<parking_lot::RwLock<crate::pgrust::database::DatabaseStatsStore>>,
    session_stats: std::sync::Arc<parking_lot::RwLock<crate::pgrust::database::SessionStatsState>>,
    snapshot: crate::backend::access::transam::xact::Snapshot,
    write_xid_override: Option<crate::backend::access::transam::xact::TransactionId>,
    transaction_state: Option<super::SharedExecutorTransactionState>,
    client_id: crate::ClientId,
    current_database_name: String,
    session_user_oid: u32,
    current_user_oid: u32,
    active_role_oid: Option<u32>,
    session_replication_role: super::SessionReplicationRole,
    statement_lock_scope_id: Option<u64>,
    transaction_lock_scope_id: Option<u64>,
    next_command_id: crate::backend::access::transam::xact::CommandId,
    default_toast_compression: crate::include::access::htup::AttributeCompression,
    expr_bindings: ExprEvalBindings,
    subplans: Vec<Plan>,
    timed: bool,
    security_restricted: bool,
    catalog: Option<super::ExecutorCatalog>,
    plpgsql_function_cache:
        std::sync::Arc<parking_lot::RwLock<crate::pl::plpgsql::PlpgsqlFunctionCache>>,
    parallel_runtime: Arc<ParallelRuntime>,
}

impl WorkerContextSeed {
    pub(crate) fn from_ctx(ctx: &ExecutorContext, parallel_runtime: Arc<ParallelRuntime>) -> Self {
        Self {
            pool: ctx.pool.clone(),
            data_dir: ctx.data_dir.clone(),
            txns: ctx.txns.clone(),
            txn_waiter: ctx.txn_waiter.clone(),
            lock_status_provider: ctx.lock_status_provider.clone(),
            sequences: ctx.sequences.clone(),
            large_objects: ctx.large_objects.clone(),
            stats_import_runtime: ctx.stats_import_runtime.clone(),
            async_notify_runtime: ctx.async_notify_runtime.clone(),
            advisory_locks: ctx.advisory_locks.clone(),
            row_locks: ctx.row_locks.clone(),
            checkpoint_stats: ctx.checkpoint_stats.clone(),
            datetime_config: ctx.datetime_config.clone(),
            statement_timestamp_usecs: ctx.statement_timestamp_usecs,
            gucs: ctx.gucs.clone(),
            interrupts: ctx.interrupts.clone(),
            stats: ctx.stats.clone(),
            session_stats: ctx.session_stats.clone(),
            snapshot: ctx.snapshot.clone(),
            write_xid_override: ctx.write_xid_override,
            transaction_state: ctx.transaction_state.clone(),
            client_id: ctx.client_id,
            current_database_name: ctx.current_database_name.clone(),
            session_user_oid: ctx.session_user_oid,
            current_user_oid: ctx.current_user_oid,
            active_role_oid: ctx.active_role_oid,
            session_replication_role: ctx.session_replication_role,
            statement_lock_scope_id: ctx.statement_lock_scope_id,
            transaction_lock_scope_id: ctx.transaction_lock_scope_id,
            next_command_id: ctx.next_command_id,
            default_toast_compression: ctx.default_toast_compression,
            expr_bindings: ctx.expr_bindings.clone(),
            subplans: ctx.subplans.clone(),
            timed: ctx.timed,
            security_restricted: ctx.security_restricted,
            catalog: ctx.catalog.clone(),
            plpgsql_function_cache: ctx.plpgsql_function_cache.clone(),
            parallel_runtime,
        }
    }

    fn build_context(&self) -> ExecutorContext {
        ExecutorContext {
            pool: self.pool.clone(),
            data_dir: self.data_dir.clone(),
            txns: self.txns.clone(),
            txn_waiter: self.txn_waiter.clone(),
            lock_status_provider: self.lock_status_provider.clone(),
            sequences: self.sequences.clone(),
            large_objects: self.large_objects.clone(),
            stats_import_runtime: self.stats_import_runtime.clone(),
            async_notify_runtime: self.async_notify_runtime.clone(),
            advisory_locks: self.advisory_locks.clone(),
            row_locks: self.row_locks.clone(),
            checkpoint_stats: self.checkpoint_stats.clone(),
            datetime_config: self.datetime_config.clone(),
            statement_timestamp_usecs: self.statement_timestamp_usecs,
            gucs: self.gucs.clone(),
            interrupts: self.interrupts.clone(),
            stats: self.stats.clone(),
            session_stats: self.session_stats.clone(),
            snapshot: self.snapshot.clone(),
            write_xid_override: self.write_xid_override,
            transaction_state: self.transaction_state.clone(),
            client_id: self.client_id,
            current_database_name: self.current_database_name.clone(),
            session_user_oid: self.session_user_oid,
            current_user_oid: self.current_user_oid,
            active_role_oid: self.active_role_oid,
            session_replication_role: self.session_replication_role,
            statement_lock_scope_id: self.statement_lock_scope_id,
            transaction_lock_scope_id: self.transaction_lock_scope_id,
            next_command_id: self.next_command_id,
            default_toast_compression: self.default_toast_compression,
            random_state: std::sync::Arc::new(parking_lot::Mutex::new(PgPrngState::default())),
            expr_bindings: self.expr_bindings.clone(),
            case_test_values: Vec::new(),
            system_bindings: Vec::new(),
            active_grouping_refs: Vec::new(),
            subplans: self.subplans.clone(),
            timed: self.timed,
            allow_side_effects: false,
            security_restricted: self.security_restricted,
            pending_async_notifications: Vec::new(),
            catalog_effects: Vec::new(),
            temp_effects: Vec::new(),
            database: None,
            pending_catalog_effects: Vec::new(),
            pending_table_locks: Vec::new(),
            pending_portals: Vec::new(),
            catalog: self.catalog.clone(),
            scalar_function_cache: HashMap::new(),
            srf_rows_cache: HashMap::new(),
            plpgsql_function_cache: self.plpgsql_function_cache.clone(),
            pinned_cte_tables: HashMap::new(),
            cte_tables: HashMap::new(),
            cte_producers: HashMap::new(),
            recursive_worktables: HashMap::new(),
            deferred_foreign_keys: None,
            trigger_depth: 0,
        }
    }
}

pub(crate) fn current_worker() -> Option<(usize, usize)> {
    PARALLEL_WORKER.with(Cell::get)
}

pub(crate) fn current_runtime() -> Option<Arc<ParallelRuntime>> {
    PARALLEL_RUNTIME.with(|slot| slot.borrow().clone())
}

pub(crate) fn with_worker_identity<T>(
    worker_index: usize,
    participant_count: usize,
    f: impl FnOnce() -> T,
) -> T {
    PARALLEL_WORKER.with(|slot| {
        let previous = slot.replace(Some((worker_index, participant_count)));
        let result = f();
        slot.set(previous);
        result
    })
}

pub(crate) fn with_parallel_runtime<T>(runtime: Arc<ParallelRuntime>, f: impl FnOnce() -> T) -> T {
    PARALLEL_RUNTIME.with(|slot| {
        let previous = slot.replace(Some(runtime));
        let result = f();
        slot.replace(previous);
        result
    })
}

pub(crate) fn launch_worker(
    seed: WorkerContextSeed,
    plan: Plan,
    worker_index: usize,
    participant_count: usize,
    sender: SyncSender<WorkerMessage>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let runtime = seed.parallel_runtime.clone();
        let result = with_parallel_runtime(runtime, || {
            with_worker_identity(worker_index, participant_count, || {
                let mut ctx = seed.build_context();
                let mut state = executor_start(plan);
                loop {
                    ctx.check_for_interrupts()?;
                    if state.exec_proc_node(&mut ctx)?.is_none() {
                        break;
                    }
                    let row = state.materialize_current_row()?;
                    let tid = row.slot.tid();
                    let table_oid = row.slot.table_oid;
                    let values = row.slot.tts_values;
                    let row = WorkerTuple {
                        values,
                        system_bindings: row.system_bindings,
                        grouping_refs: row.grouping_refs,
                        tid,
                        table_oid,
                    };
                    if sender.send(WorkerMessage::Row(row)).is_err() {
                        break;
                    }
                }
                Ok::<(), ExecError>(())
            })
        });
        if let Err(err) = result {
            let err = ExecError::WithContext {
                source: Box::new(err),
                context: "parallel worker".into(),
            };
            let _ = sender.send(WorkerMessage::Error(err));
        }
    })
}
