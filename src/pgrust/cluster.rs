#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::RwLock;

use crate::backend::access::transam::checkpoint::{CheckpointCommitBarrier, Checkpointer};
use crate::backend::access::transam::xact::TransactionManager;
use crate::backend::access::transam::xlog::{WalBgWriter, WalWriter, has_wal_segments};
use crate::backend::access::transam::{ControlFileState, ControlFileStore};
use crate::backend::catalog::object_address::ObjectAddressState;
use crate::backend::catalog::{CatalogError, CatalogStore};
use crate::backend::executor::{ExecError, SessionReplicationRole};
use crate::backend::storage::buffer::storage_backend::SmgrStorageBackend;
use crate::backend::storage::lmgr::{
    AdvisoryLockManager, PredicateLockManager, RowLockManager, TableLockManager, TransactionWaiter,
};
use crate::backend::storage::smgr::{ForkNumber, MdStorageManager, StorageManager};
use crate::backend::storage::sync::SyncQueue;
use crate::backend::utils::cache::plancache::PlanCache;
use crate::backend::utils::cache::relcache::RelCacheEntry;
use crate::backend::utils::cache::syscache::BackendCacheState;
use crate::backend::utils::misc::checkpoint::{CheckpointConfig, CheckpointStatsSnapshot};
use crate::backend::utils::misc::interrupts::InterruptState;
use crate::include::catalog::relkind_has_storage;
use crate::pgrust::auth::AuthState;
use crate::pgrust::autovacuum::{AutovacuumConfig, AutovacuumRuntime};
use crate::pgrust::database::{
    AsyncNotifyRuntime, BaseTypeEntry, ConversionEntry, Database, DatabaseCreateGrant,
    DatabaseError, DatabaseOpenOptions, DatabaseStatsStore, DomainEntry, EnumTypeEntry,
    LargeObjectRuntime, PreparedTransactionManager, RangeTypeEntry, SequenceRuntime,
    SessionStatsState, SessionViewState, StatisticsObjectEntry, TempBackendId, TempNamespace,
    load_range_type_entries,
};
use crate::pl::plpgsql::PlpgsqlFunctionCache;
use crate::{BufferPool, ClientId};

#[derive(Clone)]
pub struct Cluster {
    shared: Arc<ClusterShared>,
}

pub(crate) struct ClusterShared {
    pub base_dir: PathBuf,
    pub durable_shutdown: bool,
    pub autovacuum_config: AutovacuumConfig,
    pub autovacuum_runtime: Arc<AutovacuumRuntime>,
    pub pool: Arc<BufferPool<SmgrStorageBackend>>,
    pub wal: Option<Arc<WalWriter>>,
    pub txns: Arc<RwLock<TransactionManager>>,
    pub shared_catalog: Arc<RwLock<CatalogStore>>,
    pub txn_waiter: Arc<TransactionWaiter>,
    pub prepared_xacts: Arc<PreparedTransactionManager>,
    pub table_locks: Arc<TableLockManager>,
    pub plan_cache: Arc<PlanCache>,
    pub open_databases: Arc<RwLock<HashMap<u32, Arc<OpenDatabaseState>>>>,
    pub active_connections: Arc<RwLock<HashMap<u32, usize>>>,
    pub session_activity: Arc<RwLock<HashMap<ClientId, SessionActivityEntry>>>,
    pub next_temp_backend_id: AtomicU64,
    pub free_temp_backend_ids: Arc<RwLock<BTreeSet<TempBackendId>>>,
    pub checkpoint_config: Arc<CheckpointConfig>,
    pub checkpoint_stats: Arc<RwLock<CheckpointStatsSnapshot>>,
    pub control_file: Arc<ControlFileStore>,
    pub checkpoint_commit_barrier: Arc<CheckpointCommitBarrier>,
    pub checkpointer: Option<Arc<Checkpointer>>,
    pub wal_bg_writer: Option<Arc<WalBgWriter>>,
}

#[cfg(not(target_arch = "wasm32"))]
static NEXT_EPHEMERAL_CLUSTER_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SessionActivityState {
    Idle,
    Active,
}

impl SessionActivityState {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Active => "active",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SessionActivityEntry {
    pub client_id: ClientId,
    pub database_oid: u32,
    pub state: SessionActivityState,
    pub query: String,
    pub query_id: Option<i64>,
}

pub(crate) struct OpenDatabaseState {
    pub catalog: Arc<RwLock<CatalogStore>>,
    pub backend_cache_states: Arc<RwLock<HashMap<ClientId, BackendCacheState>>>,
    pub session_interrupt_states: Arc<RwLock<HashMap<ClientId, Arc<InterruptState>>>>,
    pub session_auth_states: Arc<RwLock<HashMap<ClientId, AuthState>>>,
    pub session_row_security_states: Arc<RwLock<HashMap<ClientId, bool>>>,
    pub session_replication_role_states: Arc<RwLock<HashMap<ClientId, SessionReplicationRole>>>,
    pub session_stats_states: Arc<RwLock<HashMap<ClientId, Arc<RwLock<SessionStatsState>>>>>,
    pub session_plpgsql_function_caches:
        Arc<RwLock<HashMap<ClientId, Arc<RwLock<PlpgsqlFunctionCache>>>>>,
    pub session_temp_backend_ids: Arc<RwLock<HashMap<ClientId, TempBackendId>>>,
    pub session_guc_states: Arc<RwLock<HashMap<ClientId, HashMap<String, String>>>>,
    pub session_view_states: Arc<RwLock<HashMap<ClientId, SessionViewState>>>,
    pub database_create_grants: Arc<RwLock<Vec<DatabaseCreateGrant>>>,
    pub temp_relations: Arc<RwLock<HashMap<TempBackendId, TempNamespace>>>,
    pub domains: Arc<RwLock<BTreeMap<String, DomainEntry>>>,
    pub enum_types: Arc<RwLock<BTreeMap<String, EnumTypeEntry>>>,
    pub range_types: Arc<RwLock<BTreeMap<String, RangeTypeEntry>>>,
    pub base_types: Arc<RwLock<BTreeMap<u32, BaseTypeEntry>>>,
    pub conversions: Arc<RwLock<BTreeMap<String, ConversionEntry>>>,
    pub statistics_objects: Arc<RwLock<BTreeMap<String, StatisticsObjectEntry>>>,
    pub object_addresses: Arc<RwLock<ObjectAddressState>>,
    pub sequences: Arc<SequenceRuntime>,
    pub advisory_locks: Arc<AdvisoryLockManager>,
    pub row_locks: Arc<RowLockManager>,
    pub predicate_locks: Arc<PredicateLockManager>,
    pub async_notify_runtime: Arc<AsyncNotifyRuntime>,
    pub next_statement_lock_scope_id: AtomicU64,
    pub stats: Arc<RwLock<DatabaseStatsStore>>,
    pub large_objects: Arc<LargeObjectRuntime>,
}

impl OpenDatabaseState {
    fn new(
        base_dir: &Path,
        database_oid: u32,
        catalog: CatalogStore,
    ) -> Result<Self, DatabaseError> {
        let sequences = Arc::new(SequenceRuntime::new_durable(base_dir));
        let range_types = load_range_type_entries(base_dir, database_oid)?;
        Ok(Self {
            catalog: Arc::new(RwLock::new(catalog)),
            backend_cache_states: Arc::new(RwLock::new(HashMap::new())),
            session_interrupt_states: Arc::new(RwLock::new(HashMap::new())),
            session_auth_states: Arc::new(RwLock::new(HashMap::new())),
            session_row_security_states: Arc::new(RwLock::new(HashMap::new())),
            session_replication_role_states: Arc::new(RwLock::new(HashMap::new())),
            session_stats_states: Arc::new(RwLock::new(HashMap::new())),
            session_plpgsql_function_caches: Arc::new(RwLock::new(HashMap::new())),
            session_temp_backend_ids: Arc::new(RwLock::new(HashMap::new())),
            session_guc_states: Arc::new(RwLock::new(HashMap::new())),
            session_view_states: Arc::new(RwLock::new(HashMap::new())),
            database_create_grants: Arc::new(RwLock::new(Vec::new())),
            temp_relations: Arc::new(RwLock::new(HashMap::new())),
            domains: Arc::new(RwLock::new(BTreeMap::new())),
            enum_types: Arc::new(RwLock::new(BTreeMap::new())),
            range_types: Arc::new(RwLock::new(range_types)),
            base_types: Arc::new(RwLock::new(BTreeMap::new())),
            conversions: Arc::new(RwLock::new(BTreeMap::new())),
            statistics_objects: Arc::new(RwLock::new(BTreeMap::new())),
            object_addresses: Arc::new(RwLock::new(ObjectAddressState::default())),
            sequences,
            advisory_locks: Arc::new(AdvisoryLockManager::new()),
            row_locks: Arc::new(RowLockManager::new()),
            predicate_locks: Arc::new(PredicateLockManager::new()),
            async_notify_runtime: Arc::new(AsyncNotifyRuntime::new()),
            next_statement_lock_scope_id: AtomicU64::new(1),
            stats: Arc::new(RwLock::new(DatabaseStatsStore::with_default_io_rows())),
            large_objects: Arc::new(LargeObjectRuntime::new_ephemeral()),
        })
    }
}

impl Drop for ClusterShared {
    fn drop(&mut self) {
        self.autovacuum_runtime.shutdown_and_join();
        if let Some(checkpointer) = self.checkpointer.as_ref() {
            if self.durable_shutdown {
                let _ = checkpointer.shutdown_and_join();
            } else {
                checkpointer.stop_and_join();
            }
        } else if self.durable_shutdown {
            let _ = self.txns.write().flush_clog();
        }
    }
}

impl Cluster {
    #[cfg(test)]
    pub fn open(base_dir: impl Into<PathBuf>, pool_size: usize) -> Result<Self, DatabaseError> {
        Self::open_with_options(base_dir.into(), DatabaseOpenOptions::for_tests(pool_size))
    }

    #[cfg(not(test))]
    pub fn open(base_dir: impl Into<PathBuf>, pool_size: usize) -> Result<Self, DatabaseError> {
        Self::open_with_options(base_dir.into(), DatabaseOpenOptions::new(pool_size))
    }

    pub(crate) fn open_with_options(
        base_dir: PathBuf,
        options: DatabaseOpenOptions,
    ) -> Result<Self, DatabaseError> {
        std::fs::create_dir_all(&base_dir)
            .map_err(|e| DatabaseError::Catalog(CatalogError::Io(e.to_string())))?;
        let base_dir_has_cluster_contents = base_dir_has_cluster_contents(&base_dir)?;
        let bootstrap_control_file =
            !ControlFileStore::path(&base_dir).exists() && !base_dir_has_cluster_contents;

        let checkpoint_config = Arc::new(
            CheckpointConfig::load_from_data_dir(&base_dir)
                .map_err(|message| DatabaseError::Catalog(CatalogError::Io(message)))?,
        );
        let mut txns = TransactionManager::new_durable(&base_dir)?;
        let control_file = if bootstrap_control_file {
            Arc::new(ControlFileStore::bootstrap(
                &base_dir,
                txns.next_xid(),
                checkpoint_config.as_ref(),
            )?)
        } else {
            Arc::new(ControlFileStore::load(&base_dir)?)
        };
        let control_snapshot = control_file.snapshot();

        let shared_catalog = CatalogStore::load_shared(&base_dir)?;
        if bootstrap_control_file {
            ensure_bootstrap_databases(&base_dir, &shared_catalog)?;
        }

        let wal_dir = base_dir.join("pg_wal");
        let prepared_xacts_for_recovery = PreparedTransactionManager::load(&base_dir)?;
        let needs_recovery = control_snapshot.state != ControlFileState::ShutDown;
        if needs_recovery && has_wal_segments(&wal_dir).map_err(DatabaseError::Wal)? {
            control_file.update(|control| {
                control.state = ControlFileState::InCrashRecovery;
                control.next_xid = txns.next_xid();
                control.full_page_writes = checkpoint_config.full_page_writes;
            })?;
            let mut recovery_smgr = MdStorageManager::new_in_recovery(&base_dir);
            create_relfiles_for_store_with_smg(&mut recovery_smgr, &shared_catalog)?;
            for row in shared_catalog.catcache()?.database_rows() {
                let local_store = CatalogStore::load_database(&base_dir, row.oid)?;
                create_relfiles_for_store_with_smg(&mut recovery_smgr, &local_store)?;
            }
            let prepared_xids = prepared_xacts_for_recovery.prepared_xids();
            let stats = crate::backend::access::transam::xlog::replay::perform_wal_recovery_from_preserving_xids(
                &wal_dir,
                &mut recovery_smgr,
                &mut txns,
                control_snapshot.redo_lsn,
                &prepared_xids,
            )
            .map_err(DatabaseError::Wal)?;
            if stats.records_replayed > 0 {
                eprintln!(
                    "WAL recovery: {} records ({} FPIs, {} inserts, {} commits, {} aborted)",
                    stats.records_replayed, stats.fpis, stats.inserts, stats.commits, stats.aborted
                );
            }
        }

        let prepared_xacts = Arc::new(PreparedTransactionManager::load(&base_dir)?);
        let wal = Arc::new(
            WalWriter::new_with_fsync(&wal_dir, checkpoint_config.fsync)
                .map_err(DatabaseError::Wal)?,
        );
        let sync_queue = Arc::new(SyncQueue::default());
        let pool = Arc::new(BufferPool::new_with_wal(
            SmgrStorageBackend::new(MdStorageManager::new_with_sync_queue(
                &base_dir,
                Arc::clone(&sync_queue),
            )),
            options.pool_size,
            Arc::clone(&wal),
        ));

        let open_databases = HashMap::new();

        let wal_bg_writer = WalBgWriter::start(Arc::clone(&wal), Duration::from_millis(200));
        let txns = Arc::new(RwLock::new(txns));
        let checkpoint_stats = Arc::new(RwLock::new(CheckpointStatsSnapshot::default()));
        let checkpoint_commit_barrier = Arc::new(CheckpointCommitBarrier::new());
        let checkpointer = Some(Checkpointer::start(
            Arc::clone(&pool),
            Some(Arc::clone(&wal)),
            Arc::clone(&txns),
            Some(Arc::clone(&control_file)),
            Arc::clone(&checkpoint_config),
            Arc::clone(&checkpoint_stats),
            Arc::clone(&sync_queue),
            Arc::clone(&checkpoint_commit_barrier),
        ));

        if needs_recovery {
            if let Some(checkpointer) = checkpointer.as_ref() {
                checkpointer
                    .request(
                        crate::backend::access::transam::CheckpointRequestFlags::end_of_recovery(),
                    )
                    .map_err(|message| {
                        DatabaseError::Control(
                            crate::backend::access::transam::ControlFileError::Io(message),
                        )
                    })?;
            }
        } else {
            control_file.update(|control| {
                control.state = ControlFileState::InProduction;
                control.next_xid = txns.read().next_xid();
                control.full_page_writes = checkpoint_config.full_page_writes;
            })?;
        }
        let autovacuum_runtime = Arc::new(AutovacuumRuntime::new());
        let shared = Arc::new(ClusterShared {
            base_dir,
            durable_shutdown: options.durable_shutdown,
            autovacuum_config: options.autovacuum,
            autovacuum_runtime: Arc::clone(&autovacuum_runtime),
            pool,
            wal: Some(wal),
            txns,
            shared_catalog: Arc::new(RwLock::new(shared_catalog)),
            txn_waiter: Arc::new(TransactionWaiter::new()),
            prepared_xacts,
            table_locks: Arc::new(TableLockManager::new()),
            plan_cache: Arc::new(PlanCache::new()),
            open_databases: Arc::new(RwLock::new(open_databases)),
            active_connections: Arc::new(RwLock::new(HashMap::new())),
            session_activity: Arc::new(RwLock::new(HashMap::new())),
            next_temp_backend_id: AtomicU64::new(1),
            free_temp_backend_ids: Arc::new(RwLock::new(BTreeSet::new())),
            checkpoint_config,
            checkpoint_stats,
            control_file,
            checkpoint_commit_barrier,
            checkpointer,
            wal_bg_writer: Some(Arc::new(wal_bg_writer)),
        });
        for record in shared.prepared_xacts.records() {
            shared
                .txn_waiter
                .register_holder(record.xid, record.prepared_client_id);
            for xid in &record.subxids {
                shared
                    .txn_waiter
                    .register_holder(*xid, record.prepared_client_id);
            }
            shared
                .table_locks
                .restore_locks_for_client(record.prepared_client_id, &record.held_table_locks);
            let state = open_database_state_for_shared(&shared, record.db_oid)?;
            state.row_locks.restore_transaction_locks(
                record.prepared_client_id,
                record.advisory_scope_id,
                &record.row_locks,
            );
            state.advisory_locks.restore_transaction_locks(
                record.prepared_client_id,
                record.advisory_scope_id,
                &record.advisory_locks,
            );
            if let Some(predicate_state) = &record.predicate_state {
                state.predicate_locks.restore_prepared(
                    record.prepared_client_id,
                    record.xid,
                    &record.subxids,
                    predicate_state.clone(),
                );
            }
        }
        autovacuum_runtime.start(Arc::downgrade(&shared), options.autovacuum);
        Ok(Self { shared })
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_ephemeral(pool_size: usize) -> Result<Self, DatabaseError> {
        use crate::backend::utils::time::system_time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        let base_dir = std::env::temp_dir().join(format!(
            "pgrust_cluster_ephemeral_{}_{}_{}",
            std::process::id(),
            nanos,
            NEXT_EPHEMERAL_CLUSTER_ID.fetch_add(1, Ordering::Relaxed),
        ));
        Self::open(base_dir, pool_size)
    }

    /// Wasm-only ephemeral constructor: builds a fully in-memory Cluster
    /// without touching the filesystem or spawning background threads.
    #[cfg(target_arch = "wasm32")]
    pub fn open_ephemeral(pool_size: usize) -> Result<Self, DatabaseError> {
        use crate::include::catalog::{CatalogScope, POSTGRES_DATABASE_OID};
        use crate::pgrust::database::bootstrap_ephemeral_catalog;

        let checkpoint_config = Arc::new(CheckpointConfig::default());
        let txns = TransactionManager::new_ephemeral();
        let next_xid = txns.next_xid();
        let control_file = Arc::new(ControlFileStore::new_in_memory(
            next_xid,
            checkpoint_config.as_ref(),
        ));
        let txns = Arc::new(RwLock::new(txns));

        let pool = Arc::new(BufferPool::new(SmgrStorageBackend::new_mem(), pool_size));

        // Seed the in-memory relfiles, rows, and catalog indexes.
        bootstrap_ephemeral_catalog(&pool, &txns)?;

        let shared_catalog = CatalogStore::new_ephemeral_scope(CatalogScope::Shared);
        let default_db =
            CatalogStore::new_ephemeral_scope(CatalogScope::Database(POSTGRES_DATABASE_OID));

        let mut open_databases = HashMap::new();
        open_databases.insert(
            POSTGRES_DATABASE_OID,
            Arc::new(OpenDatabaseState {
                catalog: Arc::new(RwLock::new(default_db)),
                backend_cache_states: Arc::new(RwLock::new(HashMap::new())),
                session_interrupt_states: Arc::new(RwLock::new(HashMap::new())),
                session_auth_states: Arc::new(RwLock::new(HashMap::new())),
                session_row_security_states: Arc::new(RwLock::new(HashMap::new())),
                session_replication_role_states: Arc::new(RwLock::new(HashMap::new())),
                session_stats_states: Arc::new(RwLock::new(HashMap::new())),
                session_plpgsql_function_caches: Arc::new(RwLock::new(HashMap::new())),
                session_temp_backend_ids: Arc::new(RwLock::new(HashMap::new())),
                session_guc_states: Arc::new(RwLock::new(HashMap::new())),
                session_view_states: Arc::new(RwLock::new(HashMap::new())),
                database_create_grants: Arc::new(RwLock::new(Vec::new())),
                temp_relations: Arc::new(RwLock::new(HashMap::new())),
                domains: Arc::new(RwLock::new(BTreeMap::new())),
                enum_types: Arc::new(RwLock::new(BTreeMap::new())),
                range_types: Arc::new(RwLock::new(BTreeMap::new())),
                base_types: Arc::new(RwLock::new(BTreeMap::new())),
                conversions: Arc::new(RwLock::new(BTreeMap::new())),
                statistics_objects: Arc::new(RwLock::new(BTreeMap::new())),
                object_addresses: Arc::new(RwLock::new(ObjectAddressState::default())),
                sequences: Arc::new(SequenceRuntime::new_ephemeral()),
                advisory_locks: Arc::new(AdvisoryLockManager::new()),
                row_locks: Arc::new(RowLockManager::new()),
                predicate_locks: Arc::new(PredicateLockManager::new()),
                async_notify_runtime: Arc::new(AsyncNotifyRuntime::new()),
                next_statement_lock_scope_id: AtomicU64::new(1),
                stats: Arc::new(RwLock::new(DatabaseStatsStore::with_default_io_rows())),
                large_objects: Arc::new(LargeObjectRuntime::new_ephemeral()),
            }),
        );

        let checkpoint_stats = Arc::new(RwLock::new(CheckpointStatsSnapshot::default()));
        let checkpoint_commit_barrier = Arc::new(CheckpointCommitBarrier::new());

        let autovacuum_runtime = Arc::new(AutovacuumRuntime::new());
        Ok(Self {
            shared: Arc::new(ClusterShared {
                base_dir: PathBuf::new(),
                durable_shutdown: false,
                autovacuum_config: AutovacuumConfig::test_default(),
                autovacuum_runtime,
                pool,
                wal: None,
                txns,
                shared_catalog: Arc::new(RwLock::new(shared_catalog)),
                txn_waiter: Arc::new(TransactionWaiter::new()),
                prepared_xacts: Arc::new(PreparedTransactionManager::new_ephemeral()),
                table_locks: Arc::new(TableLockManager::new()),
                plan_cache: Arc::new(PlanCache::new()),
                open_databases: Arc::new(RwLock::new(open_databases)),
                active_connections: Arc::new(RwLock::new(HashMap::new())),
                session_activity: Arc::new(RwLock::new(HashMap::new())),
                next_temp_backend_id: AtomicU64::new(1),
                free_temp_backend_ids: Arc::new(RwLock::new(BTreeSet::new())),
                checkpoint_config,
                checkpoint_stats,
                control_file,
                checkpoint_commit_barrier,
                checkpointer: None,
                wal_bg_writer: None,
            }),
        })
    }

    pub fn connect_database(&self, name: &str) -> Result<Database, ExecError> {
        let normalized = name.to_ascii_lowercase();
        let row = self
            .shared
            .shared_catalog
            .read()
            .catcache()?
            .database_rows()
            .into_iter()
            .find(|row| row.datname.eq_ignore_ascii_case(&normalized))
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("database \"{name}\" does not exist"),
                detail: None,
                hint: None,
                sqlstate: "3D000",
            })?;
        if !row.datallowconn {
            return Err(ExecError::DetailedError {
                message: format!(
                    "database \"{}\" is not currently accepting connections",
                    row.datname
                ),
                detail: None,
                hint: None,
                sqlstate: "55000",
            });
        }
        let state = self.open_database_state(row.oid)?;
        Ok(Database {
            cluster: Arc::clone(&self.shared),
            database_oid: row.oid,
            database_name: row.datname,
            pool: Arc::clone(&self.shared.pool),
            wal: self.shared.wal.clone(),
            autovacuum_config: self.shared.autovacuum_config,
            checkpoint_config: Arc::clone(&self.shared.checkpoint_config),
            checkpoint_stats: Arc::clone(&self.shared.checkpoint_stats),
            checkpoint_commit_barrier: Arc::clone(&self.shared.checkpoint_commit_barrier),
            checkpointer: self.shared.checkpointer.clone(),
            txns: Arc::clone(&self.shared.txns),
            shared_catalog: Arc::clone(&self.shared.shared_catalog),
            catalog: Arc::clone(&state.catalog),
            txn_waiter: Arc::clone(&self.shared.txn_waiter),
            prepared_xacts: Arc::clone(&self.shared.prepared_xacts),
            table_locks: Arc::clone(&self.shared.table_locks),
            plan_cache: Arc::clone(&self.shared.plan_cache),
            backend_cache_states: Arc::clone(&state.backend_cache_states),
            session_interrupt_states: Arc::clone(&state.session_interrupt_states),
            session_auth_states: Arc::clone(&state.session_auth_states),
            session_row_security_states: Arc::clone(&state.session_row_security_states),
            session_replication_role_states: Arc::clone(&state.session_replication_role_states),
            session_stats_states: Arc::clone(&state.session_stats_states),
            session_plpgsql_function_caches: Arc::clone(&state.session_plpgsql_function_caches),
            session_temp_backend_ids: Arc::clone(&state.session_temp_backend_ids),
            session_guc_states: Arc::clone(&state.session_guc_states),
            session_view_states: Arc::clone(&state.session_view_states),
            database_create_grants: Arc::clone(&state.database_create_grants),
            temp_relations: Arc::clone(&state.temp_relations),
            domains: Arc::clone(&state.domains),
            enum_types: Arc::clone(&state.enum_types),
            range_types: Arc::clone(&state.range_types),
            base_types: Arc::clone(&state.base_types),
            conversions: Arc::clone(&state.conversions),
            statistics_objects: Arc::clone(&state.statistics_objects),
            object_addresses: Arc::clone(&state.object_addresses),
            sequences: Arc::clone(&state.sequences),
            advisory_locks: Arc::clone(&state.advisory_locks),
            row_locks: Arc::clone(&state.row_locks),
            predicate_locks: Arc::clone(&state.predicate_locks),
            async_notify_runtime: Arc::clone(&state.async_notify_runtime),
            stats: Arc::clone(&state.stats),
            large_objects: Arc::clone(&state.large_objects),
            _wal_bg_writer: self.shared.wal_bg_writer.clone(),
        })
    }

    pub(crate) fn register_connection(&self, db_oid: u32) {
        *self
            .shared
            .active_connections
            .write()
            .entry(db_oid)
            .or_insert(0) += 1;
    }

    pub(crate) fn unregister_connection(&self, db_oid: u32) {
        let mut counts = self.shared.active_connections.write();
        let Some(count) = counts.get_mut(&db_oid) else {
            return;
        };
        if *count <= 1 {
            counts.remove(&db_oid);
        } else {
            *count -= 1;
        }
    }

    pub(crate) fn active_connection_count(&self, db_oid: u32) -> usize {
        self.shared
            .active_connections
            .read()
            .get(&db_oid)
            .copied()
            .unwrap_or(0)
    }

    pub(crate) fn allocate_temp_backend_id(&self) -> TempBackendId {
        if let Some(id) = self.shared.free_temp_backend_ids.write().pop_first() {
            return id;
        }
        self.shared
            .next_temp_backend_id
            .fetch_add(1, Ordering::Relaxed) as TempBackendId
    }

    pub(crate) fn release_temp_backend_id(&self, temp_backend_id: TempBackendId) {
        self.shared
            .free_temp_backend_ids
            .write()
            .insert(temp_backend_id);
    }

    pub(crate) fn shared(&self) -> &Arc<ClusterShared> {
        &self.shared
    }

    pub(crate) fn from_shared(shared: Arc<ClusterShared>) -> Self {
        Self { shared }
    }

    pub(crate) fn open_database_state(
        &self,
        db_oid: u32,
    ) -> Result<Arc<OpenDatabaseState>, ExecError> {
        open_database_state_for_shared(&self.shared, db_oid).map_err(|err| match err {
            DatabaseError::Catalog(err) => err.into(),
            other => ExecError::DetailedError {
                message: format!("failed to open database state: {other:?}"),
                detail: None,
                hint: None,
                sqlstate: "58000",
            },
        })
    }
}

fn base_dir_has_cluster_contents(base_dir: &Path) -> Result<bool, DatabaseError> {
    let entries = std::fs::read_dir(base_dir)
        .map_err(|e| DatabaseError::Catalog(CatalogError::Io(e.to_string())))?;
    for entry in entries {
        let entry = entry.map_err(|e| DatabaseError::Catalog(CatalogError::Io(e.to_string())))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Ok(true);
        };
        if matches!(name, "postgresql.conf" | "postgresql.auto.conf") {
            continue;
        }
        if matches!(
            name,
            "global" | "base" | "pg_wal" | "pg_tblspc" | "pg_xact" | "pg_multixact"
        ) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn ensure_bootstrap_databases(
    base_dir: &Path,
    shared_catalog: &CatalogStore,
) -> Result<(), DatabaseError> {
    for row in shared_catalog.catcache()?.database_rows() {
        let _ = CatalogStore::load_database(base_dir, row.oid)?;
    }
    Ok(())
}

fn open_database_state_for_shared(
    shared: &Arc<ClusterShared>,
    db_oid: u32,
) -> Result<Arc<OpenDatabaseState>, DatabaseError> {
    if let Some(state) = shared.open_databases.read().get(&db_oid) {
        return Ok(Arc::clone(state));
    }

    let local_store = CatalogStore::load_database(&shared.base_dir, db_oid)?;
    let state = Arc::new(OpenDatabaseState::new(
        &shared.base_dir,
        db_oid,
        local_store,
    )?);

    let mut open_databases = shared.open_databases.write();
    if let Some(existing) = open_databases.get(&db_oid) {
        return Ok(Arc::clone(existing));
    }
    open_databases.insert(db_oid, Arc::clone(&state));
    Ok(state)
}

fn create_relfiles_for_store_with_smg(
    smgr: &mut MdStorageManager,
    store: &CatalogStore,
) -> Result<(), CatalogError> {
    let relcache = store.relcache()?;
    for (_, entry) in relcache.entries() {
        if !entry_has_startup_storage(entry) {
            continue;
        }
        let rel = entry.rel;
        let _ = smgr.open(rel);
        let _ = smgr.create(rel, ForkNumber::Main, false);
    }
    Ok(())
}

fn entry_has_startup_storage(entry: &RelCacheEntry) -> bool {
    // Match PostgreSQL's split: temp relation storage is backend-local and is
    // cleaned from the temp namespace on reuse, not opened or WAL-recreated by
    // cluster startup.
    relkind_has_storage(entry.relkind) && entry.relpersistence != 't'
}
