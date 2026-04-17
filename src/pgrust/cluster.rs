use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;

use crate::backend::access::transam::xact::TransactionManager;
use crate::backend::access::transam::xlog::{WalBgWriter, WalWriter};
use crate::backend::catalog::{CatalogError, CatalogStore};
use crate::backend::executor::ExecError;
use crate::backend::storage::buffer::storage_backend::SmgrStorageBackend;
use crate::backend::storage::lmgr::{TableLockManager, TransactionWaiter};
use crate::backend::storage::smgr::{ForkNumber, MdStorageManager, StorageManager};
use crate::backend::utils::cache::plancache::PlanCache;
use crate::backend::utils::cache::syscache::BackendCacheState;
use crate::backend::utils::misc::checkpoint::{CheckpointConfig, CheckpointStatsSnapshot};
use crate::backend::utils::misc::interrupts::InterruptState;
use crate::pgrust::auth::AuthState;
use crate::pgrust::database::{
    Database, DatabaseCreateGrant, DatabaseError, DomainEntry, SequenceRuntime, TempNamespace,
};
use crate::{BufferPool, ClientId};

#[derive(Clone)]
pub struct Cluster {
    shared: Arc<ClusterShared>,
}

pub(crate) struct ClusterShared {
    pub base_dir: PathBuf,
    pub pool: Arc<BufferPool<SmgrStorageBackend>>,
    pub wal: Option<Arc<WalWriter>>,
    pub txns: Arc<RwLock<TransactionManager>>,
    pub shared_catalog: Arc<RwLock<CatalogStore>>,
    pub txn_waiter: Arc<TransactionWaiter>,
    pub table_locks: Arc<TableLockManager>,
    pub plan_cache: Arc<PlanCache>,
    pub open_databases: Arc<RwLock<HashMap<u32, Arc<OpenDatabaseState>>>>,
    pub active_connections: Arc<RwLock<HashMap<u32, usize>>>,
    pub wal_bg_writer: Option<Arc<WalBgWriter>>,
}

pub(crate) struct OpenDatabaseState {
    pub catalog: Arc<RwLock<CatalogStore>>,
    pub backend_cache_states: Arc<RwLock<HashMap<ClientId, BackendCacheState>>>,
    pub session_interrupt_states: Arc<RwLock<HashMap<ClientId, Arc<InterruptState>>>>,
    pub session_auth_states: Arc<RwLock<HashMap<ClientId, AuthState>>>,
    pub database_create_grants: Arc<RwLock<Vec<DatabaseCreateGrant>>>,
    pub temp_relations: Arc<RwLock<HashMap<ClientId, TempNamespace>>>,
    pub domains: Arc<RwLock<BTreeMap<String, DomainEntry>>>,
    pub sequences: Arc<SequenceRuntime>,
}

impl OpenDatabaseState {
    fn new(base_dir: &Path, catalog: CatalogStore) -> Result<Self, DatabaseError> {
        let sequences = Arc::new(
            SequenceRuntime::load(Some(base_dir), &catalog).map_err(DatabaseError::Catalog)?,
        );
        Ok(Self {
            catalog: Arc::new(RwLock::new(catalog)),
            backend_cache_states: Arc::new(RwLock::new(HashMap::new())),
            session_interrupt_states: Arc::new(RwLock::new(HashMap::new())),
            session_auth_states: Arc::new(RwLock::new(HashMap::new())),
            database_create_grants: Arc::new(RwLock::new(Vec::new())),
            temp_relations: Arc::new(RwLock::new(HashMap::new())),
            domains: Arc::new(RwLock::new(BTreeMap::new())),
            sequences,
        })
    }
}

impl Cluster {
    pub fn open(base_dir: impl Into<PathBuf>, pool_size: usize) -> Result<Self, DatabaseError> {
        Self::open_with_options(base_dir.into(), pool_size, false)
    }

    pub(crate) fn open_with_options(
        base_dir: PathBuf,
        pool_size: usize,
        wal_replay: bool,
    ) -> Result<Self, DatabaseError> {
        std::fs::create_dir_all(&base_dir)
            .map_err(|e| DatabaseError::Catalog(CatalogError::Io(e.to_string())))?;

        let shared_catalog = CatalogStore::load_shared(&base_dir)?;
        ensure_bootstrap_databases(&base_dir, &shared_catalog)?;

        let mut txns = TransactionManager::new_durable(&base_dir)?;
        let wal_dir = base_dir.join("pg_wal");
        if wal_replay && wal_dir.join("wal.log").exists() {
            let mut recovery_smgr = MdStorageManager::new_in_recovery(&base_dir);
            create_relfiles_for_store_with_smg(&mut recovery_smgr, &shared_catalog)?;
            for row in shared_catalog.catcache()?.database_rows() {
                let local_store = CatalogStore::load_database(&base_dir, row.oid)?;
                create_relfiles_for_store_with_smg(&mut recovery_smgr, &local_store)?;
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

        let wal = Arc::new(WalWriter::new(&wal_dir).map_err(DatabaseError::Wal)?);
        let pool = Arc::new(BufferPool::new_with_wal(
            SmgrStorageBackend::new(MdStorageManager::new(&base_dir)),
            pool_size,
            Arc::clone(&wal),
        ));

        open_relfiles_for_store(&pool, &shared_catalog)?;
        let mut open_databases = HashMap::new();
        for row in shared_catalog.catcache()?.database_rows() {
            let local_store = CatalogStore::load_database(&base_dir, row.oid)?;
            open_relfiles_for_store(&pool, &local_store)?;
            open_databases.insert(
                row.oid,
                Arc::new(OpenDatabaseState::new(&base_dir, local_store)?),
            );
        }

        let wal_bg_writer = WalBgWriter::start(Arc::clone(&wal), Duration::from_millis(200));
        Ok(Self {
            shared: Arc::new(ClusterShared {
                base_dir,
                pool,
                wal: Some(wal),
                txns: Arc::new(RwLock::new(txns)),
                shared_catalog: Arc::new(RwLock::new(shared_catalog)),
                txn_waiter: Arc::new(TransactionWaiter::new()),
                table_locks: Arc::new(TableLockManager::new()),
                plan_cache: Arc::new(PlanCache::new()),
                open_databases: Arc::new(RwLock::new(open_databases)),
                active_connections: Arc::new(RwLock::new(HashMap::new())),
                wal_bg_writer: Some(Arc::new(wal_bg_writer)),
            }),
        })
    }

    pub fn open_ephemeral(pool_size: usize) -> Result<Self, DatabaseError> {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        let base_dir = std::env::temp_dir().join(format!("pgrust_cluster_ephemeral_{nanos}"));
        Self::open(base_dir, pool_size)
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
            pool: Arc::clone(&self.shared.pool),
            wal: self.shared.wal.clone(),
            checkpoint_config: Arc::new(CheckpointConfig::default()),
            checkpoint_stats: Arc::new(RwLock::new(CheckpointStatsSnapshot::default())),
            txns: Arc::clone(&self.shared.txns),
            shared_catalog: Arc::clone(&self.shared.shared_catalog),
            catalog: Arc::clone(&state.catalog),
            txn_waiter: Arc::clone(&self.shared.txn_waiter),
            table_locks: Arc::clone(&self.shared.table_locks),
            plan_cache: Arc::clone(&self.shared.plan_cache),
            backend_cache_states: Arc::clone(&state.backend_cache_states),
            session_interrupt_states: Arc::clone(&state.session_interrupt_states),
            session_auth_states: Arc::clone(&state.session_auth_states),
            database_create_grants: Arc::clone(&state.database_create_grants),
            temp_relations: Arc::clone(&state.temp_relations),
            domains: Arc::clone(&state.domains),
            sequences: Arc::clone(&state.sequences),
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

    pub(crate) fn shared(&self) -> &Arc<ClusterShared> {
        &self.shared
    }

    pub(crate) fn open_database_state(
        &self,
        db_oid: u32,
    ) -> Result<Arc<OpenDatabaseState>, ExecError> {
        if let Some(state) = self.shared.open_databases.read().get(&db_oid) {
            return Ok(Arc::clone(state));
        }
        let local_store = CatalogStore::load_database(&self.shared.base_dir, db_oid)?;
        open_relfiles_for_store(&self.shared.pool, &local_store)?;
        let state = Arc::new(
            OpenDatabaseState::new(&self.shared.base_dir, local_store).map_err(|err| {
                ExecError::DetailedError {
                    message: format!("failed to open database state: {err:?}"),
                    detail: None,
                    hint: None,
                    sqlstate: "58000",
                }
            })?,
        );
        self.shared
            .open_databases
            .write()
            .insert(db_oid, Arc::clone(&state));
        Ok(state)
    }
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

fn open_relfiles_for_store(
    pool: &Arc<BufferPool<SmgrStorageBackend>>,
    store: &CatalogStore,
) -> Result<(), CatalogError> {
    let relcache = store.relcache()?;
    for (_, entry) in relcache.entries() {
        let rel = entry.rel;
        pool.with_storage_mut(|s| {
            let _ = s.smgr.open(rel);
            let _ = s.smgr.create(rel, ForkNumber::Main, true);
        });
    }
    Ok(())
}

fn create_relfiles_for_store_with_smg(
    smgr: &mut MdStorageManager,
    store: &CatalogStore,
) -> Result<(), CatalogError> {
    let relcache = store.relcache()?;
    for (_, entry) in relcache.entries() {
        let rel = entry.rel;
        let _ = smgr.open(rel);
        let _ = smgr.create(rel, ForkNumber::Main, false);
    }
    Ok(())
}
