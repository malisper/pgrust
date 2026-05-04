use std::sync::Arc;

use pgrust_access::access::amapi as access_amapi;

use crate::backend::access::transam::xact::Snapshot;
use crate::backend::access::transam::xact::{TransactionId, TransactionManager};
use crate::backend::catalog::CatalogError;
use crate::backend::executor::ExecutorCatalog;
use crate::backend::storage::buffer::storage_backend::SmgrStorageBackend;
use crate::backend::storage::lmgr::AdvisoryLockManager;
use crate::backend::storage::smgr::RelFileLocator;
use crate::backend::utils::activity::{DatabaseStatsStore, SessionStatsState};
use crate::backend::utils::cache::relcache::IndexRelCacheEntry;
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::backend::utils::misc::interrupts::InterruptState;
use crate::include::access::htup::AttributeCompression;
use crate::include::access::itemptr::ItemPointerData;
use crate::include::access::relscan::{IndexScanDesc, ScanDirection};
use crate::include::access::scankey::ScanKeyData;
use crate::include::access::tidbitmap::TidBitmap;
use crate::include::nodes::primnodes::{RelationDesc, ToastRelationRef};
use crate::pgrust::database::{LargeObjectRuntime, SequenceRuntime, TransactionWaiter};
use crate::{BufferPool, ClientId};
use pgrust_nodes::SessionReplicationRole;

// :HACK: root compatibility re-export while AM callbacks still use root-owned
// runtime context structs below.
pub use pgrust_access::access::amapi::{IndexBuildResult, IndexBulkDeleteResult, IndexUniqueCheck};

#[derive(Clone)]
pub struct IndexBuildExprContext {
    pub txn_waiter: Option<Arc<TransactionWaiter>>,
    pub sequences: Option<Arc<SequenceRuntime>>,
    pub large_objects: Option<Arc<LargeObjectRuntime>>,
    pub advisory_locks: Arc<AdvisoryLockManager>,
    pub datetime_config: DateTimeConfig,
    pub stats: Arc<parking_lot::RwLock<DatabaseStatsStore>>,
    pub session_stats: Arc<parking_lot::RwLock<SessionStatsState>>,
    pub current_database_name: String,
    pub session_user_oid: u32,
    pub current_user_oid: u32,
    pub current_xid: TransactionId,
    pub statement_lock_scope_id: Option<u64>,
    pub session_replication_role: SessionReplicationRole,
    pub visible_catalog: Option<ExecutorCatalog>,
}

#[derive(Clone)]
pub struct IndexBuildContext {
    pub pool: Arc<BufferPool<SmgrStorageBackend>>,
    pub txns: Arc<parking_lot::RwLock<TransactionManager>>,
    pub client_id: ClientId,
    pub interrupts: Arc<InterruptState>,
    pub snapshot: Snapshot,
    pub heap_relation: RelFileLocator,
    pub heap_desc: RelationDesc,
    pub heap_toast: Option<ToastRelationRef>,
    pub index_relation: RelFileLocator,
    pub index_name: String,
    pub index_desc: RelationDesc,
    pub index_meta: IndexRelCacheEntry,
    pub default_toast_compression: AttributeCompression,
    pub maintenance_work_mem_kb: usize,
    pub expr_eval: Option<IndexBuildExprContext>,
}

impl IndexBuildContext {
    pub(crate) fn to_access_context(&self) -> access_amapi::IndexBuildContext {
        access_amapi::IndexBuildContext {
            pool: self.pool.clone(),
            client_id: self.client_id,
            snapshot: self.snapshot.clone(),
            heap_relation: self.heap_relation,
            heap_desc: self.heap_desc.clone(),
            heap_toast: self.heap_toast.clone(),
            index_relation: self.index_relation,
            index_name: self.index_name.clone(),
            index_desc: self.index_desc.clone(),
            index_meta: self.index_meta.clone(),
            default_toast_compression: self.default_toast_compression,
            maintenance_work_mem_kb: self.maintenance_work_mem_kb,
        }
    }
}

#[derive(Clone)]
pub struct IndexInsertContext {
    pub pool: Arc<BufferPool<SmgrStorageBackend>>,
    pub txns: Arc<parking_lot::RwLock<TransactionManager>>,
    pub txn_waiter: Option<Arc<TransactionWaiter>>,
    pub client_id: ClientId,
    pub interrupts: Arc<InterruptState>,
    pub snapshot: Snapshot,
    pub heap_relation: RelFileLocator,
    pub heap_desc: RelationDesc,
    pub index_relation: RelFileLocator,
    pub index_name: String,
    pub index_desc: RelationDesc,
    pub index_meta: IndexRelCacheEntry,
    pub default_toast_compression: AttributeCompression,
    pub heap_tid: ItemPointerData,
    pub old_heap_tid: Option<ItemPointerData>,
    pub values: Vec<crate::include::nodes::datum::Value>,
    pub unique_check: IndexUniqueCheck,
}

impl IndexInsertContext {
    pub(crate) fn to_access_context(&self) -> access_amapi::IndexInsertContext {
        access_amapi::IndexInsertContext {
            pool: self.pool.clone(),
            client_id: self.client_id,
            snapshot: self.snapshot.clone(),
            heap_relation: self.heap_relation,
            heap_desc: self.heap_desc.clone(),
            index_relation: self.index_relation,
            index_name: self.index_name.clone(),
            index_desc: self.index_desc.clone(),
            index_meta: self.index_meta.clone(),
            default_toast_compression: self.default_toast_compression,
            heap_tid: self.heap_tid,
            old_heap_tid: self.old_heap_tid,
            values: self.values.clone(),
            unique_check: self.unique_check,
        }
    }
}

#[derive(Clone)]
pub struct IndexBuildEmptyContext {
    pub pool: Arc<BufferPool<SmgrStorageBackend>>,
    pub client_id: ClientId,
    pub xid: u32,
    pub index_relation: RelFileLocator,
    pub index_desc: RelationDesc,
    pub index_meta: IndexRelCacheEntry,
}

impl IndexBuildEmptyContext {
    pub(crate) fn to_access_context(&self) -> access_amapi::IndexBuildEmptyContext {
        access_amapi::IndexBuildEmptyContext {
            pool: self.pool.clone(),
            client_id: self.client_id,
            xid: self.xid,
            index_relation: self.index_relation,
            index_desc: self.index_desc.clone(),
            index_meta: self.index_meta.clone(),
        }
    }
}

#[derive(Clone)]
pub struct IndexBeginScanContext {
    pub pool: Arc<BufferPool<SmgrStorageBackend>>,
    pub client_id: ClientId,
    pub snapshot: Snapshot,
    pub heap_relation: RelFileLocator,
    pub index_relation: RelFileLocator,
    pub index_desc: RelationDesc,
    pub index_meta: IndexRelCacheEntry,
    pub key_data: Vec<ScanKeyData>,
    pub order_by_data: Vec<ScanKeyData>,
    pub direction: ScanDirection,
    pub want_itup: bool,
}

impl IndexBeginScanContext {
    pub(crate) fn to_access_context(&self) -> access_amapi::IndexBeginScanContext {
        access_amapi::IndexBeginScanContext {
            pool: self.pool.clone(),
            client_id: self.client_id,
            snapshot: self.snapshot.clone(),
            heap_relation: self.heap_relation,
            index_relation: self.index_relation,
            index_desc: self.index_desc.clone(),
            index_meta: self.index_meta.clone(),
            key_data: self.key_data.clone(),
            order_by_data: self.order_by_data.clone(),
            direction: self.direction,
            want_itup: self.want_itup,
        }
    }
}

#[derive(Clone)]
pub struct IndexVacuumContext {
    pub pool: Arc<BufferPool<SmgrStorageBackend>>,
    pub txns: Arc<parking_lot::RwLock<TransactionManager>>,
    pub client_id: ClientId,
    pub interrupts: Arc<InterruptState>,
    pub heap_relation: RelFileLocator,
    pub heap_desc: RelationDesc,
    pub heap_toast: Option<ToastRelationRef>,
    pub index_relation: RelFileLocator,
    pub index_name: String,
    pub index_desc: RelationDesc,
    pub index_meta: IndexRelCacheEntry,
    pub expr_eval: Option<IndexBuildExprContext>,
}

impl IndexVacuumContext {
    pub(crate) fn to_access_context(&self) -> access_amapi::IndexVacuumContext {
        access_amapi::IndexVacuumContext {
            pool: self.pool.clone(),
            client_id: self.client_id,
            heap_relation: self.heap_relation,
            heap_desc: self.heap_desc.clone(),
            heap_toast: self.heap_toast.clone(),
            index_relation: self.index_relation,
            index_name: self.index_name.clone(),
            index_desc: self.index_desc.clone(),
            index_meta: self.index_meta.clone(),
        }
    }
}

pub type AmBuildFn = fn(&IndexBuildContext) -> Result<IndexBuildResult, CatalogError>;
pub type AmBuildEmptyFn = fn(&IndexBuildEmptyContext) -> Result<(), CatalogError>;
pub type AmInsertFn = fn(&IndexInsertContext) -> Result<bool, CatalogError>;
pub type AmBeginScanFn = fn(&IndexBeginScanContext) -> Result<IndexScanDesc, CatalogError>;
pub type AmRescanFn =
    fn(&mut IndexScanDesc, &[ScanKeyData], ScanDirection) -> Result<(), CatalogError>;
pub type AmGetTupleFn = fn(&mut IndexScanDesc) -> Result<bool, CatalogError>;
pub type AmGetBitmapFn = fn(&mut IndexScanDesc, &mut TidBitmap) -> Result<i64, CatalogError>;
pub type AmEndScanFn = fn(IndexScanDesc) -> Result<(), CatalogError>;
pub type IndexBulkDeleteCallback<'a> = dyn Fn(ItemPointerData) -> bool + 'a;
pub type AmBulkDeleteFn = for<'a> fn(
    &IndexVacuumContext,
    &'a IndexBulkDeleteCallback<'a>,
    Option<IndexBulkDeleteResult>,
) -> Result<IndexBulkDeleteResult, CatalogError>;
pub type AmVacuumCleanupFn = fn(
    &IndexVacuumContext,
    Option<IndexBulkDeleteResult>,
) -> Result<IndexBulkDeleteResult, CatalogError>;

#[derive(Debug, Clone)]
pub struct IndexAmRoutine {
    pub amstrategies: u16,
    pub amsupport: u16,
    pub amcanorder: bool,
    pub amcanorderbyop: bool,
    pub amcanhash: bool,
    pub amconsistentordering: bool,
    pub amcanbackward: bool,
    pub amcanunique: bool,
    pub amcanmulticol: bool,
    pub amoptionalkey: bool,
    pub amsearcharray: bool,
    pub amsearchnulls: bool,
    pub amstorage: bool,
    pub amclusterable: bool,
    pub ampredlocks: bool,
    pub amsummarizing: bool,
    pub ambuild: Option<AmBuildFn>,
    pub ambuildempty: Option<AmBuildEmptyFn>,
    pub aminsert: Option<AmInsertFn>,
    pub ambeginscan: Option<AmBeginScanFn>,
    pub amrescan: Option<AmRescanFn>,
    pub amgettuple: Option<AmGetTupleFn>,
    pub amgetbitmap: Option<AmGetBitmapFn>,
    pub amendscan: Option<AmEndScanFn>,
    pub ambulkdelete: Option<AmBulkDeleteFn>,
    pub amvacuumcleanup: Option<AmVacuumCleanupFn>,
}
