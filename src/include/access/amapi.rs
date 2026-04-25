use std::sync::Arc;

use crate::backend::access::transam::xact::Snapshot;
use crate::backend::access::transam::xact::{TransactionId, TransactionManager};
use crate::backend::catalog::CatalogError;
use crate::backend::executor::{RelationDesc, SessionReplicationRole};
use crate::backend::storage::buffer::storage_backend::SmgrStorageBackend;
use crate::backend::storage::lmgr::AdvisoryLockManager;
use crate::backend::storage::smgr::RelFileLocator;
use crate::backend::utils::activity::{DatabaseStatsStore, SessionStatsState};
use crate::backend::utils::cache::relcache::IndexRelCacheEntry;
use crate::backend::utils::cache::visible_catalog::VisibleCatalog;
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::backend::utils::misc::interrupts::InterruptState;
use crate::include::access::htup::AttributeCompression;
use crate::include::access::itemptr::ItemPointerData;
use crate::include::access::relscan::{IndexScanDesc, ScanDirection};
use crate::include::access::scankey::ScanKeyData;
use crate::include::access::tidbitmap::TidBitmap;
use crate::pgrust::database::{LargeObjectRuntime, SequenceRuntime, TransactionWaiter};
use crate::{BufferPool, ClientId};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IndexBuildResult {
    pub heap_tuples: u64,
    pub index_tuples: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IndexBulkDeleteResult {
    pub num_pages: u64,
    pub num_index_tuples: u64,
    pub num_removed_tuples: u64,
    pub num_deleted_pages: u64,
}

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
    pub visible_catalog: Option<VisibleCatalog>,
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
    pub index_relation: RelFileLocator,
    pub index_name: String,
    pub index_desc: RelationDesc,
    pub index_meta: IndexRelCacheEntry,
    pub default_toast_compression: AttributeCompression,
    pub maintenance_work_mem_kb: usize,
    pub expr_eval: Option<IndexBuildExprContext>,
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
    pub values: Vec<crate::include::nodes::datum::Value>,
    pub unique_check: IndexUniqueCheck,
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

#[derive(Clone)]
pub struct IndexVacuumContext {
    pub pool: Arc<BufferPool<SmgrStorageBackend>>,
    pub txns: Arc<parking_lot::RwLock<TransactionManager>>,
    pub client_id: ClientId,
    pub interrupts: Arc<InterruptState>,
    pub heap_relation: RelFileLocator,
    pub heap_desc: RelationDesc,
    pub index_relation: RelFileLocator,
    pub index_name: String,
    pub index_desc: RelationDesc,
    pub index_meta: IndexRelCacheEntry,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexUniqueCheck {
    No,
    Yes,
    Partial,
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
