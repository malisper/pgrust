use std::sync::Arc;

use pgrust_core::{ClientId, RelFileLocator, Snapshot, TransactionId};
use pgrust_nodes::datum::Value;
use pgrust_nodes::primnodes::{RelationDesc, ToastRelationRef};
use pgrust_nodes::relcache::IndexRelCacheEntry;
use pgrust_storage::{BufferPool, SmgrStorageBackend};

use crate::AccessResult;
use crate::access::htup::AttributeCompression;
use crate::access::itemptr::ItemPointerData;
use crate::access::relscan::{IndexScanDesc, ScanDirection};
use crate::access::scankey::ScanKeyData;
use crate::access::tidbitmap::TidBitmap;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexUniqueCheck {
    No,
    Yes,
    Partial,
}

#[derive(Clone)]
pub struct IndexBuildContext {
    pub pool: Arc<BufferPool<SmgrStorageBackend>>,
    pub client_id: ClientId,
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
}

#[derive(Clone)]
pub struct IndexInsertContext {
    pub pool: Arc<BufferPool<SmgrStorageBackend>>,
    pub client_id: ClientId,
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
    pub values: Vec<Value>,
    pub unique_check: IndexUniqueCheck,
}

#[derive(Clone)]
pub struct IndexBuildEmptyContext {
    pub pool: Arc<BufferPool<SmgrStorageBackend>>,
    pub client_id: ClientId,
    pub xid: TransactionId,
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
    pub client_id: ClientId,
    pub heap_relation: RelFileLocator,
    pub heap_desc: RelationDesc,
    pub heap_toast: Option<ToastRelationRef>,
    pub index_relation: RelFileLocator,
    pub index_name: String,
    pub index_desc: RelationDesc,
    pub index_meta: IndexRelCacheEntry,
}

pub type AmBuildFn = fn(&IndexBuildContext) -> AccessResult<IndexBuildResult>;
pub type AmBuildEmptyFn = fn(&IndexBuildEmptyContext) -> AccessResult<()>;
pub type AmInsertFn = fn(&IndexInsertContext) -> AccessResult<bool>;
pub type AmBeginScanFn = fn(&IndexBeginScanContext) -> AccessResult<IndexScanDesc>;
pub type AmRescanFn = fn(&mut IndexScanDesc, &[ScanKeyData], ScanDirection) -> AccessResult<()>;
pub type AmGetTupleFn = fn(&mut IndexScanDesc) -> AccessResult<bool>;
pub type AmGetBitmapFn = fn(&mut IndexScanDesc, &mut TidBitmap) -> AccessResult<i64>;
pub type AmEndScanFn = fn(IndexScanDesc) -> AccessResult<()>;
pub type IndexBulkDeleteCallback<'a> = dyn Fn(ItemPointerData) -> bool + 'a;
pub type AmBulkDeleteFn = for<'a> fn(
    &IndexVacuumContext,
    &'a IndexBulkDeleteCallback<'a>,
    Option<IndexBulkDeleteResult>,
) -> AccessResult<IndexBulkDeleteResult>;
pub type AmVacuumCleanupFn =
    fn(&IndexVacuumContext, Option<IndexBulkDeleteResult>) -> AccessResult<IndexBulkDeleteResult>;

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
