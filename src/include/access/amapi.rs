use std::sync::Arc;

use crate::backend::access::transam::xact::Snapshot;
use crate::backend::access::transam::xact::TransactionManager;
use crate::backend::catalog::CatalogError;
use crate::backend::executor::RelationDesc;
use crate::backend::storage::buffer::storage_backend::SmgrStorageBackend;
use crate::backend::storage::smgr::RelFileLocator;
use crate::backend::utils::cache::relcache::IndexRelCacheEntry;
use crate::include::access::itemptr::ItemPointerData;
use crate::include::access::relscan::{IndexScanDesc, ScanDirection};
use crate::include::access::scankey::ScanKeyData;
use crate::{BufferPool, ClientId};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IndexBuildResult {
    pub heap_tuples: u64,
    pub index_tuples: u64,
}

#[derive(Clone)]
pub struct IndexBuildContext {
    pub pool: Arc<BufferPool<SmgrStorageBackend>>,
    pub txns: Arc<parking_lot::RwLock<TransactionManager>>,
    pub client_id: ClientId,
    pub snapshot: Snapshot,
    pub heap_relation: RelFileLocator,
    pub heap_desc: RelationDesc,
    pub index_relation: RelFileLocator,
    pub index_desc: RelationDesc,
    pub index_meta: IndexRelCacheEntry,
    pub maintenance_work_mem_kb: usize,
}

#[derive(Clone)]
pub struct IndexInsertContext {
    pub pool: Arc<BufferPool<SmgrStorageBackend>>,
    pub client_id: ClientId,
    pub heap_relation: RelFileLocator,
    pub heap_desc: RelationDesc,
    pub index_relation: RelFileLocator,
    pub index_desc: RelationDesc,
    pub index_meta: IndexRelCacheEntry,
    pub heap_tid: ItemPointerData,
    pub values: Vec<crate::include::nodes::datum::Value>,
}

#[derive(Clone)]
pub struct IndexBuildEmptyContext {
    pub pool: Arc<BufferPool<SmgrStorageBackend>>,
    pub index_relation: RelFileLocator,
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
    pub direction: ScanDirection,
}

pub type AmBuildFn = fn(&IndexBuildContext) -> Result<IndexBuildResult, CatalogError>;
pub type AmBuildEmptyFn = fn(&IndexBuildEmptyContext) -> Result<(), CatalogError>;
pub type AmInsertFn = fn(&IndexInsertContext) -> Result<bool, CatalogError>;
pub type AmBeginScanFn = fn(&IndexBeginScanContext) -> Result<IndexScanDesc, CatalogError>;
pub type AmRescanFn =
    fn(&mut IndexScanDesc, &[ScanKeyData], ScanDirection) -> Result<(), CatalogError>;
pub type AmGetTupleFn = fn(&mut IndexScanDesc) -> Result<bool, CatalogError>;
pub type AmEndScanFn = fn(IndexScanDesc) -> Result<(), CatalogError>;

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
    pub ambuild: Option<AmBuildFn>,
    pub ambuildempty: Option<AmBuildEmptyFn>,
    pub aminsert: Option<AmInsertFn>,
    pub ambeginscan: Option<AmBeginScanFn>,
    pub amrescan: Option<AmRescanFn>,
    pub amgettuple: Option<AmGetTupleFn>,
    pub amendscan: Option<AmEndScanFn>,
}
