use crate::backend::catalog::CatalogError;
use crate::backend::storage::smgr::RelFileLocator;
use crate::include::access::relscan::IndexScanDesc;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IndexBuildResult {
    pub heap_tuples: u64,
    pub index_tuples: u64,
}

pub type AmBuildFn = fn(RelFileLocator, RelFileLocator) -> Result<IndexBuildResult, CatalogError>;
pub type AmBuildEmptyFn = fn(RelFileLocator) -> Result<(), CatalogError>;
pub type AmInsertFn = fn(RelFileLocator) -> Result<bool, CatalogError>;
pub type AmBeginScanFn = fn(RelFileLocator, usize) -> Result<IndexScanDesc, CatalogError>;

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
}
