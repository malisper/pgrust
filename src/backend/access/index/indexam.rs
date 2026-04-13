use crate::backend::catalog::CatalogError;
use crate::include::access::amapi::{IndexBeginScanContext, IndexBuildContext, IndexBuildResult, IndexInsertContext};
use crate::include::access::relscan::{IndexScanDesc, ScanDirection};

pub fn index_build_stub(
    ctx: &IndexBuildContext,
    am_oid: u32,
) -> Result<IndexBuildResult, CatalogError> {
    let routine = crate::backend::access::index::amapi::index_am_handler(am_oid)
        .ok_or(CatalogError::Corrupt("unknown index access method"))?;
    if let Some(ambuild) = routine.ambuild {
        ambuild(ctx)
    } else {
        Ok(IndexBuildResult::default())
    }
}

pub fn index_insert_stub(ctx: &IndexInsertContext, am_oid: u32) -> Result<bool, CatalogError> {
    let routine = crate::backend::access::index::amapi::index_am_handler(am_oid)
        .ok_or(CatalogError::Corrupt("unknown index access method"))?;
    let aminsert = routine
        .aminsert
        .ok_or(CatalogError::Corrupt("missing index insert callback"))?;
    aminsert(ctx)
}

pub fn index_beginscan(
    ctx: &IndexBeginScanContext,
    am_oid: u32,
) -> Result<IndexScanDesc, CatalogError> {
    let routine = crate::backend::access::index::amapi::index_am_handler(am_oid)
        .ok_or(CatalogError::Corrupt("unknown index access method"))?;
    let ambeginscan = routine
        .ambeginscan
        .ok_or(CatalogError::Corrupt("missing index beginscan callback"))?;
    ambeginscan(ctx)
}

pub fn index_rescan(
    scan: &mut IndexScanDesc,
    am_oid: u32,
    keys: &[crate::include::access::scankey::ScanKeyData],
    direction: ScanDirection,
) -> Result<(), CatalogError> {
    let routine = crate::backend::access::index::amapi::index_am_handler(am_oid)
        .ok_or(CatalogError::Corrupt("unknown index access method"))?;
    let amrescan = routine
        .amrescan
        .ok_or(CatalogError::Corrupt("missing index rescan callback"))?;
    amrescan(scan, keys, direction)
}

pub fn index_getnext(scan: &mut IndexScanDesc, am_oid: u32) -> Result<bool, CatalogError> {
    let routine = crate::backend::access::index::amapi::index_am_handler(am_oid)
        .ok_or(CatalogError::Corrupt("unknown index access method"))?;
    let amgettuple = routine
        .amgettuple
        .ok_or(CatalogError::Corrupt("missing index gettuple callback"))?;
    amgettuple(scan)
}

pub fn index_endscan(scan: IndexScanDesc, am_oid: u32) -> Result<(), CatalogError> {
    let routine = crate::backend::access::index::amapi::index_am_handler(am_oid)
        .ok_or(CatalogError::Corrupt("unknown index access method"))?;
    let amendscan = routine
        .amendscan
        .ok_or(CatalogError::Corrupt("missing index endscan callback"))?;
    amendscan(scan)
}
