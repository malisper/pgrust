use crate::backend::catalog::CatalogError;
use crate::include::access::amapi::{
    IndexAmRoutine, IndexBeginScanContext, IndexBuildContext, IndexBuildEmptyContext,
    IndexBuildResult, IndexBulkDeleteResult, IndexInsertContext, IndexVacuumContext,
};
use crate::include::access::relscan::IndexScanDesc;
use crate::include::access::scankey::ScanKeyData;
use crate::include::access::tidbitmap::TidBitmap;

pub fn brin_am_handler() -> IndexAmRoutine {
    IndexAmRoutine {
        amstrategies: 5,
        amsupport: 4,
        amcanorder: false,
        amcanorderbyop: false,
        amcanhash: false,
        amconsistentordering: false,
        amcanbackward: false,
        amcanunique: false,
        amcanmulticol: true,
        amoptionalkey: true,
        amsearcharray: false,
        amsearchnulls: false,
        amstorage: false,
        amclusterable: false,
        ampredlocks: false,
        amsummarizing: true,
        ambuild: Some(brinbuild),
        ambuildempty: Some(brinbuildempty),
        aminsert: Some(brininsert),
        ambeginscan: Some(brinbeginscan),
        amrescan: Some(brinrescan),
        amgettuple: None,
        amgetbitmap: Some(bringetbitmap),
        amendscan: Some(brinendscan),
        ambulkdelete: Some(brinbulkdelete),
        amvacuumcleanup: Some(brinvacuumcleanup),
    }
}

pub(crate) fn brinbuild(_ctx: &IndexBuildContext) -> Result<IndexBuildResult, CatalogError> {
    Err(CatalogError::Io(
        "BRIN build implementation not yet wired".into(),
    ))
}

pub(crate) fn brinbuildempty(_ctx: &IndexBuildEmptyContext) -> Result<(), CatalogError> {
    Err(CatalogError::Io(
        "BRIN buildempty implementation not yet wired".into(),
    ))
}

pub(crate) fn brininsert(_ctx: &IndexInsertContext) -> Result<bool, CatalogError> {
    Err(CatalogError::Io(
        "BRIN insert implementation not yet wired".into(),
    ))
}

pub(crate) fn brinbeginscan(
    _ctx: &IndexBeginScanContext,
) -> Result<IndexScanDesc, CatalogError> {
    Err(CatalogError::Io(
        "BRIN beginscan implementation not yet wired".into(),
    ))
}

pub(crate) fn brinrescan(
    _scan: &mut IndexScanDesc,
    _keys: &[ScanKeyData],
    _direction: crate::include::access::relscan::ScanDirection,
) -> Result<(), CatalogError> {
    Ok(())
}

pub(crate) fn bringetbitmap(
    _scan: &mut IndexScanDesc,
    _bitmap: &mut TidBitmap,
) -> Result<i64, CatalogError> {
    Err(CatalogError::Io(
        "BRIN getbitmap implementation not yet wired".into(),
    ))
}

pub(crate) fn brinendscan(_scan: IndexScanDesc) -> Result<(), CatalogError> {
    Ok(())
}

pub(crate) fn brinbulkdelete(
    _ctx: &IndexVacuumContext,
    stats: Option<IndexBulkDeleteResult>,
) -> Result<IndexBulkDeleteResult, CatalogError> {
    Ok(stats.unwrap_or_default())
}

pub(crate) fn brinvacuumcleanup(
    _ctx: &IndexVacuumContext,
    stats: Option<IndexBulkDeleteResult>,
) -> Result<IndexBulkDeleteResult, CatalogError> {
    Ok(stats.unwrap_or_default())
}
