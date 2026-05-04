// :HACK: root compatibility shim while GiST scan runtime lives in
// `pgrust_access`.
use crate::backend::access::RootAccessServices;
use crate::backend::access::index::buildkeys::map_access_error;
use crate::backend::catalog::CatalogError;
use crate::include::access::amapi::IndexBeginScanContext;
use crate::include::access::relscan::{IndexScanDesc, ScanDirection};
use crate::include::access::scankey::ScanKeyData;
use crate::include::access::tidbitmap::TidBitmap;
use pgrust_access::gist as access_gist;

pub(crate) fn gistbeginscan(ctx: &IndexBeginScanContext) -> Result<IndexScanDesc, CatalogError> {
    access_gist::gistbeginscan(&ctx.to_access_context()).map_err(map_access_error)
}

pub(crate) fn gistrescan(
    scan: &mut IndexScanDesc,
    keys: &[ScanKeyData],
    direction: ScanDirection,
) -> Result<(), CatalogError> {
    access_gist::gistrescan(scan, keys, direction).map_err(map_access_error)
}

pub(crate) fn gistgettuple(scan: &mut IndexScanDesc) -> Result<bool, CatalogError> {
    access_gist::gistgettuple(scan, &RootAccessServices).map_err(map_access_error)
}

pub(crate) fn gistgetbitmap(
    scan: &mut IndexScanDesc,
    bitmap: &mut TidBitmap,
) -> Result<i64, CatalogError> {
    access_gist::gistgetbitmap(scan, bitmap, &RootAccessServices).map_err(map_access_error)
}

pub(crate) fn gistendscan(scan: IndexScanDesc) -> Result<(), CatalogError> {
    access_gist::gistendscan(scan).map_err(map_access_error)
}
