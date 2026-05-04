// :HACK: root compatibility shim while SP-GiST scan runtime lives in
// `pgrust_access`; old AM callbacks still use root context types.
use pgrust_access::spgist as access_spgist;

use crate::backend::access::RootAccessServices;
use crate::backend::access::index::buildkeys::map_access_error;
use crate::backend::catalog::CatalogError;
use crate::include::access::amapi::IndexBeginScanContext;
use crate::include::access::relscan::{IndexScanDesc, ScanDirection};
use crate::include::access::scankey::ScanKeyData;
use crate::include::access::tidbitmap::TidBitmap;

pub(crate) fn spgbeginscan(ctx: &IndexBeginScanContext) -> Result<IndexScanDesc, CatalogError> {
    access_spgist::spgbeginscan(&ctx.to_access_context(), &RootAccessServices)
        .map_err(map_access_error)
}

pub(crate) fn spgrescan(
    scan: &mut IndexScanDesc,
    keys: &[ScanKeyData],
    direction: ScanDirection,
) -> Result<(), CatalogError> {
    access_spgist::spgrescan(scan, keys, direction, &RootAccessServices).map_err(map_access_error)
}

pub(crate) fn spggettuple(scan: &mut IndexScanDesc) -> Result<bool, CatalogError> {
    access_spgist::spggettuple(scan).map_err(map_access_error)
}

pub(crate) fn spggetbitmap(
    scan: &mut IndexScanDesc,
    bitmap: &mut TidBitmap,
) -> Result<i64, CatalogError> {
    access_spgist::spggetbitmap(scan, bitmap).map_err(map_access_error)
}

pub(crate) fn spgendscan(scan: IndexScanDesc) -> Result<(), CatalogError> {
    access_spgist::spgendscan(scan).map_err(map_access_error)
}
