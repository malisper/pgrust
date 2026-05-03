// :HACK: root compatibility shim while generic index scan state lives in
// `pgrust_access`. Long term AM runtimes should call `pgrust_access::index`
// directly once root access modules are wrapper-only.
use pgrust_access::index::genam as access_genam;
use pgrust_access::{AccessError, AccessResult};

use crate::backend::catalog::CatalogError;
use crate::include::access::amapi::IndexBeginScanContext;
use crate::include::access::relscan::{IndexScanDesc, ScanDirection};

fn catalog_error(error: AccessError) -> CatalogError {
    match error {
        AccessError::Corrupt(message) => CatalogError::Corrupt(message),
        AccessError::Scalar(message) | AccessError::Unsupported(message) => {
            CatalogError::Io(message)
        }
    }
}

fn catalog_result<T>(result: AccessResult<T>) -> Result<T, CatalogError> {
    result.map_err(catalog_error)
}

pub fn index_beginscan_stub(ctx: &IndexBeginScanContext) -> Result<IndexScanDesc, CatalogError> {
    let access_ctx = access_genam::IndexBeginScanContext {
        pool: ctx.pool.clone(),
        client_id: ctx.client_id,
        snapshot: ctx.snapshot.clone(),
        heap_relation: ctx.heap_relation,
        index_relation: ctx.index_relation,
        index_desc: ctx.index_desc.clone(),
        index_meta: ctx.index_meta.clone(),
        key_data: ctx.key_data.clone(),
        order_by_data: ctx.order_by_data.clone(),
        direction: ctx.direction,
        want_itup: ctx.want_itup,
    };
    catalog_result(access_genam::index_beginscan_stub(&access_ctx))
}

pub fn index_rescan_stub(
    scan: &mut IndexScanDesc,
    keys: &[crate::include::access::scankey::ScanKeyData],
    direction: ScanDirection,
) -> Result<(), CatalogError> {
    catalog_result(access_genam::index_rescan_stub(scan, keys, direction))
}

pub fn index_endscan_stub(_scan: IndexScanDesc) -> Result<(), CatalogError> {
    catalog_result(access_genam::index_endscan_stub(_scan))
}
