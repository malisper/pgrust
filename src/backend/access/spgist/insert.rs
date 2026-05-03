// :HACK: root compatibility shim while SP-GiST insert runtime lives in
// `pgrust_access`; old AM callbacks still use root context types.
use pgrust_access::spgist as access_spgist;

use crate::backend::access::index::buildkeys::map_access_error;
use crate::backend::access::{RootAccessServices, RootAccessWal};
use crate::backend::catalog::CatalogError;
use crate::include::access::amapi::IndexInsertContext;

pub(crate) fn spginsert(ctx: &IndexInsertContext) -> Result<bool, CatalogError> {
    access_spgist::spginsert(
        &ctx.to_access_context(),
        &RootAccessServices,
        &RootAccessWal {
            pool: ctx.pool.as_ref(),
        },
    )
    .map_err(map_access_error)
}
