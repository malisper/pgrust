// :HACK: root compatibility shim while GiST insert runtime lives in
// `pgrust_access`.
use crate::backend::access::index::buildkeys::map_access_error;
use crate::backend::access::{RootAccessServices, RootAccessWal};
use crate::backend::catalog::CatalogError;
use crate::include::access::amapi::IndexInsertContext;
use pgrust_access::gist as access_gist;

pub(crate) fn gistinsert(ctx: &IndexInsertContext) -> Result<bool, CatalogError> {
    access_gist::gistinsert(
        &ctx.to_access_context(),
        &RootAccessServices,
        &RootAccessWal {
            pool: ctx.pool.as_ref(),
        },
    )
    .map_err(map_access_error)
}
