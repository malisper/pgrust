// :HACK: root compatibility shim while GiST vacuum runtime lives in
// `pgrust_access`.
use crate::backend::access::RootAccessWal;
use crate::backend::access::index::buildkeys::map_access_error;
use crate::backend::catalog::CatalogError;
use crate::include::access::amapi::{
    IndexBulkDeleteCallback, IndexBulkDeleteResult, IndexVacuumContext,
};
use pgrust_access::gist as access_gist;

pub(crate) fn gistbulkdelete(
    ctx: &IndexVacuumContext,
    callback: &IndexBulkDeleteCallback<'_>,
    stats: Option<IndexBulkDeleteResult>,
) -> Result<IndexBulkDeleteResult, CatalogError> {
    access_gist::gistbulkdelete(
        &ctx.to_access_context(),
        callback,
        stats,
        &RootAccessWal {
            pool: ctx.pool.as_ref(),
        },
    )
    .map_err(map_access_error)
}

pub(crate) fn gistvacuumcleanup(
    ctx: &IndexVacuumContext,
    stats: Option<IndexBulkDeleteResult>,
) -> Result<IndexBulkDeleteResult, CatalogError> {
    access_gist::gistvacuumcleanup(&ctx.to_access_context(), stats).map_err(map_access_error)
}
