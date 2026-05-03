// :HACK: root compatibility shim while SP-GiST vacuum runtime lives in
// `pgrust_access`; old AM callbacks still use root context types.
use pgrust_access::spgist as access_spgist;

use crate::backend::access::RootAccessWal;
use crate::backend::access::index::buildkeys::map_access_error;
use crate::backend::catalog::CatalogError;
use crate::include::access::amapi::{
    IndexBulkDeleteCallback, IndexBulkDeleteResult, IndexVacuumContext,
};

pub(crate) fn spgbulkdelete(
    ctx: &IndexVacuumContext,
    callback: &IndexBulkDeleteCallback<'_>,
    stats: Option<IndexBulkDeleteResult>,
) -> Result<IndexBulkDeleteResult, CatalogError> {
    access_spgist::spgbulkdelete(
        &ctx.to_access_context(),
        callback,
        stats,
        &RootAccessWal {
            pool: ctx.pool.as_ref(),
        },
    )
    .map_err(map_access_error)
}

pub(crate) fn spgvacuumcleanup(
    ctx: &IndexVacuumContext,
    stats: Option<IndexBulkDeleteResult>,
) -> Result<IndexBulkDeleteResult, CatalogError> {
    access_spgist::spgvacuumcleanup(&ctx.to_access_context(), stats).map_err(map_access_error)
}
