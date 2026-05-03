// :HACK: root compatibility shim while btree vacuum runtime lives in
// `pgrust_access`.
use crate::backend::access::index::buildkeys::map_access_error;
use crate::backend::access::{RootAccessRuntime, RootAccessWal};
use crate::backend::catalog::CatalogError;
use crate::include::access::amapi::{
    IndexBulkDeleteCallback, IndexBulkDeleteResult, IndexVacuumContext,
};
use pgrust_access::nbtree as access_nbtree;

pub(crate) fn btbulkdelete(
    ctx: &IndexVacuumContext,
    callback: &IndexBulkDeleteCallback<'_>,
    stats: Option<IndexBulkDeleteResult>,
) -> Result<IndexBulkDeleteResult, CatalogError> {
    let runtime = RootAccessRuntime::transaction_only(
        &ctx.txns,
        None,
        Some(ctx.interrupts.as_ref()),
        ctx.client_id,
    );
    access_nbtree::btbulkdelete(
        &ctx.to_access_context(),
        callback,
        stats,
        &runtime,
        &RootAccessWal {
            pool: ctx.pool.as_ref(),
        },
    )
    .map_err(map_access_error)
}

pub(crate) fn btvacuumcleanup(
    ctx: &IndexVacuumContext,
    stats: Option<IndexBulkDeleteResult>,
) -> Result<IndexBulkDeleteResult, CatalogError> {
    let runtime = RootAccessRuntime::transaction_only(
        &ctx.txns,
        None,
        Some(ctx.interrupts.as_ref()),
        ctx.client_id,
    );
    access_nbtree::btvacuumcleanup(
        &ctx.to_access_context(),
        stats,
        &runtime,
        &RootAccessWal {
            pool: ctx.pool.as_ref(),
        },
    )
    .map_err(map_access_error)
}
