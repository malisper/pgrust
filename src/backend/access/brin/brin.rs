// :HACK: root compatibility shim while BRIN runtime is owned by `pgrust_access`.
use crate::backend::access::index::buildkeys::{
    IndexBuildKeyProjector, RootIndexBuildServices, map_access_error,
};
use crate::backend::access::{RootAccessRuntime, RootAccessServices, RootAccessWal};
use crate::backend::catalog::CatalogError;
use crate::include::access::amapi::{
    IndexAmRoutine, IndexBeginScanContext, IndexBuildContext, IndexBuildEmptyContext,
    IndexBuildResult, IndexBulkDeleteCallback, IndexBulkDeleteResult, IndexInsertContext,
    IndexVacuumContext,
};
use crate::include::access::relscan::{IndexScanDesc, ScanDirection};
use crate::include::access::scankey::ScanKeyData;
use crate::include::access::tidbitmap::TidBitmap;

pub(crate) fn brin_summarize_new_values(ctx: &IndexVacuumContext) -> Result<i32, CatalogError> {
    let runtime = RootAccessRuntime::heap(
        &ctx.pool,
        &ctx.txns,
        Some(ctx.interrupts.as_ref()),
        ctx.client_id,
    );
    pgrust_access::brin::brin_summarize_new_values(
        &ctx.to_access_context(),
        &runtime,
        None,
        &RootAccessServices,
    )
    .map_err(map_access_error)
}

pub(crate) fn brin_summarize_range(
    ctx: &IndexVacuumContext,
    heap_block: u32,
) -> Result<i32, CatalogError> {
    let runtime = RootAccessRuntime::heap(
        &ctx.pool,
        &ctx.txns,
        Some(ctx.interrupts.as_ref()),
        ctx.client_id,
    );
    pgrust_access::brin::brin_summarize_range(
        &ctx.to_access_context(),
        heap_block,
        &runtime,
        None,
        &RootAccessServices,
    )
    .map_err(map_access_error)
}

pub(crate) fn brin_desummarize_range(
    ctx: &IndexVacuumContext,
    heap_block: u32,
) -> Result<(), CatalogError> {
    pgrust_access::brin::brin_desummarize_range(&ctx.to_access_context(), heap_block)
        .map_err(map_access_error)
}

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

pub(crate) fn brinbuild(ctx: &IndexBuildContext) -> Result<IndexBuildResult, CatalogError> {
    let mut key_projector = IndexBuildKeyProjector::new(ctx)?;
    let mut index_services = RootIndexBuildServices::new(ctx, &mut key_projector);
    let heap_services = RootAccessRuntime::heap(
        &ctx.pool,
        &ctx.txns,
        Some(ctx.interrupts.as_ref()),
        ctx.client_id,
    );
    pgrust_access::brin::brinbuild(
        &ctx.to_access_context(),
        &heap_services,
        &mut index_services,
        &RootAccessServices,
        &RootAccessWal {
            pool: ctx.pool.as_ref(),
        },
    )
    .map_err(map_access_error)
}

pub(crate) fn brinbuildempty(ctx: &IndexBuildEmptyContext) -> Result<(), CatalogError> {
    pgrust_access::brin::brinbuildempty(
        &ctx.to_access_context(),
        &RootAccessWal {
            pool: ctx.pool.as_ref(),
        },
    )
    .map_err(map_access_error)
}

pub(crate) fn brininsert(ctx: &IndexInsertContext) -> Result<bool, CatalogError> {
    pgrust_access::brin::brininsert(
        &ctx.to_access_context(),
        &RootAccessServices,
        &RootAccessWal {
            pool: ctx.pool.as_ref(),
        },
    )
    .map_err(map_access_error)
}

pub(crate) fn brinbeginscan(ctx: &IndexBeginScanContext) -> Result<IndexScanDesc, CatalogError> {
    pgrust_access::brin::brinbeginscan(&ctx.to_access_context()).map_err(map_access_error)
}

pub(crate) fn brinrescan(
    scan: &mut IndexScanDesc,
    keys: &[ScanKeyData],
    direction: ScanDirection,
) -> Result<(), CatalogError> {
    pgrust_access::brin::brinrescan(scan, keys, direction).map_err(map_access_error)
}

pub(crate) fn bringetbitmap(
    scan: &mut IndexScanDesc,
    bitmap: &mut TidBitmap,
) -> Result<i64, CatalogError> {
    pgrust_access::brin::bringetbitmap(scan, bitmap, &RootAccessServices).map_err(map_access_error)
}

pub(crate) fn brinendscan(scan: IndexScanDesc) -> Result<(), CatalogError> {
    pgrust_access::brin::brinendscan(scan).map_err(map_access_error)
}

pub(crate) fn brinbulkdelete(
    _ctx: &IndexVacuumContext,
    _callback: &IndexBulkDeleteCallback<'_>,
    stats: Option<IndexBulkDeleteResult>,
) -> Result<IndexBulkDeleteResult, CatalogError> {
    Ok(stats.unwrap_or_default())
}

pub(crate) fn brinvacuumcleanup(
    ctx: &IndexVacuumContext,
    stats: Option<IndexBulkDeleteResult>,
) -> Result<IndexBulkDeleteResult, CatalogError> {
    let runtime = RootAccessRuntime::heap(
        &ctx.pool,
        &ctx.txns,
        Some(ctx.interrupts.as_ref()),
        ctx.client_id,
    );
    pgrust_access::brin::brinvacuumcleanup(
        &ctx.to_access_context(),
        stats,
        &runtime,
        None,
        &RootAccessServices,
        &RootAccessWal {
            pool: ctx.pool.as_ref(),
        },
    )
    .map_err(map_access_error)
}
