// :HACK: root compatibility shim while GIN runtime lives in `pgrust_access`
// and root AM callbacks still carry executor/catalog-owned services.
use crate::backend::access::index::buildkeys::{
    IndexBuildKeyProjector, RootIndexBuildServices, map_access_error, map_catalog_error_to_access,
    materialize_heap_row_values,
};
use crate::backend::access::{RootAccessRuntime, RootAccessServices, RootAccessWal};
use crate::backend::catalog::CatalogError;
use crate::include::access::amapi::{
    IndexAmRoutine, IndexBeginScanContext, IndexBuildContext, IndexBuildEmptyContext,
    IndexBuildResult, IndexBulkDeleteCallback, IndexBulkDeleteResult, IndexInsertContext,
    IndexVacuumContext,
};
use crate::include::access::gin::GinOptions;
use crate::include::access::relscan::{IndexScanDesc, ScanDirection};
use crate::include::access::scankey::ScanKeyData;
use crate::include::access::tidbitmap::TidBitmap;
use pgrust_access::{AccessError, AccessHeapServices, AccessIndexServices, gin as access_gin};

fn ginbuild(ctx: &IndexBuildContext) -> Result<IndexBuildResult, CatalogError> {
    let attr_descs = ctx.heap_desc.attribute_descs();
    let mut key_projector = IndexBuildKeyProjector::new(ctx)?;
    let mut index_services = RootIndexBuildServices::new(ctx, &mut key_projector);
    let heap_services = RootAccessRuntime::heap(
        &ctx.pool,
        &ctx.txns,
        Some(ctx.interrupts.as_ref()),
        ctx.client_id,
    );
    let mut heap_tuples = 0;
    let mut pending = Vec::new();
    heap_services
        .for_each_visible_heap_tuple(
            ctx.heap_relation,
            ctx.snapshot.clone(),
            &mut |tid, tuple| {
                let datums = tuple.deform(&attr_descs).map_err(|err| {
                    AccessError::Scalar(format!("gin heap deform failed: {err:?}"))
                })?;
                let row_values = materialize_heap_row_values(&ctx.heap_desc, &datums)
                    .map_err(map_catalog_error_to_access)?;
                heap_tuples += 1;
                if let Some(key_values) =
                    index_services.project_index_row(&ctx.index_meta, &row_values, tid)?
                {
                    pending.push((tid, key_values));
                }
                Ok(())
            },
        )
        .map_err(map_access_error)?;

    drop(index_services);
    access_gin::ginbuild_projected(
        &ctx.to_access_context(),
        heap_tuples,
        pending,
        &heap_services,
        &RootAccessServices,
        &RootAccessWal {
            pool: ctx.pool.as_ref(),
        },
    )
    .map_err(map_access_error)
}

fn ginbuildempty(ctx: &IndexBuildEmptyContext) -> Result<(), CatalogError> {
    access_gin::ginbuildempty(
        &ctx.to_access_context(),
        &RootAccessWal {
            pool: ctx.pool.as_ref(),
        },
    )
    .map_err(map_access_error)
}

fn gininsert(ctx: &IndexInsertContext) -> Result<bool, CatalogError> {
    access_gin::gininsert(
        &ctx.to_access_context(),
        &RootAccessServices,
        &RootAccessWal {
            pool: ctx.pool.as_ref(),
        },
    )
    .map_err(map_access_error)
}

fn ginbeginscan(ctx: &IndexBeginScanContext) -> Result<IndexScanDesc, CatalogError> {
    access_gin::ginbeginscan(&ctx.to_access_context()).map_err(map_access_error)
}

fn ginrescan(
    scan: &mut IndexScanDesc,
    keys: &[ScanKeyData],
    direction: ScanDirection,
) -> Result<(), CatalogError> {
    access_gin::ginrescan(scan, keys, direction).map_err(map_access_error)
}

fn gingetbitmap(scan: &mut IndexScanDesc, bitmap: &mut TidBitmap) -> Result<i64, CatalogError> {
    // :HACK: root compatibility adapter while GIN scan runtime moves into
    // `pgrust_access`; old AM callbacks still use root context types and do
    // not carry transaction manager state.
    let pool = scan.pool.clone();
    let heap_services = RootAccessRuntime::heap_storage(&pool, scan.client_id);
    access_gin::gingetbitmap(scan, bitmap, &heap_services, &RootAccessServices)
        .map_err(map_access_error)
}

fn ginendscan(scan: IndexScanDesc) -> Result<(), CatalogError> {
    access_gin::ginendscan(scan).map_err(map_access_error)
}

fn ginbulkdelete(
    ctx: &IndexVacuumContext,
    callback: &IndexBulkDeleteCallback<'_>,
    stats: Option<IndexBulkDeleteResult>,
) -> Result<IndexBulkDeleteResult, CatalogError> {
    access_gin::ginbulkdelete(
        &ctx.to_access_context(),
        callback,
        stats,
        &RootAccessWal {
            pool: ctx.pool.as_ref(),
        },
    )
    .map_err(map_access_error)
}

fn ginvacuumcleanup(
    ctx: &IndexVacuumContext,
    stats: Option<IndexBulkDeleteResult>,
) -> Result<IndexBulkDeleteResult, CatalogError> {
    access_gin::ginvacuumcleanup(
        &ctx.to_access_context(),
        stats,
        &RootAccessWal {
            pool: ctx.pool.as_ref(),
        },
    )
    .map_err(map_access_error)
}

pub(crate) fn gin_clean_pending_list(ctx: &IndexVacuumContext) -> Result<i64, CatalogError> {
    access_gin::gin_clean_pending_list(
        &ctx.to_access_context(),
        &RootAccessWal {
            pool: ctx.pool.as_ref(),
        },
    )
    .map_err(map_access_error)
}

pub(crate) fn gin_update_options(
    pool: &crate::BufferPool<crate::backend::storage::buffer::storage_backend::SmgrStorageBackend>,
    client_id: crate::ClientId,
    rel: crate::backend::storage::smgr::RelFileLocator,
    options: &GinOptions,
) -> Result<(), CatalogError> {
    access_gin::gin_update_options(pool, client_id, rel, options, &RootAccessWal { pool })
        .map_err(map_access_error)
}

pub fn gin_am_handler() -> IndexAmRoutine {
    IndexAmRoutine {
        amstrategies: 0,
        amsupport: 7,
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
        amstorage: true,
        amclusterable: false,
        ampredlocks: false,
        amsummarizing: false,
        ambuild: Some(ginbuild),
        ambuildempty: Some(ginbuildempty),
        aminsert: Some(gininsert),
        ambeginscan: Some(ginbeginscan),
        amrescan: Some(ginrescan),
        amgettuple: None,
        amgetbitmap: Some(gingetbitmap),
        amendscan: Some(ginendscan),
        ambulkdelete: Some(ginbulkdelete),
        amvacuumcleanup: Some(ginvacuumcleanup),
    }
}
