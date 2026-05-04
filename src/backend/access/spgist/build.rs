// :HACK: root compatibility shim while SP-GiST runtime lives in
// `pgrust_access`; root still owns heap scans and expression/partial index
// projection.
use pgrust_access::{
    AccessError, AccessHeapServices, AccessIndexServices, spgist as access_spgist,
};

use crate::backend::access::index::buildkeys::{
    IndexBuildKeyProjector, RootIndexBuildServices, map_access_error, map_catalog_error_to_access,
    materialize_heap_row_values,
};
use crate::backend::access::{RootAccessRuntime, RootAccessServices, RootAccessWal};
use crate::backend::catalog::CatalogError;
use crate::include::access::amapi::{IndexBuildContext, IndexBuildEmptyContext, IndexBuildResult};

pub(crate) fn spgbuild(ctx: &IndexBuildContext) -> Result<IndexBuildResult, CatalogError> {
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
                    AccessError::Scalar(format!("spgist heap deform failed: {err:?}"))
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
    access_spgist::spgbuild_projected(
        &ctx.to_access_context(),
        heap_tuples,
        pending,
        &RootAccessServices,
        &RootAccessWal {
            pool: ctx.pool.as_ref(),
        },
    )
    .map_err(map_access_error)
}

pub(crate) fn spgbuildempty(ctx: &IndexBuildEmptyContext) -> Result<(), CatalogError> {
    access_spgist::spgbuildempty(
        &ctx.to_access_context(),
        &RootAccessWal {
            pool: ctx.pool.as_ref(),
        },
    )
    .map_err(map_access_error)
}
