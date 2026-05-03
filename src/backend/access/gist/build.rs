// :HACK: root compatibility shim while GiST runtime lives in
// `pgrust_access` and root AM callbacks still own heap scans and expression
// index projection.
use crate::backend::access::index::buildkeys::{
    IndexBuildKeyProjector, RootIndexBuildServices, map_access_error, map_catalog_error_to_access,
    materialize_heap_row_values,
};
use crate::backend::access::{RootAccessRuntime, RootAccessServices, RootAccessWal};
use crate::backend::catalog::CatalogError;
use crate::include::access::amapi::{IndexBuildContext, IndexBuildEmptyContext, IndexBuildResult};
use crate::include::access::itemptr::ItemPointerData;
use crate::include::nodes::datum::Value;
use pgrust_access::{
    AccessError, AccessHeapServices, AccessIndexServices, AccessResult, gist as access_gist,
};

struct RootGistBuildSource<'a> {
    ctx: &'a IndexBuildContext,
    heap_services: &'a dyn AccessHeapServices,
    key_projector: IndexBuildKeyProjector,
}

impl pgrust_access::gist::GistBuildRowSource for RootGistBuildSource<'_> {
    fn for_each_projected(
        &mut self,
        visit: &mut dyn FnMut(ItemPointerData, Vec<Value>) -> AccessResult<()>,
    ) -> AccessResult<u64> {
        let attr_descs = self.ctx.heap_desc.attribute_descs();
        let mut index_services = RootIndexBuildServices::new(self.ctx, &mut self.key_projector);
        self.heap_services.for_each_visible_heap_tuple(
            self.ctx.heap_relation,
            self.ctx.snapshot.clone(),
            &mut |tid, tuple| {
                let datums = tuple.deform(&attr_descs).map_err(|err| {
                    AccessError::Scalar(format!("gist heap deform failed: {err:?}"))
                })?;
                let row_values = materialize_heap_row_values(&self.ctx.heap_desc, &datums)
                    .map_err(map_catalog_error_to_access)?;
                if let Some(key_values) =
                    index_services.project_index_row(&self.ctx.index_meta, &row_values, tid)?
                {
                    visit(tid, key_values)?;
                }
                Ok(())
            },
        )
    }
}

pub(crate) fn gistbuild(ctx: &IndexBuildContext) -> Result<IndexBuildResult, CatalogError> {
    let key_projector = IndexBuildKeyProjector::new(ctx)?;
    let heap_services = RootAccessRuntime::heap(
        &ctx.pool,
        &ctx.txns,
        Some(ctx.interrupts.as_ref()),
        ctx.client_id,
    );
    let mut source = RootGistBuildSource {
        ctx,
        heap_services: &heap_services,
        key_projector,
    };
    access_gist::gistbuild(
        &ctx.to_access_context(),
        &mut source,
        &heap_services,
        &RootAccessServices,
        &RootAccessWal {
            pool: ctx.pool.as_ref(),
        },
    )
    .map_err(map_access_error)
}

pub(crate) fn gistbuildempty(ctx: &IndexBuildEmptyContext) -> Result<(), CatalogError> {
    access_gist::gistbuildempty(
        &ctx.to_access_context(),
        &RootAccessWal {
            pool: ctx.pool.as_ref(),
        },
    )
    .map_err(map_access_error)
}
