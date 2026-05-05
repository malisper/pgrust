// :HACK: root compatibility shim while btree runtime lives in
// `pgrust_access`; root still owns heap row materialization, TOAST fetch, and
// expression/partial index projection.
use pgrust_access::nbtree as access_nbtree;
use pgrust_access::{AccessError, AccessHeapServices, AccessIndexServices};

use crate::backend::access::index::buildkeys::{
    IndexBuildKeyProjector, RootIndexBuildServices, map_access_error, map_catalog_error_to_access,
    materialize_heap_row_values_with_toast,
};
use crate::backend::access::{RootAccessRuntime, RootAccessServices, RootAccessWal};
use crate::backend::catalog::CatalogError;
use crate::include::access::amapi::{
    IndexAmRoutine, IndexBeginScanContext, IndexBuildContext, IndexBuildEmptyContext,
    IndexBuildResult, IndexInsertContext,
};
use crate::include::access::htup::AttributeCompression;
use crate::include::access::relscan::{IndexScanDesc, ScanDirection};
use crate::include::access::scankey::ScanKeyData;
use crate::include::access::tidbitmap::TidBitmap;
use crate::include::nodes::datum::Value;
use crate::include::nodes::execnodes::ToastFetchContext;
use crate::include::nodes::primnodes::RelationDesc;

pub(crate) const UNIQUE_BUILD_DETAIL_SEPARATOR: &str = access_nbtree::UNIQUE_BUILD_DETAIL_SEPARATOR;

pub(crate) fn encode_key_payload(
    desc: &RelationDesc,
    values: &[Value],
    default_toast_compression: AttributeCompression,
) -> Result<Vec<u8>, CatalogError> {
    access_nbtree::encode_key_payload(desc, values, default_toast_compression, &RootAccessServices)
        .map_err(map_access_error)
}

pub(crate) fn decode_key_payload(
    desc: &RelationDesc,
    payload: &[u8],
) -> Result<Vec<Value>, CatalogError> {
    access_nbtree::decode_key_payload(desc, payload, &RootAccessServices).map_err(map_access_error)
}

fn btbuild(ctx: &IndexBuildContext) -> Result<IndexBuildResult, CatalogError> {
    let attr_descs = ctx.heap_desc.attribute_descs();
    let mut key_projector = IndexBuildKeyProjector::new(ctx)?;
    let mut index_services = RootIndexBuildServices::new(ctx, &mut key_projector);
    let heap_services = RootAccessRuntime::heap(
        &ctx.pool,
        &ctx.txns,
        Some(ctx.interrupts.as_ref()),
        ctx.client_id,
    );
    let toast = ctx.heap_toast.map(|relation| ToastFetchContext {
        relation,
        pool: ctx.pool.clone(),
        txns: ctx.txns.clone(),
        snapshot: ctx.snapshot.clone(),
        client_id: ctx.client_id,
    });
    let mut heap_tuples = 0;
    let mut pending = Vec::new();
    heap_services
        .for_each_visible_heap_tuple(
            ctx.heap_relation,
            ctx.snapshot.clone(),
            &mut |tid, tuple| {
                let datums = tuple
                    .deform(&attr_descs)
                    .map_err(|err| AccessError::Scalar(format!("heap deform failed: {err:?}")))?;
                let row_values =
                    materialize_heap_row_values_with_toast(&ctx.heap_desc, &datums, toast.as_ref())
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
    access_nbtree::btbuild_projected(
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

fn btbuildempty(ctx: &IndexBuildEmptyContext) -> Result<(), CatalogError> {
    access_nbtree::btbuildempty(
        &ctx.to_access_context(),
        &RootAccessWal {
            pool: ctx.pool.as_ref(),
        },
    )
    .map_err(map_access_error)
}

fn btinsert(ctx: &IndexInsertContext) -> Result<bool, CatalogError> {
    let runtime = RootAccessRuntime {
        pool: Some(&ctx.pool),
        local_buffer_manager: ctx.local_buffer_manager.as_ref(),
        txns: Some(&ctx.txns),
        txn_waiter: ctx.txn_waiter.as_deref(),
        interrupts: Some(ctx.interrupts.as_ref()),
        client_id: ctx.client_id,
    };
    access_nbtree::btinsert(
        &ctx.to_access_context(),
        &runtime,
        &RootAccessServices,
        &RootAccessWal {
            pool: ctx.pool.as_ref(),
        },
    )
    .map_err(map_access_error)
}

fn btbeginscan(ctx: &IndexBeginScanContext) -> Result<IndexScanDesc, CatalogError> {
    access_nbtree::btbeginscan(&ctx.to_access_context(), &RootAccessServices)
        .map_err(map_access_error)
}

fn btrescan(
    scan: &mut IndexScanDesc,
    keys: &[ScanKeyData],
    direction: ScanDirection,
) -> Result<(), CatalogError> {
    access_nbtree::btrescan(scan, keys, direction, &RootAccessServices).map_err(map_access_error)
}

fn btgettuple(scan: &mut IndexScanDesc) -> Result<bool, CatalogError> {
    access_nbtree::btgettuple(scan, &RootAccessServices).map_err(map_access_error)
}

fn btgetbitmap(scan: &mut IndexScanDesc, bitmap: &mut TidBitmap) -> Result<i64, CatalogError> {
    access_nbtree::btgetbitmap(scan, bitmap, &RootAccessServices).map_err(map_access_error)
}

fn btendscan(scan: IndexScanDesc) -> Result<(), CatalogError> {
    access_nbtree::btendscan(scan).map_err(map_access_error)
}

pub fn btree_am_handler() -> IndexAmRoutine {
    IndexAmRoutine {
        amstrategies: 5,
        amsupport: 5,
        amcanorder: true,
        amcanorderbyop: false,
        amcanhash: false,
        amconsistentordering: true,
        amcanbackward: true,
        amcanunique: true,
        amcanmulticol: true,
        amoptionalkey: true,
        amsearcharray: true,
        amsearchnulls: true,
        amstorage: false,
        amclusterable: true,
        ampredlocks: true,
        amsummarizing: false,
        ambuild: Some(btbuild),
        ambuildempty: Some(btbuildempty),
        aminsert: Some(btinsert),
        ambeginscan: Some(btbeginscan),
        amrescan: Some(btrescan),
        amgettuple: Some(btgettuple),
        amgetbitmap: Some(btgetbitmap),
        amendscan: Some(btendscan),
        ambulkdelete: Some(crate::backend::access::nbtree::nbtvacuum::btbulkdelete),
        amvacuumcleanup: Some(crate::backend::access::nbtree::nbtvacuum::btvacuumcleanup),
    }
}

#[cfg(test)]
mod tests {
    use super::btree_am_handler;

    #[test]
    fn btree_handler_advertises_ordered_unique_support() {
        let am = btree_am_handler();

        assert!(am.amcanorder);
        assert!(am.amcanunique);
        assert!(am.amsearcharray);
    }
}
