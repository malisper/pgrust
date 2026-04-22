use crate::backend::access::heap::heapam::{heap_scan_begin_visible, heap_scan_next_visible};
use crate::backend::access::index::buildkeys::{
    IndexBuildKeyProjector, materialize_heap_row_values,
};
use crate::backend::catalog::CatalogError;
use crate::include::access::amapi::{
    IndexBuildContext, IndexBuildEmptyContext, IndexBuildResult, IndexInsertContext,
    IndexUniqueCheck,
};
use crate::include::access::itemptr::ItemPointerData;
use crate::include::nodes::datum::Value;

use super::insert::spginsert;
use super::page::ensure_empty_spgist;

pub(crate) fn spgbuild(ctx: &IndexBuildContext) -> Result<IndexBuildResult, CatalogError> {
    if ctx.index_meta.indisunique {
        return Err(CatalogError::Io(
            "SP-GiST does not support unique indexes".into(),
        ));
    }
    ensure_empty_spgist(
        &ctx.pool,
        ctx.client_id,
        ctx.snapshot.current_xid,
        ctx.index_relation,
    )?;
    scan_visible_heap(ctx, |tid, key_values| {
        spginsert_build_tuple(ctx, tid, key_values)
    })
}

pub(crate) fn spgbuildempty(ctx: &IndexBuildEmptyContext) -> Result<(), CatalogError> {
    ensure_empty_spgist(&ctx.pool, ctx.client_id, ctx.xid, ctx.index_relation)
}

fn scan_visible_heap(
    ctx: &IndexBuildContext,
    mut visit: impl FnMut(ItemPointerData, Vec<Value>) -> Result<(), CatalogError>,
) -> Result<IndexBuildResult, CatalogError> {
    let mut scan = heap_scan_begin_visible(
        &ctx.pool,
        ctx.client_id,
        ctx.heap_relation,
        ctx.snapshot.clone(),
    )
    .map_err(|err| CatalogError::Io(format!("heap scan begin failed: {err:?}")))?;
    let attr_descs = ctx.heap_desc.attribute_descs();
    let mut key_projector = IndexBuildKeyProjector::new(ctx)?;
    let mut result = IndexBuildResult::default();
    loop {
        crate::backend::utils::misc::interrupts::check_for_interrupts(ctx.interrupts.as_ref())
            .map_err(CatalogError::Interrupted)?;
        let next = {
            let txns = ctx.txns.read();
            heap_scan_next_visible(&ctx.pool, ctx.client_id, &txns, &mut scan)
        };
        let Some((tid, tuple)) =
            next.map_err(|err| CatalogError::Io(format!("heap scan failed: {err:?}")))?
        else {
            break;
        };
        let datums = tuple
            .deform(&attr_descs)
            .map_err(|err| CatalogError::Io(format!("heap deform failed: {err:?}")))?;
        let row_values = materialize_heap_row_values(&ctx.heap_desc, &datums)?;
        let key_values = key_projector.project(ctx, &row_values, tid)?;
        result.heap_tuples += 1;
        if let Some(key_values) = key_values {
            visit(tid, key_values)?;
            result.index_tuples += 1;
        }
    }
    Ok(result)
}

fn spginsert_build_tuple(
    ctx: &IndexBuildContext,
    heap_tid: ItemPointerData,
    values: Vec<Value>,
) -> Result<(), CatalogError> {
    spginsert(&IndexInsertContext {
        pool: ctx.pool.clone(),
        txns: ctx.txns.clone(),
        txn_waiter: None,
        client_id: ctx.client_id,
        interrupts: ctx.interrupts.clone(),
        snapshot: ctx.snapshot.clone(),
        heap_relation: ctx.heap_relation,
        heap_desc: ctx.heap_desc.clone(),
        index_relation: ctx.index_relation,
        index_name: ctx.index_name.clone(),
        index_desc: ctx.index_desc.clone(),
        index_meta: ctx.index_meta.clone(),
        default_toast_compression: ctx.default_toast_compression,
        heap_tid,
        values,
        unique_check: IndexUniqueCheck::No,
    })?;
    Ok(())
}
