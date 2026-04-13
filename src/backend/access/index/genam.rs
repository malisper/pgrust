use crate::backend::catalog::CatalogError;
use crate::include::access::relscan::{
    BtIndexScanOpaque, IndexScanDesc, IndexScanOpaque, ScanDirection,
};
use crate::include::access::amapi::IndexBeginScanContext;

pub fn index_beginscan_stub(
    ctx: &IndexBeginScanContext,
) -> Result<IndexScanDesc, CatalogError> {
    Ok(IndexScanDesc {
        pool: ctx.pool.clone(),
        client_id: ctx.client_id,
        snapshot: ctx.snapshot.clone(),
        heap_relation: Some(ctx.heap_relation),
        index_relation: ctx.index_relation,
        index_desc: ctx.index_desc.clone(),
        indoption: ctx.index_meta.indoption.clone(),
        number_of_keys: ctx.key_data.len(),
        key_data: ctx.key_data.clone(),
        direction: ctx.direction,
        xs_want_itup: false,
        xs_itup: None,
        xs_heaptid: None,
        opaque: IndexScanOpaque::Btree(BtIndexScanOpaque {
            current_block: None,
            current_pin: None,
            page_prev: None,
            page_next: None,
            next_offset: 0,
            current_items: Vec::new(),
        }),
    })
}

pub fn index_rescan_stub(
    scan: &mut IndexScanDesc,
    keys: &[crate::include::access::scankey::ScanKeyData],
    direction: ScanDirection,
) -> Result<(), CatalogError> {
    scan.number_of_keys = keys.len();
    scan.key_data = keys.to_vec();
    scan.direction = direction;
    scan.xs_itup = None;
    scan.xs_heaptid = None;
    if let IndexScanOpaque::Btree(opaque) = &mut scan.opaque {
        opaque.current_block = None;
        opaque.next_offset = 0;
        opaque.current_items.clear();
    }
    Ok(())
}

pub fn index_endscan_stub(_scan: IndexScanDesc) -> Result<(), CatalogError> {
    Ok(())
}
