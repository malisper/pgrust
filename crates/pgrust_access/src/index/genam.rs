use std::sync::Arc;

use pgrust_core::{ClientId, RelFileLocator, Snapshot};
use pgrust_nodes::primnodes::RelationDesc;
use pgrust_nodes::relcache::IndexRelCacheEntry;
use pgrust_storage::{BufferPool, SmgrStorageBackend};

use crate::AccessResult;
use crate::access::relscan::{BtIndexScanOpaque, IndexScanDesc, IndexScanOpaque, ScanDirection};
use crate::access::scankey::ScanKeyData;

#[derive(Clone)]
pub struct IndexBeginScanContext {
    pub pool: Arc<BufferPool<SmgrStorageBackend>>,
    pub client_id: ClientId,
    pub snapshot: Snapshot,
    pub heap_relation: RelFileLocator,
    pub index_relation: RelFileLocator,
    pub index_desc: RelationDesc,
    pub index_meta: IndexRelCacheEntry,
    pub key_data: Vec<ScanKeyData>,
    pub order_by_data: Vec<ScanKeyData>,
    pub direction: ScanDirection,
    pub want_itup: bool,
}

pub fn index_beginscan_stub(ctx: &IndexBeginScanContext) -> AccessResult<IndexScanDesc> {
    Ok(IndexScanDesc {
        pool: ctx.pool.clone(),
        client_id: ctx.client_id,
        snapshot: ctx.snapshot.clone(),
        heap_relation: Some(ctx.heap_relation),
        index_relation: ctx.index_relation,
        index_desc: ctx.index_desc.clone(),
        index_meta: ctx.index_meta.clone(),
        indoption: ctx.index_meta.indoption.clone(),
        number_of_keys: ctx.key_data.len(),
        key_data: ctx.key_data.clone(),
        number_of_order_bys: ctx.order_by_data.len(),
        order_by_data: ctx.order_by_data.clone(),
        direction: ctx.direction,
        xs_want_itup: ctx.want_itup,
        xs_itup: None,
        xs_heaptid: None,
        xs_recheck: false,
        xs_recheck_order_by: false,
        xs_orderby_values: vec![None; ctx.order_by_data.len()],
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
    keys: &[ScanKeyData],
    direction: ScanDirection,
) -> AccessResult<()> {
    scan.number_of_keys = keys.len();
    scan.key_data = keys.to_vec();
    scan.direction = direction;
    scan.xs_itup = None;
    scan.xs_heaptid = None;
    scan.xs_recheck = false;
    scan.xs_recheck_order_by = false;
    for value in &mut scan.xs_orderby_values {
        *value = None;
    }
    if let IndexScanOpaque::Btree(opaque) = &mut scan.opaque {
        opaque.current_block = None;
        opaque.next_offset = 0;
        opaque.current_items.clear();
    }
    Ok(())
}

pub fn index_endscan_stub(_scan: IndexScanDesc) -> AccessResult<()> {
    Ok(())
}
