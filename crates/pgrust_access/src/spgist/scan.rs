use std::cmp::Ordering;

use crate::access::amapi::IndexBeginScanContext;
use crate::access::itemptr::ItemPointerData;
use crate::access::relscan::{
    IndexOrderByDistance, IndexScanDesc, IndexScanOpaque, ScanDirection, SpgistIndexScanOpaque,
    SpgistSearchItem,
};
use crate::access::scankey::ScanKeyData;
use crate::access::spgist::spgist_page_items;
use crate::access::tidbitmap::TidBitmap;
use crate::{AccessError, AccessScalarServices};

use super::page::{page_opaque, read_buffered_page, relation_nblocks};
use super::state::SpgistState;
use super::tuple::decode_tuple_values;

fn compare_distances(left: &[IndexOrderByDistance], right: &[IndexOrderByDistance]) -> Ordering {
    for (left, right) in left.iter().zip(right.iter()) {
        let cmp = match (left.is_null, right.is_null) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => left.value.total_cmp(&right.value),
        };
        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    Ordering::Equal
}

fn compare_item_ptr(left: ItemPointerData, right: ItemPointerData) -> Ordering {
    left.block_number
        .cmp(&right.block_number)
        .then(left.offset_number.cmp(&right.offset_number))
}

fn materialize_matches(
    scan: &IndexScanDesc,
    scalar: &dyn AccessScalarServices,
) -> Result<Vec<SpgistSearchItem>, AccessError> {
    let state = SpgistState::new(&scan.index_desc, &scan.index_meta, scalar)?;
    let mut items = Vec::new();
    let nblocks = relation_nblocks(&scan.pool, scan.index_relation)?;
    for block in 0..nblocks {
        let page = read_buffered_page(&scan.pool, scan.client_id, scan.index_relation, block)?;
        let opaque = page_opaque(&page)?;
        if opaque.is_deleted() || !opaque.is_leaf() {
            continue;
        }
        for tuple in spgist_page_items(&page)
            .map_err(|err| AccessError::Io(format!("spgist tuple parse failed: {err:?}")))?
        {
            let tuple_values = decode_tuple_values(&scan.index_desc, &tuple, scalar)?;
            let mut matches = true;
            for key in &scan.key_data {
                if !state.leaf_consistent(&tuple_values, key)? {
                    matches = false;
                    break;
                }
            }
            if !matches {
                continue;
            }
            let mut distances = Vec::with_capacity(scan.order_by_data.len());
            for key in &scan.order_by_data {
                let value = state.order_distance(&tuple_values, key)?;
                distances.push(IndexOrderByDistance {
                    value: value.unwrap_or(f64::INFINITY),
                    is_null: value.is_none(),
                });
            }
            items.push(SpgistSearchItem {
                tid: tuple.t_tid,
                tuple,
                recheck: false,
                recheck_order_by: false,
                distances,
            });
        }
    }
    if !scan.order_by_data.is_empty() {
        items.sort_by(|left, right| {
            compare_distances(&left.distances, &right.distances)
                .then(compare_item_ptr(left.tid, right.tid))
        });
    }
    Ok(items)
}

fn reset_scan(
    scan: &mut IndexScanDesc,
    scalar: &dyn AccessScalarServices,
) -> Result<(), AccessError> {
    let items = materialize_matches(scan, scalar)?;
    let IndexScanOpaque::Spgist(state) = &mut scan.opaque else {
        return Err(AccessError::Corrupt("SP-GiST scan state missing opaque"));
    };
    state.items = items;
    state.next_item = 0;
    scan.xs_itup = None;
    scan.xs_heaptid = None;
    scan.xs_recheck = false;
    scan.xs_recheck_order_by = false;
    scan.xs_orderby_values = vec![None; scan.number_of_order_bys];
    Ok(())
}

pub fn spgbeginscan(
    ctx: &IndexBeginScanContext,
    scalar: &dyn AccessScalarServices,
) -> Result<IndexScanDesc, AccessError> {
    if ctx.direction != ScanDirection::Forward {
        return Err(AccessError::Io(
            "SP-GiST scans only support forward direction".into(),
        ));
    }
    let mut scan = IndexScanDesc {
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
        opaque: IndexScanOpaque::Spgist(SpgistIndexScanOpaque::default()),
    };
    reset_scan(&mut scan, scalar)?;
    Ok(scan)
}

pub fn spgrescan(
    scan: &mut IndexScanDesc,
    keys: &[ScanKeyData],
    direction: ScanDirection,
    scalar: &dyn AccessScalarServices,
) -> Result<(), AccessError> {
    if direction != ScanDirection::Forward {
        return Err(AccessError::Io(
            "SP-GiST scans only support forward direction".into(),
        ));
    }
    scan.number_of_keys = keys.len();
    scan.key_data = keys.to_vec();
    scan.direction = direction;
    reset_scan(scan, scalar)
}

pub fn spggettuple(scan: &mut IndexScanDesc) -> Result<bool, AccessError> {
    let IndexScanOpaque::Spgist(state) = &mut scan.opaque else {
        return Err(AccessError::Corrupt("SP-GiST scan state missing opaque"));
    };
    let Some(item) = state.items.get(state.next_item).cloned() else {
        scan.xs_itup = None;
        scan.xs_heaptid = None;
        scan.xs_recheck = false;
        scan.xs_recheck_order_by = false;
        scan.xs_orderby_values = vec![None; scan.number_of_order_bys];
        return Ok(false);
    };
    state.next_item += 1;
    scan.xs_heaptid = Some(item.tid);
    scan.xs_itup = scan.xs_want_itup.then_some(item.tuple.clone());
    scan.xs_recheck = item.recheck;
    scan.xs_recheck_order_by = item.recheck_order_by;
    scan.xs_orderby_values = item
        .distances
        .iter()
        .map(|distance| (!distance.is_null).then_some(distance.value))
        .collect();
    Ok(true)
}

pub fn spggetbitmap(scan: &mut IndexScanDesc, bitmap: &mut TidBitmap) -> Result<i64, AccessError> {
    let mut count = 0i64;
    while spggettuple(scan)? {
        if let Some(tid) = scan.xs_heaptid {
            bitmap.add_tid(tid);
            count += 1;
        }
    }
    Ok(count)
}

pub fn spgendscan(_scan: IndexScanDesc) -> Result<(), AccessError> {
    Ok(())
}
