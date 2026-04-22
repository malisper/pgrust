use crate::backend::catalog::CatalogError;
use crate::include::access::amapi::IndexBeginScanContext;
use crate::include::access::genam::IndexScanDescData;
use crate::include::access::gist::{
    GIST_INVALID_BLOCKNO, gist_downlink_block, gist_page_get_opaque, gist_page_items_with_offsets,
};
use crate::include::access::relscan::{
    GistIndexScanOpaque, GistOrderByDistance, GistSearchItem, GistSearchItemKind, IndexScanDesc,
    IndexScanOpaque, ScanDirection,
};
use crate::include::access::scankey::ScanKeyData;

use super::page::{page_lsn, read_buffered_page};
use super::state::GistState;
use super::tuple::decode_tuple_values;

fn next_ordinal(state: &mut GistIndexScanOpaque) -> u64 {
    let ordinal = state.next_ordinal;
    state.next_ordinal = state.next_ordinal.saturating_add(1);
    ordinal
}

fn reset_scan(scan: &mut IndexScanDesc) -> Result<(), CatalogError> {
    let IndexScanOpaque::Gist(state) = &mut scan.opaque else {
        return Err(CatalogError::Corrupt("GiST scan state missing opaque"));
    };
    state.search_queue.clear();
    state.next_ordinal = 0;
    let ordinal = next_ordinal(state);
    state.search_queue.push(GistSearchItem {
        kind: GistSearchItemKind::Page {
            block: crate::include::access::gist::GIST_ROOT_BLKNO,
            parent_lsn: 0,
        },
        distances: Vec::new(),
        ordinal,
    });
    scan.xs_itup = None;
    scan.xs_heaptid = None;
    scan.xs_recheck = false;
    scan.xs_recheck_order_by = false;
    scan.xs_orderby_values = vec![None; scan.number_of_order_bys];
    Ok(())
}

pub(crate) fn gistbeginscan(ctx: &IndexBeginScanContext) -> Result<IndexScanDesc, CatalogError> {
    if !ctx.order_by_data.is_empty() && ctx.direction != ScanDirection::Forward {
        return Err(CatalogError::Io(
            "GiST ORDER BY scans only support forward direction".into(),
        ));
    }
    let mut scan = IndexScanDescData {
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
        opaque: IndexScanOpaque::Gist(GistIndexScanOpaque::default()),
    };
    reset_scan(&mut scan)?;
    Ok(scan)
}

pub(crate) fn gistrescan(
    scan: &mut IndexScanDesc,
    keys: &[ScanKeyData],
    direction: ScanDirection,
) -> Result<(), CatalogError> {
    if !scan.order_by_data.is_empty() && direction != ScanDirection::Forward {
        return Err(CatalogError::Io(
            "GiST ORDER BY scans only support forward direction".into(),
        ));
    }
    scan.number_of_keys = keys.len();
    scan.key_data = keys.to_vec();
    scan.direction = direction;
    reset_scan(scan)
}

fn tuple_matches_scan_keys(
    state: &GistState,
    tuple_values: &[crate::include::nodes::datum::Value],
    keys: &[ScanKeyData],
    is_leaf: bool,
) -> Result<(bool, bool), CatalogError> {
    let mut recheck = false;
    for key in keys {
        let result = state.consistent(tuple_values, key, is_leaf)?;
        if !result.matches {
            return Ok((false, false));
        }
        recheck |= result.recheck;
    }
    Ok((true, recheck))
}

fn tuple_order_distances(
    state: &GistState,
    tuple_values: &[crate::include::nodes::datum::Value],
    keys: &[ScanKeyData],
    is_leaf: bool,
) -> Result<(Vec<GistOrderByDistance>, bool), CatalogError> {
    let mut distances = Vec::with_capacity(keys.len());
    let mut recheck = false;
    for key in keys {
        let result = state.distance(tuple_values, key, is_leaf)?;
        distances.push(GistOrderByDistance {
            value: result.value.unwrap_or(f64::INFINITY),
            is_null: result.value.is_none(),
        });
        recheck |= result.recheck;
    }
    Ok((distances, recheck))
}

fn scan_page(scan: &mut IndexScanDesc, block: u32, parent_lsn: u64) -> Result<(), CatalogError> {
    let gist_state = GistState::new(&scan.index_desc, &scan.index_meta)?;
    let page = read_buffered_page(&scan.pool, scan.client_id, scan.index_relation, block)?;
    let opaque = gist_page_get_opaque(&page)
        .map_err(|err| CatalogError::Io(format!("gist page parse failed: {err:?}")))?;
    if opaque.is_deleted() {
        return Ok(());
    }

    let IndexScanOpaque::Gist(scan_state) = &mut scan.opaque else {
        return Err(CatalogError::Corrupt("GiST scan state missing opaque"));
    };
    if opaque.rightlink != GIST_INVALID_BLOCKNO
        && (opaque.follows_right() || opaque.nsn > parent_lsn)
    {
        let ordinal = next_ordinal(scan_state);
        scan_state.search_queue.push(GistSearchItem {
            kind: GistSearchItemKind::Page {
                block: opaque.rightlink,
                parent_lsn,
            },
            distances: Vec::new(),
            ordinal,
        });
    }

    let page_lsn = page_lsn(&page);
    let tuples = gist_page_items_with_offsets(&page)
        .map_err(|err| CatalogError::Io(format!("gist tuple parse failed: {err:?}")))?;
    for (_, tuple) in tuples.into_iter().rev() {
        let tuple_values = decode_tuple_values(&scan.index_desc, &tuple)?;
        let (matches, recheck) =
            tuple_matches_scan_keys(&gist_state, &tuple_values, &scan.key_data, opaque.is_leaf())?;
        if !matches {
            continue;
        }
        let (distances, recheck_order_by) = tuple_order_distances(
            &gist_state,
            &tuple_values,
            &scan.order_by_data,
            opaque.is_leaf(),
        )?;
        let ordinal = next_ordinal(scan_state);
        if opaque.is_leaf() {
            scan_state.search_queue.push(GistSearchItem {
                kind: GistSearchItemKind::Heap {
                    tid: tuple.t_tid,
                    tuple,
                    recheck,
                    recheck_order_by,
                },
                distances,
                ordinal,
            });
        } else {
            let child_block = gist_downlink_block(&tuple).ok_or(CatalogError::Corrupt(
                "gist internal tuple missing child block",
            ))?;
            scan_state.search_queue.push(GistSearchItem {
                kind: GistSearchItemKind::Page {
                    block: child_block,
                    parent_lsn: page_lsn,
                },
                distances,
                ordinal,
            });
        }
    }
    Ok(())
}

pub(crate) fn gistgettuple(scan: &mut IndexScanDesc) -> Result<bool, CatalogError> {
    if scan.direction != ScanDirection::Forward {
        return Err(CatalogError::Io(
            "GiST backward scans are not supported".into(),
        ));
    }
    loop {
        let item = {
            let IndexScanOpaque::Gist(state) = &mut scan.opaque else {
                return Err(CatalogError::Corrupt("GiST scan state missing opaque"));
            };
            state.search_queue.pop()
        };
        let Some(item) = item else {
            scan.xs_itup = None;
            scan.xs_heaptid = None;
            scan.xs_recheck = false;
            scan.xs_recheck_order_by = false;
            scan.xs_orderby_values = vec![None; scan.number_of_order_bys];
            return Ok(false);
        };
        match item.kind {
            GistSearchItemKind::Page { block, parent_lsn } => scan_page(scan, block, parent_lsn)?,
            GistSearchItemKind::Heap {
                tid,
                tuple,
                recheck,
                recheck_order_by,
            } => {
                scan.xs_heaptid = Some(tid);
                scan.xs_itup = Some(tuple);
                scan.xs_recheck = recheck;
                scan.xs_recheck_order_by = recheck_order_by;
                scan.xs_orderby_values = item
                    .distances
                    .iter()
                    .map(|distance| (!distance.is_null).then_some(distance.value))
                    .collect();
                return Ok(true);
            }
        }
    }
}

pub(crate) fn gistendscan(_scan: IndexScanDesc) -> Result<(), CatalogError> {
    Ok(())
}
