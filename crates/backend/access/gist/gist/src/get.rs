//! gistget.c: gistgettuple / gistgetbitmap / gistindex_keytest /
//! gistScanPage / gistkillitems.

use ::bufmgr_seams::{self as bufmgr, BufferPin};
use ::datum::Datum;
use ::mcx::Mcx;
use ::types_core::{InvalidBlockNumber, OffsetNumber};
use ::types_error::PgResult;
use ::types_gist::state::{
    GISTScanOpaqueData, GISTSearchHeapItem, GISTSearchItem, GISTSearchQueueHeapItem,
    IndexOrderByDistance, ReconTup,
};
use ::types_gist::{
    page_opaque, GistFollowRight, GistPageGetNSN, GistPageIsDeleted, GistPageIsLeaf,
    GIST_ROOT_BLKNO,
};
use ::types_rel::Relation;
use ::types_relscan::{IndexScanDescData, IndexScanOpaque};
use ::types_scan::scankey::{SK_ISNULL, SK_SEARCHNOTNULL, SK_SEARCHNULL};
use ::types_scan::sdir::ScanDirection;
use ::types_storage::bufpage::MaxIndexTuplesPerPage;

use crate::util::{
    gist_index_getattr, gist_tuple_is_invalid, gistFetchTupleValues, gistcheckpage,
    gistdentryinit, itup_get_tid, FirstOffsetNumber, ITup,
};

const GIST_SHARE: i32 = bufmgr::BUFFER_LOCK_SHARE;

// gistkillitems.
fn gistkillitems(scan: &mut IndexScanDescData<'_>) -> PgResult<()> {
    let rel = scan.index_rel().alias();
    let IndexScanOpaque::Gist(so) = &mut scan.opaque else {
        crate::non_gist_opaque()
    };

    debug_assert!(so.curBlkno != InvalidBlockNumber);
    debug_assert!(so.curPageLSN != 0);
    debug_assert!(so.killedItems.is_some());

    let pin = BufferPin::adopt(bufmgr::read_buffer::call(&rel, so.curBlkno)?)
        .expect("ReadBuffer");
    bufmgr::lock_buffer::call(pin.buffer(), GIST_SHARE)?;
    gistcheckpage(&rel, &pin)?;

    if bufmgr::buffer_get_lsn_atomic::call(pin.buffer()) != so.curPageLSN {
        bufmgr::lock_buffer::call(pin.buffer(), bufmgr::BUFFER_LOCK_UNLOCK)?;
        so.numKilled = 0;
        return Ok(());
    }

    debug_assert!(GistPageIsLeaf(&pin.page()));

    let mut killedsomething = false;
    {
        let mut pm = crate::buf_page_mut(pin.buffer());
        let killed = so.killedItems.as_ref().expect("killedItems");
        for i in 0..so.numKilled as usize {
            let offnum = killed[i];
            let mut id = pm.as_ref().item_id(offnum);
            id.mark_dead();
            pm.set_item_id(offnum, id);
            killedsomething = true;
        }
        if killedsomething {
            ::types_gist::page_opaque_update(&mut pm, |op| {
                op.flags |= ::types_gist::F_HAS_GARBAGE;
            });
        }
    }
    if killedsomething {
        bufmgr::mark_buffer_dirty_hint::call(pin.buffer(), true)?;
    }

    bufmgr::lock_buffer::call(pin.buffer(), bufmgr::BUFFER_LOCK_UNLOCK)?;
    so.numKilled = 0;
    Ok(())
}

// gistindex_keytest, driven per tuple in so.temp (reset by caller batch-wise).
// Ordered scans: fills `distances` (C so->distances) on a match; returns
// (match, recheck, recheck_distances).
#[allow(clippy::too_many_arguments)]
fn gistindex_keytest(
    mcx: Mcx<'_>,
    giststate: &mut ::types_gist::state::GistState<'_>,
    keys: &mut [::types_scan::scankey::ScanKeyData],
    orderbys: &mut [::types_scan::scankey::ScanKeyData],
    distances: &mut [IndexOrderByDistance],
    rel_name: &str,
    tuple: ITup,
    page_is_leaf: bool,
    offset: OffsetNumber,
) -> PgResult<(bool, bool, bool)> {
    let mut recheck_out = false;
    let mut recheck_distances_out = false;

    // SAFETY: tuple is a live page item under the caller's content lock.
    if unsafe { gist_tuple_is_invalid(tuple) } {
        if page_is_leaf {
            panic!("invalid GiST tuple found on leaf page");
        }
        // pre-9.1 invalid tuple: minimum possible distances so it's always
        // followed (gistget.c:147-157).
        for d in distances.iter_mut() {
            d.value = f64::NEG_INFINITY;
            d.isnull = false;
        }
        return Ok((true, false, false));
    }

    for key in keys.iter_mut() {
        let (datum, is_null) = gist_index_getattr(tuple, key.sk_attno as usize, giststate);

        if key.sk_flags & SK_ISNULL != 0 {
            if key.sk_flags & SK_SEARCHNULL != 0 {
                if page_is_leaf && !is_null {
                    return Ok((false, false, false));
                }
            } else {
                debug_assert!(key.sk_flags & SK_SEARCHNOTNULL != 0);
                if is_null {
                    return Ok((false, false, false));
                }
            }
        } else if is_null {
            return Ok((false, false, false));
        } else {
            let de = gistdentryinit(
                mcx,
                giststate,
                key.sk_attno as usize - 1,
                datum,
                offset,
                false,
                page_is_leaf,
                is_null,
            )?;

            let mut recheck = true;
            let test = giststate.call_consistent(
                mcx,
                // per-key FmgrInfo (fn_extra memo lives here, as C's sk_func)
                &mut key.sk_func,
                key.sk_collation,
                &de,
                key.sk_argument,
                key.sk_strategy,
                key.sk_subtype,
                &mut recheck,
            )?;
            let _ = rel_name;

            if !test {
                return Ok((false, false, false));
            }
            recheck_out |= recheck;
        }
    }

    // Passed the quals — compute the ORDER BY distances (gistget.c:238-297).
    for (key, distance) in orderbys.iter_mut().zip(distances.iter_mut()) {
        let (datum, is_null) = gist_index_getattr(tuple, key.sk_attno as usize, giststate);

        if key.sk_flags & SK_ISNULL != 0 || is_null {
            distance.value = 0.0;
            distance.isnull = true;
        } else {
            let de = gistdentryinit(
                mcx,
                giststate,
                key.sk_attno as usize - 1,
                datum,
                offset,
                false,
                page_is_leaf,
                is_null,
            )?;
            let mut recheck = false;
            let dist = giststate.call_distance(
                mcx,
                &mut key.sk_func,
                key.sk_collation,
                &de,
                key.sk_argument,
                key.sk_strategy,
                key.sk_subtype,
                &mut recheck,
            )?;
            recheck_distances_out |= recheck;
            distance.value = dist;
            distance.isnull = false;
        }
    }
    let _ = rel_name;

    Ok((true, recheck_out, recheck_distances_out))
}

// gistScanPage. `tbm`: bitmap-scan output; counts returned in ntids.
// `my_distances`: the queue item's distances (empty at the root), reused for
// a concurrent-split right sibling.
fn gist_scan_page(
    scan: &mut IndexScanDescData<'_>,
    page_item_blkno: ::types_core::BlockNumber,
    parentlsn: ::types_core::XLogRecPtr,
    my_distances: &[IndexOrderByDistance],
    mut tbm: Option<&mut tidbitmap::TIDBitmap<'_>>,
) -> PgResult<i64> {
    let rel = scan.index_rel().alias();
    let want_itup = scan.xs_want_itup;
    let ignore_killed = scan.ignore_killed_tuples;
    let norderbys = scan.numberOfOrderBys;
    let mut ntids = 0i64;

    let IndexScanOpaque::Gist(so) = &mut scan.opaque else {
        crate::non_gist_opaque()
    };
    let so = &mut **so;

    let pin = BufferPin::adopt(bufmgr::read_buffer::call(&rel, page_item_blkno)?)
        .expect("ReadBuffer");
    bufmgr::lock_buffer::call(pin.buffer(), GIST_SHARE)?;
    predicate_seams::predicate_lock_page::call(
        &rel,
        pin.block_number(),
        scan.xs_snapshot.as_deref().expect("gist scan has a snapshot"),
    )?;
    gistcheckpage(&rel, &pin)?;
    let page = pin.page();
    let opaque = page_opaque(&page);

    if parentlsn != 0
        && (GistFollowRight(&page) || parentlsn < GistPageGetNSN(&page))
        && opaque.rightlink != InvalidBlockNumber
    {
        // concurrent split: queue the right sibling with this page's distances
        so.queue.add(GISTSearchItem {
            blkno: opaque.rightlink,
            parentlsn,
            heap: None,
            distances: my_distances.to_vec(),
        });
    }

    if GistPageIsDeleted(&page) {
        bufmgr::lock_buffer::call(pin.buffer(), bufmgr::BUFFER_LOCK_UNLOCK)?;
        return Ok(0);
    }

    so.nPageData = 0;
    so.curPageData = 0;
    so.fetch_buf.clear();
    scan.xs_itup = None;

    so.curPageLSN = bufmgr::buffer_get_lsn_atomic::call(pin.buffer());

    let page_is_leaf = GistPageIsLeaf(&page);
    let maxoff = page.max_offset_number();
    for i in FirstOffsetNumber..=maxoff {
        let iid = page.item_id(i);
        if ignore_killed && iid.is_dead() {
            continue;
        }
        let it = page.item_raw(iid).0;

        let (matched, recheck, recheck_distances) = {
            let out = gistindex_keytest(
                so.temp.mcx(),
                &mut so.giststate,
                scan.keyData.as_mut_slice(),
                scan.orderByData.as_mut_slice(),
                so.distances.as_mut_slice(),
                rel.name(),
                it,
                page_is_leaf,
                i,
            )?;
            so.temp.reset();
            out
        };

        if !matched {
            continue;
        }

        if let Some(tbm) = tbm.as_deref_mut() {
            if page_is_leaf {
                // SAFETY: page item under our content lock.
                let tid = unsafe { itup_get_tid(it) };
                tbm.add_tuples(core::slice::from_ref(&tid), recheck)?;
                ntids += 1;
                continue;
            }
        } else if page_is_leaf && norderbys == 0 {
            // non-ordered scan: report tuples in pageData
            // SAFETY: page item under our content lock.
            let tid = unsafe { itup_get_tid(it) };
            let mut item = GISTSearchHeapItem {
                heapPtr: tid,
                recheck,
                offnum: i,
                recontup: None,
            };
            if want_itup {
                item.recontup = Some(fetch_recontup(so, &rel, it)?);
            }
            if so.pageData.len() <= so.nPageData {
                so.pageData.resize(so.nPageData + 1, GISTSearchHeapItem::default());
            }
            so.pageData[so.nPageData] = item;
            so.nPageData += 1;
            continue;
        } else if page_is_leaf {
            // ordered scan: heap tuples go through the search queue with
            // their distances (gistget.c:477-505)
            // SAFETY: page item under our content lock.
            let tid = unsafe { itup_get_tid(it) };
            let recontup = if want_itup {
                Some(fetch_recontup_owned(so, &rel, it)?)
            } else {
                None
            };
            so.queue.add(GISTSearchItem {
                blkno: InvalidBlockNumber,
                parentlsn: 0,
                heap: Some(GISTSearchQueueHeapItem {
                    heapPtr: tid,
                    recheck,
                    recheck_distances,
                    recontup,
                }),
                distances: so.distances.clone(),
            });
            continue;
        }

        if !page_is_leaf {
            // push the child page into the search queue
            // SAFETY: page item under our content lock.
            let child = unsafe { crate::util::itup_block_number(it) };
            so.queue.add(GISTSearchItem {
                blkno: child,
                parentlsn: so.curPageLSN,
                heap: None,
                distances: so.distances.clone(),
            });
        }
    }

    bufmgr::lock_buffer::call(pin.buffer(), bufmgr::BUFFER_LOCK_UNLOCK)?;
    Ok(ntids)
}

// IOS reconstruction: form an index tuple over fetchTupdesc from the fetched
// values into so.fetch_buf (C forms a heap tuple; StoreIndexTuple deforms to
// the same column values).
// Extends fetch_buf: a realloc dangles every outstanding xs_itup into the
// buffer — callers must run only from gist_scan_page's fill, after it nulls
// xs_itup and before any item is published.
fn fetch_recontup(
    so: &mut GISTScanOpaqueData<'_>,
    rel: &Relation<'_>,
    it: ITup,
) -> PgResult<(u32, u32)> {
    const K: usize = ::types_core::fmgr::INDEX_MAX_KEYS as usize;
    let natts = rel.rd_att.natts as usize;
    let mut fetchatt = [Datum::null(); K];
    let mut isnull = [false; K];
    let (off, len) = {
        let mcx = so.temp.mcx();
        gistFetchTupleValues(mcx, &mut so.giststate, rel, it, &mut fetchatt[..natts], &mut isnull[..natts])?;
        let tupdesc = so
            .giststate
            .fetchTupdesc
            .clone()
            .expect("gistrescan set fetchTupdesc for IOS");
        let formed =
            ::nbtree::itup::index_form_tuple(mcx, &tupdesc, &fetchatt[..natts], &isnull[..natts])?;
        let off = so.fetch_buf.len() as u32;
        // SAFETY: formed owned image of formed.size() bytes.
        let img = unsafe { core::slice::from_raw_parts(formed.as_ptr(), formed.size()) };
        so.fetch_buf.extend_from_slice(img);
        // 8-align the next tuple (itup deform requires it)
        while so.fetch_buf.len() % 8 != 0 {
            so.fetch_buf.push(0);
        }
        (off, img.len() as u32)
    };
    so.temp.reset();
    Ok((off, len))
}

// Ordered-scan recontup: queue items outlive the page (and fetch_buf's
// per-page lifetime), so each carries an owned aligned image.
fn fetch_recontup_owned(
    so: &mut GISTScanOpaqueData<'_>,
    rel: &Relation<'_>,
    it: ITup,
) -> PgResult<ReconTup> {
    const K: usize = ::types_core::fmgr::INDEX_MAX_KEYS as usize;
    let natts = rel.rd_att.natts as usize;
    let mut fetchatt = [Datum::null(); K];
    let mut isnull = [false; K];
    let out = {
        let mcx = so.temp.mcx();
        gistFetchTupleValues(mcx, &mut so.giststate, rel, it, &mut fetchatt[..natts], &mut isnull[..natts])?;
        let tupdesc = so
            .giststate
            .fetchTupdesc
            .clone()
            .expect("gistrescan set fetchTupdesc for IOS");
        let formed =
            ::nbtree::itup::index_form_tuple(mcx, &tupdesc, &fetchatt[..natts], &isnull[..natts])?;
        // SAFETY: formed owned image of formed.size() bytes.
        let img = unsafe { core::slice::from_raw_parts(formed.as_ptr(), formed.size()) };
        ReconTup::from_bytes(img)
    };
    so.temp.reset();
    Ok(out)
}

fn get_next_search_item(so: &mut GISTScanOpaqueData<'_>) -> Option<GISTSearchItem> {
    so.queue.remove_first()
}

// index_store_float8_orderby_distances (indexam.c:975): convert the AM's
// float8 distances to the ordering operators' result types in
// xs_orderbyvals/xs_orderbynulls.
fn store_orderby_distances(
    scan: &mut IndexScanDescData<'_>,
    distances: &[IndexOrderByDistance],
    recheck_orderby: bool,
) -> PgResult<()> {
    let IndexScanOpaque::Gist(so) = &mut scan.opaque else {
        crate::non_gist_opaque()
    };
    scan.xs_recheckorderby = recheck_orderby;
    for i in 0..scan.numberOfOrderBys as usize {
        let d = distances.get(i);
        match so.orderByTypes[i] {
            ::types_core::FLOAT8OID => match d {
                Some(d) if !d.isnull => {
                    scan.xs_orderbyvals[i] = Datum::from_f64(d.value);
                    scan.xs_orderbynulls[i] = false;
                }
                _ => {
                    scan.xs_orderbyvals[i] = Datum::null();
                    scan.xs_orderbynulls[i] = true;
                }
            },
            ::types_core::FLOAT4OID => match d {
                Some(d) if !d.isnull => {
                    scan.xs_orderbyvals[i] = Datum::from_f32(d.value as f32);
                    scan.xs_orderbynulls[i] = false;
                }
                _ => {
                    scan.xs_orderbyvals[i] = Datum::null();
                    scan.xs_orderbynulls[i] = true;
                }
            },
            _ => {
                // only float distances are convertible; a lossy distance
                // with another ordering type cannot be rechecked
                if scan.xs_recheckorderby {
                    return Err(Box::new(::types_error::PgError::error(
                        "ORDER BY operator must return float8 or float4 if the distance function is lossy"
                            .to_string(),
                    )));
                }
                scan.xs_orderbynulls[i] = true;
            }
        }
    }
    Ok(())
}

// getNextNearest (gistget.c:497).
fn get_next_nearest(scan: &mut IndexScanDescData<'_>) -> PgResult<bool> {
    scan.xs_itup = None;
    {
        let IndexScanOpaque::Gist(so) = &mut scan.opaque else {
            crate::non_gist_opaque()
        };
        so.cur_recontup = None;
    }

    loop {
        let next = {
            let IndexScanOpaque::Gist(so) = &mut scan.opaque else {
                crate::non_gist_opaque()
            };
            get_next_search_item(so)
        };
        let Some(mut item) = next else {
            return Ok(false);
        };

        if item.is_heap() {
            let heap = item.heap.take().expect("heap search item payload");
            scan.xs_heaptid = heap.heapPtr;
            scan.xs_recheck = heap.recheck;
            store_orderby_distances(scan, &item.distances, heap.recheck_distances)?;
            if scan.xs_want_itup {
                let IndexScanOpaque::Gist(so) = &mut scan.opaque else {
                    crate::non_gist_opaque()
                };
                so.cur_recontup = heap.recontup;
                scan.xs_itup = so
                    .cur_recontup
                    .as_ref()
                    .map(|t| core::ptr::NonNull::new(t.as_ptr() as *mut u8).expect("recontup ptr"));
            }
            return Ok(true);
        }

        crate::check_for_interrupts();
        gist_scan_page(scan, item.blkno, item.parentlsn, &item.distances, None)?;
    }
}

fn record_killed(so: &mut GISTScanOpaqueData<'_>, offnum: OffsetNumber) {
    let killed = so
        .killedItems
        .get_or_insert_with(|| Vec::with_capacity(MaxIndexTuplesPerPage));
    if (so.numKilled as usize) < MaxIndexTuplesPerPage {
        if killed.len() <= so.numKilled as usize {
            killed.resize(so.numKilled as usize + 1, 0);
        }
        killed[so.numKilled as usize] = offnum;
        so.numKilled += 1;
    }
}

fn publish_item(scan: &mut IndexScanDescData<'_>) {
    let IndexScanOpaque::Gist(so) = &mut scan.opaque else {
        crate::non_gist_opaque()
    };
    let item = so.pageData[so.curPageData];
    scan.xs_heaptid = item.heapPtr;
    scan.xs_recheck = item.recheck;
    if scan.xs_want_itup {
        let (off, _len) = item.recontup.expect("IOS items carry recontup");
        // xs_itup points into fetch_buf; valid until the next page/rescan.
        scan.xs_itup = core::ptr::NonNull::new(
            so.fetch_buf[off as usize..].as_ptr() as *mut u8,
        );
    }
    so.curPageData += 1;
}

/// gistgettuple.
pub fn gistgettuple(scan: &mut IndexScanDescData<'_>, dir: ScanDirection) -> PgResult<bool> {
    if dir != ::types_scan::sdir::ForwardScanDirection {
        panic!("GiST only supports forward scan direction");
    }

    {
        let IndexScanOpaque::Gist(so) = &mut scan.opaque else {
            crate::non_gist_opaque()
        };
        if !so.qual_ok {
            return Ok(false);
        }
    }

    let first_call = {
        let IndexScanOpaque::Gist(so) = &mut scan.opaque else {
            crate::non_gist_opaque()
        };
        let fc = so.firstCall;
        if fc {
            so.firstCall = false;
            so.curPageData = 0;
            so.nPageData = 0;
        }
        fc
    };
    if first_call {
        scan.xs_pgstat_index_scans += 1;
        scan.xs_nsearches += 1;
        scan.xs_itup = None;
        gist_scan_page(scan, GIST_ROOT_BLKNO, 0, &[], None)?;
    }

    if scan.numberOfOrderBys > 0 {
        // ordered scan: strict distance order via the search queue
        return get_next_nearest(scan);
    }

    loop {
        let kill_prior_tuple = scan.kill_prior_tuple;
        {
            let IndexScanOpaque::Gist(so) = &mut scan.opaque else {
                crate::non_gist_opaque()
            };
            if so.curPageData < so.nPageData {
                if kill_prior_tuple && so.curPageData > 0 {
                    let off = so.pageData[so.curPageData - 1].offnum;
                    record_killed(so, off);
                }
                publish_item(scan);
                return Ok(true);
            }

            if kill_prior_tuple && so.curPageData > 0 && so.curPageData == so.nPageData {
                let off = so.pageData[so.curPageData - 1].offnum;
                record_killed(so, off);
            }
        }

        // find and process the next index page
        loop {
            let (do_kill, item) = {
                let IndexScanOpaque::Gist(so) = &mut scan.opaque else {
                    crate::non_gist_opaque()
                };
                let do_kill = so.curBlkno != InvalidBlockNumber && so.numKilled > 0;
                (do_kill, ())
            };
            let _ = item;
            if do_kill {
                gistkillitems(scan)?;
            }

            let next = {
                let IndexScanOpaque::Gist(so) = &mut scan.opaque else {
                    crate::non_gist_opaque()
                };
                get_next_search_item(so)
            };
            let Some(item) = next else {
                return Ok(false);
            };

            crate::check_for_interrupts();

            {
                let IndexScanOpaque::Gist(so) = &mut scan.opaque else {
                    crate::non_gist_opaque()
                };
                so.curBlkno = item.blkno;
            }

            gist_scan_page(scan, item.blkno, item.parentlsn, &[], None)?;

            let has_data = {
                let IndexScanOpaque::Gist(so) = &mut scan.opaque else {
                    crate::non_gist_opaque()
                };
                so.nPageData > 0
            };
            if has_data {
                break;
            }
        }
    }
}

/// gistgetbitmap.
pub fn gistgetbitmap(
    scan: &mut IndexScanDescData<'_>,
    tbm: &mut tidbitmap::TIDBitmap<'_>,
) -> PgResult<i64> {
    {
        let IndexScanOpaque::Gist(so) = &mut scan.opaque else {
            crate::non_gist_opaque()
        };
        if !so.qual_ok {
            return Ok(0);
        }
        so.curPageData = 0;
        so.nPageData = 0;
    }
    scan.xs_pgstat_index_scans += 1;
    scan.xs_nsearches += 1;
    scan.xs_itup = None;

    let mut ntids = gist_scan_page(scan, GIST_ROOT_BLKNO, 0, &[], Some(tbm))?;

    loop {
        let next = {
            let IndexScanOpaque::Gist(so) = &mut scan.opaque else {
                crate::non_gist_opaque()
            };
            get_next_search_item(so)
        };
        let Some(item) = next else {
            break;
        };
        crate::check_for_interrupts();
        ntids += gist_scan_page(scan, item.blkno, item.parentlsn, &[], Some(tbm))?;
    }

    Ok(ntids)
}
