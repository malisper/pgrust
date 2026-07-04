//! gistget.c: gistgettuple / gistgetbitmap / gistindex_keytest /
//! gistScanPage / gistkillitems.

use ::bufmgr_seams::{self as bufmgr, BufferPin};
use ::datum::Datum;
use ::mcx::Mcx;
use ::types_core::{InvalidBlockNumber, OffsetNumber};
use ::types_error::PgResult;
use ::types_gist::state::{GISTScanOpaqueData, GISTSearchHeapItem, GISTSearchItem};
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
    let rel = scan.indexRelation.alias();
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
#[allow(clippy::too_many_arguments)]
fn gistindex_keytest(
    mcx: Mcx<'_>,
    giststate: &mut ::types_gist::state::GistState<'_>,
    keys: &mut [::types_scan::scankey::ScanKeyData],
    norderbys: i32,
    rel_name: &str,
    tuple: ITup,
    page_is_leaf: bool,
    offset: OffsetNumber,
) -> PgResult<(bool, bool)> {
    let mut recheck_out = false;

    // SAFETY: tuple is a live page item under the caller's content lock.
    if unsafe { gist_tuple_is_invalid(tuple) } {
        if page_is_leaf {
            panic!("invalid GiST tuple found on leaf page");
        }
        debug_assert!(norderbys == 0);
        return Ok((true, false));
    }

    for key in keys.iter_mut() {
        let (datum, is_null) = gist_index_getattr(tuple, key.sk_attno as usize, giststate);

        if key.sk_flags & SK_ISNULL != 0 {
            if key.sk_flags & SK_SEARCHNULL != 0 {
                if page_is_leaf && !is_null {
                    return Ok((false, false));
                }
            } else {
                debug_assert!(key.sk_flags & SK_SEARCHNOTNULL != 0);
                if is_null {
                    return Ok((false, false));
                }
            }
        } else if is_null {
            return Ok((false, false));
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
                return Ok((false, false));
            }
            recheck_out |= recheck;
        }
    }

    Ok((true, recheck_out))
}

// gistScanPage. `tbm`: bitmap-scan output; counts returned in ntids.
fn gist_scan_page(
    scan: &mut IndexScanDescData<'_>,
    page_item_blkno: ::types_core::BlockNumber,
    parentlsn: ::types_core::XLogRecPtr,
    mut tbm: Option<&mut tidbitmap::TIDBitmap<'_>>,
) -> PgResult<i64> {
    let rel = scan.indexRelation.alias();
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
        // concurrent split: queue the right sibling
        so.queue.add(GISTSearchItem {
            blkno: opaque.rightlink,
            parentlsn,
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

        let (matched, recheck) = {
            let out = gistindex_keytest(
                so.temp.mcx(),
                &mut so.giststate,
                scan.keyData.as_mut_slice(),
                norderbys,
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
        } else if page_is_leaf {
            // non-ordered scan: report tuples in pageData
            debug_assert!(norderbys == 0);
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
        }

        if !page_is_leaf {
            // push the child page into the search queue
            // SAFETY: page item under our content lock.
            let child = unsafe { crate::util::itup_block_number(it) };
            so.queue.add(GISTSearchItem {
                blkno: child,
                parentlsn: so.curPageLSN,
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

fn get_next_search_item(so: &mut GISTScanOpaqueData<'_>) -> Option<GISTSearchItem> {
    so.queue.remove_first()
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
    debug_assert!(scan.numberOfOrderBys == 0);

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
        gist_scan_page(scan, GIST_ROOT_BLKNO, 0, None)?;
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

            gist_scan_page(scan, item.blkno, item.parentlsn, None)?;

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

    let mut ntids = gist_scan_page(scan, GIST_ROOT_BLKNO, 0, Some(tbm))?;

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
        ntids += gist_scan_page(scan, item.blkno, item.parentlsn, Some(tbm))?;
    }

    Ok(ntids)
}
