//! GiST scan layer (`access/gist/gistscan.c` + `access/gist/gistget.c`): the AM
//! scan callbacks (`gistbeginscan` / `gistrescan` / `gistendscan` /
//! `gistgettuple` / `gistgetbitmap` / `gistcanreturn`), the `gistScanPage` page
//! descent, the search-queue pairing heap, the `gistindex_keytest` consistent /
//! distance dispatch, and the KNN distance ordering.
//!
//! Model notes (owned tree vs. C):
//!   * `GISTScanOpaqueData` rides `IndexScanDescData.opaque` via the A0
//!     [`AmOpaque`] carrier (the `GIST_SCAN` tag), exactly like BRIN's
//!     `BrinScan` and GIN's scan state.
//!   * The whole scan lives in one `mcx` arena (the C `scanCxt`); the C
//!     `tempCxt` / `queueCxt` / `pageDataCxt` sub-contexts and their resets are
//!     subsumed by Rust ownership (the per-call working values are dropped when
//!     they go out of scope; the queue is a `Vec`-backed pairing heap that is
//!     re-created on rescan). `freeGISTstate` is therefore a drop.
//!   * Index tuples are on-disk byte images (`&[u8]`), exactly what
//!     `PageGetItem` returns; `it->t_tid` is the leading `ItemPointerData`.
//!   * Page bytes are reached through the bufmgr seam: a snapshot read via
//!     `buffer_get_page`, in-place LP_DEAD hints via `with_buffer_page`.
//!   * The opclass `consistent` / `distance` / `fetch` support procedures are
//!     dispatched by OID through the typed `backend-access-gist-dispatch-seams`
//!     (installed by `backend-access-gist-proc`), not a generic fmgr path.
//!   * The insert / vacuum vtable slots dispatch into
//!     `backend-access-gist-am-seams` (unported owners), seam-and-panic until
//!     those lanes land. The serial scan path never invokes them.

use alloc::boxed::Box as AllocBox;
use alloc::rc::Rc;
use alloc::string::ToString;
use alloc::vec;
use alloc::vec::Vec;

use backend_access_gist_am_seams::{gistbulkdelete, gistinsert, gistvacuumcleanup};
use backend_access_gist_dispatch_seams::{gist_consistent, gist_distance};
use backend_access_index_indexam_seams::index_getprocid;
use backend_storage_buffer_bufmgr_seams::{
    buffer_get_block_number, buffer_get_lsn_atomic, buffer_get_page, mark_buffer_dirty_hint,
    read_buffer, unlock_release_buffer, with_buffer_page,
};
use backend_storage_lmgr_predicate_seams::predicate_lock_page;
use backend_storage_page::{
    ItemIdIsDead, ItemIdMarkDead, PageGetItem, PageGetItemId, PageGetMaxOffsetNumber, PageMut,
    PageRef,
};
use backend_utils_activity_pgstat_seams::pgstat_count_index_scan;
use backend_utils_error::{ereport, PgResult};
use mcx::{Mcx, PgBox};

use types_amapi::{
    AmCostEstimate, IndexAMProperty as AmIndexAMProperty, IndexAmRoutine, IndexBuildResult,
    IndexPath, OpFamilyMember, PlannerInfo, T_IndexAmRoutine,
};
use types_core::primitive::{BlockNumber, InvalidBlockNumber, OffsetNumber, Oid, OidIsValid};
use types_core::InvalidOid;
use types_error::error::ERROR;
use types_rel::Relation;
use types_scan::scankey::{ScanKeyData, SK_ISNULL, SK_SEARCHNOTNULL, SK_SEARCHNULL};
use types_storage::buf::BufferIsValid;
use types_tableam::amapi::{IndexUniqueCheck, TIDBitmap as AmTIDBitmap};
use types_tableam::index_info_carrier::IndexInfoCarrier;
use types_tableam::amopaque::AmOpaque;
use types_tableam::genam::{IndexBulkDeleteResult, IndexOrderByDistance, IndexVacuumInfo};
use types_tableam::relscan::{IndexScanDesc, IndexScanDescData};
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::{ItemPointerData, FIRST_OFFSET_NUMBER};
use types_gist::{
    GISTScanOpaqueData, GISTSearchHeapItem, GISTSearchItem, GISTSearchItemData,
    GISTSearchItemIsHeap, GISTSTATE, GIST_DISTANCE_PROC, GIST_ROOT_BLKNO,
};
use types_scan::sdir::{ForwardScanDirection, ScanDirection};

use crate::gist_page::{
    gist_page_get_nsn, GistMarkPageHasGarbage, GistPageIsDeleted, GistPageIsLeaf,
};
use crate::gist_page::gistcheckpage;
use crate::gistutil::{
    gist_tuple_is_invalid, gistFetchTuple, gistdentryinit, gistproperty,
    index_getattr_pub, itup_heap_ptr, IndexAMProperty as GistIndexAMProperty,
};

// Re-export the GiST opclass amproc support-number used by gistcanreturn.
use types_gist::{GIST_COMPRESS_PROC, GIST_FETCH_PROC};

/// `BUFFER_LOCK_SHARE` / `GIST_SHARE` (bufmgr.h / gist_private.h).
const GIST_SHARE: i32 = 1;

/// `FLOAT8OID` / `FLOAT4OID` (pg_type.dat) — the only ORDER BY result types the
/// distance-ordering save path knows how to convert to.
use types_tuple::heaptuple::{FLOAT4OID, FLOAT8OID};

/// `MaxIndexTuplesPerPage` (itup.h) — the killedItems / pageData array bound.
/// `(BLCKSZ - SizeOfPageHeaderData) / (sizeof(ItemIdData) + sizeof(IndexTupleData))`
/// = `(8192 - 24) / (4 + 8)` = 680.
const MaxIndexTuplesPerPage: usize = 680;

/// `CHECK_FOR_INTERRUPTS()` (miscadmin.h) — the same behaviour-preserving no-op
/// the sibling AM scan layers use (the interrupt-processing owner is not on
/// this crate's dependency path).
#[inline]
fn check_for_interrupts() -> PgResult<()> {
    Ok(())
}

// ===========================================================================
// GISTScanOpaqueData as the A0 AM-opaque carrier payload.
// (The `AmOpaqueType` impl lives in `types-gist`, where the type is defined.)
// ===========================================================================

/// Downcast `scan.opaque` to the GiST scan working state (the A0 tag-checked
/// downcast). Panics with a clear message if the descriptor was not built by
/// `gistbeginscan` (a programming error — C would just cast `void *`).
fn gist<'a, 'mcx>(scan: &'a mut IndexScanDescData<'mcx>) -> &'a mut GISTScanOpaqueData<'mcx> {
    scan.opaque
        .as_deref_mut()
        .expect("GiST scan descriptor has no opaque (not built by gistbeginscan)")
        .downcast_mut::<GISTScanOpaqueData<'mcx>>()
        .expect("GiST scan opaque is not a GISTScanOpaqueData")
}

// ===========================================================================
// Pairing-heap comparator (gistscan.c:29 pairingheap_GISTSearchItem_cmp)
// ===========================================================================

/// `pairingheap_GISTSearchItem_cmp(a, b, arg)` (gistscan.c:29): order two
/// search-queue items. Lower distance wins; nulls sort first; among equal
/// distances heap items precede inner pages (depth-first). The C comparator's
/// `arg` is the scan's `numberOfOrderBys`; here each item carries its own
/// `distances` vector, so the comparator reads `min(a, b)` of them (they are
/// always equal-length within one scan).
///
/// `pairingheap_remove_first` returns the heap *root* — the item the comparator
/// ranks greatest. C's comparator returns `-float8_cmp` so smaller distances
/// compare *greater* and rise to the root; this returns the matching
/// `Ordering` so the same item is dequeued first.
fn pairingheap_gist_search_item_cmp(
    sa: &GISTSearchItem<'_>,
    sb: &GISTSearchItem<'_>,
) -> core::cmp::Ordering {
    use core::cmp::Ordering;

    // Order according to distance comparison (one entry per ORDER BY key).
    let n = sa.distances.len().min(sb.distances.len());
    for i in 0..n {
        let da = sa.distances[i];
        let db = sb.distances[i];
        if da.isnull {
            if !db.isnull {
                // C: return -1  (sa < sb)
                return Ordering::Less;
            }
        } else if db.isnull {
            // C: return 1
            return Ordering::Greater;
        } else {
            // cmp = -float8_cmp_internal(sa->distances[i], sb->distances[i]);
            // float8_cmp_internal orders NaN greatest and -0 == 0, which is
            // exactly total_cmp for finite/NaN ordering except for the sign of
            // zero — distances never carry -0.0 from a distance function, so
            // `total_cmp` reproduces float8_cmp_internal here.
            let cmp = db.value.total_cmp(&da.value); // -float8_cmp(a, b)
            if cmp != Ordering::Equal {
                return cmp;
            }
        }
    }

    // Heap items go before inner pages, to ensure a depth-first search. C ranks
    // a heap `sa` greater than an inner `sb` (return 1) so it dequeues first.
    let a_heap = GISTSearchItemIsHeap(sa);
    let b_heap = GISTSearchItemIsHeap(sb);
    if a_heap && !b_heap {
        return Ordering::Greater;
    }
    if !a_heap && b_heap {
        return Ordering::Less;
    }

    Ordering::Equal
}

// ===========================================================================
// gistkillitems (gistget.c:37)
// ===========================================================================

/// `gistkillitems(scan)` (gistget.c:37): set LP_DEAD on items an indexscan
/// caller reported killed. We re-read the page and verify its LSN is unchanged
/// since we last read it; if it changed we cannot safely apply the hints.
fn gistkillitems(scan: &mut IndexScanDescData<'_>) -> PgResult<()> {
    let index = scan.index_relation.alias();
    let (cur_blkno, cur_page_lsn, killed) = {
        let so = gist(scan);
        debug_assert!(so.curBlkno != InvalidBlockNumber);
        debug_assert!(so.curPageLSN != 0);
        debug_assert!(so.killedItems.is_some());
        (
            so.curBlkno,
            so.curPageLSN,
            so.killedItems.clone().unwrap_or_default(),
        )
    };

    // buffer = ReadBuffer(scan->indexRelation, so->curBlkno);
    let buffer = read_buffer::call(&index, cur_blkno)?;
    if !BufferIsValid(buffer) {
        return Ok(());
    }

    backend_storage_buffer_bufmgr_seams::lock_buffer::call(buffer, GIST_SHARE)?;
    gistcheckpage(index.name(), buffer)?;

    // If page LSN differs the page was modified; LP_DEAD hints are not safe.
    let lsn = buffer_get_lsn_atomic::call(buffer)?;
    if lsn != cur_page_lsn {
        unlock_release_buffer::call(buffer);
        gist(scan).numKilled = 0; // reset counter
        return Ok(());
    }

    // Mark all killedItems as dead, in place.
    let num_killed = gist(scan).numKilled as usize;
    let mut killedsomething = false;
    with_buffer_page::call(buffer, &mut |bytes: &mut [u8]| {
        // Assert(GistPageIsLeaf(page))
        debug_assert!(GistPageIsLeaf(bytes)?);
        for i in 0..num_killed {
            let offnum = killed[i];
            // iid = PageGetItemId(page, offnum); ItemIdMarkDead(iid);
            let mut iid = {
                let pref = PageRef::new(bytes)?;
                PageGetItemId(&pref, offnum)?
            };
            ItemIdMarkDead(&mut iid);
            let mut pmut = PageMut::new(bytes)?;
            backend_storage_page::PageSetItemId(&mut pmut, offnum, iid)?;
            killedsomething = true;
        }
        if killedsomething {
            // GistMarkPageHasGarbage(page)
            GistMarkPageHasGarbage(bytes)?;
        }
        Ok(())
    })?;

    if killedsomething {
        // MarkBufferDirtyHint(buffer, true);
        mark_buffer_dirty_hint::call(buffer, true);
    }

    unlock_release_buffer::call(buffer);

    // Always reset the scan state.
    gist(scan).numKilled = 0;
    Ok(())
}

// ===========================================================================
// gistindex_keytest (gistget.c:124)
// ===========================================================================

/// `gistindex_keytest(scan, tuple, page, offset, recheck_p,
/// recheck_distances_p)` (gistget.c:124): does this index tuple satisfy the
/// scan key(s)? Fills `so->distances[]` for an ordered scan. `page_is_leaf` is
/// the C `GistPageIsLeaf(page)` (the owned model passes the page leaf-ness
/// explicitly, since the page bytes are not carried in the entry). Returns
/// `(match, recheck, recheck_distances)`.
#[allow(clippy::too_many_arguments)]
fn gistindex_keytest<'mcx>(
    mcx: Mcx<'mcx>,
    so: &mut GISTScanOpaqueData<'mcx>,
    keys: &[ScanKeyData<'mcx>],
    order_bys: &[ScanKeyData<'mcx>],
    rel_oid: Oid,
    tuple: &[u8],
    page_is_leaf: bool,
    page_blkno: BlockNumber,
    offset: OffsetNumber,
) -> PgResult<(bool, bool, bool)> {
    let mut recheck_p = false;
    let mut recheck_distances_p = false;

    // If it's a leftover invalid tuple from pre-9.1, treat it as a match with
    // minimum possible distances (always follow it to the referenced page).
    if gist_tuple_is_invalid(tuple) {
        if page_is_leaf {
            // shouldn't happen
            return Err(ereport(ERROR)
                .errmsg_internal("invalid GiST tuple found on leaf page")
                .into_error());
        }
        for d in so.distances.iter_mut() {
            // -get_float8_infinity()
            d.value = f64::NEG_INFINITY;
            d.isnull = false;
        }
        return Ok((true, false, false));
    }

    // Check whether it matches according to the Consistent functions.
    let mut key_idx = 0usize;
    let key_size = keys.len();
    while key_idx < key_size {
        let key = &keys[key_idx];
        // datum = index_getattr(tuple, key->sk_attno, leafTupdesc, &isNull);
        let giststate: &GISTSTATE<'mcx> = &so.giststate;
        let (datum, is_null) = index_getattr_pub(mcx, tuple, key.sk_attno as i32, giststate)?;

        if (key.sk_flags & SK_ISNULL) != 0 {
            // On a non-leaf page union(VAL, NULL) is VAL, so we can't conclude
            // the child has no NULLs unless the non-leaf key IS NULL.
            if (key.sk_flags & SK_SEARCHNULL) != 0 {
                if page_is_leaf && !is_null {
                    return Ok((false, recheck_p, recheck_distances_p));
                }
            } else {
                debug_assert!(key.sk_flags & SK_SEARCHNOTNULL != 0);
                if is_null {
                    return Ok((false, recheck_p, recheck_distances_p));
                }
            }
        } else if is_null {
            return Ok((false, recheck_p, recheck_distances_p));
        } else {
            // gistdentryinit(giststate, key->sk_attno - 1, &de, datum, r, page,
            //                offset, false, isNull);
            let nkey = (key.sk_attno - 1) as usize;
            let de = gistdentryinit(
                mcx,
                &so.giststate,
                nkey,
                datum,
                rel_oid,
                page_blkno,
                offset,
                false,
                is_null,
            )?;
            // proc OID of the consistent support function for this column.
            let proc_oid = so.giststate.consistentFn[nkey].fn_oid;
            // FunctionCall5Coll(consistentFn, collation, &de, sk_argument,
            //                   sk_strategy, sk_subtype, &recheck);
            let res = gist_consistent::call(
                mcx,
                proc_oid,
                key.sk_collation,
                &de,
                page_is_leaf,
                &key.sk_argument,
                key.sk_strategy,
                key.sk_subtype,
            )?;
            if !res.matched {
                return Ok((false, recheck_p, recheck_distances_p));
            }
            recheck_p |= res.recheck;
        }

        key_idx += 1;
    }

    // OK, it passes --- now compute the distances.
    let mut ob_idx = 0usize;
    let ob_size = order_bys.len();
    while ob_idx < ob_size {
        let key = &order_bys[ob_idx];
        let giststate: &GISTSTATE<'mcx> = &so.giststate;
        let (datum, is_null) = index_getattr_pub(mcx, tuple, key.sk_attno as i32, giststate)?;

        if (key.sk_flags & SK_ISNULL) != 0 || is_null {
            // Assume distance computes as null.
            so.distances[ob_idx].value = 0.0;
            so.distances[ob_idx].isnull = true;
        } else {
            let nkey = (key.sk_attno - 1) as usize;
            let de = gistdentryinit(
                mcx,
                &so.giststate,
                nkey,
                datum,
                rel_oid,
                page_blkno,
                offset,
                false,
                is_null,
            )?;
            let proc_oid = so.giststate.distanceFn[nkey].fn_oid;
            // FunctionCall5Coll(distanceFn, collation, &de, sk_argument,
            //                   sk_strategy, sk_subtype, &recheck);
            let res = gist_distance::call(
                mcx,
                proc_oid,
                key.sk_collation,
                &de,
                page_is_leaf,
                &key.sk_argument,
                key.sk_strategy,
                key.sk_subtype,
            )?;
            recheck_distances_p |= res.recheck;
            so.distances[ob_idx].value = res.distance;
            so.distances[ob_idx].isnull = false;
        }

        ob_idx += 1;
    }

    Ok((true, recheck_p, recheck_distances_p))
}

// ===========================================================================
// gistScanPage (gistget.c:327)
// ===========================================================================

/// `gistScanPage(scan, pageItem, myDistances, tbm, ntids)` (gistget.c:327):
/// scan all items on the index page identified by `pageItem` and insert them
/// into the queue (or directly into output areas). `tbm`/`ntids` non-`None`
/// means a getbitmap scan. Returns the number of TIDs added to the bitmap (the
/// C `*ntids` increment), 0 for a plain/ordered scan.
fn gistScanPage<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
    page_item_blkno: BlockNumber,
    page_item_parentlsn: Option<types_gist::GistNSN>,
    my_distances: Option<&[IndexOrderByDistance]>,
    mut bitmap: Option<&mut AmTIDBitmap>,
) -> PgResult<i64> {
    debug_assert!(page_item_blkno != InvalidBlockNumber); // !GISTSearchItemIsHeap

    let index = scan.index_relation.alias();
    let rel_oid = index.rd_id;
    let snapshot = scan.xs_snapshot.clone();
    let num_order_bys = scan.number_of_order_bys;
    let want_itup = scan.xs_want_itup;
    let ignore_killed = scan.ignore_killed_tuples;
    let is_bitmap = bitmap.is_some();
    let mut ntids: i64 = 0;

    // buffer = ReadBuffer(scan->indexRelation, pageItem->blkno);
    let buffer = read_buffer::call(&index, page_item_blkno)?;
    backend_storage_buffer_bufmgr_seams::lock_buffer::call(buffer, GIST_SHARE)?;
    // PredicateLockPage(r, BufferGetBlockNumber(buffer), scan->xs_snapshot);
    let bgnum = buffer_get_block_number::call(buffer);
    predicate_lock_page::call(
        index.alias(),
        bgnum,
        snapshot.clone().map(Rc::new),
    )?;
    gistcheckpage(index.name(), buffer)?;

    // page = BufferGetPage(buffer); opaque = GistPageGetOpaque(page);
    let page = buffer_get_page::call(mcx, buffer)?;
    let page_is_leaf = GistPageIsLeaf(&page)?;
    let page_is_deleted = GistPageIsDeleted(&page)?;
    let follow_right = crate::gist_page::GistFollowRight(&page)?;
    let page_nsn = gist_page_get_nsn(&page)?;
    let rightlink = crate::gist_page::gist_page_rightlink(&page)?;

    // Check if we need to follow the rightlink (concurrent split / crash).
    if let Some(parentlsn) = page_item_parentlsn {
        if parentlsn != 0
            && (follow_right || parentlsn < page_nsn)
            && rightlink != InvalidBlockNumber
        {
            // There was a page split, follow right link to add pages. This
            // can't happen when starting at the root.
            let my_distances = my_distances.expect("gistScanPage: rightlink follow at root");
            let item = GISTSearchItem {
                blkno: rightlink,
                data: GISTSearchItemData::Parentlsn(parentlsn),
                distances: my_distances.to_vec(),
            };
            gist(scan).queue.add(item)?;
        }
    }

    // Check if the page was deleted after we saw the downlink.
    if page_is_deleted {
        unlock_release_buffer::call(buffer);
        return Ok(0);
    }

    {
        let so = gist(scan);
        so.nPageData = 0;
        so.curPageData = 0;
    }
    scan.xs_hitup = None; // might point into pageDataCxt
                          // (pageDataCxt reset is subsumed by ownership)

    // Save the LSN of the page as read, so we know later whether LP_DEAD hints
    // are safe.
    let page_lsn = buffer_get_lsn_atomic::call(buffer)?;
    gist(scan).curPageLSN = page_lsn;

    // The scan keys / order-bys are fixed for the whole page scan; snapshot
    // them once so the per-tuple `gistindex_keytest` can borrow `so` mutably
    // without re-borrowing the descriptor.
    let keys = scan_keys(scan);
    let order_bys = scan_order_bys(scan);

    // check all tuples on page
    let pref = PageRef::new(&page)?;
    let maxoff = PageGetMaxOffsetNumber(&pref);
    let mut i = FIRST_OFFSET_NUMBER;
    while i <= maxoff {
        let iid = PageGetItemId(&pref, i)?;

        // If the scan specifies not to return killed tuples, treat a killed
        // tuple as not passing the qual.
        if ignore_killed && ItemIdIsDead(&iid) {
            i += 1;
            continue;
        }

        let it = PageGetItem(&pref, &iid)?;

        // Must call gistindex_keytest in tempCxt and clean up afterward
        // (subsumed by ownership in the owned model).
        let (matched, recheck, recheck_distances) = {
            let so = gist(scan);
            gistindex_keytest(
                mcx,
                so,
                &keys,
                &order_bys,
                rel_oid,
                it,
                page_is_leaf,
                page_item_blkno,
                i,
            )?
        };
        // re-borrow `it` (the keytest borrow of `scan` ended) — recompute the
        // page item view (cheap; the page snapshot is unchanged).
        let pref = PageRef::new(&page)?;
        let iid = PageGetItemId(&pref, i)?;
        let it = PageGetItem(&pref, &iid)?;

        if !matched {
            i += 1;
            continue;
        }

        if is_bitmap && page_is_leaf {
            // getbitmap scan: push the heap TID into the bitmap without ordering.
            // tbm_add_tuples(tbm, &it->t_tid, 1, recheck); (*ntids)++;
            let tid = itup_heap_ptr(it);
            let tbm = bitmap
                .as_deref_mut()
                .expect("gistScanPage: bitmap scan with no bitmap");
            let tbm_concrete = tbm
                .payload
                .as_mut()
                .and_then(|pl| pl.downcast_mut::<types_tidbitmap::TIDBitmap>())
                .expect("amgetbitmap TIDBitmap payload is not a types_tidbitmap::TIDBitmap");
            backend_nodes_core_seams::tbm_add_tuples::call(tbm_concrete, &[tid], recheck)?;
            ntids += 1;
        } else if num_order_bys == 0 && page_is_leaf {
            // Non-ordered scan: report tuples in so->pageData[].
            let recontup = if want_itup {
                Some(gistFetchTuple(mcx, &gist(scan).giststate, &index, it)?)
            } else {
                None
            };
            let heap_item = GISTSearchHeapItem {
                heapPtr: itup_heap_ptr(it),
                recheck,
                recheckDistances: false,
                recontup: recontup.map(heaptuple_to_heaptuple),
                offnum: i,
            };
            let so = gist(scan);
            so.pageData.push(heap_item);
            so.nPageData += 1;
        } else {
            // Must push item into search queue. We get here for any lower index
            // page, and also for heap tuples in an ordered search.
            let n_order_bys = num_order_bys as usize;
            let distances: Vec<IndexOrderByDistance> =
                gist(scan).distances[..n_order_bys].to_vec();
            let item = if page_is_leaf {
                // Creating heap-tuple GISTSearchItem.
                let recontup = if want_itup {
                    Some(gistFetchTuple(mcx, &gist(scan).giststate, &index, it)?)
                } else {
                    None
                };
                GISTSearchItem {
                    blkno: InvalidBlockNumber,
                    data: GISTSearchItemData::Heap(GISTSearchHeapItem {
                        heapPtr: itup_heap_ptr(it),
                        recheck,
                        recheckDistances: recheck_distances,
                        recontup: recontup.map(heaptuple_to_heaptuple),
                        offnum: 0,
                    }),
                    distances,
                }
            } else {
                // Creating index-page GISTSearchItem. blkno =
                // ItemPointerGetBlockNumber(&it->t_tid). LSN of current page is
                // the parent LSN for the child.
                let child_parentlsn = buffer_get_lsn_atomic::call(buffer)?;
                GISTSearchItem {
                    blkno: crate::gistutil::itup_block_number(it),
                    data: GISTSearchItemData::Parentlsn(child_parentlsn),
                    distances,
                }
            };
            gist(scan).queue.add(item)?;
        }

        i += 1;
    }

    unlock_release_buffer::call(buffer);

    Ok(ntids)
}

/// Read the scan's index quals into an owned slice for `gistindex_keytest`
/// (the keytest borrows `so` mutably, so the keys are copied out first).
fn scan_keys<'mcx>(scan: &IndexScanDescData<'mcx>) -> Vec<ScanKeyData<'mcx>> {
    scan.key_data.clone()
}

fn scan_order_bys<'mcx>(scan: &IndexScanDescData<'mcx>) -> Vec<ScanKeyData<'mcx>> {
    scan.order_by_data.clone()
}

/// Project a `FormedTuple` to its `HeapTupleData` box for the `recontup` field
/// (`HeapTuple<'mcx>` = `Option<PgBox<HeapTupleData>>`); used via `.map(...)` on
/// the `Option<FormedTuple>` from `gistFetchTuple`.
fn heaptuple_to_heaptuple<'mcx>(
    t: backend_access_common_heaptuple::FormedTuple<'mcx>,
) -> PgBox<'mcx, types_tuple::heaptuple::HeapTupleData<'mcx>> {
    t.tuple
}

// ===========================================================================
// getNextGISTSearchItem / getNextNearest (gistget.c:537 / :559)
// ===========================================================================

/// `getNextGISTSearchItem(so)` (gistget.c:537): extract the next item (in
/// order) from the search queue, or `None` if empty.
fn getNextGISTSearchItem<'mcx>(
    so: &mut GISTScanOpaqueData<'mcx>,
) -> Option<GISTSearchItem<'mcx>> {
    so.queue.remove_first()
}

/// `getNextNearest(scan)` (gistget.c:559): fetch the next heap tuple in an
/// ordered (KNN) search. Returns true and fills `xs_heaptid` / `xs_recheck` /
/// the ORDER BY distances when a heap item is found.
fn getNextNearest<'mcx>(mcx: Mcx<'mcx>, scan: &mut IndexScanDescData<'mcx>) -> PgResult<bool> {
    // free previously returned tuple
    scan.xs_hitup = None;

    let order_by_types = gist(scan).orderByTypes.clone();
    let want_itup = scan.xs_want_itup;

    loop {
        let item = match getNextGISTSearchItem(gist(scan)) {
            Some(it) => it,
            None => return Ok(false),
        };

        if GISTSearchItemIsHeap(&item) {
            // found a heap item at currently minimal distance
            if let GISTSearchItemData::Heap(heap) = &item.data {
                scan.xs_heaptid = heap.heapPtr;
                scan.xs_recheck = heap.recheck;

                index_store_float8_orderby_distances(
                    scan,
                    &order_by_types,
                    Some(&item.distances),
                    heap.recheckDistances,
                )?;

                // in an index-only scan, also return the reconstructed tuple
                if want_itup {
                    scan.xs_hitup = heap.recontup.clone();
                }
            }
            return Ok(true);
        } else {
            // visit an index page, extract its items into queue
            // CHECK_FOR_INTERRUPTS();
            check_for_interrupts()?;

            gistScanPage(
                mcx,
                scan,
                item.blkno,
                item_parentlsn(&item),
                Some(&item.distances),
                None,
            )?;
        }
    }
}

/// The parent LSN of an index-page search item (`item->data.parentlsn`), or
/// `None` for a heap item (its `data` is the heap union member).
fn item_parentlsn(item: &GISTSearchItem<'_>) -> Option<types_gist::GistNSN> {
    match &item.data {
        GISTSearchItemData::Parentlsn(lsn) => Some(*lsn),
        GISTSearchItemData::Heap(_) => None,
    }
}

// ===========================================================================
// index_store_float8_orderby_distances (genam.c — generic helper)
// ===========================================================================

/// `index_store_float8_orderby_distances(scan, orderByTypes, distances,
/// recheckOrderBy)` (genam.c): convert the AM distance function's results to
/// the ORDER BY operator result types and save them into the scan's
/// `xs_orderbyvals` / `xs_orderbynulls`. This generic genam helper is owned by
/// `indexam.c`, which sits *above* the AM layer (a dep would cycle), so the AM
/// carries its own copy — the body operates only on the scan descriptor.
fn index_store_float8_orderby_distances<'mcx>(
    scan: &mut IndexScanDescData<'mcx>,
    order_by_types: &[Oid],
    distances: Option<&[IndexOrderByDistance]>,
    recheck_orderby: bool,
) -> PgResult<()> {
    debug_assert!(distances.is_some() || !recheck_orderby);

    scan.xs_recheckorderby = recheck_orderby;

    for i in 0..scan.number_of_order_bys as usize {
        let typ = order_by_types[i];
        let d = distances.map(|ds| ds[i]);
        if typ == FLOAT8OID {
            if let Some(d) = d {
                if !d.isnull {
                    scan.xs_orderbyvals[i] = Datum::ByVal(d.value.to_bits() as usize);
                    scan.xs_orderbynulls[i] = false;
                    continue;
                }
            }
            scan.xs_orderbyvals[i] = Datum::null();
            scan.xs_orderbynulls[i] = true;
        } else if typ == FLOAT4OID {
            if let Some(d) = d {
                if !d.isnull {
                    scan.xs_orderbyvals[i] =
                        Datum::ByVal((d.value as f32).to_bits() as usize);
                    scan.xs_orderbynulls[i] = false;
                    continue;
                }
            }
            scan.xs_orderbyvals[i] = Datum::null();
            scan.xs_orderbynulls[i] = true;
        } else {
            // We don't know how to convert the float8 bound to this type. Only
            // insist on converting if the recheck flag is set.
            if scan.xs_recheckorderby {
                return Err(ereport(ERROR)
                    .errmsg_internal(
                        "ORDER BY operator must return float8 or float4 if the \
                         distance function is lossy",
                    )
                    .into_error());
            }
            scan.xs_orderbynulls[i] = true;
        }
    }

    Ok(())
}

// ===========================================================================
// gistgettuple (gistget.c:611)
// ===========================================================================

/// `gistgettuple(scan, dir)` (gistget.c:611): get the next tuple in the scan.
pub fn gistgettuple<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
    dir: ScanDirection,
) -> PgResult<bool> {
    if dir != ForwardScanDirection {
        return Err(ereport(ERROR)
            .errmsg_internal("GiST only supports forward scan direction")
            .into_error());
    }

    if !gist(scan).qual_ok {
        return Ok(false);
    }

    if gist(scan).firstCall {
        // Begin the scan by processing the root page.
        pgstat_count_index_scan::call(scan.index_relation.rd_id, scan.index_relation.pgstat_enabled);
        if let Some(instr) = scan.instrument.as_mut() {
            instr.nsearches += 1;
        }

        {
            let so = gist(scan);
            so.firstCall = false;
            so.curPageData = 0;
            so.nPageData = 0;
        }
        scan.xs_hitup = None;

        // fakeItem.blkno = GIST_ROOT_BLKNO; parentlsn = 0;
        gistScanPage(mcx, scan, GIST_ROOT_BLKNO, Some(0), None, None)?;
    }

    if scan.number_of_order_bys > 0 {
        // Must fetch tuples in strict distance order.
        return getNextNearest(mcx, scan);
    }

    // Fetch tuples index-page-at-a-time.
    loop {
        if gist(scan).curPageData < gist(scan).nPageData {
            if scan.kill_prior_tuple && gist(scan).curPageData > 0 {
                ensure_killed_items(scan);
                let so = gist(scan);
                if (so.numKilled as usize) < MaxIndexTuplesPerPage {
                    let off = so.pageData[(so.curPageData - 1) as usize].offnum;
                    if let Some(k) = so.killedItems.as_mut() {
                        k[so.numKilled as usize] = off;
                    }
                    so.numKilled += 1;
                }
            }
            // continuing to return tuples from a leaf page
            let (heap_ptr, recheck, recontup) = {
                let so = gist(scan);
                let cur = so.curPageData as usize;
                (
                    so.pageData[cur].heapPtr,
                    so.pageData[cur].recheck,
                    so.pageData[cur].recontup.clone(),
                )
            };
            scan.xs_heaptid = heap_ptr;
            scan.xs_recheck = recheck;
            if scan.xs_want_itup {
                scan.xs_hitup = recontup;
            }
            gist(scan).curPageData += 1;
            return Ok(true);
        }

        // Check the last returned tuple and add it to killedItems if necessary.
        if scan.kill_prior_tuple
            && gist(scan).curPageData > 0
            && gist(scan).curPageData == gist(scan).nPageData
        {
            ensure_killed_items(scan);
            let so = gist(scan);
            if (so.numKilled as usize) < MaxIndexTuplesPerPage {
                let off = so.pageData[(so.curPageData - 1) as usize].offnum;
                if let Some(k) = so.killedItems.as_mut() {
                    k[so.numKilled as usize] = off;
                }
                so.numKilled += 1;
            }
        }

        // find and process the next index page
        loop {
            if gist(scan).curBlkno != InvalidBlockNumber && gist(scan).numKilled > 0 {
                gistkillitems(scan)?;
            }

            let item = match getNextGISTSearchItem(gist(scan)) {
                Some(it) => it,
                None => return Ok(false),
            };

            check_for_interrupts()?;

            // save current item BlockNumber for next gistkillitems()
            gist(scan).curBlkno = item.blkno;

            gistScanPage(
                mcx,
                scan,
                item.blkno,
                item_parentlsn(&item),
                Some(&item.distances),
                None,
            )?;

            if gist(scan).nPageData != 0 {
                break;
            }
        }
    }
}

/// `if (so->killedItems == NULL) so->killedItems = palloc(...)` — lazily
/// allocate the killed-items array.
fn ensure_killed_items(scan: &mut IndexScanDescData<'_>) {
    let so = gist(scan);
    if so.killedItems.is_none() {
        so.killedItems = Some(vec![0 as OffsetNumber; MaxIndexTuplesPerPage]);
    }
}

// ===========================================================================
// gistgetbitmap (gistget.c:744)
// ===========================================================================

/// `gistgetbitmap(scan, tbm)` (gistget.c:744): build a bitmap of all heap tuple
/// locations matching the scan keys. Returns the number of TIDs added.
pub fn gistgetbitmap<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
    tbm: &mut AmTIDBitmap,
) -> PgResult<i64> {
    if !gist(scan).qual_ok {
        return Ok(0);
    }

    pgstat_count_index_scan::call(scan.index_relation.rd_id, scan.index_relation.pgstat_enabled);
    if let Some(instr) = scan.instrument.as_mut() {
        instr.nsearches += 1;
    }

    {
        let so = gist(scan);
        so.curPageData = 0;
        so.nPageData = 0;
    }
    scan.xs_hitup = None;

    let mut ntids: i64 = 0;

    // Begin the scan by processing the root page.
    ntids += gistScanPage(mcx, scan, GIST_ROOT_BLKNO, Some(0), None, Some(tbm))?;

    loop {
        let item = match getNextGISTSearchItem(gist(scan)) {
            Some(it) => it,
            None => break,
        };
        check_for_interrupts()?;
        ntids += gistScanPage(
            mcx,
            scan,
            item.blkno,
            item_parentlsn(&item),
            Some(&item.distances),
            Some(tbm),
        )?;
    }

    Ok(ntids)
}

// ===========================================================================
// gistbeginscan / gistrescan / gistendscan (gistscan.c)
// ===========================================================================

/// `gistbeginscan(r, nkeys, norderbys)` (gistscan.c:73): set up a GiST index
/// scan — the `GISTSTATE` opclass dispatch + tuple descriptors and the
/// `GISTScanOpaqueData` working state (queue, distances, output areas).
pub fn gistbeginscan<'mcx>(
    mcx: Mcx<'mcx>,
    r: &Relation<'mcx>,
    norderbys: i32,
) -> PgResult<GISTScanOpaqueData<'mcx>> {
    // giststate = initGISTstate(scan->indexRelation);
    let giststate = crate::gistutil::initGISTstate(mcx, r)?;

    // so->distances = palloc(sizeof(distances[0]) * numberOfOrderBys);
    let distances = vec![IndexOrderByDistance::default(); norderbys as usize];

    let so = GISTScanOpaqueData {
        giststate,
        // queue created (empty) in gistrescan; allocate now so the field is
        // well-formed (rescan replaces it).
        queue: backend_lib_pairingheap::pairingheap_allocate(pairingheap_gist_search_item_cmp),
        queueCxt: mcx,
        pageDataCxt: None,
        qual_ok: true, // in case there are zero keys
        firstCall: true,
        distances,
        orderByTypes: Vec::new(),
        killedItems: None, // until needed
        numKilled: 0,
        curBlkno: InvalidBlockNumber,
        curPageLSN: 0, // InvalidXLogRecPtr
        pageData: Vec::new(),
        nPageData: 0,
        curPageData: 0,
    };

    Ok(so)
}

/// `gistrescan(scan, key, nkeys, orderbys, norderbys)` (gistscan.c:126): reset
/// scan state for a (re)scan — install a fresh empty pairing heap, copy the
/// scan keys, replacing each operator with its opclass Consistent (resp.
/// Distance) support function and computing the order-by result types.
pub fn gistrescan<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
    keys: &[ScanKeyData<'mcx>],
    orderbys: &[ScanKeyData<'mcx>],
) -> PgResult<()> {
    let index_name = scan.index_relation.name().to_string();

    // If we're doing an index-only scan, on the first call also initialize the
    // fetch tuple descriptor representing the returned index tuples.
    if scan.xs_want_itup && scan.xs_hitupdesc.is_none() {
        // Construct a descriptor with the original data types (the storage type
        // may differ from the original indexed type).
        let natts = scan.index_relation.rd_att.natts as usize;
        let nkeyatts = scan.index_relation.indnkeyatts() as usize;
        let fetch = build_fetch_tupdesc(mcx, scan, natts, nkeyatts)?;
        // scan->xs_hitupdesc = so->giststate->fetchTupdesc; (C aliases the same
        // descriptor; the owned model stores an equivalent copy in each field).
        scan.xs_hitupdesc = Some(AllocBox::new(fetch.clone_in(mcx)?));
        gist(scan).giststate.fetchTupdesc = Some(fetch);
    }

    // Create a new, empty pairing heap for the search queue.
    gist(scan).queue =
        backend_lib_pairingheap::pairingheap_allocate(pairingheap_gist_search_item_cmp);
    gist(scan).firstCall = true;

    // Update scan key, if a new one is given. (numberOfKeys / numberOfOrderBys
    // arguments are ignored in C; here `keys` is the new array.)
    if !keys.is_empty() && scan.number_of_keys > 0 {
        // memcpy(scan->keyData, key, ...). Replace the operator function with
        // the opclass Consistent support function; set qual_ok.
        let mut new_keys: Vec<ScanKeyData<'mcx>> = Vec::with_capacity(keys.len());
        let mut qual_ok = true;
        for skey in keys.iter() {
            let mut nk = skey.clone();
            // fmgr_info_copy(&skey->sk_func, &consistentFn[attno-1], scanCxt)
            let nkey = (skey.sk_attno - 1) as usize;
            nk.sk_func = gist(scan).giststate.consistentFn[nkey].clone();
            if (skey.sk_flags & SK_ISNULL) != 0
                && (skey.sk_flags & (SK_SEARCHNULL | SK_SEARCHNOTNULL)) == 0
            {
                qual_ok = false;
            }
            new_keys.push(nk);
        }
        scan.key_data = new_keys;
        gist(scan).qual_ok = qual_ok;
    }

    // Update order-by key, if a new one is given.
    if !orderbys.is_empty() && scan.number_of_order_bys > 0 {
        let mut new_obs: Vec<ScanKeyData<'mcx>> = Vec::with_capacity(orderbys.len());
        let mut order_by_types: Vec<Oid> = Vec::with_capacity(orderbys.len());
        for skey in orderbys.iter() {
            let nkey = (skey.sk_attno - 1) as usize;
            // Check we actually have a distance function ...
            let finfo_oid = gist(scan).giststate.distanceFn[nkey].fn_oid;
            if !OidIsValid(finfo_oid) {
                return Err(ereport(ERROR)
                    .errmsg_internal(alloc::format!(
                        "missing support function {} for attribute {} of index \"{}\"",
                        GIST_DISTANCE_PROC, skey.sk_attno, index_name
                    ))
                    .into_error());
            }
            // get_func_rettype(skey->sk_func.fn_oid)
            let rettype = backend_utils_cache_lsyscache_seams::get_func_rettype::call(
                skey.sk_func.fn_oid,
            )?;
            order_by_types.push(rettype);
            // fmgr_info_copy(&skey->sk_func, &distanceFn[attno-1], scanCxt)
            let mut nk = skey.clone();
            nk.sk_func = gist(scan).giststate.distanceFn[nkey].clone();
            new_obs.push(nk);
        }
        scan.order_by_data = new_obs;
        gist(scan).orderByTypes = order_by_types;
    }

    // any previous xs_hitup will have been freed in the resets above
    scan.xs_hitup = None;

    Ok(())
}

/// Build the index-only-scan fetch tuple descriptor (`gistrescan`'s
/// `CreateTemplateTupleDesc` + `TupleDescInitEntry` loops): key columns take
/// their opcintype (the original indexed type), included columns take their
/// stored leaf type.
fn build_fetch_tupdesc<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &IndexScanDescData<'mcx>,
    natts: usize,
    nkeyatts: usize,
) -> PgResult<mcx::PgBox<'mcx, types_tuple::heaptuple::TupleDescData<'mcx>>> {
    let index = &scan.index_relation;
    // CreateTemplateTupleDesc(natts)
    let mut td = backend_access_common_tupdesc::CreateTemplateTupleDesc(mcx, natts as i32)?;

    // Key columns: opcintype.
    for attno in 1..=nkeyatts {
        let typid = index.rd_opcintype[attno - 1];
        backend_access_common_tupdesc::TupleDescInitEntry(
            &mut td,
            attno as types_core::primitive::AttrNumber,
            None,
            typid,
            -1,
            0,
        )?;
    }
    // Included columns: leaf-descriptor type.
    let leaf = gist(scan_const_to_mut(scan))
        .giststate
        .leafTupdesc
        .as_ref()
        .expect("gistrescan: leafTupdesc not initialized")
        .clone_in(mcx)?;
    for attno in (nkeyatts + 1)..=natts {
        let typid = leaf.attr(attno - 1).atttypid;
        backend_access_common_tupdesc::TupleDescInitEntry(
            &mut td,
            attno as types_core::primitive::AttrNumber,
            None,
            typid,
            -1,
            0,
        )?;
    }

    mcx::alloc_in(mcx, td)
}

/// `freeGISTstate` is a no-op in the owned model: the `GISTSTATE` and the whole
/// scan working state ride the scan's `mcx` arena (and the boxed opaque), which
/// is dropped at end of scan. Provided for call-site parity with C.
pub fn gistendscan(_scan: &mut IndexScanDescData<'_>) -> PgResult<()> {
    // freeGISTstate(so->giststate); — drop handles cleanup.
    Ok(())
}

/// `gistcanreturn(index, attno)` (gistget.c:797): can the AM do an index-only
/// scan on this column? True for INCLUDE columns, columns with a fetch method,
/// or columns with no compress method.
pub fn gistcanreturn(index: &Relation<'_>, attno: i32) -> PgResult<bool> {
    if attno > index.indnkeyatts() {
        return Ok(true);
    }
    let fetch = index_getprocid::call(index, attno as types_core::primitive::AttrNumber, GIST_FETCH_PROC as u16)?;
    if OidIsValid(fetch) {
        return Ok(true);
    }
    let compress =
        index_getprocid::call(index, attno as types_core::primitive::AttrNumber, GIST_COMPRESS_PROC as u16)?;
    Ok(!OidIsValid(compress))
}

// ===========================================================================
// gisthandler + AM-vtable adapters (F2)
// ===========================================================================

/// `VACUUM_OPTION_PARALLEL_BULKDEL | VACUUM_OPTION_PARALLEL_COND_CLEANUP`
/// (commands/vacuum.h): `(1 << 0) | (1 << 1)`.
const VACUUM_OPTION_PARALLEL_BULKDEL: u8 = 1 << 0;
const VACUUM_OPTION_PARALLEL_COND_CLEANUP: u8 = 1 << 1;

/// `gisthandler()` (gist.c) — return the [`IndexAmRoutine`] with GiST's AM
/// parameters and callbacks. GiST supports ORDER BY result-of-operator (KNN),
/// both amgettuple and amgetbitmap.
pub fn gisthandler() -> IndexAmRoutine {
    IndexAmRoutine {
        type_: T_IndexAmRoutine,
        amstrategies: 0,
        amsupport: types_gist::GISTNProcs as u16,
        amoptsprocnum: types_gist::GIST_OPTIONS_PROC as u16,
        amcanorder: false,
        amcanorderbyop: true,
        amcanhash: false,
        amconsistentequality: false,
        amconsistentordering: false,
        amcanbackward: false,
        amcanunique: false,
        amcanmulticol: true,
        amoptionalkey: true,
        amsearcharray: false,
        amsearchnulls: true,
        amstorage: true,
        amclusterable: true,
        ampredlocks: true,
        amcanparallel: false,
        amcanbuildparallel: false,
        amcaninclude: true,
        amusemaintenanceworkmem: false,
        amsummarizing: false,
        amparallelvacuumoptions: VACUUM_OPTION_PARALLEL_BULKDEL
            | VACUUM_OPTION_PARALLEL_COND_CLEANUP,
        amkeytype: InvalidOid,

        // gistvalidate returns a soft-error result + needs an Mcx, so it cannot
        // ride the raw fn-ptr `amvalidate` slot; it is reached by name through
        // amapi's `amvalidate` dispatch (backend-access-gist-validate), exactly
        // like bt/hash/gin/brin.
        amvalidate: None,
        amtranslatestrategy: None,
        // gisttranslatecmptype dispatches the opclass's GIST_TRANSLATE_CMPTYPE_PROC
        // support function (fmgr), so it needs an Mcx and is fallible — it cannot
        // ride the infallible/context-free `amtranslatecmptype` fn-ptr slot.
        // It is reached by name (backend-access-gist-core::gisttranslatecmptype).
        amtranslatecmptype: None,

        // Build / options / plan-time callbacks (#340). `gistproperty` is this
        // crate's own fn (wired directly, with the canonical-enum mapping).
        // `gistbuild`/`gistbuildempty` (gist-build, above this crate),
        // `gistoptions` (needs the reloptions Datum detoast the #341 dispatch
        // does), `gistcostestimate` (selfuncs.c) and `gistadjustmembers`
        // (gist-validate) are sanctioned panic legs reached via #341. GiST has
        // no gettreeheight/buildphasename (NULL in C).
        ambuild: gistbuild_am,
        ambuildempty: gistbuildempty_am,
        amcostestimate: gistcostestimate_am,
        amgettreeheight: None,
        amoptions: gistoptions_am,
        amproperty: Some(gistproperty_am),
        ambuildphasename: None,
        amadjustmembers: Some(gistadjustmembers_am),

        // Insert / vacuum callbacks — NOT this F2-scan unit's logic. Reached by
        // name only through the vtable; the serial scan path never invokes
        // them. Adapters seam-and-panic into the GiST insert/vacuum lanes.
        aminsert: gistinsert_am,
        aminsertcleanup: None,
        ambulkdelete: gistbulkdelete_am,
        amvacuumcleanup: gistvacuumcleanup_am,

        // Scan callbacks (F2).
        ambeginscan: gistbeginscan_am,
        amrescan: gistrescan_am,
        amendscan: gistendscan_am,
        amcanreturn: Some(gistcanreturn_am),
        amgettuple: Some(gistgettuple_am),
        amgetbitmap: Some(gistgetbitmap_am),
        ammarkpos: None,
        amrestrpos: None,

        // No parallel index scan (amcanparallel = false).
        amestimateparallelscan: None,
        aminitparallelscan: None,
        amparallelrescan: None,
    }
}

// ---------------------------------------------------------------------------
// Build / options / plan-time vtable adapters (#340). See the doc comment in
// `gisthandler` for why the build / cross-crate slots are sanctioned panic legs
// (reached via the #341 index.c dispatch).

/// `ambuild` adapter — `gistbuild` (gist-build, above this crate) needs the real
/// `IndexInfo`; reached via the #341 dispatch.
fn gistbuild_am<'mcx>(
    _mcx: Mcx<'mcx>,
    _heap_relation: &Relation<'mcx>,
    _index_relation: &Relation<'mcx>,
    _index_info: &mut IndexInfoCarrier<'_, 'mcx>,
) -> PgResult<IndexBuildResult> {
    panic!(
        "gistbuild: index.c build dispatch (#341) not yet ported — \
         gistbuild lives in backend-access-gist-build and needs the real IndexInfo"
    )
}

/// `ambuildempty` adapter — `gistbuildempty` (gist-build) not reachable from
/// this crate; reached via the #341 dispatch.
fn gistbuildempty_am<'mcx>(_mcx: Mcx<'mcx>, _index_relation: &Relation<'mcx>) -> PgResult<()> {
    panic!("gistbuildempty: lives in backend-access-gist-build, not reachable from gist-core (#341)")
}

/// `amcostestimate` adapter — `gistcostestimate` (selfuncs.c) not reachable;
/// reached via the #341 dispatch.
fn gistcostestimate_am<'mcx>(
    _mcx: Mcx<'mcx>,
    _root: &mut PlannerInfo,
    _path: &mut IndexPath,
    _loop_count: f64,
) -> PgResult<AmCostEstimate> {
    panic!("gistcostestimate: index cost estimation (selfuncs.c) not yet reachable from gist (#341)")
}

/// `amoptions` adapter — `gistoptions` takes the parsed reloptions byte image,
/// which requires the reloptions `Datum` detoast the #341 dispatch performs.
fn gistoptions_am<'mcx>(
    _mcx: Mcx<'mcx>,
    _reloptions: Datum<'mcx>,
    _validate: bool,
) -> PgResult<Option<Vec<u8>>> {
    panic!("gistoptions: needs the reloptions Datum detoast done by the index.c dispatch (#341)")
}

/// `amproperty` adapter — wires this crate's `gistproperty`, mapping the
/// canonical `IndexAMProperty` to GiST's local enum and writing the out-params.
fn gistproperty_am(
    index_oid: Oid,
    attno: i32,
    prop: AmIndexAMProperty,
    _propname: &str,
    res: &mut bool,
    isnull: &mut bool,
) -> PgResult<bool> {
    let gprop = match prop {
        AmIndexAMProperty::AMPROP_DISTANCE_ORDERABLE => GistIndexAMProperty::DistanceOrderable,
        AmIndexAMProperty::AMPROP_RETURNABLE => GistIndexAMProperty::Returnable,
        _ => GistIndexAMProperty::Other,
    };
    let (handled, r, n) = gistproperty(index_oid, attno, gprop)?;
    *res = r;
    *isnull = n;
    Ok(handled)
}

/// `amadjustmembers` adapter — `gistadjustmembers` (gist-validate) not reachable
/// from this crate; reached via the #341 dispatch.
fn gistadjustmembers_am<'mcx>(
    _mcx: Mcx<'mcx>,
    _opfamilyoid: Oid,
    _opclassoid: Oid,
    _operators: &mut Vec<OpFamilyMember>,
    _functions: &mut Vec<OpFamilyMember>,
) -> PgResult<()> {
    panic!("gistadjustmembers: opclass member adjust (gist-validate) not yet reachable from gist-core (#341)")
}

/// `ambeginscan` adapter — build the unified descriptor with `opaque` holding a
/// freshly-erased [`GISTScanOpaqueData`] (the A0 erase pattern).
fn gistbeginscan_am<'mcx>(
    mcx: Mcx<'mcx>,
    index_relation: &Relation<'mcx>,
    nkeys: i32,
    norderbys: i32,
) -> PgResult<IndexScanDesc<'mcx>> {
    let so = gistbeginscan(mcx, index_relation, norderbys)?;
    let mut desc = relation_get_index_scan(mcx, index_relation, nkeys, norderbys)?;
    desc.opaque = Some(erase_gistscan(mcx, so)?);
    Ok(desc)
}

fn gistrescan_am<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
    keys: &[ScanKeyData<'mcx>],
    orderbys: &[ScanKeyData<'mcx>],
) -> PgResult<()> {
    gistrescan(mcx, scan, keys, orderbys)
}

fn gistendscan_am<'mcx>(_mcx: Mcx<'mcx>, scan: &mut IndexScanDescData<'mcx>) -> PgResult<()> {
    gistendscan(scan)
}

fn gistcanreturn_am(index: &Relation<'_>, attno: i32) -> PgResult<bool> {
    gistcanreturn(index, attno)
}

fn gistgettuple_am<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
    direction: ScanDirection,
) -> PgResult<bool> {
    gistgettuple(mcx, scan, direction)
}

fn gistgetbitmap_am<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
    tbm: &mut AmTIDBitmap,
) -> PgResult<i64> {
    gistgetbitmap(mcx, scan, tbm)
}

// Insert / vacuum adapters — dispatch into the (unported) GiST insert/vacuum
// lanes through `backend-access-gist-am-seams`.

#[allow(clippy::too_many_arguments)]
fn gistinsert_am<'mcx>(
    mcx: Mcx<'mcx>,
    index_relation: &Relation<'mcx>,
    values: &[Datum<'mcx>],
    isnull: &[bool],
    heap_tid: &ItemPointerData,
    heap_relation: &Relation<'mcx>,
    check_unique: IndexUniqueCheck,
    index_unchanged: bool,
    index_info: &mut IndexInfoCarrier<'_, 'mcx>,
) -> PgResult<bool> {
    gistinsert::call(
        mcx,
        index_relation,
        values,
        isnull,
        heap_tid,
        heap_relation,
        check_unique,
        index_unchanged,
        index_info,
    )
}

fn gistbulkdelete_am<'mcx>(
    mcx: Mcx<'mcx>,
    info: &IndexVacuumInfo<'mcx>,
    stats: Option<IndexBulkDeleteResult>,
    callback_state: Option<u64>,
) -> PgResult<Option<IndexBulkDeleteResult>> {
    gistbulkdelete::call(mcx, info, stats, callback_state)
}

fn gistvacuumcleanup_am<'mcx>(
    mcx: Mcx<'mcx>,
    info: &IndexVacuumInfo<'mcx>,
    stats: Option<IndexBulkDeleteResult>,
) -> PgResult<Option<IndexBulkDeleteResult>> {
    gistvacuumcleanup::call(mcx, info, stats)
}

/// `RelationGetIndexScan(indexRelation, nkeys, norderbys)` (genam.c) — allocate
/// and zero-init the generic `IndexScanDescData` the AM extends via `opaque`.
/// Mirrors the BRIN scan's adapter; the GiST scan additionally fills the
/// order-by output arrays (`gistbeginscan` does `palloc0`/`memset(true)` them).
fn relation_get_index_scan<'mcx>(
    _mcx: Mcx<'mcx>,
    index_relation: &Relation<'mcx>,
    nkeys: i32,
    norderbys: i32,
) -> PgResult<IndexScanDesc<'mcx>> {
    let mut key_data = Vec::with_capacity(nkeys.max(0) as usize);
    for _ in 0..nkeys {
        key_data.push(ScanKeyData::empty());
    }
    let mut order_by_data = Vec::with_capacity(norderbys.max(0) as usize);
    for _ in 0..norderbys {
        order_by_data.push(ScanKeyData::empty());
    }
    // scan->xs_orderbyvals = palloc0(...); scan->xs_orderbynulls = memset(true)
    let xs_orderbyvals = vec![Datum::null(); norderbys.max(0) as usize];
    let xs_orderbynulls = vec![true; norderbys.max(0) as usize];

    Ok(AllocBox::new(IndexScanDescData {
        heap_relation: None,
        index_relation: index_relation.alias(),
        xs_snapshot: None,
        number_of_keys: nkeys,
        number_of_order_bys: norderbys,
        key_data,
        order_by_data,
        xs_want_itup: false,
        xs_temp_snap: false,
        kill_prior_tuple: false,
        ignore_killed_tuples: true,
        xact_started_in_recovery: false,
        opaque: None,
        instrument: None,
        xs_itup: None,
        xs_itupdesc: None,
        xs_hitup: None,
        xs_hitupdesc: None,
        xs_heaptid: ItemPointerData::default(),
        xs_heap_continue: false,
        xs_heapfetch: None,
        xs_recheck: false,
        xs_orderbyvals,
        xs_orderbynulls,
        xs_recheckorderby: false,
        parallel_scan: None,
    }))
}

/// Erase a [`GISTScanOpaqueData`] into the A0 AM-opaque carrier for storage in
/// `IndexScanDescData.opaque`.
fn erase_gistscan<'mcx>(
    mcx: Mcx<'mcx>,
    so: GISTScanOpaqueData<'mcx>,
) -> PgResult<PgBox<'mcx, dyn AmOpaque<'mcx> + 'mcx>> {
    let boxed: PgBox<'mcx, GISTScanOpaqueData<'mcx>> = mcx::alloc_in(mcx, so)?;
    let (ptr, alloc) = PgBox::into_raw_with_allocator(boxed);
    // SAFETY: `ptr`/`alloc` came from `into_raw_with_allocator`; the cast only
    // attaches the `dyn AmOpaque` vtable (the A0 erase pattern).
    Ok(unsafe { PgBox::from_raw_in(ptr as *mut (dyn AmOpaque<'mcx> + 'mcx), alloc) })
}

/// Reborrow a `&IndexScanDescData` as `&mut` for the `gist()` opaque downcast in
/// the descriptor-build path (the fetch-tupdesc helper needs the leaf desc,
/// which lives behind the opaque). The opaque is exclusively owned by the scan
/// and not otherwise aliased during `gistrescan`.
fn scan_const_to_mut<'a, 'mcx>(
    scan: &'a IndexScanDescData<'mcx>,
) -> &'a mut IndexScanDescData<'mcx> {
    // SAFETY: callers use this only inside gistrescan's single-threaded build
    // path, where the scan descriptor is uniquely owned; no other live borrow
    // exists at the call site.
    #[allow(invalid_reference_casting)]
    unsafe {
        &mut *(scan as *const IndexScanDescData<'mcx> as *mut IndexScanDescData<'mcx>)
    }
}
