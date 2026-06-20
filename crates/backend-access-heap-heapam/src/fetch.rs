//! The heap access method's single-tuple **fetch** routines
//! (`access/heap/heapam.c`): `heap_fetch` / `heap_hot_search_buffer` /
//! `heap_get_latest_tid`.
//!
//! These pin/lock a heap page through the bufmgr seams, materialize the
//! page-resident tuple (header + user-data area) into an owned [`FormedTuple`],
//! test visibility, and (for the HOT chain / latest-TID chase) follow `t_ctid`
//! links. The carrier is the full [`FormedTuple`] (`None` is the C `t_data ==
//! NULL` "not found" sentinel) so that the table-AM read-path callbacks
//! (`heapam_index_fetch_tuple` / `heapam_fetch_row_version`) can store the
//! result into a `BufferHeapTupleTableSlot` via `ExecStoreBufferHeapTuple` /
//! `ExecStorePinnedBufferHeapTuple`. C leaves `t_data` pointing into the still
//! pinned buffer and the slot store copies it out under the pin; the owned model
//! materializes the bytes up front while the pin is held, which is behaviorally
//! identical.
//!
//! ## What this owns
//!
//! These three functions install the [`backend_access_heap_heapam_seams`]
//! `heap_fetch` / `heap_hot_search_buffer` seams (see `lib.rs::init_seams`),
//! flipping their already-merged consumers live:
//! - `lock.rs::heap_lock_updated_tuple_rec` calls `heap_fetch` (walks the update
//!   chain under `SnapshotAny`).
//! - `index_delete.rs::heap_index_delete_tuples` calls `heap_hot_search_buffer`
//!   (tests whether a whole HOT chain is vacuumable).
//!
//! `heap_get_latest_tid` is the heap AM's `tuple_get_latest_tid` table-AM
//! vtable callback; it is exported for the vtable assembly to wire.

use mcx::Mcx;
use types_core::primitive::{Oid, TransactionId};
use types_core::xact::InvalidTransactionId;
use types_error::PgResult;
use types_rel::Relation;
use types_snapshot::SnapshotData;
use types_storage::{Buffer, InvalidBuffer};
use types_tuple::heaptuple::{
    HeapTupleHeaderData, ItemPointerData, FIRST_OFFSET_NUMBER as FirstOffsetNumber,
    HEAP_HOT_UPDATED, HEAP_ONLY_TUPLE, HEAP_XMAX_INVALID,
};
use types_tuple::backend_access_common_heaptuple::FormedTuple;

use backend_storage_page::{
    ItemIdGetLength, ItemIdGetRedirect, ItemIdIsNormal, ItemIdIsRedirected, ItemPointerGetBlockNumber,
    ItemPointerGetOffsetNumber, ItemPointerIndicatesMovedPartitions, PageGetItem, PageGetItemId,
    PageGetMaxOffsetNumber, PageRef,
};

use backend_access_heap_heapam_seams::{HeapDirtyFetchResult, HeapFetchResult, HotSearchResult};
use backend_access_heap_heapam_visibility::htup::{
    HeapTupleHeaderGetXmin, HeapTupleHeaderXminInvalid, ItemPointerEquals, ItemPointerIsValid,
};
use backend_access_heap_heapam_visibility::{
    HeapTupleHeaderGetUpdateXid, HeapTupleHeaderIsOnlyLocked, HeapTupleIsSurelyDead,
    HeapTupleSatisfiesVisibility,
};

use backend_storage_buffer_bufmgr_seams as bufmgr_seam;
use backend_storage_lmgr_predicate_seams as predicate_seam;
use backend_access_heap_vacuumlazy_seams as vacuumlazy_seam;
use backend_access_transam_transam::TransactionIdEquals;

/// `BUFFER_LOCK_UNLOCK` (bufmgr.h).
const BUFFER_LOCK_UNLOCK: i32 = 0;
/// `BUFFER_LOCK_SHARE` (bufmgr.h).
const BUFFER_LOCK_SHARE: i32 = 1;

/// `TransactionIdIsValid(xid)`.
#[inline]
fn transaction_id_is_valid(xid: TransactionId) -> bool {
    xid != InvalidTransactionId
}

/// `HeapTupleIsHeapOnly(tuple)` — `t_infomask2 & HEAP_ONLY_TUPLE` (htup_details.h).
#[inline]
fn heap_tuple_header_is_heap_only(tup: &HeapTupleHeaderData<'_>) -> bool {
    (tup.t_infomask2 & HEAP_ONLY_TUPLE) != 0
}

/// `HeapTupleHeaderIsHotUpdated(tup)` (htup_details.h): the chain continues
/// past this tuple as a HOT update — `(t_infomask2 & HEAP_HOT_UPDATED) != 0 &&
/// (t_infomask & HEAP_XMAX_INVALID) == 0 && !HeapTupleHeaderXminInvalid(tup)`.
#[inline]
fn heap_tuple_header_is_hot_updated(tup: &HeapTupleHeaderData<'_>) -> bool {
    (tup.t_infomask2 & HEAP_HOT_UPDATED) != 0
        && (tup.t_infomask & HEAP_XMAX_INVALID) == 0
        && !HeapTupleHeaderXminInvalid(tup)
}

/// `HeapTupleHeaderIndicatesMovedPartitions(tup)` (htup_details.h) —
/// `ItemPointerIndicatesMovedPartitions(&tup->t_ctid)`.
#[inline]
fn heap_tuple_header_indicates_moved_partitions(tup: &HeapTupleHeaderData<'_>) -> bool {
    ItemPointerIndicatesMovedPartitions(&tup.t_ctid)
}

/// `tup->t_data` (shared); a normal line pointer always carries a header. The
/// header lives in the [`FormedTuple`]'s owned [`HeapTupleData`].
#[inline]
fn data_ref<'a, 'mcx>(tuple: &'a FormedTuple<'mcx>) -> &'a HeapTupleHeaderData<'mcx> {
    tuple
        .tuple
        .t_data
        .as_ref()
        .expect("heap fetch: normal line-pointer tuple has no t_data")
}

/// The outcome of decoding the on-page line pointer at `(buffer, offnum)`.
enum PageItem<'mcx> {
    /// `ItemIdIsNormal` — a live tuple materialized into a [`FormedTuple`]
    /// (header + user-data area).
    Normal(FormedTuple<'mcx>),
    /// `ItemIdIsRedirected` — a HOT redirect to the given offset.
    Redirected(u16),
    /// Unused / dead line pointer (`!ItemIdIsNormal && !ItemIdIsRedirected`).
    Dead,
}

/// Read the line pointer at `offnum` and, when normal, materialize the on-page
/// tuple (header incl. its `t_bits` null bitmap + user-data area) into an owned
/// [`FormedTuple`] with identity `(block, offnum)`. Mirrors C's `lp =
/// PageGetItemId(page, offnum); ... loctup.t_data = PageGetItem(page, lp);
/// loctup.t_len = ItemIdGetLength(lp)` — except the user-data area is copied out
/// of the page rather than left aliasing it (the buffer stays pinned while the
/// caller holds the result, so this is behavior-preserving).
fn read_page_item<'mcx>(
    mcx: Mcx<'mcx>,
    buffer: Buffer,
    block: u32,
    offnum: u16,
    table_oid: Oid,
) -> PgResult<PageItem<'mcx>> {
    let mut out: Option<PageItem<'mcx>> = None;
    bufmgr_seam::with_buffer_page::call(buffer, &mut |page_bytes| {
        let page = PageRef::new(page_bytes)?;
        let item_id = PageGetItemId(&page, offnum)?;
        if ItemIdIsRedirected(&item_id) {
            out = Some(PageItem::Redirected(ItemIdGetRedirect(&item_id)));
            return Ok(());
        }
        if !ItemIdIsNormal(&item_id) {
            out = Some(PageItem::Dead);
            return Ok(());
        }
        let item = PageGetItem(&page, &item_id)?;
        let len = ItemIdGetLength(&item_id) as usize;
        let tuple = FormedTuple::read_on_page_full(mcx, &item[..len], block, offnum, table_oid)?;
        out = Some(PageItem::Normal(tuple));
        Ok(())
    })?;
    Ok(out.expect("with_buffer_page closure must have run"))
}

/// `LockBuffer(buffer, BUFFER_LOCK_UNLOCK); ReleaseBuffer(buffer)`.
#[inline]
fn unlock_release(buffer: Buffer) -> PgResult<()> {
    bufmgr_seam::lock_buffer::call(buffer, BUFFER_LOCK_UNLOCK)?;
    bufmgr_seam::release_buffer::call(buffer);
    Ok(())
}

/// `heap_fetch(relation, snapshot, tuple, &userbuf, keep_buf)` (heapam.c) —
/// fetch the tuple at `tid`. The relation crosses as `&Relation` (the
/// detached-value model). The result carries C's `bool` return, the
/// `*userbuf` (pinned on success / `keep_buf`), and the filled header-only
/// [`HeapTupleData`] (`t_data == None` = the C "not found" sentinel).
pub fn heap_fetch<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    snapshot: &SnapshotData,
    tid: ItemPointerData,
    keep_buf: bool,
) -> PgResult<HeapFetchResult<'mcx>> {
    let relid = relation.rd_id;

    let not_found = |userbuf: Buffer| HeapFetchResult {
        found: false,
        userbuf,
        tuple: None,
    };

    // Fetch and pin the appropriate page of the relation.
    let buffer = bufmgr_seam::read_buffer::call(relation, ItemPointerGetBlockNumber(&tid))?;

    // Need share lock on buffer to examine tuple commit status.
    bufmgr_seam::lock_buffer::call(buffer, BUFFER_LOCK_SHARE)?;

    // We'd better check for out-of-range offnum in case of VACUUM since the
    // TID was obtained.
    let offnum = ItemPointerGetOffsetNumber(&tid);
    let max = page_get_max_offset_number(buffer)?;
    if offnum < FirstOffsetNumber || offnum > max {
        unlock_release(buffer)?;
        return Ok(not_found(InvalidBuffer));
    }

    // Read the line pointer; must check for a deleted (non-normal) tuple.
    let block = ItemPointerGetBlockNumber(&tid);
    let mut tuple = match read_page_item(mcx, buffer, block, offnum, relid)? {
        PageItem::Normal(t) => t,
        PageItem::Redirected(_) | PageItem::Dead => {
            unlock_release(buffer)?;
            return Ok(not_found(InvalidBuffer));
        }
    };
    // fill in *tuple fields (t_self / t_tableOid already set by read_page_item).
    tuple.tuple.t_self = tid;

    // check tuple visibility, then release lock
    let valid = HeapTupleSatisfiesVisibility(&mut tuple.tuple, &mut snapshot.clone(), buffer)?;

    if valid {
        let xmin = HeapTupleHeaderGetXmin(data_ref(&tuple));
        predicate_seam::predicate_lock_tid::call(relid, tuple.tuple.t_self, snapshot, xmin)?;
    }

    predicate_seam::heap_check_for_serializable_conflict_out::call(
        valid, relid, &tuple.tuple, buffer, snapshot,
    )?;

    bufmgr_seam::lock_buffer::call(buffer, BUFFER_LOCK_UNLOCK)?;

    if valid {
        // All checks passed; caller is now responsible for releasing the buffer.
        return Ok(HeapFetchResult { found: true, userbuf: buffer, tuple: Some(tuple) });
    }

    // Tuple failed time qual, but maybe caller wants to see it anyway.
    if keep_buf {
        Ok(HeapFetchResult { found: false, userbuf: buffer, tuple: Some(tuple) })
    } else {
        bufmgr_seam::release_buffer::call(buffer);
        Ok(not_found(InvalidBuffer))
    }
}

/// `heap_fetch(relation, &SnapshotDirty, tuple, &buffer, true)` (heapam.c)
/// specialized for the `heapam_tuple_lock` FIND_LAST_VERSION chase. Identical to
/// [`heap_fetch`] with a fresh `SNAPSHOT_DIRTY` snapshot and `keep_buf = true`,
/// but threads that dirty snapshot **mutably** through
/// [`HeapTupleSatisfiesVisibility`] so `HeapTupleSatisfiesDirty` can stamp
/// `snapshot.xmin`/`snapshot.xmax` (the in-progress inserter/deleter xids the
/// chase loop inspects), and returns those stamped fields. C's
/// `InitDirtySnapshot(SnapshotDirty)` is the zeroed `SNAPSHOT_DIRTY` sentinel.
pub fn heap_fetch_dirty<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    tid: ItemPointerData,
) -> PgResult<HeapDirtyFetchResult<'mcx>> {
    use types_snapshot::snapshot::SnapshotType;

    let relid = relation.rd_id;

    // InitDirtySnapshot(SnapshotDirty): zeroed SNAPSHOT_DIRTY (xmin == xmax ==
    // InvalidTransactionId). HeapTupleSatisfiesDirty writes xmin/xmax back.
    let mut snapshot = SnapshotData::sentinel(SnapshotType::SNAPSHOT_DIRTY);

    let not_found = |userbuf: Buffer, snapshot: &SnapshotData| HeapDirtyFetchResult {
        found: false,
        userbuf,
        tuple: None,
        snapshot_xmin: snapshot.xmin,
        snapshot_xmax: snapshot.xmax,
    };

    // Fetch and pin the appropriate page of the relation.
    let buffer = bufmgr_seam::read_buffer::call(relation, ItemPointerGetBlockNumber(&tid))?;

    // Need share lock on buffer to examine tuple commit status.
    bufmgr_seam::lock_buffer::call(buffer, BUFFER_LOCK_SHARE)?;

    // Out-of-range offnum guard (VACUUM may have shrunk the page).
    let offnum = ItemPointerGetOffsetNumber(&tid);
    let max = page_get_max_offset_number(buffer)?;
    if offnum < FirstOffsetNumber || offnum > max {
        unlock_release(buffer)?;
        return Ok(not_found(InvalidBuffer, &snapshot));
    }

    // Read the line pointer; must check for a deleted (non-normal) tuple.
    let block = ItemPointerGetBlockNumber(&tid);
    let mut tuple = match read_page_item(mcx, buffer, block, offnum, relid)? {
        PageItem::Normal(t) => t,
        PageItem::Redirected(_) | PageItem::Dead => {
            unlock_release(buffer)?;
            return Ok(not_found(InvalidBuffer, &snapshot));
        }
    };
    tuple.tuple.t_self = tid;

    // check tuple visibility (DIRTY: stamps snapshot.xmin/xmax), then release lock
    let valid = HeapTupleSatisfiesVisibility(&mut tuple.tuple, &mut snapshot, buffer)?;

    if valid {
        let xmin = HeapTupleHeaderGetXmin(data_ref(&tuple));
        predicate_seam::predicate_lock_tid::call(relid, tuple.tuple.t_self, &snapshot, xmin)?;
    }

    predicate_seam::heap_check_for_serializable_conflict_out::call(
        valid, relid, &tuple.tuple, buffer, &snapshot,
    )?;

    bufmgr_seam::lock_buffer::call(buffer, BUFFER_LOCK_UNLOCK)?;

    // keep_buf = true: the buffer stays pinned regardless of visibility so the
    // caller can inspect the materialized tuple under the pin.
    Ok(HeapDirtyFetchResult {
        found: valid,
        userbuf: buffer,
        tuple: Some(tuple),
        snapshot_xmin: snapshot.xmin,
        snapshot_xmax: snapshot.xmax,
    })
}

/// `heap_hot_search_buffer(tid, relation, buffer, snapshot, heapTuple,
/// &all_dead, first_call)` (heapam.c) — search the HOT chain rooted at `tid`
/// for the first member satisfying `snapshot`. The caller must already hold pin
/// + (at least) share lock on `buffer`; it is still pinned/locked at exit.
/// `want_all_dead` corresponds to a non-NULL `all_dead`.
pub fn heap_hot_search_buffer<'mcx>(
    mcx: Mcx<'mcx>,
    tid: ItemPointerData,
    relation: &Relation<'mcx>,
    buffer: Buffer,
    snapshot: &mut SnapshotData,
    want_all_dead: bool,
    first_call: bool,
) -> PgResult<HotSearchResult<'mcx>> {
    let relid = relation.rd_id;
    let mut prev_xmax: TransactionId = InvalidTransactionId;
    let blkno = ItemPointerGetBlockNumber(&tid);
    let mut offnum = ItemPointerGetOffsetNumber(&tid);
    let mut at_chain_start = first_call;
    let mut skip = !first_call;
    // GlobalVisState — fetched lazily on the first all-dead test (C's `vistest`).
    let mut vistest: Option<types_snapshot::snapshot::GlobalVisStateHandle> = None;

    // If this is not the first call, the previous call returned a (live) tuple.
    let mut all_dead = if want_all_dead { Some(first_call) } else { None };

    debug_assert!(bufmgr_seam::buffer_get_block_number::call(buffer) == blkno);

    let max = page_get_max_offset_number(buffer)?;
    let mut result_tid = tid;

    // Scan through possible multiple members of the HOT chain.
    loop {
        // check for bogus TID
        if offnum < FirstOffsetNumber || offnum > max {
            break;
        }

        // check for unused, dead, or redirected items
        let mut heap_tuple = match read_page_item(mcx, buffer, blkno, offnum, relid)? {
            PageItem::Redirected(redirect) => {
                // We should only see a redirect at start of chain.
                if at_chain_start {
                    offnum = redirect;
                    at_chain_start = false;
                    continue;
                }
                break; // else must be end of chain
            }
            PageItem::Dead => break, // end of chain
            PageItem::Normal(t) => t,
        };

        // heap_tuple now points at the chain member being investigated; t_self
        // is set to (blkno, offnum) by read_page_item.

        // Shouldn't see a HEAP_ONLY tuple at chain start.
        if at_chain_start && heap_tuple_header_is_heap_only(data_ref(&heap_tuple)) {
            break;
        }

        // The xmin should match the previous xmax value, else chain is broken.
        if transaction_id_is_valid(prev_xmax)
            && !TransactionIdEquals(prev_xmax, HeapTupleHeaderGetXmin(data_ref(&heap_tuple)))
        {
            break;
        }

        // Return the first match we find (skip the just-returned one on later
        // passes to avoid an infinite loop / duplicate).
        if !skip {
            // Pass the real snapshot (not a clone): HeapTupleSatisfiesDirty
            // writes the in-progress xmin/xmax/speculativeToken back into it,
            // and the index scan's owner reads those to decide on conflict wait.
            let valid =
                HeapTupleSatisfiesVisibility(&mut heap_tuple.tuple, snapshot, buffer)?;
            predicate_seam::heap_check_for_serializable_conflict_out::call(
                valid, relid, &heap_tuple.tuple, buffer, snapshot,
            )?;

            if valid {
                set_offset_number(&mut result_tid, offnum);
                let xmin = HeapTupleHeaderGetXmin(data_ref(&heap_tuple));
                predicate_seam::predicate_lock_tid::call(
                    relid,
                    heap_tuple.tuple.t_self,
                    snapshot,
                    xmin,
                )?;
                if want_all_dead {
                    all_dead = Some(false);
                }
                return Ok(HotSearchResult {
                    found: true,
                    tid: result_tid,
                    heap_tuple: Some(heap_tuple),
                    all_dead,
                });
            }
        }
        skip = false;

        // If we can't see it, maybe no one else can either. At caller request,
        // check whether all chain members are dead to all transactions.
        if want_all_dead && all_dead == Some(true) {
            if vistest.is_none() {
                vistest = Some(vacuumlazy_seam::global_vis_test_for::call(relation)?);
            }
            if !HeapTupleIsSurelyDead(&heap_tuple.tuple, vistest.unwrap())? {
                all_dead = Some(false);
            }
        }

        // Check whether the HOT chain continues past this tuple.
        if heap_tuple_header_is_hot_updated(data_ref(&heap_tuple)) {
            debug_assert!(
                ItemPointerGetBlockNumber(&data_ref(&heap_tuple).t_ctid) == blkno
            );
            offnum = ItemPointerGetOffsetNumber(&data_ref(&heap_tuple).t_ctid);
            at_chain_start = false;
            prev_xmax = HeapTupleHeaderGetUpdateXid(data_ref(&heap_tuple))?;
        } else {
            break; // end of chain
        }
    }

    Ok(HotSearchResult {
        found: false,
        tid: result_tid,
        heap_tuple: None,
        all_dead,
    })
}

/// `heap_get_latest_tid(sscan, tid)` (heapam.c) — chase `t_ctid` links to the
/// latest version of the row visible to `snapshot`. `tid` is updated to (and
/// the new value returned for) the latest version; it is unchanged if no
/// version passes the snapshot test.
///
/// This is the heap AM's `tuple_get_latest_tid` table-AM callback. Convention A
/// passes the relation/snapshot the generic `TableScanDesc` carries (`rs_rd` /
/// `rs_snapshot`) directly, rather than re-deriving them from a downcast scan.
pub fn heap_get_latest_tid<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    snapshot: &SnapshotData,
    mut tid: ItemPointerData,
) -> PgResult<ItemPointerData> {
    let relid = relation.rd_id;

    // table_tuple_get_latest_tid() verified that the passed-in tid is valid.
    debug_assert!(ItemPointerIsValid(&tid));

    let mut ctid = tid;
    let mut prior_xmax: TransactionId = InvalidTransactionId; // cannot check first XMIN

    // Loop to chase down t_ctid links until we reach the end of the chain.
    loop {
        // Read, pin, and lock the page.
        let buffer = bufmgr_seam::read_buffer::call(relation, ItemPointerGetBlockNumber(&ctid))?;
        bufmgr_seam::lock_buffer::call(buffer, BUFFER_LOCK_SHARE)?;

        // Check for bogus item number (can happen following a t_ctid link); not
        // an error — assume the prior tid is OK and return it unchanged.
        let offnum = ItemPointerGetOffsetNumber(&ctid);
        let max = page_get_max_offset_number(buffer)?;
        if offnum < FirstOffsetNumber || offnum > max {
            unlock_release(buffer)?;
            break;
        }
        let block = ItemPointerGetBlockNumber(&ctid);
        let mut tp = match read_page_item(mcx, buffer, block, offnum, relid)? {
            PageItem::Normal(t) => t,
            PageItem::Redirected(_) | PageItem::Dead => {
                unlock_release(buffer)?;
                break;
            }
        };
        // OK to access the tuple.
        tp.tuple.t_self = ctid;

        // After following a t_ctid link, we might arrive at an unrelated tuple.
        // Check for XMIN match.
        if transaction_id_is_valid(prior_xmax)
            && !TransactionIdEquals(prior_xmax, HeapTupleHeaderGetXmin(data_ref(&tp)))
        {
            unlock_release(buffer)?;
            break;
        }

        // Check tuple visibility; if visible, set the new result candidate.
        let valid = HeapTupleSatisfiesVisibility(&mut tp.tuple, &mut snapshot.clone(), buffer)?;
        predicate_seam::heap_check_for_serializable_conflict_out::call(
            valid, relid, &tp.tuple, buffer, snapshot,
        )?;
        if valid {
            tid = ctid;
        }

        // If there's a valid t_ctid link, follow it, else we're done.
        let header = data_ref(&tp);
        if (header.t_infomask & HEAP_XMAX_INVALID) != 0
            || HeapTupleHeaderIsOnlyLocked(header)?
            || heap_tuple_header_indicates_moved_partitions(header)
            || ItemPointerEquals(&tp.tuple.t_self, &header.t_ctid)
        {
            unlock_release(buffer)?;
            break;
        }

        ctid = header.t_ctid;
        prior_xmax = HeapTupleHeaderGetUpdateXid(header)?;
        unlock_release(buffer)?;
    }

    Ok(tid)
}

/// `PageGetMaxOffsetNumber(BufferGetPage(buffer))` across the buffer boundary.
fn page_get_max_offset_number(buffer: Buffer) -> PgResult<u16> {
    let mut out: u16 = 0;
    bufmgr_seam::with_buffer_page::call(buffer, &mut |page_bytes| {
        let page = PageRef::new(page_bytes)?;
        out = PageGetMaxOffsetNumber(&page);
        Ok(())
    })?;
    Ok(out)
}

/// `ItemPointerSetOffsetNumber(pointer, offnum)` (itemptr.h).
#[inline]
fn set_offset_number(pointer: &mut ItemPointerData, offnum: u16) {
    pointer.ip_posid = offnum;
}
