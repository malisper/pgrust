//! nbtinsert.c: descent-for-insert (rightmost-block fastpath cache),
//! _bt_check_unique (YES + PARTIAL + EXISTING arms, with the conflict-wait
//! restart), _bt_findinsertloc, _bt_insertonpg incl. posting splits
//! (_bt_binsrch_posting, _bt_swap_posting, the page-split coincidence),
//! _bt_split + parent insertion + root split, dedup trigger (dedup.rs).
//! Loud: !heapkeyspace.


use ::bufmgr_seams::{self as bufmgr, BufferPin};
use ::datum::Datum;
use ::mcx::Mcx;
use ::types_core::xact::{InvalidTransactionId, TransactionIdIsValid};
use ::types_core::{
    AttrNumber, BlockNumber, InvalidBlockNumber, OffsetNumber, TransactionId, INDEX_MAX_KEYS,
};
use ::types_error::{PgError, PgResult, ERRCODE_UNIQUE_VIOLATION};
use ::types_nbtree::{
    BTMetaPageData, BTPageOpaqueData, BTP_HAS_GARBAGE, BTP_INCOMPLETE_SPLIT, BTP_ROOT,
    BTP_SPLIT_END, BTREE_METAPAGE, BTREE_NOVAC_VERSION, BT_READ, BT_WRITE, P_FIRSTDATAKEY,
    P_FIRSTKEY, P_HIKEY, P_IGNORE, P_INCOMPLETE_SPLIT, P_ISLEAF, P_ISROOT, P_LEFTMOST, P_NONE,
    P_RIGHTMOST, XLOG_BTREE_INSERT_LEAF, XLOG_BTREE_INSERT_META, XLOG_BTREE_INSERT_POST,
    XLOG_BTREE_INSERT_UPPER, XLOG_BTREE_NEWROOT, XLOG_BTREE_SPLIT_L, XLOG_BTREE_SPLIT_R,
};
use ::types_nbtree::genam::IndexUniqueCheck;
use ::types_rel::Relation;
use ::types_snapshot::{SnapshotData, SnapshotType};
use ::types_storage::bufpage::{PageMut, PageRef, SizeOfPageHeaderData};
use ::types_tuple::itemptr::{
    InvalidOffsetNumber, ItemPointerCompare, ItemPointerData, ItemPointerGetBlockNumberNoCheck,
};
use ::xloginsert_seams::{XLogRegBuf, REGBUF_STANDARD, REGBUF_WILL_INIT};

use crate::fcframe::OrderProcFrame;
use crate::itup::{
    bt_tuple_get_downlink, bt_tuple_get_max_heap_tid, bt_tuple_get_natts, bt_tuple_get_nposting,
    bt_tuple_get_posting_n, bt_tuple_get_posting_offset, bt_tuple_is_pivot, bt_tuple_is_posting,
    bt_tuple_set_downlink, bt_tuple_set_natts, copy_index_tuple, index_form_tuple,
    index_tuple_size, maxalign, set_t_info, set_t_tid, t_tid, ITup, ItupBuf,
    INDEX_TUPLE_HEADER_SIZE,
};
use crate::page::{
    bt_allocbuf, bt_checkpage, bt_conditionallockbuf, bt_getbuf, bt_getroot, bt_lockbuf,
    bt_pageinit, bt_relandgetbuf, bt_relbuf, bt_unlockbuf, buf_page_mut, page_item, page_of_mut,
    page_opaque, write_opaque,
};
use crate::search::{bt_binsrch, bt_compare, BtScanInsert};
use crate::utils::{bt_check_third_page, bt_mkscankey, bt_truncate, bt_vacuum_cycleid};
use crate::{relation_needs_wal, unported_phase2};

const BTREE_FASTPATH_MIN_LEVEL: i32 = 2;

#[derive(Clone, Copy)]
pub(crate) struct StackEntry {
    pub(crate) blkno: BlockNumber,
    pub(crate) offset: OffsetNumber,
}

// RelationGetTargetBlock/RelationSetTargetBlock: the cache is C's
// rd_smgr->smgr_targblock, so RelationTruncate's smgr reset invalidates it.
fn target_block(rel: &Relation<'_>) -> BlockNumber {
    let locator = rel.rd_locator.get();
    if locator.relNumber == 0 {
        return InvalidBlockNumber;
    }
    smgr::smgrgettargblock(::types_storage::RelFileLocatorBackend {
        locator,
        backend: rel.rd_backend,
    })
}

fn set_target_block(rel: &Relation<'_>, blk: BlockNumber) {
    let locator = rel.rd_locator.get();
    if locator.relNumber == 0 {
        return;
    }
    let key = ::types_storage::RelFileLocatorBackend { locator, backend: rel.rd_backend };
    if smgr::smgropen(key.locator, key.backend).is_ok() {
        smgr::smgrsettargblock(key, blk);
    }
}

struct InsertState<'k> {
    itup: ITup,
    itemsz: usize,
    itup_key: &'k mut BtScanInsert,
    buf: Option<BufferPin>,
    bounds_valid: bool,
    low: OffsetNumber,
    stricthigh: OffsetNumber,
    postingoff: i32,
}

/// btinsert (nbtree.c).
pub fn btinsert<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    values: &[Datum],
    isnull: &[bool],
    ht_ctid: &ItemPointerData,
    heap_rel: &Relation<'mcx>,
    check_unique: IndexUniqueCheck,
    index_unchanged: bool,
) -> PgResult<bool> {
    let mut itup = index_form_tuple(mcx, &rel.rd_att, values, isnull)?;
    // SAFETY: freshly built owned image.
    unsafe { set_t_tid(itup.as_mut_ptr(), *ht_ctid) };
    bt_doinsert(mcx, rel, itup.as_ptr(), check_unique, index_unchanged, heap_rel)
}

/// _bt_doinsert.
fn bt_doinsert<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    itup: ITup,
    check_unique: IndexUniqueCheck,
    index_unchanged: bool,
    heap_rel: &Relation<'mcx>,
) -> PgResult<bool> {
    let mut is_unique = false;
    let mut checkingunique = !matches!(check_unique, IndexUniqueCheck::UNIQUE_CHECK_NO);

    let mut itup_key = bt_mkscankey(rel, Some(itup))?;
    let mut frame = OrderProcFrame::new();

    if checkingunique {
        if !itup_key.anynullkeys {
            itup_key.scantid = None;
        } else {
            // NULL keys can't conflict; the recheck path never sees them
            // because a NULL-keyed row was never queued as a conflict.
            debug_assert!(!matches!(check_unique, IndexUniqueCheck::UNIQUE_CHECK_EXISTING));
            checkingunique = false;
            is_unique = true;
        }
    }

    let mut insertstate = InsertState {
        itup,
        // SAFETY: owned image per btinsert.
        itemsz: maxalign(unsafe { index_tuple_size(itup) }),
        itup_key: &mut itup_key,
        buf: None,
        bounds_valid: false,
        low: InvalidOffsetNumber,
        stricthigh: InvalidOffsetNumber,
        postingoff: 0,
    };

    let mut stack: ::mcx::PgVec<'mcx, StackEntry> = ::mcx::PgVec::new_in(mcx);
    // C's `goto search` restart after waiting out a conflicting inserter.
    loop {
        bt_search_insert(rel, heap_rel, &mut insertstate, &mut frame, &mut stack)?;

        if !checkingunique {
            break;
        }
        // SAFETY: insertstate.buf pinned + write-locked by the search.
        let (xwait, speculative_token) = unsafe {
            bt_check_unique(
                mcx,
                rel,
                &mut insertstate,
                heap_rel,
                check_unique,
                &mut is_unique,
                &mut frame,
            )?
        };
        if TransactionIdIsValid(xwait) {
            let pin = insertstate.buf.take().expect("leaf pinned");
            bt_relbuf(rel, pin)?;
            // Speculative insertion: wait for its verdict, not the whole xact.
            let heap_tid = unsafe { t_tid(itup) };
            if speculative_token != 0 {
                lmgr::SpeculativeInsertionWait(xwait, speculative_token)?;
            } else {
                lmgr::XactLockTableWait(
                    xwait,
                    Some(rel),
                    Some(&heap_tid),
                    ::types_storage::lock::XLTW_Oper::InsertIndex,
                )?;
            }
            stack.clear();
            debug_assert!(!insertstate.bounds_valid);
            continue;
        }

        if insertstate.itup_key.heapkeyspace {
            // SAFETY: owned image.
            insertstate.itup_key.scantid = Some(unsafe { t_tid(itup) });
        }
        break;
    }

    if !matches!(check_unique, IndexUniqueCheck::UNIQUE_CHECK_EXISTING) {
        {
            let buf = insertstate.buf.as_ref().expect("leaf pinned");
            predicate_seams::check_for_serializable_conflict_in::call(
                rel,
                None,
                buf.block_number(),
            )?;
        }
        // SAFETY: insertstate.buf pinned + write-locked.
        unsafe {
            let newitemoff = bt_findinsertloc(
                mcx,
                rel,
                &mut insertstate,
                checkingunique,
                index_unchanged,
                heap_rel,
                &mut frame,
            )?;
            let buf = insertstate.buf.take().expect("leaf pinned");
            let itemsz = insertstate.itemsz;
            let postingoff = insertstate.postingoff;
            bt_insertonpg(
                mcx, rel, heap_rel, Some(insertstate.itup_key), &mut frame, buf, None, &mut stack,
                itup, itemsz, newitemoff, postingoff, false,
            )?;
        }
    } else {
        // Recheck-only call: the tuple is already in the index.
        let buf = insertstate.buf.take().expect("leaf pinned");
        bt_relbuf(rel, buf)?;
    }

    Ok(is_unique)
}

/// _bt_search_insert: rightmost-leaf fastpath cache, else full descent.
fn bt_search_insert<'mcx>(
    rel: &Relation<'mcx>,
    heaprel: &::types_rel::RelationData<'mcx>,
    insertstate: &mut InsertState<'_>,
    frame: &mut OrderProcFrame,
    stack: &mut ::mcx::PgVec<'mcx, StackEntry>,
) -> PgResult<()> {
    debug_assert!(insertstate.buf.is_none());
    debug_assert!(!insertstate.bounds_valid);
    debug_assert!(insertstate.postingoff == 0);

    if target_block(rel) != InvalidBlockNumber {
        let pin = BufferPin::adopt(bufmgr::read_buffer::call(rel, target_block(rel))?)
            .expect("ReadBuffer returned InvalidBuffer");
        if bt_conditionallockbuf(rel, &pin)? {
            bt_checkpage(rel, &pin)?;
            let usable = {
                let page = pin.page();
                let opaque = page_opaque(&page);
                P_RIGHTMOST(&opaque)
                    && P_ISLEAF(&opaque)
                    && !P_IGNORE(&opaque)
                    && page.free_space() > insertstate.itemsz
                    && page.max_offset_number() >= P_HIKEY
                    && bt_compare(rel, insertstate.itup_key, &page, P_HIKEY, frame)? > 0
            };
            if usable {
                insertstate.buf = Some(pin);
                return Ok(());
            }
            bt_relbuf(rel, pin)?;
        } else {
            pin.release();
        }
        set_target_block(rel, InvalidBlockNumber);
    }

    insertstate.buf = Some(bt_search_write(rel, heaprel, insertstate.itup_key, frame, stack)?);
    Ok(())
}

/// _bt_search, BT_WRITE arm with descent stack (C's one fn splits on access).
fn bt_search_write<'mcx>(
    rel: &Relation<'mcx>,
    heaprel: &::types_rel::RelationData<'mcx>,
    key: &mut BtScanInsert,
    frame: &mut OrderProcFrame,
    stack: &mut ::mcx::PgVec<'mcx, StackEntry>,
) -> PgResult<BufferPin> {
    let mut page_access = BT_READ;

    let mut pin = bt_getroot(rel, Some(heaprel), BT_WRITE)?.expect("BT_WRITE getroot creates the root");

    loop {
        pin = bt_moveright_for_update(rel, heaprel, key, pin, stack, page_access, frame)?;

        let (child, offnum, level) = {
            let page = pin.page();
            let opaque = page_opaque(&page);
            if P_ISLEAF(&opaque) {
                break;
            }
            let offnum = bt_binsrch(rel, key, &page, frame)?;
            // SAFETY: binsrch offset within the pinned+locked page.
            let itup = page_item(&page, unsafe { page.item_id_unchecked(offnum) });
            // SAFETY: pinned+locked page item.
            let child = unsafe {
                debug_assert!(bt_tuple_is_pivot(itup) || !key.heapkeyspace);
                bt_tuple_get_downlink(itup)
            };
            (child, offnum, opaque.btpo_level)
        };

        stack.push(StackEntry {
            blkno: pin.block_number(),
            offset: offnum,
        });

        if level == 1 {
            page_access = BT_WRITE;
        }

        pin = bt_relandgetbuf(rel, Some(pin), child, page_access)?;
    }

    if page_access == BT_READ {
        bt_unlockbuf(rel, &pin)?;
        bt_lockbuf(rel, &pin, BT_WRITE)?;
        pin = bt_moveright_for_update(rel, heaprel, key, pin, stack, BT_WRITE, frame)?;
    }

    Ok(pin)
}

/// _bt_moveright, forupdate arm (read arm lives in search.rs).
fn bt_moveright_for_update<'mcx>(
    rel: &Relation<'mcx>,
    heaprel: &::types_rel::RelationData<'mcx>,
    key: &mut BtScanInsert,
    mut pin: BufferPin,
    stack: &mut [StackEntry],
    access: i32,
    frame: &mut OrderProcFrame,
) -> PgResult<BufferPin> {
    let cmpval: i32 = if key.nextkey { 0 } else { 1 };

    loop {
        let (rightmost, incomplete, ignore, next) = {
            let page = pin.page();
            let opaque = page_opaque(&page);
            (
                P_RIGHTMOST(&opaque),
                P_INCOMPLETE_SPLIT(&opaque),
                P_IGNORE(&opaque),
                opaque.btpo_next,
            )
        };

        if rightmost {
            if ignore {
                return Err(fell_off_the_end(rel));
            }
            return Ok(pin);
        }

        if incomplete {
            let blkno = pin.block_number();
            if access == BT_READ {
                bt_unlockbuf(rel, &pin)?;
                bt_lockbuf(rel, &pin, BT_WRITE)?;
            }
            if P_INCOMPLETE_SPLIT(&page_opaque(&pin.page())) {
                // SAFETY: pin write-locked just above with the flag set.
                unsafe { bt_finish_split(rel, heaprel, pin, stack, frame)? };
            } else {
                bt_relbuf(rel, pin)?;
            }
            pin = bt_getbuf(rel, blkno, access)?;
            continue;
        }

        if ignore || bt_compare(rel, key, &pin.page(), P_HIKEY, frame)? >= cmpval {
            pin = bt_relandgetbuf(rel, Some(pin), next, access)?;
            continue;
        }
        return Ok(pin);
    }
}

#[track_caller]
#[cold]
#[inline(never)]
fn fell_off_the_end(rel: &Relation<'_>) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "fell off the end of index \"{}\"",
        rel.name()
    )))
}

/// _bt_binsrch_insert.
///
/// # Safety
/// `insertstate.buf` pinned + locked.
unsafe fn bt_binsrch_insert(
    rel: &Relation<'_>,
    insertstate: &mut InsertState<'_>,
    frame: &mut OrderProcFrame,
) -> PgResult<OffsetNumber> {
    let pin = insertstate.buf.as_ref().expect("pinned");
    let page = crate::search::buf_page(pin.buffer());
    let opaque = page_opaque(&page);
    let key = &mut *insertstate.itup_key;

    debug_assert!(P_ISLEAF(&opaque));
    debug_assert!(!key.nextkey);
    debug_assert!(insertstate.postingoff == 0);

    let (mut low, mut high) = if !insertstate.bounds_valid {
        (P_FIRSTDATAKEY(&opaque), page.max_offset_number())
    } else {
        (insertstate.low, insertstate.stricthigh)
    };

    if high < low {
        insertstate.low = InvalidOffsetNumber;
        insertstate.stricthigh = InvalidOffsetNumber;
        insertstate.bounds_valid = false;
        return Ok(low);
    }

    if !insertstate.bounds_valid {
        high += 1;
    }
    let mut stricthigh = high;
    let cmpval: i32 = 1;

    while high > low {
        let mid = low + (high - low) / 2;
        let result = bt_compare(rel, key, &page, mid, frame)?;
        if result >= cmpval {
            low = mid + 1;
        } else {
            high = mid;
            if result != 0 {
                stricthigh = high;
            }
        }

        if result == 0 && key.scantid.is_some() {
            if insertstate.postingoff != 0 {
                return Err(no_insert_offset(rel, key, low, stricthigh, pin.block_number()));
            }
            insertstate.postingoff = bt_binsrch_posting(key, &page, mid);
        }
    }

    insertstate.low = low;
    insertstate.stricthigh = stricthigh;
    insertstate.bounds_valid = true;
    Ok(low)
}

/// _bt_binsrch_posting (nbtsearch.c): 0 if not a posting list, -1 if LP_DEAD.
///
/// # Safety
/// `page` pinned + locked; `offnum` a live offset on it.
unsafe fn bt_binsrch_posting(
    key: &BtScanInsert,
    page: &PageRef<'_>,
    offnum: OffsetNumber,
) -> i32 {
    let itemid = page.item_id(offnum);
    let itup = page_item(page, itemid);
    if !bt_tuple_is_posting(itup) {
        return 0;
    }
    debug_assert!(key.heapkeyspace && key.allequalimage);

    if itemid.is_dead() {
        return -1;
    }

    let scantid = key.scantid.as_ref().expect("posting binsrch requires scantid");
    let mut low: i32 = 0;
    let mut high: i32 = bt_tuple_get_nposting(itup) as i32;
    debug_assert!(high >= 2);

    while high > low {
        let mid = low + (high - low) / 2;
        let res = ItemPointerCompare(scantid, &bt_tuple_get_posting_n(itup, mid as usize));
        if res > 0 {
            low = mid + 1;
        } else if res < 0 {
            high = mid;
        } else {
            return mid;
        }
    }

    low
}

/// _bt_swap_posting (nbtdedup.c): the gap gets `newitem`'s TID and `newitem`
/// takes oposting's rightmost TID.
///
/// # Safety
/// `newitem` owned + writable; `oposting` a live posting tuple.
unsafe fn bt_swap_posting<'mcx>(
    mcx: Mcx<'mcx>,
    newitem: *mut u8,
    oposting: ITup,
    postingoff: i32,
) -> PgResult<ItupBuf<'mcx>> {
    const IPD_SIZE: usize = core::mem::size_of::<ItemPointerData>();
    let nhtids = bt_tuple_get_nposting(oposting) as i32;

    if !(postingoff > 0 && postingoff < nhtids) {
        return Err(posting_split_failed(nhtids, postingoff));
    }

    let mut nposting = copy_index_tuple(mcx, oposting)?;
    let postoff = bt_tuple_get_posting_offset(nposting.as_ptr());
    let replacepos = nposting.as_mut_ptr().add(postoff + postingoff as usize * IPD_SIZE);
    let replaceposright = replacepos.add(IPD_SIZE);
    let nmovebytes = (nhtids - postingoff - 1) as usize * IPD_SIZE;
    core::ptr::copy(replacepos, replaceposright, nmovebytes);

    debug_assert!(!bt_tuple_is_pivot(newitem) && !bt_tuple_is_posting(newitem));
    replacepos.cast::<ItemPointerData>().write_unaligned(t_tid(newitem));

    set_t_tid(newitem, bt_tuple_get_max_heap_tid(oposting));

    debug_assert!(
        ItemPointerCompare(&bt_tuple_get_max_heap_tid(nposting.as_ptr()), &t_tid(newitem)) < 0
    );
    Ok(nposting)
}

#[track_caller]
#[cold]
#[inline(never)]
fn no_insert_offset(
    rel: &Relation<'_>,
    key: &BtScanInsert,
    low: OffsetNumber,
    stricthigh: OffsetNumber,
    blkno: BlockNumber,
) -> Box<PgError> {
    let scantid = key.scantid.as_ref().expect("posting corruption check has scantid");
    Box::new(
        PgError::error(format!(
            "table tid from new index tuple ({},{}) cannot find insert offset between offsets {low} and {stricthigh} of block {blkno} in index \"{}\"",
            ItemPointerGetBlockNumberNoCheck(scantid),
            scantid.ip_posid,
            rel.name()
        ))
        .with_sqlstate(::types_error::ERRCODE_INDEX_CORRUPTED),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn posting_split_failed(nhtids: i32, postingoff: i32) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "posting list tuple with {nhtids} items cannot be split at offset {postingoff}"
    )))
}

#[track_caller]
#[cold]
#[inline(never)]
fn invalid_duplicate_tuple(
    rel: &Relation<'_>,
    tid: &ItemPointerData,
    offnum: OffsetNumber,
    blkno: BlockNumber,
) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "table tid from new index tuple ({},{}) overlaps with invalid duplicate tuple at offset {offnum} of block {blkno} in index \"{}\"",
            ItemPointerGetBlockNumberNoCheck(tid),
            tid.ip_posid,
            rel.name()
        ))
        .with_sqlstate(::types_error::ERRCODE_INDEX_CORRUPTED),
    )
}

/// _bt_check_unique, YES + PARTIAL arms: dirty-snapshot recheck via the tableam.
/// A valid returned xwait means wait it out + restart the search; PARTIAL never
/// waits — it reports the potential conflict via `is_unique` and proceeds.
///
/// # Safety
/// `insertstate.buf` pinned + write-locked.
unsafe fn bt_check_unique<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    insertstate: &mut InsertState<'_>,
    heap_rel: &Relation<'mcx>,
    check_unique: IndexUniqueCheck,
    is_unique: &mut bool,
    frame: &mut OrderProcFrame,
) -> PgResult<(TransactionId, u32)> {
    let itup = insertstate.itup;
    let mut nbuf: Option<BufferPin> = None;
    let mut found = false;

    *is_unique = true;

    // Dirty write-back (xmin/xmax/speculativeToken) rides the snapshot's dirty_* Cells.
    let mut snapshot_dirty: ::tableam::Snapshot<'mcx> = Some(std::rc::Rc::new(
        SnapshotData::sentinel(mcx, SnapshotType::SNAPSHOT_DIRTY),
    ));

    let mut buf = insertstate.buf.as_ref().expect("pinned").buffer();
    let mut page = crate::search::buf_page(buf);
    let mut opaque = page_opaque(&page);
    let mut maxoff = page.max_offset_number();

    debug_assert!(!insertstate.bounds_valid);
    let mut offset = bt_binsrch_insert(rel, insertstate, frame)?;

    debug_assert!(!insertstate.itup_key.anynullkeys);
    debug_assert!(insertstate.itup_key.scantid.is_none());

    // one heap TID per iteration: posting TIDs advance curposti, not offset.
    let mut curitup: ITup = core::ptr::null();
    let mut curitemid_dead = false;
    let mut inposting = false;
    let mut prevalldead = true;
    let mut curposti: usize = 0;

    loop {
        if offset <= maxoff {
            if nbuf.is_none() && offset == insertstate.stricthigh {
                debug_assert!(insertstate.bounds_valid);
                debug_assert!(insertstate.low >= P_FIRSTDATAKEY(&opaque));
                debug_assert!(insertstate.low <= insertstate.stricthigh);
                break;
            }

            if !inposting {
                curitemid_dead = page.item_id(offset).is_dead();
            }
            if inposting || !curitemid_dead {
                if !inposting {
                    if bt_compare(rel, insertstate.itup_key, &page, offset, frame)? != 0 {
                        break;
                    }
                    curitup = page_item(&page, page.item_id(offset));
                    debug_assert!(!bt_tuple_is_pivot(curitup));
                }
                let mut htid = if !bt_tuple_is_posting(curitup) {
                    debug_assert!(!inposting);
                    t_tid(curitup)
                } else if !inposting {
                    inposting = true;
                    prevalldead = true;
                    curposti = 0;
                    bt_tuple_get_posting_n(curitup, 0)
                } else {
                    debug_assert!(curposti > 0);
                    bt_tuple_get_posting_n(curitup, curposti)
                };

                let mut all_dead = false;
                // A recheck expects to re-find its own tuple: not a duplicate,
                // but the scan must go on.
                if matches!(check_unique, IndexUniqueCheck::UNIQUE_CHECK_EXISTING)
                    && ItemPointerCompare(&htid, &t_tid(itup)) == 0
                {
                    found = true;
                } else if ::tableam::table_index_fetch_tuple_check(
                    mcx,
                    heap_rel,
                    &mut htid,
                    &mut snapshot_dirty,
                    Some(&mut all_dead),
                )? {
                    // PARTIAL: report the potential conflict; bounds stay valid.
                    if matches!(check_unique, IndexUniqueCheck::UNIQUE_CHECK_PARTIAL) {
                        if let Some(pin) = nbuf.take() {
                            bt_relbuf(rel, pin)?;
                        }
                        *is_unique = false;
                        return Ok((InvalidTransactionId, 0));
                    }

                    let (dirty_xmin, dirty_xmax, dirty_token) = {
                        let snap = snapshot_dirty.as_deref().expect("dirty snapshot");
                        (
                            snap.dirty_xmin.get(),
                            snap.dirty_xmax.get(),
                            snap.dirty_speculative_token.get(),
                        )
                    };
                    let xwait = if TransactionIdIsValid(dirty_xmin) {
                        dirty_xmin
                    } else {
                        dirty_xmax
                    };
                    if TransactionIdIsValid(xwait) {
                        if let Some(pin) = nbuf.take() {
                            bt_relbuf(rel, pin)?;
                        }
                        // Caller releases the lock on insertstate.buf.
                        insertstate.bounds_valid = false;
                        return Ok((xwait, dirty_token));
                    }

                    let mut selftid = t_tid(itup);
                    let mut snapshot_self: ::tableam::Snapshot<'mcx> = Some(std::rc::Rc::new(
                        SnapshotData::sentinel(mcx, SnapshotType::SNAPSHOT_SELF),
                    ));
                    if !::tableam::table_index_fetch_tuple_check(
                        mcx,
                        heap_rel,
                        &mut selftid,
                        &mut snapshot_self,
                        None,
                    )? {
                        break; // our tuple died: no error, stop searching
                    }

                    {
                        let leafbuf = insertstate.buf.as_ref().expect("pinned");
                        predicate_seams::check_for_serializable_conflict_in::call(
                            rel,
                            None,
                            leafbuf.block_number(),
                        )?;
                    }

                    if let Some(pin) = nbuf.take() {
                        bt_relbuf(rel, pin)?;
                    }
                    let leafpin = insertstate.buf.take().expect("pinned");
                    bt_relbuf(rel, leafpin)?;
                    insertstate.bounds_valid = false;

                    return Err(unique_violation(mcx, rel, heap_rel, itup));
                } else if all_dead
                    && (!inposting
                        || (prevalldead && curposti == bt_tuple_get_nposting(curitup) - 1))
                {
                    mark_item_dead(&page, offset);
                    set_page_has_garbage(&page);
                    let dirty_buf = match nbuf.as_ref() {
                        Some(pin) => pin.buffer(),
                        None => insertstate.buf.as_ref().expect("pinned").buffer(),
                    };
                    bufmgr::mark_buffer_dirty_hint::call(dirty_buf, true)?;
                }

                if !all_dead && inposting {
                    prevalldead = false;
                }
            }
        }

        if inposting && curposti < bt_tuple_get_nposting(curitup) - 1 {
            curposti += 1;
            continue;
        }
        if offset < maxoff {
            curposti = 0;
            inposting = false;
            offset += 1;
        } else {
            if P_RIGHTMOST(&opaque) {
                break;
            }
            let highkeycmp = bt_compare(rel, insertstate.itup_key, &page, P_HIKEY, frame)?;
            debug_assert!(highkeycmp <= 0);
            if highkeycmp != 0 {
                break;
            }
            loop {
                let nblkno = opaque.btpo_next;
                let pin = bt_relandgetbuf(rel, nbuf.take(), nblkno, BT_READ)?;
                page = crate::search::buf_page(pin.buffer());
                buf = pin.buffer();
                nbuf = Some(pin);
                opaque = page_opaque(&page);
                if !P_IGNORE(&opaque) {
                    break;
                }
                if P_RIGHTMOST(&opaque) {
                    return Err(fell_off_the_end(rel));
                }
            }
            let _ = buf;
            curposti = 0;
            inposting = false;
            maxoff = page.max_offset_number();
            offset = P_FIRSTDATAKEY(&opaque);
        }
    }

    if matches!(check_unique, IndexUniqueCheck::UNIQUE_CHECK_EXISTING) && !found {
        if let Some(pin) = nbuf.take() {
            bt_relbuf(rel, pin)?;
        }
        let leafpin = insertstate.buf.take().expect("pinned");
        bt_relbuf(rel, leafpin)?;
        insertstate.bounds_valid = false;
        return Err(refind_failed(mcx, rel, heap_rel));
    }

    if let Some(pin) = nbuf {
        bt_relbuf(rel, pin)?;
    }
    Ok((InvalidTransactionId, 0))
}

#[track_caller]
#[cold]
#[inline(never)]
fn refind_failed<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    heap_rel: &Relation<'mcx>,
) -> Box<PgError> {
    let mut e = PgError::error(format!(
        "failed to re-find tuple within index \"{}\"",
        rel.name()
    ))
    .with_sqlstate(::types_error::ERRCODE_INTERNAL_ERROR)
    .with_hint("This may be because of a non-immutable index expression.".to_string());
    if let Ok(Some(nsp)) = lsyscache::misc::get_namespace_name(mcx, heap_rel.namespace()) {
        e = e.with_schema_name(nsp.as_str().to_owned());
    }
    Box::new(
        e.with_table_name(heap_rel.name().to_owned())
            .with_constraint_name(rel.name().to_owned()),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn unique_violation<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    heap_rel: &Relation<'mcx>,
    itup: crate::itup::ITup,
) -> Box<PgError> {
    let mut e = PgError::error(format!(
        "duplicate key value violates unique constraint \"{}\"",
        rel.name()
    ))
    .with_sqlstate(ERRCODE_UNIQUE_VIOLATION);

    let tupdesc = rel.descr();
    let natts = tupdesc.natts as usize;
    let mut values = [Datum::null(); INDEX_MAX_KEYS as usize];
    let mut isnull = [false; INDEX_MAX_KEYS as usize];
    for i in 0..natts {
        // SAFETY: itup is the caller-owned insert tuple, live and MAXALIGNed
        // for this call; attnums 1..=natts of its own tupdesc.
        values[i] = unsafe {
            crate::itup::index_getattr(itup, (i + 1) as AttrNumber, tupdesc, &mut isnull[i])
        };
    }
    match genam_seams::build_index_value_description::call(rel, &values[..natts], &isnull[..natts])
    {
        Ok(Some(desc)) => e = e.with_detail(format!("Key {desc} already exists.")),
        Ok(None) => {}
        Err(err) => return err,
    }

    match lsyscache::misc::get_namespace_name(mcx, heap_rel.namespace()) {
        Ok(Some(nsp)) => e = e.with_schema_name(nsp.as_str().to_owned()),
        Ok(None) => {}
        Err(err) => return err,
    }
    Box::new(
        e.with_table_name(heap_rel.name().to_owned())
            .with_constraint_name(rel.name().to_owned()),
    )
}

// ItemIdMarkDead + BTP_HAS_GARBAGE hint stores (same contract as killitems).
unsafe fn mark_item_dead(page: &PageRef<'_>, offnum: OffsetNumber) {
    let off = SizeOfPageHeaderData
        + (offnum as usize - 1) * core::mem::size_of::<::types_storage::bufpage::ItemIdData>();
    let p = page
        .as_ptr()
        .add(off)
        .cast::<::types_storage::bufpage::ItemIdData>()
        .cast_mut();
    let mut iid = p.read();
    iid.mark_dead();
    p.write(iid);
}

unsafe fn set_page_has_garbage(page: &PageRef<'_>) {
    let off = crate::page::page_special_off(page)
        + core::mem::offset_of!(BTPageOpaqueData, btpo_flags);
    let p = page.as_ptr().add(off).cast::<u16>().cast_mut();
    p.write(p.read() | BTP_HAS_GARBAGE);
}

/// _bt_findinsertloc, heapkeyspace arm.
///
/// # Safety
/// `insertstate.buf` pinned + write-locked.
unsafe fn bt_findinsertloc<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    insertstate: &mut InsertState<'_>,
    checkingunique: bool,
    index_unchanged: bool,
    heap_rel: &Relation<'mcx>,
    frame: &mut OrderProcFrame,
) -> PgResult<OffsetNumber> {
    if insertstate.itemsz > ::types_nbtree::BTMaxItemSize {
        let pin = insertstate.buf.as_ref().expect("pinned");
        bt_check_third_page(
            rel,
            heap_rel,
            insertstate.itup_key.heapkeyspace,
            &pin.page(),
            insertstate.itup,
        )?;
    }

    {
        let pin = insertstate.buf.as_ref().expect("pinned");
        let opaque = page_opaque(&pin.page());
        debug_assert!(P_ISLEAF(&opaque) && !P_INCOMPLETE_SPLIT(&opaque));
    }
    debug_assert!(!insertstate.bounds_valid || checkingunique);
    if !insertstate.itup_key.heapkeyspace {
        unported_phase2("!heapkeyspace (btree version 2/3) insert lane");
    }
    debug_assert!(insertstate.itup_key.scantid.is_some());

    let mut uniquedup = index_unchanged;

    if checkingunique {
        if insertstate.low < insertstate.stricthigh {
            debug_assert!(insertstate.bounds_valid);
            uniquedup = true;
        }

        loop {
            let pin = insertstate.buf.as_ref().expect("pinned");
            let page = pin.page();
            if insertstate.bounds_valid
                && insertstate.low <= insertstate.stricthigh
                && insertstate.stricthigh <= page.max_offset_number()
            {
                break;
            }
            let opaque = page_opaque(&page);
            if P_RIGHTMOST(&opaque)
                || bt_compare(rel, insertstate.itup_key, &page, P_HIKEY, frame)? <= 0
            {
                break;
            }
            bt_stepright(rel, heap_rel, insertstate, frame)?;
            uniquedup = true;
        }
    }

    {
        let pin = insertstate.buf.as_ref().expect("pinned");
        if pin.page().free_space() < insertstate.itemsz {
            bt_delete_or_dedup_one_page(
                mcx, rel, heap_rel, insertstate, false, checkingunique, uniquedup,
            )?;
        }
    }

    debug_assert!({
        let pin = insertstate.buf.as_ref().expect("pinned");
        let page = pin.page();
        P_RIGHTMOST(&page_opaque(&page))
            || bt_compare(rel, insertstate.itup_key, &page, P_HIKEY, frame)? <= 0
    });

    let mut newitemoff = bt_binsrch_insert(rel, insertstate, frame)?;

    if insertstate.postingoff == -1 {
        bt_delete_or_dedup_one_page(mcx, rel, heap_rel, insertstate, true, false, false)?;
        debug_assert!(!insertstate.bounds_valid);
        insertstate.postingoff = 0;
        newitemoff = bt_binsrch_insert(rel, insertstate, frame)?;
        debug_assert!(insertstate.postingoff == 0);
    }

    Ok(newitemoff)
}

/// _bt_stepright (write-coupled).
///
/// # Safety
/// As [`bt_findinsertloc`].
unsafe fn bt_stepright<'mcx>(
    rel: &Relation<'mcx>,
    heaprel: &::types_rel::RelationData<'mcx>,
    insertstate: &mut InsertState<'_>,
    frame: &mut OrderProcFrame,
) -> PgResult<()> {
    let mut rblkno = {
        let pin = insertstate.buf.as_ref().expect("pinned");
        page_opaque(&pin.page()).btpo_next
    };

    let mut rbuf: Option<BufferPin> = None;
    loop {
        let pin = bt_relandgetbuf(rel, rbuf.take(), rblkno, BT_WRITE)?;
        let opaque = page_opaque(&pin.page());
        if P_INCOMPLETE_SPLIT(&opaque) {
            bt_finish_split(rel, heaprel, pin, &mut [], frame)?;
            continue;
        }
        if !P_IGNORE(&opaque) {
            rbuf = Some(pin);
            break;
        }
        if P_RIGHTMOST(&opaque) {
            return Err(fell_off_the_end(rel));
        }
        rblkno = opaque.btpo_next;
        rbuf = Some(pin);
    }

    let old = insertstate.buf.take().expect("pinned");
    bt_relbuf(rel, old)?;
    insertstate.buf = rbuf;
    insertstate.bounds_valid = false;
    Ok(())
}

/// _bt_delete_or_dedup_one_page.
///
/// # Safety
/// As [`bt_findinsertloc`].
unsafe fn bt_delete_or_dedup_one_page<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    heap_rel: &Relation<'mcx>,
    insertstate: &mut InsertState<'_>,
    simpleonly: bool,
    checkingunique: bool,
    uniquedup: bool,
) -> PgResult<()> {
    let mut uniquedup = uniquedup;
    let mut deletable = [0 as OffsetNumber; ::types_storage::bufpage::MaxIndexTuplesPerPage];
    let mut ndeletable = 0usize;
    let (minoff, maxoff);
    {
        let pin = insertstate.buf.as_ref().expect("pinned");
        let page = pin.page();
        let opaque = page_opaque(&page);
        debug_assert!(P_ISLEAF(&opaque));
        debug_assert!(!simpleonly || (!checkingunique && !uniquedup));

        minoff = P_FIRSTDATAKEY(&opaque);
        maxoff = page.max_offset_number();
        for offnum in minoff..=maxoff {
            if page.item_id(offnum).is_dead() {
                deletable[ndeletable] = offnum;
                ndeletable += 1;
            }
        }
    }

    if ndeletable > 0 {
        {
            let pin = insertstate.buf.as_ref().expect("pinned");
            crate::delete::bt_simpledel_pass(
                mcx,
                rel,
                pin,
                heap_rel,
                &deletable[..ndeletable],
                insertstate.itup,
                minoff,
                maxoff,
            )?;
        }
        insertstate.bounds_valid = false;

        let pin = insertstate.buf.as_ref().expect("pinned");
        if pin.page().free_space() >= insertstate.itemsz {
            return Ok(());
        }

        uniquedup = true;
    }

    if simpleonly || (checkingunique && !uniquedup) {
        return Ok(());
    }

    insertstate.bounds_valid = false;

    // C divergence: indexUnchanged folded into uniquedup (C only ORs them).
    if uniquedup {
        let pin = insertstate.buf.as_ref().expect("pinned");
        if crate::dedup::bt_bottomupdel_pass(mcx, rel, pin, heap_rel, insertstate.itemsz)? {
            return Ok(());
        }
    }

    // BTGetDeduplicateItems
    let dedup_items = rel
        .rd_options
        .as_ref()
        .and_then(|o| o.btree())
        .map(|o| o.deduplicate_items)
        .unwrap_or(true);
    if insertstate.itup_key.allequalimage && dedup_items {
        let pin = insertstate.buf.as_ref().expect("pinned");
        crate::dedup::bt_dedup_pass(rel, pin, insertstate.itup, insertstate.itemsz, uniquedup)?;
    }
    Ok(())
}

/// _bt_insertonpg; `cbuf` given iff inserting a downlink on an internal page;
/// `postingoff != 0` splits the posting tuple at `newitemoff` first.
///
/// # Safety
/// `buf` pinned + write-locked; `itup` a live owned tuple image.
unsafe fn bt_insertonpg<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    heaprel: &::types_rel::RelationData<'mcx>,
    itup_key: Option<&mut BtScanInsert>,
    frame: &mut OrderProcFrame,
    buf: BufferPin,
    cbuf: Option<BufferPin>,
    stack: &mut [StackEntry],
    itup: ITup,
    itemsz: usize,
    newitemoff: OffsetNumber,
    postingoff: i32,
    split_only_page: bool,
) -> PgResult<()> {
    let (isleaf, isroot, isrightmost, isonly, level) = {
        let page = buf.page();
        let opaque = page_opaque(&page);
        (
            P_ISLEAF(&opaque),
            P_ISROOT(&opaque),
            P_RIGHTMOST(&opaque),
            P_LEFTMOST(&opaque) && P_RIGHTMOST(&opaque),
            opaque.btpo_level,
        )
    };

    debug_assert!(isleaf == cbuf.is_none());
    debug_assert!(
        !isleaf || bt_tuple_get_natts(itup, rel.indnatts()) == rel.indnatts()
    );
    debug_assert!(
        isleaf || bt_tuple_get_natts(itup, rel.indnatts()) <= rel.indnkeyatts()
    );
    debug_assert!(!bt_tuple_is_posting(itup));
    debug_assert!(maxalign(index_tuple_size(itup)) == itemsz);
    debug_assert!(!P_INCOMPLETE_SPLIT(&page_opaque(&buf.page())));
    debug_assert!(isleaf || newitemoff > P_FIRSTDATAKEY(&page_opaque(&buf.page())));

    // posting split: itup becomes a copy carrying oposting's max TID
    let mut itup = itup;
    let mut newitemoff = newitemoff;
    let mut swapped: Option<(ItupBuf<'mcx>, ItupBuf<'mcx>, ITup)> = None;
    if postingoff != 0 {
        let page = buf.page();
        let itemid = page.item_id(newitemoff);
        debug_assert!(isleaf);
        debug_assert!(itup_key
            .as_ref()
            .is_some_and(|k| k.heapkeyspace && k.allequalimage));
        let oposting = page_item(&page, itemid);

        if !bt_tuple_is_posting(oposting) || itemid.is_dead() {
            return Err(invalid_duplicate_tuple(
                rel,
                &t_tid(itup),
                newitemoff,
                buf.block_number(),
            ));
        }

        let origitup = itup;
        let mut itupcopy = copy_index_tuple(mcx, origitup)?;
        let nposting = bt_swap_posting(mcx, itupcopy.as_mut_ptr(), oposting, postingoff)?;
        itup = itupcopy.as_ptr();
        newitemoff += 1;
        swapped = Some((itupcopy, nposting, origitup));
    }

    if buf.page().free_space() < itemsz {
        debug_assert!(!split_only_page);
        let (orignewitem, npostingp) = match swapped.as_ref() {
            Some((_, nposting, origitup)) => (*origitup, nposting.as_ptr() as ITup),
            None => (core::ptr::null(), core::ptr::null()),
        };
        let rbuf = bt_split(
            mcx, rel, heaprel, itup_key, frame, &buf, cbuf, newitemoff, itemsz, itup,
            orignewitem, npostingp, postingoff as u16,
        )?;
        predicate_seams::predicate_lock_page_split::call(
            rel,
            buf.block_number(),
            rbuf.block_number(),
        )?;
        bt_insert_parent(mcx, rel, heaprel, frame, buf, rbuf, stack, isroot, isonly)
    } else {
        let mut metabuf: Option<BufferPin> = None;
        if split_only_page {
            debug_assert!(!isleaf);
            debug_assert!(cbuf.is_some());

            let pin = bt_getbuf(rel, BTREE_METAPAGE, BT_WRITE)?;
            let metad = crate::page::page_meta(&pin.page());
            if metad.btm_fastlevel >= level {
                bt_relbuf(rel, pin)?;
            } else {
                metabuf = Some(pin);
            }
        }

        // critical section: page image mutation + WAL, no early returns.
        {
            let mut page = page_of_mut(&buf);
            if let Some((_, nposting, _)) = swapped.as_ref() {
                // overwrite oposting in place (same size — nposting is its copy)
                let itemid = page.as_ref().item_id(newitemoff - 1);
                let dst = page.as_ref().as_ptr().cast_mut().add(itemid.lp_off() as usize);
                core::ptr::copy_nonoverlapping(
                    nposting.as_ptr(),
                    dst,
                    maxalign(index_tuple_size(nposting.as_ptr())),
                );
            }
            if page
                .add_item(
                    core::slice::from_raw_parts(itup, index_tuple_size(itup)),
                    newitemoff,
                    0,
                )
                .is_none()
            {
                panic!(
                    "failed to add new item to block {} in index \"{}\"",
                    buf.block_number(),
                    rel.name()
                );
            }
        }
        bufmgr::mark_buffer_dirty::call(buf.buffer())?;

        let mut metad_for_wal: Option<BTMetaPageData> = None;
        if let Some(metapin) = metabuf.as_ref() {
            let mut metad = crate::page::page_meta(&metapin.page());
            if metad.btm_version < BTREE_NOVAC_VERSION {
                unported_phase2("_bt_upgrademetapage (v2/v3 pg_upgrade metapages)");
            }
            metad.btm_fastroot = buf.block_number();
            metad.btm_fastlevel = level;
            crate::page::write_meta(metapin, &metad);
            bufmgr::mark_buffer_dirty::call(metapin.buffer())?;
            metad_for_wal = Some(metad);
        }

        if let Some(cpin) = cbuf.as_ref() {
            let page = cpin.page();
            let mut copaque = page_opaque(&page);
            debug_assert!(P_INCOMPLETE_SPLIT(&copaque));
            copaque.btpo_flags &= !BTP_INCOMPLETE_SPLIT;
            write_opaque(&mut buf_page_mut(cpin.buffer()), &copaque);
            bufmgr::mark_buffer_dirty::call(cpin.buffer())?;
        }

        if relation_needs_wal(rel) {
            let xlrec = crate::wal::xl_btree_insert(newitemoff);
            // INSERT_POST block-0 data is uint16 postingoff + origitup
            let upostingoff = (postingoff as u16).to_ne_bytes();
            let itup_frag: [&[u8]; 1] = [core::slice::from_raw_parts(itup, index_tuple_size(itup))];
            let posting_frags: [&[u8]; 2];
            let bufdata: &[&[u8]] = if let Some((_, _, origitup)) = swapped.as_ref() {
                posting_frags = [
                    &upostingoff,
                    core::slice::from_raw_parts(*origitup, index_tuple_size(*origitup)),
                ];
                &posting_frags
            } else {
                &itup_frag
            };
            let reg0 = XLogRegBuf {
                block_id: 0,
                buffer: buf.buffer(),
                flags: REGBUF_STANDARD,
                bufdata,
            };

            let call = |xlinfo: u8, regbufs: &[XLogRegBuf<'_>]| {
                ::xloginsert_seams::xlog_insert_record::call(
                    ::rmgr::RM_BTREE_ID as u8,
                    xlinfo,
                    0,
                    &[&xlrec],
                    regbufs,
                )
            };

            let recptr = if isleaf && postingoff == 0 {
                call(XLOG_BTREE_INSERT_LEAF, &[reg0])?
            } else if postingoff != 0 {
                debug_assert!(isleaf);
                call(XLOG_BTREE_INSERT_POST, &[reg0])?
            } else {
                let reg1 = XLogRegBuf {
                    block_id: 1,
                    buffer: cbuf.as_ref().expect("internal insert has cbuf").buffer(),
                    flags: REGBUF_STANDARD,
                    bufdata: &[],
                };
                if let Some(metapin) = metabuf.as_ref() {
                    let md = crate::wal::xl_btree_metadata(metad_for_wal.as_ref().expect("meta"));
                    let mdfrags: [&[u8]; 1] = [&md];
                    let reg2 = XLogRegBuf {
                        block_id: 2,
                        buffer: metapin.buffer(),
                        flags: REGBUF_WILL_INIT | REGBUF_STANDARD,
                        bufdata: &mdfrags,
                    };
                    call(XLOG_BTREE_INSERT_META, &[reg0, reg1, reg2])?
                } else {
                    call(XLOG_BTREE_INSERT_UPPER, &[reg0, reg1])?
                }
            };

            if let Some(metapin) = metabuf.as_ref() {
                page_of_mut(metapin).set_lsn(recptr);
            }
            if let Some(cpin) = cbuf.as_ref() {
                buf_page_mut(cpin.buffer()).set_lsn(recptr);
            }
            page_of_mut(&buf).set_lsn(recptr);
        }

        if let Some(metapin) = metabuf {
            bt_relbuf(rel, metapin)?;
        }
        if let Some(cpin) = cbuf {
            bt_relbuf(rel, cpin)?;
        }

        let blockcache = if isrightmost && isleaf && !isroot {
            buf.block_number()
        } else {
            InvalidBlockNumber
        };

        bt_relbuf(rel, buf)?;

        if blockcache != InvalidBlockNumber
            && crate::page::bt_getrootheight(rel)? >= BTREE_FASTPATH_MIN_LEVEL
        {
            set_target_block(rel, blockcache);
        }

        Ok(())
    }
}

/// _bt_split. Returns the new right sibling, pinned + write-locked; the pin
/// and lock on `buf` are kept. `orignewitem`/`nposting` are non-null iff
/// `postingoff != 0` (posting-list split coinciding with the page split).
///
/// # Safety
/// As [`bt_insertonpg`].
unsafe fn bt_split<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    heaprel: &::types_rel::RelationData<'mcx>,
    itup_key: Option<&mut BtScanInsert>,
    frame: &mut OrderProcFrame,
    buf: &BufferPin,
    cbuf: Option<BufferPin>,
    newitemoff: OffsetNumber,
    newitemsz: usize,
    newitem: ITup,
    orignewitem: ITup,
    nposting: ITup,
    postingoff: u16,
) -> PgResult<BufferPin> {
    let origpagenumber = buf.block_number();
    let (isleaf, isrightmost, maxoff, orig_flags, orig_prev, orig_next, orig_level, orig_lsn) = {
        let page = buf.page();
        let opaque = page_opaque(&page);
        (
            P_ISLEAF(&opaque),
            P_RIGHTMOST(&opaque),
            page.max_offset_number(),
            opaque.btpo_flags,
            opaque.btpo_prev,
            opaque.btpo_next,
            opaque.btpo_level,
            page.lsn(),
        )
    };

    let origpage = buf.page();
    let (firstrightoff, newitemonleft) =
        crate::splitloc::bt_findsplitloc(mcx, rel, &origpage, newitemoff, newitemsz, newitem)?;

    // PageGetTempPage: MAXALIGNed scratch image (ItupBuf carries align 8).
    let mut lefttemp = ItupBuf::with_size(mcx, ::types_core::BLCKSZ)?;
    let leftptr = core::ptr::NonNull::new(lefttemp.as_mut_ptr()).expect("page");
    // SAFETY: owned, zeroed, 8-aligned BLCKSZ scratch.
    let mut leftpage = PageMut::from_raw(leftptr);
    bt_pageinit(&mut leftpage);

    let mut lopaque = BTPageOpaqueData {
        btpo_prev: orig_prev,
        btpo_next: 0, // set after rightpage is acquired
        btpo_level: orig_level,
        btpo_flags: (orig_flags & !(BTP_ROOT | BTP_SPLIT_END | BTP_HAS_GARBAGE))
            | BTP_INCOMPLETE_SPLIT,
        btpo_cycleid: 0, // set after rightpage is acquired
    };
    write_opaque(&mut leftpage, &lopaque);
    leftpage.set_lsn(orig_lsn);

    // lastleft/firstright come from an imaginary origpage already holding nposting.
    let origpagepostingoff: OffsetNumber = if postingoff != 0 {
        debug_assert!(isleaf);
        debug_assert!(ItemPointerCompare(&t_tid(orignewitem), &t_tid(newitem)) < 0);
        debug_assert!(bt_tuple_is_posting(nposting));
        newitemoff - 1
    } else {
        InvalidOffsetNumber
    };

    let (firstright, firstright_sz): (ITup, usize) =
        if !newitemonleft && newitemoff == firstrightoff {
            (newitem, newitemsz)
        } else {
            let itemid = origpage.item_id(firstrightoff);
            let mut fr = page_item(&origpage, itemid);
            if firstrightoff == origpagepostingoff {
                fr = nposting;
            }
            (fr, itemid.lp_len() as usize)
        };

    let lefthighkey_owned: ItupBuf<'mcx>;
    let (lefthighkey, lefthighkey_sz): (ITup, usize) = if isleaf {
        let lastleft: ITup = if newitemonleft && newitemoff == firstrightoff {
            newitem
        } else {
            let lastleftoff = firstrightoff - 1;
            debug_assert!(lastleftoff >= P_FIRSTDATAKEY(&page_opaque(&origpage)));
            if lastleftoff == origpagepostingoff {
                nposting
            } else {
                page_item(&origpage, origpage.item_id(lastleftoff))
            }
        };

        let itup_key = itup_key.expect("leaf split has an insertion key");
        lefthighkey_owned = bt_truncate(mcx, rel, lastleft, firstright, itup_key, frame)?;
        // IndexTupleSize, not the buffer size: the posting-chop arm of
        // _bt_truncate shrinks t_info below the allocation.
        (
            lefthighkey_owned.as_ptr(),
            maxalign(index_tuple_size(lefthighkey_owned.as_ptr())),
        )
    } else {
        (firstright, maxalign(firstright_sz))
    };

    let mut afterleftoff = P_HIKEY;
    debug_assert!(bt_tuple_get_natts(lefthighkey, rel.indnatts()) > 0);
    debug_assert!(bt_tuple_get_natts(lefthighkey, rel.indnatts()) <= rel.indnkeyatts());
    debug_assert!(lefthighkey_sz == maxalign(index_tuple_size(lefthighkey)));
    if leftpage
        .add_item(
            core::slice::from_raw_parts(lefthighkey, lefthighkey_sz),
            afterleftoff,
            0,
        )
        .is_none()
    {
        return Err(split_failed(rel, origpagenumber, "high key", "left"));
    }
    afterleftoff += 1;

    let rbuf = bt_allocbuf(rel, heaprel)?;
    let rightpagenumber = rbuf.block_number();

    lopaque.btpo_next = rightpagenumber;
    lopaque.btpo_cycleid = bt_vacuum_cycleid(rel);
    write_opaque(&mut leftpage, &lopaque);

    let mut ropaque = BTPageOpaqueData {
        btpo_prev: origpagenumber,
        btpo_next: orig_next,
        btpo_level: orig_level,
        btpo_flags: orig_flags & !(BTP_ROOT | BTP_SPLIT_END | BTP_HAS_GARBAGE),
        btpo_cycleid: lopaque.btpo_cycleid,
    };
    write_opaque(&mut page_of_mut(&rbuf), &ropaque);

    let mut afterrightoff = P_HIKEY;
    if !isrightmost {
        let itemid = origpage.item_id(P_HIKEY);
        let righthighkey = page_item(&origpage, itemid);
        debug_assert!(bt_tuple_get_natts(righthighkey, rel.indnatts()) > 0);
        debug_assert!(bt_tuple_get_natts(righthighkey, rel.indnatts()) <= rel.indnkeyatts());
        if page_of_mut(&rbuf)
            .add_item(
                core::slice::from_raw_parts(righthighkey, itemid.lp_len() as usize),
                afterrightoff,
                0,
            )
            .is_none()
        {
            zero_page(&rbuf);
            return Err(split_failed(rel, origpagenumber, "high key", "right"));
        }
        afterrightoff += 1;
    }

    let minusinfoff: OffsetNumber = if !isleaf { afterrightoff } else { InvalidOffsetNumber };

    let mut i = P_FIRSTDATAKEY(&page_opaque(&origpage));
    while i <= maxoff {
        let itemid = origpage.item_id(i);
        let dataitemsz = itemid.lp_len() as usize;
        let mut dataitem = page_item(&origpage, itemid);

        if i == origpagepostingoff {
            debug_assert!(bt_tuple_is_posting(dataitem));
            debug_assert!(dataitemsz == maxalign(index_tuple_size(nposting)));
            dataitem = nposting;
        } else if i == newitemoff {
            if newitemonleft {
                debug_assert!(newitemoff <= firstrightoff);
                if !bt_pgaddtup(&mut leftpage, newitemsz, newitem, afterleftoff, false) {
                    zero_page(&rbuf);
                    return Err(split_failed(rel, origpagenumber, "new item", "left"));
                }
                afterleftoff += 1;
            } else {
                debug_assert!(newitemoff >= firstrightoff);
                if !bt_pgaddtup(
                    &mut page_of_mut(&rbuf),
                    newitemsz,
                    newitem,
                    afterrightoff,
                    afterrightoff == minusinfoff,
                ) {
                    zero_page(&rbuf);
                    return Err(split_failed(rel, origpagenumber, "new item", "right"));
                }
                afterrightoff += 1;
            }
        }

        if i < firstrightoff {
            if !bt_pgaddtup(&mut leftpage, dataitemsz, dataitem, afterleftoff, false) {
                zero_page(&rbuf);
                return Err(split_failed(rel, origpagenumber, "old item", "left"));
            }
            afterleftoff += 1;
        } else {
            if !bt_pgaddtup(
                &mut page_of_mut(&rbuf),
                dataitemsz,
                dataitem,
                afterrightoff,
                afterrightoff == minusinfoff,
            ) {
                zero_page(&rbuf);
                return Err(split_failed(rel, origpagenumber, "old item", "right"));
            }
            afterrightoff += 1;
        }
        i += 1;
    }

    if i <= newitemoff {
        debug_assert!(!newitemonleft && newitemoff == maxoff + 1);
        if !bt_pgaddtup(
            &mut page_of_mut(&rbuf),
            newitemsz,
            newitem,
            afterrightoff,
            afterrightoff == minusinfoff,
        ) {
            zero_page(&rbuf);
            return Err(split_failed(rel, origpagenumber, "new item", "right"));
        }
        #[allow(unused_assignments)]
        {
            afterrightoff += 1;
        }
    }

    let mut sbuf: Option<BufferPin> = None;
    if !isrightmost {
        let pin = bt_getbuf(rel, orig_next, BT_WRITE)?;
        let sopaque = page_opaque(&pin.page());
        if sopaque.btpo_prev != origpagenumber {
            zero_page(&rbuf);
            return Err(Box::new(
                PgError::error(format!(
                    "right sibling's left-link doesn't match: block {} links to {} instead of expected {} in index \"{}\"",
                    orig_next, sopaque.btpo_prev, origpagenumber, rel.name()
                ))
                .with_sqlstate(::types_error::ERRCODE_INDEX_CORRUPTED),
            ));
        }
        if sopaque.btpo_cycleid != ropaque.btpo_cycleid {
            ropaque.btpo_flags |= BTP_SPLIT_END;
            write_opaque(&mut page_of_mut(&rbuf), &ropaque);
        }
        sbuf = Some(pin);
    }

    {
        let orig = page_of_mut(buf);
        // SAFETY: PageRestoreTempPage — whole-page overwrite under the
        // exclusive lock held since descent.
        core::ptr::copy_nonoverlapping(
            lefttemp.as_ptr(),
            orig.as_ref().as_ptr().cast_mut(),
            ::types_core::BLCKSZ,
        );
    }

    bufmgr::mark_buffer_dirty::call(buf.buffer())?;
    bufmgr::mark_buffer_dirty::call(rbuf.buffer())?;

    if let Some(spin) = sbuf.as_ref() {
        let mut sopaque = page_opaque(&spin.page());
        sopaque.btpo_prev = rightpagenumber;
        write_opaque(&mut buf_page_mut(spin.buffer()), &sopaque);
        bufmgr::mark_buffer_dirty::call(spin.buffer())?;
    }

    if let Some(cpin) = cbuf.as_ref() {
        let mut copaque = page_opaque(&cpin.page());
        copaque.btpo_flags &= !BTP_INCOMPLETE_SPLIT;
        write_opaque(&mut buf_page_mut(cpin.buffer()), &copaque);
        bufmgr::mark_buffer_dirty::call(cpin.buffer())?;
    }

    if relation_needs_wal(rel) {
        // postingoff stays zero when nposting and newitem both go right; else
        // orignewitem is logged and redo re-runs _bt_swap_posting.
        let xl_postingoff: u16 = if postingoff != 0 && origpagepostingoff < firstrightoff {
            postingoff
        } else {
            0
        };
        let xlrec =
            crate::wal::xl_btree_split(ropaque.btpo_level, firstrightoff, newitemoff, xl_postingoff);

        let newitem_bytes = core::slice::from_raw_parts(newitem, newitemsz);
        // the left high key is re-read from the restored origpage (C reads it
        // post-restore for the !isleaf case; the image is identical for leaf).
        let restored = buf.page();
        let hk_id = restored.item_id(P_HIKEY);
        let hk = page_item(&restored, hk_id);
        let hk_bytes = core::slice::from_raw_parts(hk, maxalign(index_tuple_size(hk)));

        let mut leftfrags: [&[u8]; 2] = [&[], &[]];
        let mut nleft = 0;
        if newitemonleft && xl_postingoff == 0 {
            leftfrags[nleft] = newitem_bytes;
            nleft += 1;
        } else if xl_postingoff != 0 {
            debug_assert!(isleaf);
            debug_assert!(newitemonleft || firstrightoff == newitemoff);
            debug_assert!(newitemsz == maxalign(index_tuple_size(orignewitem)));
            leftfrags[nleft] = core::slice::from_raw_parts(orignewitem, newitemsz);
            nleft += 1;
        }
        leftfrags[nleft] = hk_bytes;
        nleft += 1;

        let rpage = rbuf.page();
        let rupper = rpage.pd_upper() as usize;
        let rspecial = rpage.pd_special() as usize;
        let rcontents =
            core::slice::from_raw_parts(rpage.as_ptr().add(rupper), rspecial - rupper);
        let rfrags: [&[u8]; 1] = [rcontents];

        let mut regbufs: [XLogRegBuf<'_>; 4] = [
            XLogRegBuf {
                block_id: 0,
                buffer: buf.buffer(),
                flags: REGBUF_STANDARD,
                bufdata: &leftfrags[..nleft],
            },
            XLogRegBuf {
                block_id: 1,
                buffer: rbuf.buffer(),
                flags: REGBUF_WILL_INIT,
                bufdata: &rfrags,
            },
            XLogRegBuf { block_id: 0, buffer: 0, flags: 0, bufdata: &[] },
            XLogRegBuf { block_id: 0, buffer: 0, flags: 0, bufdata: &[] },
        ];
        let mut n = 2;
        if let Some(spin) = sbuf.as_ref() {
            regbufs[n] = XLogRegBuf {
                block_id: 2,
                buffer: spin.buffer(),
                flags: REGBUF_STANDARD,
                bufdata: &[],
            };
            n += 1;
        }
        if let Some(cpin) = cbuf.as_ref() {
            regbufs[n] = XLogRegBuf {
                block_id: 3,
                buffer: cpin.buffer(),
                flags: REGBUF_STANDARD,
                bufdata: &[],
            };
            n += 1;
        }

        let xlinfo = if newitemonleft {
            XLOG_BTREE_SPLIT_L
        } else {
            XLOG_BTREE_SPLIT_R
        };
        let recptr = ::xloginsert_seams::xlog_insert_record::call(
            ::rmgr::RM_BTREE_ID as u8,
            xlinfo,
            0,
            &[&xlrec],
            &regbufs[..n],
        )?;

        page_of_mut(buf).set_lsn(recptr);
        page_of_mut(&rbuf).set_lsn(recptr);
        if let Some(spin) = sbuf.as_ref() {
            buf_page_mut(spin.buffer()).set_lsn(recptr);
        }
        if let Some(cpin) = cbuf.as_ref() {
            buf_page_mut(cpin.buffer()).set_lsn(recptr);
        }
    }

    if let Some(spin) = sbuf {
        bt_relbuf(rel, spin)?;
    }
    if let Some(cpin) = cbuf {
        bt_relbuf(rel, cpin)?;
    }

    Ok(rbuf)
}

fn zero_page(pin: &BufferPin) {
    let mut page = page_of_mut(pin);
    // SAFETY: rightpage error path — never leave a half-built page behind.
    unsafe {
        core::ptr::write_bytes(page.as_ref().as_ptr().cast_mut(), 0, ::types_core::BLCKSZ)
    };
    let _ = &mut page;
}

#[track_caller]
#[cold]
#[inline(never)]
fn split_failed(rel: &Relation<'_>, blkno: BlockNumber, what: &str, side: &str) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "failed to add {what} to the {side} sibling while splitting block {blkno} of index \"{}\"",
        rel.name()
    )))
}

/// _bt_insert_parent.
///
/// # Safety
/// `buf`/`rbuf` are the write-locked split halves.
unsafe fn bt_insert_parent<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    heaprel: &::types_rel::RelationData<'mcx>,
    frame: &mut OrderProcFrame,
    buf: BufferPin,
    rbuf: BufferPin,
    stack: &mut [StackEntry],
    isroot: bool,
    isonly: bool,
) -> PgResult<()> {
    if isroot {
        debug_assert!(stack.is_empty());
        debug_assert!(isonly);
        let rootbuf = bt_newlevel(mcx, rel, heaprel, &buf, &rbuf)?;
        bt_relbuf(rel, rootbuf)?;
        bt_relbuf(rel, rbuf)?;
        bt_relbuf(rel, buf)?;
        return Ok(());
    }

    let bknum = buf.block_number();
    let rbknum = rbuf.block_number();

    let new_item: ItupBuf<'mcx> = {
        let page = buf.page();
        let ritem = page_item(&page, page.item_id(P_HIKEY));
        let mut c = copy_index_tuple(mcx, ritem)?;
        bt_tuple_set_downlink(c.as_mut_ptr(), rbknum);
        c
    };

    // No descent stack (concurrent root split, or _bt_finish_split with a
    // stackless caller): phony entry at the leftmost page one level up;
    // bt_getstackbuf corrects blkno/offset. C's elog(DEBUG2) elided.
    let mut fakestack = StackEntry { blkno: InvalidBlockNumber, offset: InvalidOffsetNumber };
    let mut empty: [StackEntry; 0] = [];
    let (top, parent_stack) = if stack.is_empty() {
        let opaque = page_opaque(&buf.page());
        // fastpath never splits the rightmost leaf without a stack
        debug_assert!(!(P_ISLEAF(&opaque) && target_block(rel) != InvalidBlockNumber));
        let pbuf = crate::search::bt_get_endpoint(rel, opaque.btpo_level + 1, false)?
            .expect("split page implies a non-empty index");
        fakestack.blkno = pbuf.block_number();
        bt_relbuf(rel, pbuf)?;
        (&mut fakestack, &mut empty[..])
    } else {
        stack.split_last_mut().expect("non-empty")
    };
    let pbuf = bt_getstackbuf(rel, heaprel, frame, top, parent_stack, bknum)?;

    bt_relbuf(rel, rbuf)?;

    let Some(pbuf) = pbuf else {
        return Err(Box::new(
            PgError::error(format!(
                "failed to re-find parent key in index \"{}\" for split pages {}/{}",
                rel.name(),
                bknum,
                rbknum
            ))
            .with_sqlstate(::types_error::ERRCODE_INDEX_CORRUPTED),
        ));
    };

    let sz = maxalign(index_tuple_size(new_item.as_ptr()));
    bt_insertonpg(
        mcx,
        rel,
        heaprel,
        None,
        frame,
        pbuf,
        Some(buf),
        parent_stack,
        new_item.as_ptr(),
        sz,
        top.offset + 1,
        0,
        isonly,
    )
}

/// _bt_finish_split.
///
/// # Safety
/// `lbuf` pinned + write-locked with P_INCOMPLETE_SPLIT set.
pub(crate) unsafe fn bt_finish_split<'mcx>(
    rel: &Relation<'mcx>,
    heaprel: &::types_rel::RelationData<'mcx>,
    lbuf: BufferPin,
    stack: &mut [StackEntry],
    frame: &mut OrderProcFrame,
) -> PgResult<()> {
    let lopaque = page_opaque(&lbuf.page());
    debug_assert!(P_INCOMPLETE_SPLIT(&lopaque));

    let rbuf = bt_getbuf(rel, lopaque.btpo_next, BT_WRITE)?;

    let wasroot = if stack.is_empty() {
        let metapin = bt_getbuf(rel, BTREE_METAPAGE, BT_WRITE)?;
        let metad = crate::page::page_meta(&metapin.page());
        let wasroot = metad.btm_root == lbuf.block_number();
        bt_relbuf(rel, metapin)?;
        wasroot
    } else {
        false
    };

    let wasonly = P_LEFTMOST(&lopaque) && P_RIGHTMOST(&page_opaque(&rbuf.page()));

    // no bump allocations outlive this call: scratch context suffices
    let cx = ::mcx::MemoryContext::new("bt_finish_split");
    bt_insert_parent(cx.mcx(), rel, heaprel, frame, lbuf, rbuf, stack, wasroot, wasonly)
}

/// _bt_getstackbuf.
///
/// # Safety
/// caller in the parent-insertion protocol (child pages locked).
pub(crate) unsafe fn bt_getstackbuf<'mcx>(
    rel: &Relation<'mcx>,
    heaprel: &::types_rel::RelationData<'mcx>,
    frame: &mut OrderProcFrame,
    top: &mut StackEntry,
    parent_stack: &mut [StackEntry],
    child: BlockNumber,
) -> PgResult<Option<BufferPin>> {
    let mut blkno = top.blkno;
    let mut start = top.offset;

    loop {
        let pin = bt_getbuf(rel, blkno, BT_WRITE)?;
        let opaque = page_opaque(&pin.page());

        if P_INCOMPLETE_SPLIT(&opaque) {
            bt_finish_split(rel, heaprel, pin, parent_stack, frame)?;
            continue;
        }

        if !P_IGNORE(&opaque) {
            let page = pin.page();
            let minoff = P_FIRSTDATAKEY(&opaque);
            let maxoff = page.max_offset_number();

            if start < minoff {
                start = minoff;
            }
            if start > maxoff {
                start = maxoff + 1;
            }

            let mut offnum = start;
            while offnum <= maxoff {
                let item = page_item(&page, page.item_id(offnum));
                if bt_tuple_get_downlink(item) == child {
                    top.blkno = blkno;
                    top.offset = offnum;
                    return Ok(Some(pin));
                }
                offnum += 1;
            }

            let mut offnum = start;
            while offnum > minoff {
                offnum -= 1;
                let item = page_item(&page, page.item_id(offnum));
                if bt_tuple_get_downlink(item) == child {
                    top.blkno = blkno;
                    top.offset = offnum;
                    return Ok(Some(pin));
                }
            }
        }

        if P_RIGHTMOST(&opaque) {
            bt_relbuf(rel, pin)?;
            return Ok(None);
        }
        blkno = opaque.btpo_next;
        start = InvalidOffsetNumber;
        bt_relbuf(rel, pin)?;
    }
}

/// _bt_newlevel: root split.
///
/// # Safety
/// `lbuf` (old root) and `rbuf` (its new sibling) write-locked.
unsafe fn bt_newlevel<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    heaprel: &::types_rel::RelationData<'mcx>,
    lbuf: &BufferPin,
    rbuf: &BufferPin,
) -> PgResult<BufferPin> {
    let lbkno = lbuf.block_number();
    let rbkno = rbuf.block_number();

    let rootbuf = bt_allocbuf(rel, heaprel)?;
    let rootblknum = rootbuf.block_number();

    let metabuf = bt_getbuf(rel, BTREE_METAPAGE, BT_WRITE)?;

    // left downlink: "minus infinity" 8-byte pivot.
    let mut left_item = ItupBuf::with_size(mcx, INDEX_TUPLE_HEADER_SIZE)?;
    let left_ptr = left_item.as_mut_ptr();
    set_t_info(left_ptr, INDEX_TUPLE_HEADER_SIZE as u16);
    bt_tuple_set_downlink(left_ptr, lbkno);
    bt_tuple_set_natts(left_ptr, 0, false);

    let lpage = lbuf.page();
    let hk_id = lpage.item_id(P_HIKEY);
    let hk = page_item(&lpage, hk_id);
    let right_item_sz = hk_id.lp_len() as usize;
    let mut right_item = ItupBuf::with_size(mcx, maxalign(right_item_sz))?;
    core::ptr::copy_nonoverlapping(hk, right_item.as_mut_ptr(), right_item_sz);
    bt_tuple_set_downlink(right_item.as_mut_ptr(), rbkno);

    if crate::page::page_meta(&metabuf.page()).btm_version < BTREE_NOVAC_VERSION {
        unported_phase2("_bt_upgrademetapage (v2/v3 pg_upgrade metapages)");
    }

    let rootlevel = page_opaque(&lpage).btpo_level + 1;
    write_opaque(
        &mut page_of_mut(&rootbuf),
        &BTPageOpaqueData {
            btpo_prev: P_NONE,
            btpo_next: P_NONE,
            btpo_level: rootlevel,
            btpo_flags: BTP_ROOT,
            btpo_cycleid: 0,
        },
    );

    let mut metad = crate::page::page_meta(&metabuf.page());
    metad.btm_root = rootblknum;
    metad.btm_level = rootlevel;
    metad.btm_fastroot = rootblknum;
    metad.btm_fastlevel = rootlevel;
    crate::page::write_meta(&metabuf, &metad);

    debug_assert!(bt_tuple_get_natts(left_item.as_ptr(), rel.indnatts()) == 0);
    if page_of_mut(&rootbuf)
        .add_item(
            core::slice::from_raw_parts(left_item.as_ptr(), INDEX_TUPLE_HEADER_SIZE),
            P_HIKEY,
            0,
        )
        .is_none()
    {
        panic!(
            "failed to add leftkey to new root page while splitting block {} of index \"{}\"",
            lbkno,
            rel.name()
        );
    }
    debug_assert!(bt_tuple_get_natts(right_item.as_ptr(), rel.indnatts()) > 0);
    debug_assert!(bt_tuple_get_natts(right_item.as_ptr(), rel.indnatts()) <= rel.indnkeyatts());
    if page_of_mut(&rootbuf)
        .add_item(
            core::slice::from_raw_parts(right_item.as_ptr(), right_item_sz),
            P_FIRSTKEY,
            0,
        )
        .is_none()
    {
        panic!(
            "failed to add rightkey to new root page while splitting block {} of index \"{}\"",
            lbkno,
            rel.name()
        );
    }

    {
        let mut lopaque = page_opaque(&lpage);
        debug_assert!(P_INCOMPLETE_SPLIT(&lopaque));
        lopaque.btpo_flags &= !BTP_INCOMPLETE_SPLIT;
        write_opaque(&mut buf_page_mut(lbuf.buffer()), &lopaque);
    }
    bufmgr::mark_buffer_dirty::call(lbuf.buffer())?;
    bufmgr::mark_buffer_dirty::call(rootbuf.buffer())?;
    bufmgr::mark_buffer_dirty::call(metabuf.buffer())?;

    if relation_needs_wal(rel) {
        let xlrec = crate::wal::xl_btree_newroot(rootblknum, metad.btm_level);
        let md = crate::wal::xl_btree_metadata(&metad);

        let rootpage = rootbuf.page();
        let rupper = rootpage.pd_upper() as usize;
        let rspecial = rootpage.pd_special() as usize;
        let rcontents =
            core::slice::from_raw_parts(rootpage.as_ptr().add(rupper), rspecial - rupper);
        let rootfrags: [&[u8]; 1] = [rcontents];
        let mdfrags: [&[u8]; 1] = [&md];

        let recptr = ::xloginsert_seams::xlog_insert_record::call(
            ::rmgr::RM_BTREE_ID as u8,
            XLOG_BTREE_NEWROOT,
            0,
            &[&xlrec],
            &[
                XLogRegBuf {
                    block_id: 0,
                    buffer: rootbuf.buffer(),
                    flags: REGBUF_WILL_INIT,
                    bufdata: &rootfrags,
                },
                XLogRegBuf {
                    block_id: 1,
                    buffer: lbuf.buffer(),
                    flags: REGBUF_STANDARD,
                    bufdata: &[],
                },
                XLogRegBuf {
                    block_id: 2,
                    buffer: metabuf.buffer(),
                    flags: REGBUF_WILL_INIT | REGBUF_STANDARD,
                    bufdata: &mdfrags,
                },
            ],
        )?;

        buf_page_mut(lbuf.buffer()).set_lsn(recptr);
        page_of_mut(&rootbuf).set_lsn(recptr);
        page_of_mut(&metabuf).set_lsn(recptr);
    }

    bt_relbuf(rel, metabuf)?;
    Ok(rootbuf)
}

/// _bt_pgaddtup.
///
/// # Safety
/// `itup` live; `page` exclusively held.
unsafe fn bt_pgaddtup(
    page: &mut PageMut<'_>,
    itemsize: usize,
    itup: ITup,
    itup_off: OffsetNumber,
    newfirstdataitem: bool,
) -> bool {
    if newfirstdataitem {
        #[repr(C, align(8))]
        struct Trunc([u8; INDEX_TUPLE_HEADER_SIZE]);
        let mut trunc = Trunc([0u8; INDEX_TUPLE_HEADER_SIZE]);
        core::ptr::copy_nonoverlapping(itup, trunc.0.as_mut_ptr(), INDEX_TUPLE_HEADER_SIZE);
        set_t_info(trunc.0.as_mut_ptr(), INDEX_TUPLE_HEADER_SIZE as u16);
        bt_tuple_set_natts(trunc.0.as_mut_ptr(), 0, false);
        return page.add_item(&trunc.0, itup_off, 0).is_some();
    }
    page.add_item(
        core::slice::from_raw_parts(itup, itemsize),
        itup_off,
        0,
    )
    .is_some()
}

/// amcheck bt_rootdescend (verify_nbtree.c): search for `itup` (a non-pivot
/// tuple) starting from the fast root; returns whether an exact match exists.
/// Requires a heapkeyspace index. `itup` must be a live index-tuple image.
///
/// # Safety
/// `itup` points at a valid on-page non-pivot IndexTuple for `rel`.
pub unsafe fn bt_rootdescend<'mcx>(
    rel: &Relation<'mcx>,
    itup: ITup,
) -> PgResult<bool> {
    let mut key = crate::utils::bt_mkscankey(rel, Some(itup))?;
    debug_assert!(key.heapkeyspace && key.scantid.is_some());

    let mut frame = OrderProcFrame::new();
    let Some(leaf) = crate::search::bt_search(rel, &mut key, &mut frame)? else {
        return Ok(false);
    };

    let mut insertstate = InsertState {
        itup,
        itemsz: maxalign(index_tuple_size(itup)),
        itup_key: &mut key,
        buf: Some(leaf),
        bounds_valid: false,
        low: InvalidOffsetNumber,
        stricthigh: InvalidOffsetNumber,
        postingoff: 0,
    };

    // SAFETY: insertstate.buf is the pinned+locked leaf from bt_search.
    let offnum = unsafe { bt_binsrch_insert(rel, &mut insertstate, &mut frame)? };
    let pin = insertstate.buf.as_ref().expect("pinned leaf");
    let page = pin.page();

    let exists = offnum <= page.max_offset_number()
        && insertstate.postingoff <= 0
        && crate::search::bt_compare(rel, insertstate.itup_key, &page, offnum, &mut frame)? == 0;

    bt_relbuf(rel, insertstate.buf.take().expect("pinned leaf"))?;
    Ok(exists)
}
