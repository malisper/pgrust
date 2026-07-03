//! nbtinsert.c: descent-for-insert (rightmost-block fastpath cache),
//! _bt_check_unique (UNIQUE_CHECK_YES arm), _bt_findinsertloc, _bt_insertonpg,
//! _bt_split + parent insertion + root split. Loud: posting-list splits and
//! deduplication (nbtdedup unit), simple/bottom-up deletion, speculative and
//! deferred unique checks, unique-conflict waits, !heapkeyspace indexes.

use std::cell::Cell;

use ::bufmgr_seams::{self as bufmgr, BufferPin};
use ::datum::Datum;
use ::mcx::Mcx;
use ::types_core::{BlockNumber, InvalidBlockNumber, OffsetNumber, Oid};
use ::types_error::{PgError, PgResult, ERRCODE_UNIQUE_VIOLATION};
use ::types_nbtree::{
    BTMetaPageData, BTPageOpaqueData, BTP_HAS_GARBAGE, BTP_INCOMPLETE_SPLIT, BTP_ROOT,
    BTP_SPLIT_END, BTREE_METAPAGE, BTREE_NOVAC_VERSION, BT_READ, BT_WRITE, P_FIRSTDATAKEY,
    P_FIRSTKEY, P_HIKEY, P_IGNORE, P_INCOMPLETE_SPLIT, P_ISLEAF, P_ISROOT, P_LEFTMOST, P_NONE,
    P_RIGHTMOST, XLOG_BTREE_INSERT_LEAF, XLOG_BTREE_INSERT_META, XLOG_BTREE_INSERT_UPPER,
    XLOG_BTREE_NEWROOT, XLOG_BTREE_SPLIT_L, XLOG_BTREE_SPLIT_R,
};
use ::types_nbtree::genam::IndexUniqueCheck;
use ::types_rel::Relation;
use ::types_snapshot::{SnapshotData, SnapshotType};
use ::types_storage::bufpage::{PageMut, PageRef, SizeOfPageHeaderData};
use ::types_tuple::itemptr::{InvalidOffsetNumber, ItemPointerData};
use ::xloginsert_seams::{XLogRegBuf, REGBUF_STANDARD, REGBUF_WILL_INIT};

use crate::fcframe::OrderProcFrame;
use crate::itup::{
    bt_tuple_get_downlink, bt_tuple_get_natts, bt_tuple_is_pivot, bt_tuple_is_posting,
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
    blkno: BlockNumber,
    offset: OffsetNumber,
}

// RelationGetTargetBlock/RelationSetTargetBlock over the index relation
// (rd_smgr targblock cache); keyed thread-local since rd_smgr is unported.
thread_local! {
    static TARGET_BLOCK: Cell<(Oid, BlockNumber)> = const { Cell::new((0, InvalidBlockNumber)) };
}

fn target_block(rel: &Relation<'_>) -> BlockNumber {
    let (oid, blk) = TARGET_BLOCK.get();
    if oid == rel.rd_id {
        blk
    } else {
        InvalidBlockNumber
    }
}

fn set_target_block(rel: &Relation<'_>, blk: BlockNumber) {
    TARGET_BLOCK.set((rel.rd_id, blk));
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
    if matches!(
        check_unique,
        IndexUniqueCheck::UNIQUE_CHECK_PARTIAL | IndexUniqueCheck::UNIQUE_CHECK_EXISTING
    ) {
        unported_phase2("UNIQUE_CHECK_PARTIAL/EXISTING (speculative/deferred unique lane)");
    }

    let mut itup_key = bt_mkscankey(rel, Some(itup))?;
    let mut frame = OrderProcFrame::new();

    if checkingunique {
        if !itup_key.anynullkeys {
            itup_key.scantid = None;
        } else {
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
    bt_search_insert(rel, &mut insertstate, &mut frame, &mut stack)?;

    if checkingunique {
        // SAFETY: insertstate.buf pinned + write-locked by the search.
        unsafe { bt_check_unique(mcx, rel, &mut insertstate, heap_rel, &mut frame)? };
        is_unique = true;

        if insertstate.itup_key.heapkeyspace {
            // SAFETY: owned image.
            insertstate.itup_key.scantid = Some(unsafe { t_tid(itup) });
        }
    }

    {
        let buf = insertstate.buf.as_ref().expect("leaf pinned");
        predicate_seams::check_for_serializable_conflict_in::call(rel, None, buf.block_number())?;
    }
    // SAFETY: insertstate.buf pinned + write-locked.
    unsafe {
        let newitemoff = bt_findinsertloc(
            rel,
            &mut insertstate,
            checkingunique,
            index_unchanged,
            heap_rel,
            &mut frame,
        )?;
        let buf = insertstate.buf.take().expect("leaf pinned");
        let itemsz = insertstate.itemsz;
        bt_insertonpg(
            mcx, rel, Some(insertstate.itup_key), &mut frame, buf, None, &mut stack, itup,
            itemsz, newitemoff, false,
        )?;
    }

    Ok(is_unique)
}

/// _bt_search_insert: rightmost-leaf fastpath cache, else full descent.
fn bt_search_insert<'mcx>(
    rel: &Relation<'mcx>,
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

    insertstate.buf = Some(bt_search_write(rel, insertstate.itup_key, frame, stack)?);
    Ok(())
}

/// _bt_search, BT_WRITE arm with descent stack (C's one fn splits on access).
fn bt_search_write<'mcx>(
    rel: &Relation<'mcx>,
    key: &mut BtScanInsert,
    frame: &mut OrderProcFrame,
    stack: &mut ::mcx::PgVec<'mcx, StackEntry>,
) -> PgResult<BufferPin> {
    let mut page_access = BT_READ;

    let mut pin = bt_getroot(rel, BT_WRITE)?.expect("BT_WRITE getroot creates the root");

    loop {
        pin = bt_moveright_for_update(rel, key, pin, stack, page_access, frame)?;

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
        pin = bt_moveright_for_update(rel, key, pin, stack, BT_WRITE, frame)?;
    }

    Ok(pin)
}

/// _bt_moveright, forupdate arm (read arm lives in search.rs).
fn bt_moveright_for_update(
    rel: &Relation<'_>,
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
                unsafe { bt_finish_split(rel, pin, stack, frame)? };
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
            let itup = page_item(&page, page.item_id(mid));
            if bt_tuple_is_posting(itup) {
                unported_phase2("_bt_binsrch_posting split (nbtdedup posting lane)");
            }
        }
    }

    insertstate.low = low;
    insertstate.stricthigh = stricthigh;
    insertstate.bounds_valid = true;
    Ok(low)
}

/// _bt_check_unique, UNIQUE_CHECK_YES arm: dirty-snapshot visibility recheck
/// through the tableam; conflicts with in-flight xacts hit the loud wait arm.
///
/// # Safety
/// `insertstate.buf` pinned + write-locked.
unsafe fn bt_check_unique<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    insertstate: &mut InsertState<'_>,
    heap_rel: &Relation<'mcx>,
    frame: &mut OrderProcFrame,
) -> PgResult<()> {
    let itup = insertstate.itup;
    let mut nbuf: Option<BufferPin> = None;

    // The wait lane (valid xwait from the dirty snapshot) panics inside the
    // visibility read marshal; the sentinel here never sees the write-back.
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

    loop {
        if offset <= maxoff {
            if nbuf.is_none() && offset == insertstate.stricthigh {
                debug_assert!(insertstate.bounds_valid);
                debug_assert!(insertstate.low >= P_FIRSTDATAKEY(&opaque));
                debug_assert!(insertstate.low <= insertstate.stricthigh);
                break;
            }

            let curitemid = page.item_id(offset);
            if !curitemid.is_dead() {
                if bt_compare(rel, insertstate.itup_key, &page, offset, frame)? != 0 {
                    break;
                }
                let curitup = page_item(&page, curitemid);
                debug_assert!(!bt_tuple_is_pivot(curitup));
                if bt_tuple_is_posting(curitup) {
                    unported_phase2("_bt_check_unique posting-list TIDs (nbtdedup lane)");
                }
                let mut htid = t_tid(curitup);

                let mut all_dead = false;
                if ::tableam::table_index_fetch_tuple_check(
                    mcx,
                    heap_rel,
                    &mut htid,
                    &mut snapshot_dirty,
                    Some(&mut all_dead),
                )? {
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

                    // C divergence: errdetail Key/BuildIndexValueDescription
                    // omitted (genam lane unported); primary + SQLSTATE match.
                    return Err(unique_violation(rel));
                } else if all_dead {
                    mark_item_dead(&page, offset);
                    set_page_has_garbage(&page);
                    let dirty_buf = match nbuf.as_ref() {
                        Some(pin) => pin.buffer(),
                        None => insertstate.buf.as_ref().expect("pinned").buffer(),
                    };
                    bufmgr::mark_buffer_dirty_hint::call(dirty_buf, true)?;
                }
            }
        }

        if offset < maxoff {
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
            maxoff = page.max_offset_number();
            offset = P_FIRSTDATAKEY(&opaque);
        }
    }

    if let Some(pin) = nbuf {
        bt_relbuf(rel, pin)?;
    }
    Ok(())
}

#[cold]
#[inline(never)]
fn unique_violation(rel: &Relation<'_>) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "duplicate key value violates unique constraint \"{}\"",
            rel.name()
        ))
        .with_sqlstate(ERRCODE_UNIQUE_VIOLATION),
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
unsafe fn bt_findinsertloc(
    rel: &Relation<'_>,
    insertstate: &mut InsertState<'_>,
    checkingunique: bool,
    index_unchanged: bool,
    heap_rel: &Relation<'_>,
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
            bt_stepright(rel, insertstate, frame)?;
            uniquedup = true;
        }
    }

    {
        let pin = insertstate.buf.as_ref().expect("pinned");
        if pin.page().free_space() < insertstate.itemsz {
            bt_delete_or_dedup_one_page(rel, insertstate, false, checkingunique, uniquedup)?;
        }
    }

    debug_assert!({
        let pin = insertstate.buf.as_ref().expect("pinned");
        let page = pin.page();
        P_RIGHTMOST(&page_opaque(&page))
            || bt_compare(rel, insertstate.itup_key, &page, P_HIKEY, frame)? <= 0
    });

    let newitemoff = bt_binsrch_insert(rel, insertstate, frame)?;
    debug_assert!(insertstate.postingoff == 0);
    Ok(newitemoff)
}

/// _bt_stepright (write-coupled).
///
/// # Safety
/// As [`bt_findinsertloc`].
unsafe fn bt_stepright(
    rel: &Relation<'_>,
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
            bt_finish_split(rel, pin, &mut [], frame)?;
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

/// _bt_delete_or_dedup_one_page: LP_DEAD scan live; deletion/dedup own units.
///
/// # Safety
/// As [`bt_findinsertloc`].
unsafe fn bt_delete_or_dedup_one_page(
    rel: &Relation<'_>,
    insertstate: &mut InsertState<'_>,
    simpleonly: bool,
    checkingunique: bool,
    uniquedup: bool,
) -> PgResult<()> {
    let _ = rel;
    let pin = insertstate.buf.as_ref().expect("pinned");
    let page = pin.page();
    let opaque = page_opaque(&page);
    debug_assert!(P_ISLEAF(&opaque));

    let minoff = P_FIRSTDATAKEY(&opaque);
    let maxoff = page.max_offset_number();
    for offnum in minoff..=maxoff {
        if page.item_id(offnum).is_dead() {
            unported_phase2("_bt_simpledel_pass (simple index deletion lane)");
        }
    }

    if simpleonly || (checkingunique && !uniquedup) {
        return Ok(());
    }

    insertstate.bounds_valid = false;

    if uniquedup {
        unported_phase2("_bt_bottomupdel_pass (bottom-up deletion lane)");
    }

    // BTGetDeduplicateItems: index reloptions unported, default is on.
    if insertstate.itup_key.allequalimage {
        unported_phase2("_bt_dedup_pass (nbtdedup unit, own CATALOG row)");
    }
    Ok(())
}

/// _bt_insertonpg; postingoff arms panic earlier (nbtdedup lane), parameter
/// dropped; `cbuf` given iff inserting a downlink on an internal page.
///
/// # Safety
/// `buf` pinned + write-locked; `itup` a live owned tuple image.
unsafe fn bt_insertonpg<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    itup_key: Option<&mut BtScanInsert>,
    frame: &mut OrderProcFrame,
    buf: BufferPin,
    cbuf: Option<BufferPin>,
    stack: &mut [StackEntry],
    itup: ITup,
    itemsz: usize,
    newitemoff: OffsetNumber,
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

    if buf.page().free_space() < itemsz {
        debug_assert!(!split_only_page);
        let rbuf = bt_split(mcx, rel, itup_key, frame, &buf, cbuf, newitemoff, itemsz, itup)?;
        predicate_seams::predicate_lock_page_split::call(
            rel,
            buf.block_number(),
            rbuf.block_number(),
        )?;
        bt_insert_parent(mcx, rel, frame, buf, rbuf, stack, isroot, isonly)
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
            let itup_bytes = core::slice::from_raw_parts(itup, index_tuple_size(itup));
            let itup_frag: [&[u8]; 1] = [itup_bytes];
            let reg0 = XLogRegBuf {
                block_id: 0,
                buffer: buf.buffer(),
                flags: REGBUF_STANDARD,
                bufdata: &itup_frag,
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

            let recptr = if isleaf {
                call(XLOG_BTREE_INSERT_LEAF, &[reg0])?
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
/// and lock on `buf` are kept.
///
/// # Safety
/// As [`bt_insertonpg`].
unsafe fn bt_split<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    itup_key: Option<&mut BtScanInsert>,
    frame: &mut OrderProcFrame,
    buf: &BufferPin,
    cbuf: Option<BufferPin>,
    newitemoff: OffsetNumber,
    newitemsz: usize,
    newitem: ITup,
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

    // posting-split coincidence arms panic in bt_binsrch_insert; newitem is
    // always the caller's tuple here.

    let (firstright, firstright_sz): (ITup, usize) =
        if !newitemonleft && newitemoff == firstrightoff {
            (newitem, newitemsz)
        } else {
            let itemid = origpage.item_id(firstrightoff);
            (page_item(&origpage, itemid), itemid.lp_len() as usize)
        };

    let lefthighkey_owned: ItupBuf<'mcx>;
    let (lefthighkey, lefthighkey_sz): (ITup, usize) = if isleaf {
        let lastleft: ITup = if newitemonleft && newitemoff == firstrightoff {
            newitem
        } else {
            let lastleftoff = firstrightoff - 1;
            debug_assert!(lastleftoff >= P_FIRSTDATAKEY(&page_opaque(&origpage)));
            page_item(&origpage, origpage.item_id(lastleftoff))
        };

        let itup_key = itup_key.expect("leaf split has an insertion key");
        lefthighkey_owned = bt_truncate(mcx, rel, lastleft, firstright, itup_key, frame)?;
        (lefthighkey_owned.as_ptr(), lefthighkey_owned.size())
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

    let rbuf = bt_allocbuf(rel)?;
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
        let dataitem = page_item(&origpage, itemid);

        if i == newitemoff {
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

    // critical section: restore left image over origpage, then WAL.
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
        let xlrec = crate::wal::xl_btree_split(ropaque.btpo_level, firstrightoff, newitemoff, 0);

        let newitem_bytes = core::slice::from_raw_parts(newitem, newitemsz);
        // the left high key is re-read from the restored origpage (C reads it
        // post-restore for the !isleaf case; the image is identical for leaf).
        let restored = buf.page();
        let hk_id = restored.item_id(P_HIKEY);
        let hk = page_item(&restored, hk_id);
        let hk_bytes = core::slice::from_raw_parts(hk, maxalign(index_tuple_size(hk)));

        let mut leftfrags: [&[u8]; 2] = [&[], &[]];
        let mut nleft = 0;
        if newitemonleft {
            leftfrags[nleft] = newitem_bytes;
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
        let rootbuf = bt_newlevel(mcx, rel, &buf, &rbuf)?;
        bt_relbuf(rel, rootbuf)?;
        bt_relbuf(rel, rbuf)?;
        bt_relbuf(rel, buf)?;
        return Ok(());
    }

    if stack.is_empty() {
        // C re-finds the parent level via _bt_get_endpoint after a concurrent
        // root split; unreachable with one backend per thread of control.
        unported_phase2("concurrent-root-split parent re-descent (_bt_insert_parent NULL stack)");
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

    let (top, parent_stack) = stack.split_last_mut().expect("non-empty");
    let pbuf = bt_getstackbuf(rel, frame, top, parent_stack, bknum)?;

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
        None,
        frame,
        pbuf,
        Some(buf),
        parent_stack,
        new_item.as_ptr(),
        sz,
        top.offset + 1,
        isonly,
    )
}

/// _bt_finish_split.
///
/// # Safety
/// `lbuf` pinned + write-locked with P_INCOMPLETE_SPLIT set.
pub(crate) unsafe fn bt_finish_split(
    rel: &Relation<'_>,
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
    bt_insert_parent(cx.mcx(), rel, frame, lbuf, rbuf, stack, wasroot, wasonly)
}

/// _bt_getstackbuf.
///
/// # Safety
/// caller in the parent-insertion protocol (child pages locked).
unsafe fn bt_getstackbuf<'mcx>(
    rel: &Relation<'mcx>,
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
            bt_finish_split(rel, pin, parent_stack, frame)?;
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
    lbuf: &BufferPin,
    rbuf: &BufferPin,
) -> PgResult<BufferPin> {
    let lbkno = lbuf.block_number();
    let rbkno = rbuf.block_number();

    let rootbuf = bt_allocbuf(rel)?;
    let rootblknum = rootbuf.block_number();

    let metabuf = bt_getbuf(rel, BTREE_METAPAGE, BT_WRITE)?;

    // left downlink: "minus infinity" 8-byte pivot.
    let mut left_item = ItupBuf::with_size(mcx, INDEX_TUPLE_HEADER_SIZE)?;
    let left_ptr = left_item.as_mut_ptr();
    set_t_info(left_ptr, INDEX_TUPLE_HEADER_SIZE as u16);
    bt_tuple_set_downlink(left_ptr, lbkno);
    bt_tuple_set_natts(left_ptr, 0, false);

    // right downlink: the left page's high key.
    let lpage = lbuf.page();
    let hk_id = lpage.item_id(P_HIKEY);
    let hk = page_item(&lpage, hk_id);
    let right_item_sz = hk_id.lp_len() as usize;
    let mut right_item = ItupBuf::with_size(mcx, maxalign(right_item_sz))?;
    core::ptr::copy_nonoverlapping(hk, right_item.as_mut_ptr(), right_item_sz);
    bt_tuple_set_downlink(right_item.as_mut_ptr(), rbkno);

    // critical section.
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
