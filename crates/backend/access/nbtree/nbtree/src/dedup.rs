//! nbtdedup.c write side: _bt_dedup_pass + single value strategy. The
//! interval state machine is shared with redo (types_nbtree::dedup). Loud:
//! _bt_bottomupdel_pass (deletion lane), _bt_update_posting (vacuum lane).

use ::bufmgr_seams::{self as bufmgr, BufferPin};
use ::types_core::{OffsetNumber, BLCKSZ};
use ::types_error::{PgError, PgResult};
use ::types_nbtree::dedup::BTDedupState;
use ::types_nbtree::{
    BTMaxItemSize, BTPageOpaqueData, BTP_HAS_GARBAGE, BTREE_SINGLEVAL_FILLFACTOR,
    P_FIRSTDATAKEY, P_HAS_GARBAGE, P_HIKEY, P_RIGHTMOST, XLOG_BTREE_DEDUP,
};
use ::types_rel::Relation;
use ::types_storage::bufpage::{PageMut, SizeOfPageHeaderData};
use ::types_tuple::itemptr::ItemPointerData;
use ::xloginsert_seams::{XLogRegBuf, REGBUF_STANDARD};

use crate::itup::{maxalign, ITup, INDEX_SIZE_MASK};
use crate::page::{page_item, page_of_mut, page_opaque, write_opaque};
use crate::relation_needs_wal;
use crate::utils::bt_keep_natts_fast;

const SizeOfBtreeOpaque: usize = core::mem::size_of::<BTPageOpaqueData>();
const SizeOfItemId: usize = core::mem::size_of::<::types_storage::bufpage::ItemIdData>();

#[repr(align(8))]
struct TempPage([u8; BLCKSZ]);

#[cold]
#[inline(never)]
fn dedup_add_failed(what: &str) -> Box<PgError> {
    Box::new(PgError::error(format!("deduplication failed to add {what}")))
}

/// _bt_dedup_pass.
///
/// # Safety
/// `buf` pinned + write-locked leaf page with no LP_DEAD items; `newitem` a
/// live tuple image; `newitemsz` its MAXALIGNed size sans line pointer.
pub(crate) unsafe fn bt_dedup_pass(
    rel: &Relation<'_>,
    buf: &BufferPin,
    newitem: ITup,
    newitemsz: usize,
    bottomupdedup: bool,
) -> PgResult<()> {
    let page = buf.page();
    let opaque = page_opaque(&page);
    let mut pagesaving = 0usize;
    let mut singlevalstrat = false;
    let nkeyatts = rel.indnkeyatts();

    let newitemsz = newitemsz + SizeOfItemId;

    let mut state = BTDedupState::new((BTMaxItemSize / 2).min(INDEX_SIZE_MASK as usize));

    let minoff = P_FIRSTDATAKEY(&opaque);
    let maxoff = page.max_offset_number();

    if !bottomupdedup {
        singlevalstrat = bt_do_singleval(rel, &page, &state, minoff, newitem);
    }

    // PageGetTempPageCopySpecial + LSN carry-over (XLogInsert may dump the
    // image, so the copy must claim the original's LSN).
    let mut temp = TempPage([0u8; BLCKSZ]);
    let mut newpage =
        PageMut::from_raw(core::ptr::NonNull::new(temp.0.as_mut_ptr()).expect("stack page"));
    newpage.init(SizeOfBtreeOpaque);
    core::ptr::copy_nonoverlapping(
        page.as_ptr().add(page.pd_special() as usize),
        newpage.as_ref().as_ptr().cast_mut().add(BLCKSZ - SizeOfBtreeOpaque),
        SizeOfBtreeOpaque,
    );
    newpage.set_lsn(page.lsn());

    if !P_RIGHTMOST(&opaque) {
        let hitemid = page.item_id(P_HIKEY);
        let hitem = page_item(&page, hitemid);
        let hslice = core::slice::from_raw_parts(hitem, hitemid.lp_len() as usize);
        if newpage.add_item(hslice, P_HIKEY, 0).is_none() {
            return Err(dedup_add_failed("highkey"));
        }
    }

    for offnum in minoff..=maxoff {
        let itemid = page.item_id(offnum);
        let itup = page_item(&page, itemid);
        debug_assert!(!itemid.is_dead());

        if offnum == minoff {
            state.start_pending(itup, offnum);
        } else if state.deduplicate
            && bt_keep_natts_fast(rel, state.base, itup) > nkeyatts
            && state.save_htid(itup)
        {
            // merged into the pending posting list
        } else {
            pagesaving += state
                .finish_pending(&mut newpage)
                .map_err(|()| dedup_add_failed("tuple to page"))?;

            if singlevalstrat {
                // cap the sixth and final large posting list tuple, then stop
                // merging altogether: remaining tuples wait for the page split
                if state.nmaxitems == 5 {
                    bt_singleval_fillfactor(&mut state, newitemsz);
                } else if state.nmaxitems == 6 {
                    state.deduplicate = false;
                    singlevalstrat = false;
                }
            }

            state.start_pending(itup, offnum);
        }
    }

    pagesaving += state
        .finish_pending(&mut newpage)
        .map_err(|()| dedup_add_failed("tuple to page"))?;

    // newpage identical to page: nothing merged, leave the page alone
    if state.nintervals == 0 {
        return Ok(());
    }

    if P_HAS_GARBAGE(&opaque) {
        let mut nopaque = page_opaque(&newpage.as_ref());
        nopaque.btpo_flags &= !BTP_HAS_GARBAGE;
        write_opaque(&mut newpage, &nopaque);
    }

    // critical section: PageRestoreTempPage + WAL, no early returns.
    {
        let orig = page_of_mut(buf);
        // SAFETY: whole-page overwrite under the exclusive lock held by caller.
        core::ptr::copy_nonoverlapping(
            temp.0.as_ptr(),
            orig.as_ref().as_ptr().cast_mut(),
            BLCKSZ,
        );
    }
    bufmgr::mark_buffer_dirty::call(buf.buffer())?;

    if relation_needs_wal(rel) {
        let xlrec = crate::wal::xl_btree_dedup(state.nintervals as u16);
        // the intervals array rides as block 0 data: dropped whenever the
        // whole buffer image is stored
        let frags: [&[u8]; 1] = [state.intervals_bytes()];
        let reg0 = XLogRegBuf {
            block_id: 0,
            buffer: buf.buffer(),
            flags: REGBUF_STANDARD,
            bufdata: &frags,
        };
        let recptr = ::xloginsert_seams::xlog_insert_record::call(
            ::rmgr::RM_BTREE_ID as u8,
            XLOG_BTREE_DEDUP,
            0,
            &[&xlrec],
            &[reg0],
        )?;
        page_of_mut(buf).set_lsn(recptr);
    }

    debug_assert!(
        pagesaving < newitemsz || buf.page().exact_free_space() >= newitemsz
    );
    Ok(())
}

/// _bt_do_singleval: whole page is one value (first and last data items both
/// equal newitem) — prepare for nbtsplitloc.c's own single value strategy.
///
/// # Safety
/// As [`bt_dedup_pass`].
unsafe fn bt_do_singleval(
    rel: &Relation<'_>,
    page: &::types_storage::bufpage::PageRef<'_>,
    _state: &BTDedupState,
    minoff: OffsetNumber,
    newitem: ITup,
) -> bool {
    let nkeyatts = rel.indnkeyatts();

    let itup = page_item(page, page.item_id(minoff));
    if bt_keep_natts_fast(rel, newitem, itup) > nkeyatts {
        let itup = page_item(page, page.item_id(page.max_offset_number()));
        if bt_keep_natts_fast(rel, newitem, itup) > nkeyatts {
            return true;
        }
    }

    false
}

// _bt_singleval_fillfactor: leave BTREE_SINGLEVAL_FILLFACTOR% headroom so the
// anticipated split lands like a dedup-disabled one (matches nbtsplitloc.c).
fn bt_singleval_fillfactor(state: &mut BTDedupState, newitemsz: usize) {
    let mut leftfree =
        BLCKSZ - SizeOfPageHeaderData - maxalign(core::mem::size_of::<BTPageOpaqueData>());
    // new high key includes pivot heap TID space
    leftfree -= newitemsz + maxalign(core::mem::size_of::<ItemPointerData>());

    let reduction =
        (leftfree as f64 * ((100 - BTREE_SINGLEVAL_FILLFACTOR) as f64 / 100.0)) as usize;
    if state.maxpostingsize > reduction {
        state.maxpostingsize -= reduction;
    } else {
        state.maxpostingsize = 0;
    }
}
