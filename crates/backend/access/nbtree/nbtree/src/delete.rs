//! Simple index deletion (nbtinsert.c + nbtpage.c arms). Posting images are
//! owned copies (same recorded divergence as vacuum.rs).

use ::bufmgr_seams::{self as bufmgr, BufferPin};
use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::tableam::{table_index_delete_tuples, TM_IndexDelete, TM_IndexDeleteOp, TM_IndexStatus};
use ::types_core::xact::InvalidTransactionId;
use ::types_core::{BlockNumber, OffsetNumber, TransactionId};
use ::types_error::PgResult;
use ::types_nbtree::{MaxTIDsPerBTreePage, BTP_HAS_GARBAGE, XLOG_BTREE_DELETE};
use ::types_rel::Relation;
use ::types_storage::bufpage::{MaxIndexTuplesPerPage, PageRef};
use ::types_tuple::itemptr::{
    InvalidOffsetNumber, ItemPointerCompare, ItemPointerGetBlockNumber,
};
use ::xloginsert_seams::{XLogRegBuf, REGBUF_STANDARD};
use init_small::globals::{EndCriticalSection, StartCriticalSection};

use crate::itup::{
    bt_tuple_get_nposting, bt_tuple_get_posting_n, bt_tuple_is_pivot, bt_tuple_is_posting,
    copy_index_tuple, index_tuple_size, maxalign, t_tid, ITup,
};
use crate::page::{page_item, page_of_mut, page_opaque, write_opaque};
use crate::pagedel::{bt_delitems_update, offsets_as_bytes};
use crate::relation_needs_wal;
use crate::vacuum::VacPosting;

/// _bt_simpledel_pass.
///
/// # Safety
/// `buf` pinned + write-locked leaf; `deletable` = the page's LP_DEAD offsets
/// ascending, nonempty; `newitem` a live plain non-pivot tuple.
pub(crate) unsafe fn bt_simpledel_pass<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    buf: &BufferPin,
    heap_rel: &Relation<'mcx>,
    deletable: &[OffsetNumber],
    newitem: ITup,
    minoff: OffsetNumber,
    maxoff: OffsetNumber,
) -> PgResult<()> {
    let page = buf.page();
    let deadblocks = bt_deadblocks(mcx, &page, deletable, newitem)?;

    let mut delstate = TM_IndexDeleteOp {
        irel: rel.alias(),
        iblknum: buf.block_number(),
        bottomup: false,
        bottomupfreespace: 0,
        ndeltids: 0,
        deltids: vec_with_capacity_in(mcx, MaxTIDsPerBTreePage)?,
        status: vec_with_capacity_in(mcx, MaxTIDsPerBTreePage)?,
    };

    for offnum in minoff..=maxoff {
        let itemid = page.item_id(offnum);
        let itup = page_item(&page, itemid);

        if !bt_tuple_is_posting(itup) {
            let tid = t_tid(itup);
            if deadblocks.binary_search(&ItemPointerGetBlockNumber(&tid)).is_err() {
                debug_assert!(!itemid.is_dead());
                continue;
            }
            let id = delstate.deltids.len() as i16;
            delstate.deltids.push(TM_IndexDelete { tid, id });
            delstate.status.push(TM_IndexStatus {
                idxoffnum: offnum,
                knowndeletable: itemid.is_dead(),
                promising: false,
                freespace: 0,
            });
        } else {
            for p in 0..bt_tuple_get_nposting(itup) {
                let tid = bt_tuple_get_posting_n(itup, p);
                if deadblocks.binary_search(&ItemPointerGetBlockNumber(&tid)).is_err() {
                    debug_assert!(!itemid.is_dead());
                    continue;
                }
                let id = delstate.deltids.len() as i16;
                delstate.deltids.push(TM_IndexDelete { tid, id });
                delstate.status.push(TM_IndexStatus {
                    idxoffnum: offnum,
                    knowndeletable: itemid.is_dead(),
                    promising: false,
                    freespace: 0,
                });
            }
        }
    }

    delstate.ndeltids = delstate.deltids.len() as i32;
    debug_assert!(delstate.ndeltids as usize >= deletable.len());

    bt_delitems_delete_check(mcx, rel, buf, heap_rel, &mut delstate)
}

/// _bt_deadblocks.
unsafe fn bt_deadblocks<'mcx>(
    mcx: Mcx<'mcx>,
    page: &PageRef<'_>,
    deletable: &[OffsetNumber],
    newitem: ITup,
) -> PgResult<PgVec<'mcx, BlockNumber>> {
    let mut tidblocks: PgVec<'mcx, BlockNumber> = vec_with_capacity_in(mcx, deletable.len() + 1)?;

    debug_assert!(!bt_tuple_is_posting(newitem) && !bt_tuple_is_pivot(newitem));
    tidblocks.push(ItemPointerGetBlockNumber(&t_tid(newitem)));

    for &off in deletable {
        let itemid = page.item_id(off);
        let itup = page_item(page, itemid);
        debug_assert!(itemid.is_dead());

        if !bt_tuple_is_posting(itup) {
            tidblocks.push(ItemPointerGetBlockNumber(&t_tid(itup)));
        } else {
            for p in 0..bt_tuple_get_nposting(itup) {
                tidblocks.push(ItemPointerGetBlockNumber(&bt_tuple_get_posting_n(itup, p)));
            }
        }
    }

    tidblocks.sort_unstable();
    tidblocks.dedup();
    Ok(tidblocks)
}

/// _bt_delitems_delete_check.
///
/// # Safety
/// `buf` pinned + write-locked leaf; `delstate.deltids` in leaf-page-wise
/// order with `id` capturing that order.
pub(crate) unsafe fn bt_delitems_delete_check<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    buf: &BufferPin,
    heap_rel: &Relation<'mcx>,
    delstate: &mut TM_IndexDeleteOp<'mcx>,
) -> PgResult<()> {
    let page = buf.page();

    let mut snapshot_conflict_horizon = table_index_delete_tuples(mcx, heap_rel, delstate)?;
    // RelationIsAccessibleInLogicalDecoding const-false (heapam DML divergence)
    let is_catalog_rel = false;

    if !transam_xlog_seams::xlog_standby_info_active::call() {
        snapshot_conflict_horizon = InvalidTransactionId;
    }

    let n = delstate.ndeltids as usize;
    delstate.deltids[..n].sort_unstable_by_key(|d| d.id);
    if n == 0 {
        debug_assert!(delstate.bottomup);
        return Ok(());
    }


    let mut deletable = [0 as OffsetNumber; MaxIndexTuplesPerPage];
    let mut ndeletable = 0usize;
    let mut updatable: PgVec<'mcx, VacPosting<'mcx>> = PgVec::new_in(mcx);
    let mut postingidxoffnum = InvalidOffsetNumber;

    for i in 0..n {
        let dstatus = delstate.status[delstate.deltids[i].id as usize];
        let idxoffnum = dstatus.idxoffnum;
        let itemid = page.item_id(idxoffnum);
        let itup = page_item(&page, itemid);

        debug_assert!(idxoffnum != InvalidOffsetNumber);

        if idxoffnum == postingidxoffnum {
            debug_assert!(bt_tuple_is_posting(itup));
            continue;
        }

        if !bt_tuple_is_posting(itup) {
            debug_assert!(t_tid(itup) == delstate.deltids[i].tid);
            if dstatus.knowndeletable {
                deletable[ndeletable] = idxoffnum;
                ndeletable += 1;
            }
            continue;
        }

        postingidxoffnum = idxoffnum;
        let mut nestedi = i;
        let mut vacposting: Option<VacPosting<'mcx>> = None;
        let nitem = bt_tuple_get_nposting(itup);

        for p in 0..nitem {
            let ptid = bt_tuple_get_posting_n(itup, p);
            let mut ptidcmp = -1;

            while nestedi < n {
                let tcdeltid = delstate.deltids[nestedi];
                let tdstatus = delstate.status[tcdeltid.id as usize];

                debug_assert!(tdstatus.idxoffnum >= idxoffnum);
                if tdstatus.idxoffnum != idxoffnum {
                    break;
                }
                if !tdstatus.knowndeletable {
                    nestedi += 1;
                    continue;
                }
                ptidcmp = ItemPointerCompare(&tcdeltid.tid, &ptid);
                if ptidcmp >= 0 {
                    break;
                }
                nestedi += 1;
            }

            if ptidcmp != 0 {
                continue;
            }

            if vacposting.is_none() {
                vacposting = Some(VacPosting {
                    itup: copy_index_tuple(mcx, itup)?,
                    updatedoffset: idxoffnum,
                    deletetids: vec_with_capacity_in(mcx, nitem)?,
                });
            }
            vacposting.as_mut().expect("created above").deletetids.push(p as u16);
        }

        match vacposting {
            None => {}
            Some(vp) if vp.deletetids.len() == nitem => {
                deletable[ndeletable] = idxoffnum;
                ndeletable += 1;
            }
            Some(vp) => {
                debug_assert!(!vp.deletetids.is_empty() && vp.deletetids.len() < nitem);
                updatable.push(vp);
            }
        }
    }

    bt_delitems_delete(
        mcx,
        rel,
        buf,
        snapshot_conflict_horizon,
        is_catalog_rel,
        &deletable[..ndeletable],
        &mut updatable,
    )
}

/// _bt_delitems_delete: like _bt_delitems_vacuum but with the tableam's
/// snapshotConflictHorizon and no vacuum-cycle-ID clear.
///
/// # Safety
/// As [`bt_delitems_delete_check`]; arrays leaf-page-wise sorted.
unsafe fn bt_delitems_delete<'s>(
    scx: Mcx<'s>,
    rel: &Relation<'_>,
    buf: &BufferPin,
    snapshot_conflict_horizon: TransactionId,
    is_catalog_rel: bool,
    deletable: &[OffsetNumber],
    updatable: &mut PgVec<'s, VacPosting<'s>>,
) -> PgResult<()> {
    debug_assert!(!deletable.is_empty() || !updatable.is_empty());
    let needswal = relation_needs_wal(rel);

    let mut updatedoffsets = [0 as OffsetNumber; MaxIndexTuplesPerPage];
    let mut updatedbuf: PgVec<'s, u8> = PgVec::new_in(scx);
    if !updatable.is_empty() {
        bt_delitems_update(scx, updatable, &mut updatedoffsets, needswal, &mut updatedbuf)?;
    }

    StartCriticalSection();

    for vacposting in updatable.iter() {
        let itup = vacposting.itup.as_ptr();
        let itemsz = maxalign(index_tuple_size(itup));
        // SAFETY: owned updated image, zero-padded to MAXALIGN by ItupBuf.
        let img = core::slice::from_raw_parts(itup, itemsz);
        if !page_of_mut(buf).index_tuple_overwrite(vacposting.updatedoffset, img) {
            panic!(
                "failed to update partially dead item in block {} of index \"{}\"",
                buf.block_number(),
                rel.name()
            );
        }
    }

    if !deletable.is_empty() {
        page_of_mut(buf).index_multi_delete(deletable);
    }

    // *must not* clear btpo_cycleid here: VACUUM alone owns vacuum cycle IDs
    let mut opaque = page_opaque(&buf.page());
    opaque.btpo_flags &= !BTP_HAS_GARBAGE;
    write_opaque(&mut page_of_mut(buf), &opaque);

    bufmgr::mark_buffer_dirty::call(buf.buffer())?;

    if needswal {
        let xlrec = crate::wal::xl_btree_delete(
            snapshot_conflict_horizon,
            deletable.len() as u16,
            updatable.len() as u16,
            is_catalog_rel,
        );
        let mut bufdata: [&[u8]; 3] = [&[], &[], &[]];
        let mut nfrag = 0;
        if !deletable.is_empty() {
            bufdata[nfrag] = offsets_as_bytes(deletable);
            nfrag += 1;
        }
        if !updatable.is_empty() {
            bufdata[nfrag] = offsets_as_bytes(&updatedoffsets[..updatable.len()]);
            nfrag += 1;
            bufdata[nfrag] = &updatedbuf;
            nfrag += 1;
        }
        let recptr = ::xloginsert_seams::xlog_insert_record::call(
            ::rmgr::RM_BTREE_ID as u8,
            XLOG_BTREE_DELETE,
            0,
            &[&xlrec],
            &[XLogRegBuf {
                block_id: 0,
                buffer: buf.buffer(),
                flags: REGBUF_STANDARD,
                bufdata: &bufdata[..nfrag],
            }],
        )?;
        page_of_mut(buf).set_lsn(recptr);
    }

    EndCriticalSection();
    Ok(())
}
