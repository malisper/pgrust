//! nbtpage.c VACUUM write arms: _bt_delitems_vacuum, page deletion
//! (_bt_pagedel/_bt_mark_page_halfdead/_bt_unlink_halfdead_page), cleanup-info
//! metapage maintenance, pending-FSM recycling. C divergences (recorded):
//! index-corruption LOG chatter elided (the abandon-and-continue control flow
//! is kept); posting images are owned copies (see vacuum.rs).

use ::bufmgr_seams::{self as bufmgr, BufferPin};
use ::mcx::{Mcx, PgVec};
use ::types_core::xact::FullTransactionId;
use ::types_core::{BlockNumber, InvalidBlockNumber, OffsetNumber};
use ::types_error::{PgError, PgResult, ERRCODE_INDEX_CORRUPTED};
use ::types_nbtree::{
    BTPendingFSM, BTP_HALF_DEAD, BTREE_METAPAGE, BTREE_NOVAC_VERSION, BT_READ, BT_WRITE,
    P_FIRSTDATAKEY, P_HIKEY, P_IGNORE, P_INCOMPLETE_SPLIT, P_ISDELETED, P_ISHALFDEAD, P_ISLEAF,
    P_ISROOT, P_NONE, P_RIGHTMOST, XLOG_BTREE_MARK_PAGE_HALFDEAD, XLOG_BTREE_META_CLEANUP,
    XLOG_BTREE_UNLINK_PAGE, XLOG_BTREE_UNLINK_PAGE_META, XLOG_BTREE_VACUUM,
};
use ::types_rel::Relation;
use ::types_storage::bufpage::MaxIndexTuplesPerPage;
use ::xloginsert_seams::{XLogRegBuf, REGBUF_STANDARD, REGBUF_WILL_INIT};
use init_small::globals::{EndCriticalSection, StartCriticalSection};

use crate::fcframe::OrderProcFrame;
use crate::insert::{bt_getstackbuf, StackEntry};
use crate::itup::{
    bt_tuple_get_downlink, bt_tuple_set_downlink, bt_tuple_set_natts, index_tuple_size, maxalign,
    set_t_info, ITup,
};
use crate::page::{
    bt_getbuf, bt_lockbuf, bt_page_set_deleted, bt_relbuf, bt_unlockbuf, page_item, page_meta,
    page_of_mut, page_opaque, write_meta, write_opaque,
};
use crate::search::{bt_binsrch, bt_moveright};
use crate::unported_phase2;
use crate::utils::bt_mkscankey;
use crate::vacuum::{bt_update_posting, BTVacState, VacPosting};

fn needs_wal(rel: &Relation<'_>) -> bool {
    crate::relation_needs_wal(rel)
}

pub(crate) fn offsets_as_bytes(offs: &[OffsetNumber]) -> &[u8] {
    // SAFETY: OffsetNumber is u16 POD; the WAL image is native-endian, as C.
    unsafe { core::slice::from_raw_parts(offs.as_ptr().cast::<u8>(), offs.len() * 2) }
}

/// _bt_delitems_vacuum. Caller holds pin + cleanup lock; arrays sorted.
pub(crate) fn bt_delitems_vacuum<'s>(
    scx: Mcx<'s>,
    rel: &Relation<'_>,
    buf: &BufferPin,
    deletable: &[OffsetNumber],
    updatable: &mut PgVec<'s, VacPosting<'s>>,
) -> PgResult<()> {
    debug_assert!(!deletable.is_empty() || !updatable.is_empty());
    let needswal = needs_wal(rel);

    let mut updatedoffsets = [0 as OffsetNumber; MaxIndexTuplesPerPage];
    let mut updatedbuf: PgVec<'s, u8> = PgVec::new_in(scx);
    if !updatable.is_empty() {
        bt_delitems_update(scx, updatable, &mut updatedoffsets, needswal, &mut updatedbuf)?;
    }

    StartCriticalSection();

    for vacposting in updatable.iter() {
        let itup = vacposting.itup.as_ptr();
        // SAFETY: owned updated image from bt_delitems_update.
        let itemsz = maxalign(unsafe { index_tuple_size(itup) });
        // SAFETY: image bytes; zero-padded to MAXALIGN by ItupBuf.
        let img = unsafe { core::slice::from_raw_parts(itup, itemsz) };
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

    let mut opaque = page_opaque(&buf.page());
    opaque.btpo_cycleid = 0;
    opaque.btpo_flags &= !::types_nbtree::BTP_HAS_GARBAGE;
    write_opaque(&mut page_of_mut(buf), &opaque);

    bufmgr::mark_buffer_dirty::call(buf.buffer())?;

    if needswal {
        let xlrec = crate::wal::xl_btree_vacuum(deletable.len() as u16, updatable.len() as u16);
        let mut bufdata: [&[u8]; 3] = [&[], &[], &[]];
        let mut n = 0;
        if !deletable.is_empty() {
            bufdata[n] = offsets_as_bytes(deletable);
            n += 1;
        }
        if !updatable.is_empty() {
            bufdata[n] = offsets_as_bytes(&updatedoffsets[..updatable.len()]);
            n += 1;
            bufdata[n] = &updatedbuf;
            n += 1;
        }
        let recptr = ::xloginsert_seams::xlog_insert_record::call(
            ::rmgr::RM_BTREE_ID as u8,
            XLOG_BTREE_VACUUM,
            0,
            &[&xlrec],
            &[XLogRegBuf {
                block_id: 0,
                buffer: buf.buffer(),
                flags: REGBUF_STANDARD,
                bufdata: &bufdata[..n],
            }],
        )?;
        page_of_mut(buf).set_lsn(recptr);
    }

    EndCriticalSection();
    Ok(())
}

/// _bt_delitems_update: generate replacement posting tuples and the WAL
/// xl_btree_update stream.
pub(crate) fn bt_delitems_update<'s>(
    scx: Mcx<'s>,
    updatable: &mut PgVec<'s, VacPosting<'s>>,
    updatedoffsets: &mut [OffsetNumber],
    needswal: bool,
    updatedbuf: &mut PgVec<'s, u8>,
) -> PgResult<()> {
    debug_assert!(!updatable.is_empty());

    for (i, vacposting) in updatable.iter_mut().enumerate() {
        bt_update_posting(scx, vacposting)?;
        updatedoffsets[i] = vacposting.updatedoffset;
    }

    if needswal {
        for vacposting in updatable.iter() {
            let ndeletedtids = vacposting.deletetids.len() as u16;
            updatedbuf.extend_from_slice(&ndeletedtids.to_ne_bytes());
            updatedbuf.extend_from_slice(offsets_as_bytes(&vacposting.deletetids));
        }
    }
    Ok(())
}

/// _bt_vacuum_needs_cleanup.
pub(crate) fn bt_vacuum_needs_cleanup(rel: &Relation<'_>) -> PgResult<bool> {
    let metapin = bt_getbuf(rel, BTREE_METAPAGE, BT_READ)?;
    let metad = page_meta(&metapin.page());
    if metad.btm_version < BTREE_NOVAC_VERSION {
        bt_relbuf(rel, metapin)?;
        return Ok(true);
    }
    let prev_num_delpages = metad.btm_last_cleanup_num_delpages;
    bt_relbuf(rel, metapin)?;

    let nblocks = bufmgr::relation_get_number_of_blocks_in_fork::call(
        rel,
        ::types_core::ForkNumber::MAIN_FORKNUM,
    )?;
    Ok(prev_num_delpages > 0 && prev_num_delpages > nblocks / 20)
}

/// _bt_set_cleanup_info.
pub(crate) fn bt_set_cleanup_info(rel: &Relation<'_>, num_delpages: BlockNumber) -> PgResult<()> {
    let metapin = bt_getbuf(rel, BTREE_METAPAGE, BT_READ)?;
    let metad = page_meta(&metapin.page());

    if metad.btm_version >= BTREE_NOVAC_VERSION
        && metad.btm_last_cleanup_num_delpages == num_delpages
    {
        bt_relbuf(rel, metapin)?;
        return Ok(());
    }

    bt_unlockbuf(rel, &metapin)?;
    bt_lockbuf(rel, &metapin, BT_WRITE)?;

    StartCriticalSection();

    let mut metad = page_meta(&metapin.page());
    if metad.btm_version < BTREE_NOVAC_VERSION {
        unported_phase2("_bt_upgrademetapage (v2/v3 pg_upgrade metapages)");
    }
    metad.btm_last_cleanup_num_delpages = num_delpages;
    metad.btm_last_cleanup_num_heap_tuples = -1.0;
    write_meta(&metapin, &metad);
    bufmgr::mark_buffer_dirty::call(metapin.buffer())?;

    if needs_wal(rel) {
        debug_assert!(metad.btm_version >= BTREE_NOVAC_VERSION);
        let md = crate::wal::xl_btree_metadata(&metad);
        let recptr = ::xloginsert_seams::xlog_insert_record::call(
            ::rmgr::RM_BTREE_ID as u8,
            XLOG_BTREE_META_CLEANUP,
            0,
            &[],
            &[XLogRegBuf {
                block_id: 0,
                buffer: metapin.buffer(),
                flags: REGBUF_WILL_INIT | REGBUF_STANDARD,
                bufdata: &[&md],
            }],
        )?;
        page_of_mut(&metapin).set_lsn(recptr);
    }

    EndCriticalSection();

    bt_relbuf(rel, metapin)
}

/// _bt_leftsib_splitflag.
fn bt_leftsib_splitflag(
    rel: &Relation<'_>,
    leftsib: BlockNumber,
    target: BlockNumber,
) -> PgResult<bool> {
    if leftsib == P_NONE {
        return Ok(false);
    }
    let pin = bt_getbuf(rel, leftsib, BT_READ)?;
    let opaque = page_opaque(&pin.page());
    let result = opaque.btpo_next == target && P_INCOMPLETE_SPLIT(&opaque);
    bt_relbuf(rel, pin)?;
    Ok(result)
}

/// _bt_rightsib_halfdeadflag.
fn bt_rightsib_halfdeadflag(rel: &Relation<'_>, leafrightsib: BlockNumber) -> PgResult<bool> {
    debug_assert!(leafrightsib != P_NONE);
    let pin = bt_getbuf(rel, leafrightsib, BT_READ)?;
    let opaque = page_opaque(&pin.page());
    debug_assert!(P_ISLEAF(&opaque) && !P_ISDELETED(&opaque));
    let result = P_ISHALFDEAD(&opaque);
    bt_relbuf(rel, pin)?;
    Ok(result)
}

// _bt_search descent for the deletion target's high key, read-locked,
// recording the stack (C's stacked _bt_search, BT_READ arm).
fn bt_search_stacked<'mcx>(
    rel: &Relation<'mcx>,
    key: &mut crate::search::BtScanInsert,
    frame: &mut OrderProcFrame,
    stack: &mut PgVec<'_, StackEntry>,
) -> PgResult<Option<BufferPin>> {
    let Some(mut pin) = crate::page::bt_getroot(rel, None, BT_READ)? else {
        return Ok(None);
    };

    loop {
        pin = bt_moveright(rel, key, pin, frame)?;

        let (child, offnum) = {
            let page = pin.page();
            let opaque = page_opaque(&page);
            if P_ISLEAF(&opaque) {
                break;
            }
            let offnum = bt_binsrch(rel, key, &page, frame)?;
            let itup = page_item(&page, page.item_id(offnum));
            // SAFETY: pinned+locked page item.
            (unsafe { bt_tuple_get_downlink(itup) }, offnum)
        };

        stack.push(StackEntry {
            blkno: pin.block_number(),
            offset: offnum,
        });

        pin = crate::page::bt_relandgetbuf(rel, Some(pin), child, BT_READ)?;
    }

    Ok(Some(pin))
}

/// _bt_pagedel. Consumes the leafbuf pin (dropped before return, as C).
pub(crate) fn bt_pagedel<'s>(
    scx: Mcx<'s>,
    rel: &Relation<'_>,
    mut leafbuf: BufferPin,
    vstate: &mut BTVacState<'_, '_, '_>,
) -> PgResult<()> {
    let scanblkno = leafbuf.block_number();
    let mut stack: PgVec<'s, StackEntry> = PgVec::new_in(scx);
    let mut have_stack = false;
    let mut frame = OrderProcFrame::new();

    loop {
        let opaque = page_opaque(&leafbuf.page());

        debug_assert!(!P_ISDELETED(&opaque));
        if !P_ISLEAF(&opaque) || P_ISDELETED(&opaque) {
            // Half-dead internal or deleted page via right link: corrupt.
            // C LOGs and presses on.
            bt_relbuf(rel, leafbuf)?;
            return Ok(());
        }

        if P_RIGHTMOST(&opaque)
            || P_ISROOT(&opaque)
            || P_FIRSTDATAKEY(&opaque) <= leafbuf.page().max_offset_number()
            || P_INCOMPLETE_SPLIT(&opaque)
        {
            debug_assert!(!P_ISHALFDEAD(&opaque));
            bt_relbuf(rel, leafbuf)?;
            return Ok(());
        }

        if !P_ISHALFDEAD(&opaque) {
            if !have_stack {
                let targetkey = {
                    let page = leafbuf.page();
                    let itup = page_item(&page, page.item_id(P_HIKEY));
                    // SAFETY: high key on the pinned+locked page.
                    unsafe { crate::itup::copy_index_tuple(scx, itup)? }
                };
                let leftsib = opaque.btpo_prev;
                let leafblkno = leafbuf.block_number();

                bt_unlockbuf(rel, &leafbuf)?;

                debug_assert!(leafblkno == scanblkno);
                if bt_leftsib_splitflag(rel, leftsib, leafblkno)? {
                    leafbuf.release();
                    return Ok(());
                }

                let mut itup_key = bt_mkscankey(rel, Some(targetkey.as_ptr()))?;
                itup_key.nextkey = false;
                itup_key.backward = true;
                if let Some(sleafbuf) = bt_search_stacked(rel, &mut itup_key, &mut frame, &mut stack)?
                {
                    bt_relbuf(rel, sleafbuf)?;
                }
                have_stack = true;

                bt_lockbuf(rel, &leafbuf, BT_WRITE)?;
                continue;
            }

            debug_assert!(P_ISLEAF(&opaque) && !P_IGNORE(&opaque));
            if !bt_mark_page_halfdead(scx, rel, vstate, &leafbuf, &mut stack, &mut frame)? {
                bt_relbuf(rel, leafbuf)?;
                return Ok(());
            }
        }

        let mut rightsib_empty = false;
        debug_assert!(P_ISHALFDEAD(&page_opaque(&leafbuf.page())));
        while P_ISHALFDEAD(&page_opaque(&leafbuf.page())) {
            match bt_unlink_halfdead_page(rel, leafbuf, scanblkno, &mut rightsib_empty, vstate)? {
                Some(kept) => leafbuf = kept,
                None => {
                    // Corruption path: locks and pins already released.
                    debug_assert!(false);
                    return Ok(());
                }
            }
        }

        let opaque = page_opaque(&leafbuf.page());
        debug_assert!(P_ISLEAF(&opaque) && P_ISDELETED(&opaque));

        let rightsib = opaque.btpo_next;
        bt_relbuf(rel, leafbuf)?;

        crate::check_for_interrupts()?;

        if !rightsib_empty {
            return Ok(());
        }
        leafbuf = bt_getbuf(rel, rightsib, BT_WRITE)?;
    }
}

/// _bt_mark_page_halfdead: first stage of deletion. `false` = unsafe to
/// delete leafbuf (nothing changed).
fn bt_mark_page_halfdead(
    scx: Mcx<'_>,
    rel: &Relation<'_>,
    vstate: &mut BTVacState<'_, '_, '_>,
    leafbuf: &BufferPin,
    stack: &mut [StackEntry],
    frame: &mut OrderProcFrame,
) -> PgResult<bool> {
    let heaprel = vstate.info.heaprel;
    let opaque = page_opaque(&leafbuf.page());
    debug_assert!(
        !P_RIGHTMOST(&opaque)
            && !P_ISROOT(&opaque)
            && P_ISLEAF(&opaque)
            && !P_IGNORE(&opaque)
            && P_FIRSTDATAKEY(&opaque) > leafbuf.page().max_offset_number()
    );

    let leafblkno = leafbuf.block_number();
    let leafrightsib = opaque.btpo_next;

    if bt_rightsib_halfdeadflag(rel, leafrightsib)? {
        return Ok(false);
    }

    let mut topparent = leafblkno;
    let mut topparentrightsib = leafrightsib;
    let Some((subtreeparent, poffset)) = bt_lock_subtree_parent(
        scx,
        rel,
        heaprel,
        leafblkno,
        stack,
        frame,
        &mut topparent,
        &mut topparentrightsib,
    )?
    else {
        return Ok(false);
    };

    {
        let page = subtreeparent.page();
        #[cfg(debug_assertions)]
        {
            let itup = page_item(&page, page.item_id(poffset));
            // SAFETY: pinned+locked parent page item.
            debug_assert!(unsafe { bt_tuple_get_downlink(itup) } == topparent);
        }
        let nextoffset = poffset + 1;
        let itup = page_item(&page, page.item_id(nextoffset));
        // SAFETY: pinned+locked parent page item.
        if unsafe { bt_tuple_get_downlink(itup) } != topparentrightsib {
            // C LOGs INDEX_CORRUPTED and backs out.
            bt_relbuf(rel, subtreeparent)?;
            debug_assert!(false);
            return Ok(false);
        }
    }

    predicate_seams::predicate_lock_page_combine::call(rel, leafblkno, leafrightsib)?;

    StartCriticalSection();

    {
        let page = subtreeparent.page();
        let itup: ITup = page_item(&page, page.item_id(poffset));
        // SAFETY: exclusive lock on subtreeparent; in-place downlink store,
        // same-size overwrite as C.
        unsafe { bt_tuple_set_downlink(itup.cast_mut(), topparentrightsib) };
        page_of_mut(&subtreeparent).index_tuple_delete(poffset + 1);
    }

    let (leftblk, rightblk) = {
        let mut o = page_opaque(&leafbuf.page());
        o.btpo_flags |= BTP_HALF_DEAD;
        write_opaque(&mut page_of_mut(leafbuf), &o);
        (o.btpo_prev, o.btpo_next)
    };

    debug_assert!(leafbuf.page().max_offset_number() == P_HIKEY);
    let mut trunctuple = [0u8; 8];
    // SAFETY: owned 8-byte IndexTupleData image.
    unsafe {
        set_t_info(trunctuple.as_mut_ptr(), 8);
        let tp = if topparent != leafblkno {
            topparent
        } else {
            InvalidBlockNumber
        };
        bt_tuple_set_downlink(trunctuple.as_mut_ptr(), tp);
        bt_tuple_set_natts(trunctuple.as_mut_ptr(), 0, false);
    }
    if !page_of_mut(leafbuf).index_tuple_overwrite(P_HIKEY, &trunctuple) {
        panic!("could not overwrite high key in half-dead page");
    }

    bufmgr::mark_buffer_dirty::call(subtreeparent.buffer())?;
    bufmgr::mark_buffer_dirty::call(leafbuf.buffer())?;

    if needs_wal(rel) {
        let xlrec = crate::wal::xl_btree_mark_page_halfdead(
            poffset,
            leafblkno,
            leftblk,
            rightblk,
            if topparent != leafblkno {
                topparent
            } else {
                InvalidBlockNumber
            },
        );
        let recptr = ::xloginsert_seams::xlog_insert_record::call(
            ::rmgr::RM_BTREE_ID as u8,
            XLOG_BTREE_MARK_PAGE_HALFDEAD,
            0,
            &[&xlrec],
            &[
                XLogRegBuf {
                    block_id: 0,
                    buffer: leafbuf.buffer(),
                    flags: REGBUF_WILL_INIT,
                    bufdata: &[],
                },
                XLogRegBuf {
                    block_id: 1,
                    buffer: subtreeparent.buffer(),
                    flags: REGBUF_STANDARD,
                    bufdata: &[],
                },
            ],
        )?;
        page_of_mut(&subtreeparent).set_lsn(recptr);
        page_of_mut(leafbuf).set_lsn(recptr);
    }

    EndCriticalSection();

    bt_relbuf(rel, subtreeparent)?;
    Ok(true)
}

/// _bt_unlink_halfdead_page: unlink one subtree page. `Some(leafbuf)` on
/// success (pin + write lock retained); `None` = corruption, everything
/// released.
fn bt_unlink_halfdead_page(
    rel: &Relation<'_>,
    leafbuf: BufferPin,
    scanblkno: BlockNumber,
    rightsib_empty: &mut bool,
    vstate: &mut BTVacState<'_, '_, '_>,
) -> PgResult<Option<BufferPin>> {
    let leafblkno = leafbuf.block_number();

    let (target, leafleftsib, leafrightsib) = {
        let page = leafbuf.page();
        let opaque = page_opaque(&page);
        debug_assert!(P_ISLEAF(&opaque) && !P_ISDELETED(&opaque) && P_ISHALFDEAD(&opaque));
        let hikey = page_item(&page, page.item_id(P_HIKEY));
        // SAFETY: pinned+locked leaf's high key (BTreeTupleGetTopParent).
        let target = unsafe { bt_tuple_get_downlink(hikey) };
        (target, opaque.btpo_prev, opaque.btpo_next)
    };

    bt_unlockbuf(rel, &leafbuf)?;
    crate::check_for_interrupts()?;

    let target_is_leaf = target == InvalidBlockNumber;
    let (target, target_pin, mut leftsib, targetlevel) = if target_is_leaf {
        (leafblkno, leafbuf.incr_clone(), leafleftsib, 0u32)
    } else {
        debug_assert!(target != leafblkno);
        let pin = bt_getbuf(rel, target, BT_READ)?;
        let opaque = page_opaque(&pin.page());
        let leftsib = opaque.btpo_prev;
        let targetlevel = opaque.btpo_level;
        debug_assert!(targetlevel > 0);
        bt_unlockbuf(rel, &pin)?;
        (target, pin, leftsib, targetlevel)
    };

    if !target_is_leaf {
        bt_lockbuf(rel, &leafbuf, BT_WRITE)?;
    }

    let mut lbuf: Option<BufferPin> = None;
    if leftsib != P_NONE {
        let mut pin = bt_getbuf(rel, leftsib, BT_WRITE)?;
        loop {
            let opaque = page_opaque(&pin.page());
            if !P_ISDELETED(&opaque) && opaque.btpo_next == target {
                break;
            }
            let leftsibvalid = !(P_RIGHTMOST(&opaque)
                || P_ISDELETED(&opaque)
                || leftsib == opaque.btpo_next);
            leftsib = opaque.btpo_next;
            bt_relbuf(rel, pin)?;

            if !leftsibvalid {
                // Sibling-link corruption (C LOGs): release and bail.
                target_pin.release();
                if !target_is_leaf {
                    bt_relbuf(rel, leafbuf)?;
                } else {
                    leafbuf.release();
                }
                return Ok(None);
            }
            crate::check_for_interrupts()?;
            pin = bt_getbuf(rel, leftsib, BT_WRITE)?;
        }
        lbuf = Some(pin);
    }

    bt_lockbuf(rel, &target_pin, BT_WRITE)?;
    {
        let opaque = page_opaque(&target_pin.page());
        if P_RIGHTMOST(&opaque) || P_ISROOT(&opaque) || P_ISDELETED(&opaque) {
            return Err(corrupt(format!(
                "target page changed status unexpectedly in block {} of index \"{}\"",
                target,
                rel.name()
            )));
        }
        if opaque.btpo_prev != leftsib {
            return Err(corrupt(format!(
                "target page left link unexpectedly changed from {} to {} in block {} of index \"{}\"",
                leftsib,
                opaque.btpo_prev,
                target,
                rel.name()
            )));
        }
    }

    let leaftopparent = if target_is_leaf {
        let opaque = page_opaque(&target_pin.page());
        if P_FIRSTDATAKEY(&opaque) <= target_pin.page().max_offset_number()
            || !P_ISLEAF(&opaque)
            || !P_ISHALFDEAD(&opaque)
        {
            return Err(corrupt(format!(
                "target leaf page changed status unexpectedly in block {} of index \"{}\"",
                target,
                rel.name()
            )));
        }
        InvalidBlockNumber
    } else {
        let page = target_pin.page();
        let opaque = page_opaque(&page);
        if P_FIRSTDATAKEY(&opaque) != page.max_offset_number() || P_ISLEAF(&opaque) {
            return Err(corrupt(format!(
                "target internal page on level {} changed status unexpectedly in block {} of index \"{}\"",
                targetlevel,
                target,
                rel.name()
            )));
        }
        let finaldataitem = page_item(&page, page.item_id(P_FIRSTDATAKEY(&opaque)));
        // SAFETY: pinned+locked page item.
        let ltp = unsafe { bt_tuple_get_downlink(finaldataitem) };
        if ltp == leafblkno {
            InvalidBlockNumber
        } else {
            ltp
        }
    };
    debug_assert!(leaftopparent == InvalidBlockNumber || targetlevel > 1);

    let rightsib = page_opaque(&target_pin.page()).btpo_next;
    let rbuf = bt_getbuf(rel, rightsib, BT_WRITE)?;
    {
        let opaque = page_opaque(&rbuf.page());
        if opaque.btpo_prev != target {
            // Right sibling's left-link mismatch (C LOGs): release all.
            if let Some(lb) = lbuf {
                bt_relbuf(rel, lb)?;
            }
            bt_relbuf(rel, rbuf)?;
            if target_is_leaf {
                bt_relbuf(rel, target_pin.incr_clone())?;
                target_pin.release();
                leafbuf.release();
            } else {
                bt_relbuf(rel, target_pin)?;
                bt_relbuf(rel, leafbuf)?;
            }
            return Ok(None);
        }
        *rightsib_empty = P_FIRSTDATAKEY(&opaque) > rbuf.page().max_offset_number();
    }
    let rightsib_is_rightmost = P_RIGHTMOST(&page_opaque(&rbuf.page()));

    let mut metabuf: Option<BufferPin> = None;
    if leftsib == P_NONE && rightsib_is_rightmost {
        let pin = bt_getbuf(rel, BTREE_METAPAGE, BT_WRITE)?;
        let metad = page_meta(&pin.page());
        if metad.btm_fastlevel > targetlevel + 1 {
            bt_relbuf(rel, pin)?;
        } else {
            metabuf = Some(pin);
        }
    }

    StartCriticalSection();

    if let Some(lb) = &lbuf {
        let mut o = page_opaque(&lb.page());
        debug_assert!(o.btpo_next == target);
        o.btpo_next = rightsib;
        write_opaque(&mut page_of_mut(lb), &o);
    }
    {
        let mut o = page_opaque(&rbuf.page());
        debug_assert!(o.btpo_prev == target);
        o.btpo_prev = leftsib;
        write_opaque(&mut page_of_mut(&rbuf), &o);
    }

    if !target_is_leaf {
        // Update the leaf's top-parent link in place (pin held throughout).
        let page = leafbuf.page();
        let hikey: ITup = page_item(&page, page.item_id(P_HIKEY));
        // SAFETY: exclusive lock on leafbuf; downlink store, size unchanged.
        unsafe {
            bt_tuple_set_downlink(hikey.cast_mut(), leaftopparent);
            bt_tuple_set_natts(hikey.cast_mut(), 0, false);
        }
    }

    debug_assert!(
        P_ISHALFDEAD(&page_opaque(&target_pin.page()))
            || !P_ISLEAF(&page_opaque(&target_pin.page()))
    );
    let safexid: FullTransactionId = varsup::ReadNextFullTransactionId()?;
    bt_page_set_deleted(&mut page_of_mut(&target_pin), safexid);
    {
        let mut o = page_opaque(&target_pin.page());
        o.btpo_cycleid = 0;
        write_opaque(&mut page_of_mut(&target_pin), &o);
    }

    let mut xlmeta = None;
    if let Some(mb) = &metabuf {
        let mut metad = page_meta(&mb.page());
        if metad.btm_version < BTREE_NOVAC_VERSION {
            unported_phase2("_bt_upgrademetapage (v2/v3 pg_upgrade metapages)");
        }
        metad.btm_fastroot = rightsib;
        metad.btm_fastlevel = targetlevel;
        write_meta(mb, &metad);
        bufmgr::mark_buffer_dirty::call(mb.buffer())?;
        xlmeta = Some(crate::wal::xl_btree_metadata(&metad));
    }

    bufmgr::mark_buffer_dirty::call(rbuf.buffer())?;
    bufmgr::mark_buffer_dirty::call(target_pin.buffer())?;
    if let Some(lb) = &lbuf {
        bufmgr::mark_buffer_dirty::call(lb.buffer())?;
    }
    if !target_is_leaf {
        bufmgr::mark_buffer_dirty::call(leafbuf.buffer())?;
    }

    if needs_wal(rel) {
        let xlrec = crate::wal::xl_btree_unlink_page(
            leftsib,
            rightsib,
            targetlevel,
            safexid,
            leafleftsib,
            leafrightsib,
            leaftopparent,
        );
        let mut regbufs: [XLogRegBuf<'_>; 5] = core::array::from_fn(|_| XLogRegBuf {
            block_id: 0,
            buffer: target_pin.buffer(),
            flags: 0,
            bufdata: &[],
        });
        let mut n = 0;
        regbufs[n] = XLogRegBuf {
            block_id: 0,
            buffer: target_pin.buffer(),
            flags: REGBUF_WILL_INIT,
            bufdata: &[],
        };
        n += 1;
        if let Some(lb) = &lbuf {
            regbufs[n] = XLogRegBuf {
                block_id: 1,
                buffer: lb.buffer(),
                flags: REGBUF_STANDARD,
                bufdata: &[],
            };
            n += 1;
        }
        regbufs[n] = XLogRegBuf {
            block_id: 2,
            buffer: rbuf.buffer(),
            flags: REGBUF_STANDARD,
            bufdata: &[],
        };
        n += 1;
        if !target_is_leaf {
            regbufs[n] = XLogRegBuf {
                block_id: 3,
                buffer: leafbuf.buffer(),
                flags: REGBUF_WILL_INIT,
                bufdata: &[],
            };
            n += 1;
        }
        let md_bufdata: [&[u8]; 1];
        let info = if let Some(md) = &xlmeta {
            md_bufdata = [md];
            regbufs[n] = XLogRegBuf {
                block_id: 4,
                buffer: metabuf.as_ref().expect("metad set with metabuf").buffer(),
                flags: REGBUF_WILL_INIT | REGBUF_STANDARD,
                bufdata: &md_bufdata,
            };
            n += 1;
            XLOG_BTREE_UNLINK_PAGE_META
        } else {
            XLOG_BTREE_UNLINK_PAGE
        };

        let recptr = ::xloginsert_seams::xlog_insert_record::call(
            ::rmgr::RM_BTREE_ID as u8,
            info,
            0,
            &[&xlrec],
            &regbufs[..n],
        )?;

        if let Some(mb) = &metabuf {
            page_of_mut(mb).set_lsn(recptr);
        }
        page_of_mut(&rbuf).set_lsn(recptr);
        page_of_mut(&target_pin).set_lsn(recptr);
        if let Some(lb) = &lbuf {
            page_of_mut(lb).set_lsn(recptr);
        }
        if !target_is_leaf {
            page_of_mut(&leafbuf).set_lsn(recptr);
        }
    }

    EndCriticalSection();

    if let Some(mb) = metabuf {
        bt_relbuf(rel, mb)?;
    }
    if let Some(lb) = lbuf {
        bt_relbuf(rel, lb)?;
    }
    bt_relbuf(rel, rbuf)?;

    if target_is_leaf {
        // target_pin is the incr_clone of leafbuf: drop the extra pin, keep
        // the write lock with leafbuf's own pin.
        target_pin.release();
    } else {
        bt_relbuf(rel, target_pin)?;
    }

    vstate.stats.pages_newly_deleted += 1;
    if target <= scanblkno {
        vstate.stats.pages_deleted += 1;
    }

    bt_pendingfsm_add(vstate, target, safexid);

    Ok(Some(leafbuf))
}

#[track_caller]
#[cold]
#[inline(never)]
fn corrupt(msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_INDEX_CORRUPTED))
}

/// _bt_lock_subtree_parent: `Some((subtreeparent, poffset))` when deletion is
/// safe; updates topparent/topparentrightsib when internal pages join the
/// subtree.
#[allow(clippy::too_many_arguments)]
fn bt_lock_subtree_parent(
    scx: Mcx<'_>,
    rel: &Relation<'_>,
    heaprel: &::types_rel::RelationData<'_>,
    child: BlockNumber,
    stack: &mut [StackEntry],
    frame: &mut OrderProcFrame,
    topparent: &mut BlockNumber,
    topparentrightsib: &mut BlockNumber,
) -> PgResult<Option<(BufferPin, OffsetNumber)>> {
    let Some((top, parent_stack)) = stack.split_last_mut() else {
        debug_assert!(false);
        return Ok(None);
    };
    // SAFETY: parent-insertion protocol (child pages locked by caller).
    let pbuf = unsafe { bt_getstackbuf(rel, heaprel, frame, top, parent_stack, child)? };
    let Some(pbuf) = pbuf else {
        // Failed to re-find the downlink (C LOGs INDEX_CORRUPTED).
        debug_assert!(false);
        return Ok(None);
    };

    let parent = top.blkno;
    let parentoffset = top.offset;

    let (maxoff, leftsibparent, parent_rightsib, parent_rightmost, parent_firstdatakey) = {
        let page = pbuf.page();
        let opaque = page_opaque(&page);
        debug_assert!(!P_INCOMPLETE_SPLIT(&opaque));
        (
            page.max_offset_number(),
            opaque.btpo_prev,
            opaque.btpo_next,
            P_RIGHTMOST(&opaque),
            P_FIRSTDATAKEY(&opaque),
        )
    };

    if parentoffset < maxoff {
        return Ok(Some((pbuf, parentoffset)));
    }

    debug_assert!(parentoffset == maxoff);
    if parentoffset != parent_firstdatakey || parent_rightmost {
        bt_relbuf(rel, pbuf)?;
        return Ok(None);
    }

    *topparent = parent;
    *topparentrightsib = parent_rightsib;

    bt_relbuf(rel, pbuf)?;

    if bt_leftsib_splitflag(rel, leftsibparent, parent)? {
        return Ok(None);
    }

    bt_lock_subtree_parent(
        scx,
        rel,
        heaprel,
        parent,
        parent_stack,
        frame,
        topparent,
        topparentrightsib,
    )
}

const MAX_ALLOC_SIZE: usize = 0x3fffffff;

/// _bt_pendingfsm_init.
pub(crate) fn bt_pendingfsm_init(
    vstate: &mut BTVacState<'_, '_, '_>,
    cleanuponly: bool,
) -> PgResult<()> {
    if cleanuponly {
        return Ok(());
    }
    let mut maxbufsize =
        (init_small::globals::work_mem() as usize * 1024) / core::mem::size_of::<BTPendingFSM>();
    maxbufsize = maxbufsize.min(MAX_ALLOC_SIZE / core::mem::size_of::<BTPendingFSM>());
    maxbufsize = maxbufsize.min(i32::MAX as usize);
    maxbufsize = maxbufsize.max(256);
    vstate.maxbufsize = maxbufsize;
    vstate.pendingpages.reserve(256);
    Ok(())
}

/// _bt_pendingfsm_finalize.
pub(crate) fn bt_pendingfsm_finalize(vstate: &mut BTVacState<'_, '_, '_>) -> PgResult<()> {
    let rel = vstate.info.index;
    let heaprel = vstate.info.heaprel;
    debug_assert!(vstate.stats.pages_newly_deleted >= vstate.pendingpages.len() as u32);

    if vstate.pendingpages.is_empty() {
        return Ok(());
    }

    // Forcibly refresh this backend's horizon state; the recyclability test
    // below is unreliable without it (C's comment).
    procarray_seams::get_oldest_non_removable_transaction_id::call(heaprel)?;

    for i in 0..vstate.pendingpages.len() {
        let BTPendingFSM { target, safexid } = vstate.pendingpages[i];
        if !procarray_seams::global_vis_check_removable_full_xid::call(heaprel, safexid)? {
            break;
        }
        ::freespace::RecordFreeIndexPage(rel, target)?;
        vstate.stats.pages_free += 1;
    }
    Ok(())
}

fn bt_pendingfsm_add(
    vstate: &mut BTVacState<'_, '_, '_>,
    target: BlockNumber,
    safexid: FullTransactionId,
) {
    debug_assert!(vstate
        .pendingpages
        .last()
        .is_none_or(|last| last.safexid.precedes_or_equals(safexid)));
    if vstate.pendingpages.len() == vstate.maxbufsize {
        return;
    }
    vstate.pendingpages.push(BTPendingFSM { target, safexid });
}
