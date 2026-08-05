//! spgxlog.c — spgist rmgr redo, all record types.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use types_core::{BlockNumber, Buffer, InvalidBlockNumber, OffsetNumber};
use types_error::PgResult;
use types_spgist::xlog::*;
use types_spgist::*;
use types_storage::bufpage::{PageMut, SizeOfPageHeaderData};

use types_spgist::{spgFormDeadTuple, spgPageIndexMultiDelete, spgUpdateNodeLink};
use xlogreader_seams::XLogReaderState;
use xlogutils::{XLogInitBufferForRedo, XLogReadBufferForRedo, BLK_NEEDS_REDO};

const XLR_INFO_MASK: u8 = 0x0F;

const InvalidOffsetNumber: OffsetNumber = 0;
const InvalidBuffer: Buffer = 0;

fn main_data<'a>(record: &'a XLogReaderState) -> &'a [u8] {
    let rec = record.record.as_ref().expect("spg redo with no decoded record");
    // SAFETY: points into the reader's decode buffer, valid for the redo
    // callback's duration.
    unsafe { rec.main_data_bytes() }
}

// SAFETY contract shared by the redo arms: buffer pinned and exclusively
// locked (XLogReadBufferForRedo protocol) until the unlock below.
unsafe fn page_mut<'p>(buffer: Buffer) -> PageMut<'p> {
    unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(buffer)) }
}

fn unlock_release(buffer: Buffer) -> PgResult<()> {
    bufmgr_seams::lock_buffer::call(buffer, bufmgr_seams::BUFFER_LOCK_UNLOCK)?;
    bufmgr_seams::release_buffer::call(buffer)
}

fn spg_init_buffer(buffer: Buffer, flags: u16) {
    // SAFETY: init-for-redo pin+lock contract.
    let mut pm = unsafe { page_mut(buffer) };
    types_spgist::SpGistInitPage(&mut pm, flags);
}

fn block_blkno(record: &XLogReaderState, block_id: u8) -> BlockNumber {
    record
        .block_tag_extended(block_id)
        .expect("spg redo block ref")
        .2
}

fn item_slice_mut<'a>(pm: &'a mut PageMut<'_>, offnum: OffsetNumber) -> &'a mut [u8] {
    let r = pm.as_ref();
    let id = r.item_id(offnum);
    let (p, len) = r.item_raw(id);
    let off = p as usize - r.as_ptr() as usize;
    // SAFETY: in-page span from item_raw; redo pin+lock contract.
    unsafe { core::slice::from_raw_parts_mut(pm.as_mut_ptr().add(off), len as usize) }
}

fn item_slice<'a>(pm: &'a PageMut<'_>, offnum: OffsetNumber) -> &'a [u8] {
    let r = pm.as_ref();
    let id = r.item_id(offnum);
    let (p, len) = r.item_raw(id);
    // SAFETY: in-page span from item_raw; redo pin+lock contract.
    unsafe { core::slice::from_raw_parts(p, len as usize) }
}

// addOrReplaceTuple.
fn add_or_replace_tuple(pm: &mut PageMut<'_>, tuple: &[u8], offset: OffsetNumber) {
    if offset <= pm.as_ref().max_offset_number() {
        let st = tuple_state(item_slice(pm, offset));
        if st != SPGIST_PLACEHOLDER {
            panic!("SPGiST tuple to be replaced is not a placeholder");
        }
        page_opaque_update(pm, |op| {
            debug_assert!(op.nPlaceholder > 0);
            op.nPlaceholder -= 1;
        });
        pm.index_tuple_delete(offset);
    }
    debug_assert!(offset <= pm.as_ref().max_offset_number() + 1);
    if pm.add_item(tuple, offset, 0) != Some(offset) {
        panic!("failed to add item of size {} to SPGiST index page", tuple.len());
    }
}

fn u16s_at(data: &[u8], off: usize, n: usize) -> Vec<OffsetNumber> {
    (0..n)
        .map(|i| OffsetNumber::from_ne_bytes([data[off + i * 2], data[off + i * 2 + 1]]))
        .collect()
}

fn spgRedoAddLeaf(record: &XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let md = main_data(record);
    let xldata = spgxlogAddLeaf::decode(md);
    let leaf_tuple = &md[SizeOfSpgxlogAddLeaf..];
    let leaf_size = leaf_size(leaf_tuple);

    let (action, buffer) = if xldata.newPage {
        let b = XLogInitBufferForRedo(record, 0)?;
        spg_init_buffer(
            b,
            SPGIST_LEAF | if xldata.storesNulls { SPGIST_NULLS } else { 0 },
        );
        (BLK_NEEDS_REDO, b)
    } else {
        XLogReadBufferForRedo(record, 0)?
    };

    if action == BLK_NEEDS_REDO {
        // SAFETY: redo pin+lock contract.
        let mut pm = unsafe { page_mut(buffer) };

        if xldata.offnumLeaf != xldata.offnumHeadLeaf {
            add_or_replace_tuple(&mut pm, &leaf_tuple[..leaf_size], xldata.offnumLeaf);
            if xldata.offnumHeadLeaf != InvalidOffsetNumber {
                let new_next = leaf_next_offset(leaf_tuple);
                let head = item_slice_mut(&mut pm, xldata.offnumHeadLeaf);
                debug_assert_eq!(leaf_next_offset(head), new_next);
                leaf_set_next_offset(head, xldata.offnumLeaf);
            }
        } else {
            pm.index_tuple_delete(xldata.offnumLeaf);
            if pm.add_item(&leaf_tuple[..leaf_size], xldata.offnumLeaf, 0)
                != Some(xldata.offnumLeaf)
            {
                panic!("failed to add item of size {leaf_size} to SPGiST index page");
            }
        }

        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    }
    if buffer != InvalidBuffer {
        unlock_release(buffer)?;
    }

    if xldata.offnumParent != InvalidOffsetNumber {
        let (action, buffer) = XLogReadBufferForRedo(record, 1)?;
        if action == BLK_NEEDS_REDO {
            let blkno_leaf = block_blkno(record, 0);
            // SAFETY: redo pin+lock contract.
            let mut pm = unsafe { page_mut(buffer) };
            let tuple = item_slice_mut(&mut pm, xldata.offnumParent);
            spgUpdateNodeLink(tuple, xldata.nodeI as i32, blkno_leaf, xldata.offnumLeaf);
            pm.set_lsn(lsn);
            bufmgr_seams::mark_buffer_dirty::call(buffer)?;
        }
        if buffer != InvalidBuffer {
            unlock_release(buffer)?;
        }
    }
    Ok(())
}

fn spgRedoMoveLeafs(record: &XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let md = main_data(record);
    let xldata = spgxlogMoveLeafs::decode(md);
    let blkno_dst = block_blkno(record, 1);

    let n_insert = if xldata.replaceDead { 1 } else { xldata.nMoves as usize + 1 };
    let mut ptr = SizeOfSpgxlogMoveLeafs;
    let to_delete = u16s_at(md, ptr, xldata.nMoves as usize);
    ptr += 2 * xldata.nMoves as usize;
    let to_insert = u16s_at(md, ptr, n_insert);
    ptr += 2 * n_insert;

    // dest page first, so the redirect is valid
    let (action, buffer) = if xldata.newPage {
        let b = XLogInitBufferForRedo(record, 1)?;
        spg_init_buffer(
            b,
            SPGIST_LEAF | if xldata.storesNulls { SPGIST_NULLS } else { 0 },
        );
        (BLK_NEEDS_REDO, b)
    } else {
        XLogReadBufferForRedo(record, 1)?
    };

    if action == BLK_NEEDS_REDO {
        // SAFETY: redo pin+lock contract.
        let mut pm = unsafe { page_mut(buffer) };
        let mut p = ptr;
        for &off in to_insert.iter() {
            let lt = &md[p..];
            let sz = leaf_size(lt);
            add_or_replace_tuple(&mut pm, &lt[..sz], off);
            p += sz;
        }
        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    }
    if buffer != InvalidBuffer {
        unlock_release(buffer)?;
    }

    // source page
    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action == BLK_NEEDS_REDO {
        // SAFETY: redo pin+lock contract.
        let mut pm = unsafe { page_mut(buffer) };
        spgPageIndexMultiDelete(
            xldata.stateSrc.redirectXid,
            &mut pm,
            &to_delete,
            if xldata.stateSrc.isBuild { SPGIST_PLACEHOLDER } else { SPGIST_REDIRECT },
            SPGIST_PLACEHOLDER,
            blkno_dst,
            to_insert[n_insert - 1],
        );
        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    }
    if buffer != InvalidBuffer {
        unlock_release(buffer)?;
    }

    // parent downlink
    let (action, buffer) = XLogReadBufferForRedo(record, 2)?;
    if action == BLK_NEEDS_REDO {
        // SAFETY: redo pin+lock contract.
        let mut pm = unsafe { page_mut(buffer) };
        let tuple = item_slice_mut(&mut pm, xldata.offnumParent);
        spgUpdateNodeLink(tuple, xldata.nodeI as i32, blkno_dst, to_insert[n_insert - 1]);
        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    }
    if buffer != InvalidBuffer {
        unlock_release(buffer)?;
    }
    Ok(())
}

fn spgRedoAddNode(record: &XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let md = main_data(record);
    let xldata = spgxlogAddNode::decode(md);
    let inner_tuple = &md[SizeOfSpgxlogAddNode..];
    let inner_size = SpGistInnerTupleHeader::decode(inner_tuple).size as usize;

    if !record.has_block_ref(1) {
        debug_assert!(xldata.parentBlk == -1);
        let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
        if action == BLK_NEEDS_REDO {
            // SAFETY: redo pin+lock contract.
            let mut pm = unsafe { page_mut(buffer) };
            pm.index_tuple_delete(xldata.offnum);
            if pm.add_item(&inner_tuple[..inner_size], xldata.offnum, 0) != Some(xldata.offnum)
            {
                panic!("failed to add item of size {inner_size} to SPGiST index page");
            }
            pm.set_lsn(lsn);
            bufmgr_seams::mark_buffer_dirty::call(buffer)?;
        }
        if buffer != InvalidBuffer {
            unlock_release(buffer)?;
        }
    } else {
        let blkno_new = block_blkno(record, 1);

        // install new tuple first so redirect is valid
        let (action, buffer) = if xldata.newPage {
            let b = XLogInitBufferForRedo(record, 1)?;
            // AddNode is not used for nulls pages
            spg_init_buffer(b, 0);
            (BLK_NEEDS_REDO, b)
        } else {
            XLogReadBufferForRedo(record, 1)?
        };
        if action == BLK_NEEDS_REDO {
            // SAFETY: redo pin+lock contract.
            let mut pm = unsafe { page_mut(buffer) };
            add_or_replace_tuple(&mut pm, &inner_tuple[..inner_size], xldata.offnumNew);
            if xldata.parentBlk == 1 {
                let parent = item_slice_mut(&mut pm, xldata.offnumParent);
                spgUpdateNodeLink(parent, xldata.nodeI as i32, blkno_new, xldata.offnumNew);
            }
            pm.set_lsn(lsn);
            bufmgr_seams::mark_buffer_dirty::call(buffer)?;
        }
        if buffer != InvalidBuffer {
            unlock_release(buffer)?;
        }

        // replace old tuple with redirect/placeholder
        let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
        if action == BLK_NEEDS_REDO {
            // SAFETY: redo pin+lock contract.
            let mut pm = unsafe { page_mut(buffer) };
            let dt = if xldata.stateSrc.isBuild {
                spgFormDeadTuple(
                    xldata.stateSrc.redirectXid,
                    SPGIST_PLACEHOLDER,
                    InvalidBlockNumber,
                    InvalidOffsetNumber,
                )
            } else {
                spgFormDeadTuple(
                    xldata.stateSrc.redirectXid,
                    SPGIST_REDIRECT,
                    blkno_new,
                    xldata.offnumNew,
                )
            };
            pm.index_tuple_delete(xldata.offnum);
            if pm.add_item(&dt, xldata.offnum, 0) != Some(xldata.offnum) {
                panic!("failed to add item of size {SGDTSIZE} to SPGiST index page");
            }
            if xldata.stateSrc.isBuild {
                page_opaque_update(&mut pm, |op| op.nPlaceholder += 1);
            } else {
                page_opaque_update(&mut pm, |op| op.nRedirection += 1);
            }
            if xldata.parentBlk == 0 {
                let parent = item_slice_mut(&mut pm, xldata.offnumParent);
                spgUpdateNodeLink(parent, xldata.nodeI as i32, blkno_new, xldata.offnumNew);
            }
            pm.set_lsn(lsn);
            bufmgr_seams::mark_buffer_dirty::call(buffer)?;
        }
        if buffer != InvalidBuffer {
            unlock_release(buffer)?;
        }

        if xldata.parentBlk == 2 {
            let (action, buffer) = XLogReadBufferForRedo(record, 2)?;
            if action == BLK_NEEDS_REDO {
                // SAFETY: redo pin+lock contract.
                let mut pm = unsafe { page_mut(buffer) };
                let parent = item_slice_mut(&mut pm, xldata.offnumParent);
                spgUpdateNodeLink(parent, xldata.nodeI as i32, blkno_new, xldata.offnumNew);
                pm.set_lsn(lsn);
                bufmgr_seams::mark_buffer_dirty::call(buffer)?;
            }
            if buffer != InvalidBuffer {
                unlock_release(buffer)?;
            }
        }
    }
    Ok(())
}

fn spgRedoSplitTuple(record: &XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let md = main_data(record);
    let xldata = spgxlogSplitTuple::decode(md);
    let prefix_tuple = &md[SizeOfSpgxlogSplitTuple..];
    let prefix_size = SpGistInnerTupleHeader::decode(prefix_tuple).size as usize;
    let postfix_tuple = &prefix_tuple[prefix_size..];
    let postfix_size = SpGistInnerTupleHeader::decode(postfix_tuple).size as usize;

    // insert postfix tuple first to avoid dangling link
    if !xldata.postfixBlkSame {
        let (action, buffer) = if xldata.newPage {
            let b = XLogInitBufferForRedo(record, 1)?;
            // SplitTuple is not used for nulls pages
            spg_init_buffer(b, 0);
            (BLK_NEEDS_REDO, b)
        } else {
            XLogReadBufferForRedo(record, 1)?
        };
        if action == BLK_NEEDS_REDO {
            // SAFETY: redo pin+lock contract.
            let mut pm = unsafe { page_mut(buffer) };
            add_or_replace_tuple(&mut pm, &postfix_tuple[..postfix_size], xldata.offnumPostfix);
            pm.set_lsn(lsn);
            bufmgr_seams::mark_buffer_dirty::call(buffer)?;
        }
        if buffer != InvalidBuffer {
            unlock_release(buffer)?;
        }
    }

    // original page
    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action == BLK_NEEDS_REDO {
        // SAFETY: redo pin+lock contract.
        let mut pm = unsafe { page_mut(buffer) };
        pm.index_tuple_delete(xldata.offnumPrefix);
        if pm.add_item(&prefix_tuple[..prefix_size], xldata.offnumPrefix, 0)
            != Some(xldata.offnumPrefix)
        {
            panic!("failed to add item of size {prefix_size} to SPGiST index page");
        }
        if xldata.postfixBlkSame {
            add_or_replace_tuple(&mut pm, &postfix_tuple[..postfix_size], xldata.offnumPostfix);
        }
        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    }
    if buffer != InvalidBuffer {
        unlock_release(buffer)?;
    }
    Ok(())
}

fn spgRedoPickSplit(record: &XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let md = main_data(record);
    let xldata = spgxlogPickSplit::decode(md);
    let blkno_inner = block_blkno(record, 2);

    let mut ptr = SizeOfSpgxlogPickSplit;
    let to_delete = u16s_at(md, ptr, xldata.nDelete as usize);
    ptr += 2 * xldata.nDelete as usize;
    let to_insert = u16s_at(md, ptr, xldata.nInsert as usize);
    ptr += 2 * xldata.nInsert as usize;
    let leaf_page_select = &md[ptr..ptr + xldata.nInsert as usize];
    ptr += xldata.nInsert as usize;

    let inner_tuple = &md[ptr..];
    let inner_size = SpGistInnerTupleHeader::decode(inner_tuple).size as usize;
    ptr += inner_size;

    // src page
    let mut src_buffer = InvalidBuffer;
    let mut src_needs_redo = false;
    if xldata.isRootSplit {
        // touched only in the guise of the new inner page
    } else if xldata.initSrc {
        src_buffer = XLogInitBufferForRedo(record, 0)?;
        spg_init_buffer(
            src_buffer,
            SPGIST_LEAF | if xldata.storesNulls { SPGIST_NULLS } else { 0 },
        );
        src_needs_redo = true;
    } else {
        let (action, b) = XLogReadBufferForRedo(record, 0)?;
        src_buffer = b;
        if action == BLK_NEEDS_REDO {
            src_needs_redo = true;
            // SAFETY: redo pin+lock contract.
            let mut pm = unsafe { page_mut(src_buffer) };
            if !xldata.stateSrc.isBuild {
                spgPageIndexMultiDelete(
                    xldata.stateSrc.redirectXid,
                    &mut pm,
                    &to_delete,
                    SPGIST_REDIRECT,
                    SPGIST_PLACEHOLDER,
                    blkno_inner,
                    xldata.offnumInner,
                );
            } else {
                spgPageIndexMultiDelete(
                    xldata.stateSrc.redirectXid,
                    &mut pm,
                    &to_delete,
                    SPGIST_PLACEHOLDER,
                    SPGIST_PLACEHOLDER,
                    InvalidBlockNumber,
                    InvalidOffsetNumber,
                );
            }
        }
    }

    // dest page
    let mut dest_buffer = InvalidBuffer;
    let mut dest_needs_redo = false;
    if record.has_block_ref(1) {
        if xldata.initDest {
            dest_buffer = XLogInitBufferForRedo(record, 1)?;
            spg_init_buffer(
                dest_buffer,
                SPGIST_LEAF | if xldata.storesNulls { SPGIST_NULLS } else { 0 },
            );
            dest_needs_redo = true;
        } else {
            let (action, b) = XLogReadBufferForRedo(record, 1)?;
            dest_buffer = b;
            dest_needs_redo = action == BLK_NEEDS_REDO;
        }
    }

    // restore leaf tuples
    for i in 0..xldata.nInsert as usize {
        let lt = &md[ptr..];
        let sz = leaf_size(lt);
        ptr += sz;

        let (buffer, needs) = if leaf_page_select[i] != 0 {
            (dest_buffer, dest_needs_redo)
        } else {
            (src_buffer, src_needs_redo)
        };
        if !needs || buffer == InvalidBuffer {
            continue;
        }
        // SAFETY: redo pin+lock contract.
        let mut pm = unsafe { page_mut(buffer) };
        add_or_replace_tuple(&mut pm, &lt[..sz], to_insert[i]);
    }

    if src_needs_redo && src_buffer != InvalidBuffer {
        // SAFETY: redo pin+lock contract.
        let mut pm = unsafe { page_mut(src_buffer) };
        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(src_buffer)?;
    }
    if dest_needs_redo && dest_buffer != InvalidBuffer {
        // SAFETY: redo pin+lock contract.
        let mut pm = unsafe { page_mut(dest_buffer) };
        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(dest_buffer)?;
    }

    // inner tuple
    let (action, inner_buffer) = if xldata.initInner {
        let b = XLogInitBufferForRedo(record, 2)?;
        spg_init_buffer(b, if xldata.storesNulls { SPGIST_NULLS } else { 0 });
        (BLK_NEEDS_REDO, b)
    } else {
        XLogReadBufferForRedo(record, 2)?
    };

    if action == BLK_NEEDS_REDO {
        // SAFETY: redo pin+lock contract.
        let mut pm = unsafe { page_mut(inner_buffer) };
        add_or_replace_tuple(&mut pm, &inner_tuple[..inner_size], xldata.offnumInner);
        if xldata.innerIsParent {
            let parent = item_slice_mut(&mut pm, xldata.offnumParent);
            spgUpdateNodeLink(parent, xldata.nodeI as i32, blkno_inner, xldata.offnumInner);
        }
        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(inner_buffer)?;
    }
    if inner_buffer != InvalidBuffer {
        unlock_release(inner_buffer)?;
    }

    if src_buffer != InvalidBuffer {
        unlock_release(src_buffer)?;
    }
    if dest_buffer != InvalidBuffer {
        unlock_release(dest_buffer)?;
    }

    // parent downlink, unless done above
    if record.has_block_ref(3) {
        let (action, parent_buffer) = XLogReadBufferForRedo(record, 3)?;
        if action == BLK_NEEDS_REDO {
            // SAFETY: redo pin+lock contract.
            let mut pm = unsafe { page_mut(parent_buffer) };
            let parent = item_slice_mut(&mut pm, xldata.offnumParent);
            spgUpdateNodeLink(parent, xldata.nodeI as i32, blkno_inner, xldata.offnumInner);
            pm.set_lsn(lsn);
            bufmgr_seams::mark_buffer_dirty::call(parent_buffer)?;
        }
        if parent_buffer != InvalidBuffer {
            unlock_release(parent_buffer)?;
        }
    } else {
        debug_assert!(xldata.innerIsParent || xldata.isRootSplit);
    }
    Ok(())
}

fn spgRedoVacuumLeaf(record: &XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let md = main_data(record);
    let xldata = spgxlogVacuumLeaf::decode(md);

    let mut ptr = SizeOfSpgxlogVacuumLeaf;
    let to_dead = u16s_at(md, ptr, xldata.nDead as usize);
    ptr += 2 * xldata.nDead as usize;
    let to_placeholder = u16s_at(md, ptr, xldata.nPlaceholder as usize);
    ptr += 2 * xldata.nPlaceholder as usize;
    let move_src = u16s_at(md, ptr, xldata.nMove as usize);
    ptr += 2 * xldata.nMove as usize;
    let move_dest = u16s_at(md, ptr, xldata.nMove as usize);
    ptr += 2 * xldata.nMove as usize;
    let chain_src = u16s_at(md, ptr, xldata.nChain as usize);
    ptr += 2 * xldata.nChain as usize;
    let chain_dest = u16s_at(md, ptr, xldata.nChain as usize);

    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action == BLK_NEEDS_REDO {
        // SAFETY: redo pin+lock contract.
        let mut pm = unsafe { page_mut(buffer) };

        spgPageIndexMultiDelete(
            xldata.stateSrc.redirectXid,
            &mut pm,
            &to_dead,
            SPGIST_DEAD,
            SPGIST_DEAD,
            InvalidBlockNumber,
            InvalidOffsetNumber,
        );
        spgPageIndexMultiDelete(
            xldata.stateSrc.redirectXid,
            &mut pm,
            &to_placeholder,
            SPGIST_PLACEHOLDER,
            SPGIST_PLACEHOLDER,
            InvalidBlockNumber,
            InvalidOffsetNumber,
        );

        for i in 0..xldata.nMove as usize {
            let id_src = pm.as_ref().item_id(move_src[i]);
            let id_dest = pm.as_ref().item_id(move_dest[i]);
            pm.set_item_id(move_src[i], id_dest);
            pm.set_item_id(move_dest[i], id_src);
        }

        spgPageIndexMultiDelete(
            xldata.stateSrc.redirectXid,
            &mut pm,
            &move_src,
            SPGIST_PLACEHOLDER,
            SPGIST_PLACEHOLDER,
            InvalidBlockNumber,
            InvalidOffsetNumber,
        );

        for i in 0..xldata.nChain as usize {
            let lt = item_slice_mut(&mut pm, chain_src[i]);
            debug_assert!(tuple_state(lt) == SPGIST_LIVE);
            leaf_set_next_offset(lt, chain_dest[i]);
        }

        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    }
    if buffer != InvalidBuffer {
        unlock_release(buffer)?;
    }
    Ok(())
}

fn spgRedoVacuumRoot(record: &XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let md = main_data(record);
    let xldata = spgxlogVacuumRoot::decode(md);
    let to_delete = u16s_at(md, SizeOfSpgxlogVacuumRoot, xldata.nDelete as usize);

    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action == BLK_NEEDS_REDO {
        // SAFETY: redo pin+lock contract.
        let mut pm = unsafe { page_mut(buffer) };
        pm.index_multi_delete(&to_delete);
        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    }
    if buffer != InvalidBuffer {
        unlock_release(buffer)?;
    }
    Ok(())
}

fn spgRedoVacuumRedirect(record: &XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let md = main_data(record);
    let xldata = spgxlogVacuumRedirect::decode(md);
    let item_to_placeholder = u16s_at(md, SizeOfSpgxlogVacuumRedirect, xldata.nToPlaceholder as usize);

    if xlogutils::InHotStandby() {
        let (rlocator, _, _, _) =
            record.block_tag_extended(0).expect("spgRedoVacuumRedirect: no block 0");
        standby::ResolveRecoveryConflictWithSnapshot(
            xldata.snapshotConflictHorizon,
            xldata.isCatalogRel,
            rlocator,
        )?;
    }

    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action == BLK_NEEDS_REDO {
        // SAFETY: redo pin+lock contract.
        let mut pm = unsafe { page_mut(buffer) };

        for &off in item_to_placeholder.iter() {
            let dt = item_slice_mut(&mut pm, off);
            debug_assert!(tuple_state(dt) == SPGIST_REDIRECT);
            let mut hdr = SpGistDeadTupleHeader::decode(dt);
            hdr.tupstate = SPGIST_PLACEHOLDER;
            hdr.pointer = ::types_tuple::itemptr::ItemPointerData::invalid();
            hdr.encode(dt);
        }

        page_opaque_update(&mut pm, |op| {
            debug_assert!(op.nRedirection >= xldata.nToPlaceholder);
            op.nRedirection -= xldata.nToPlaceholder;
            op.nPlaceholder += xldata.nToPlaceholder;
        });

        if xldata.firstPlaceholder != InvalidOffsetNumber {
            let max = pm.as_ref().max_offset_number();
            let n = (max - xldata.firstPlaceholder + 1) as usize;
            let to_delete: Vec<OffsetNumber> =
                (xldata.firstPlaceholder..=max).collect();
            page_opaque_update(&mut pm, |op| {
                debug_assert!(op.nPlaceholder as usize >= n);
                op.nPlaceholder -= n as u16;
            });
            pm.index_multi_delete(&to_delete);
        }

        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    }
    if buffer != InvalidBuffer {
        unlock_release(buffer)?;
    }
    Ok(())
}

/// spg_redo.
pub fn spg_redo(record: &mut XLogReaderState) -> PgResult<()> {
    let info = record
        .record
        .as_ref()
        .expect("spg_redo with no decoded record")
        .xl_info
        & !XLR_INFO_MASK;
    match info {
        XLOG_SPGIST_ADD_LEAF => spgRedoAddLeaf(record),
        XLOG_SPGIST_MOVE_LEAFS => spgRedoMoveLeafs(record),
        XLOG_SPGIST_ADD_NODE => spgRedoAddNode(record),
        XLOG_SPGIST_SPLIT_TUPLE => spgRedoSplitTuple(record),
        XLOG_SPGIST_PICKSPLIT => spgRedoPickSplit(record),
        XLOG_SPGIST_VACUUM_LEAF => spgRedoVacuumLeaf(record),
        XLOG_SPGIST_VACUUM_ROOT => spgRedoVacuumRoot(record),
        XLOG_SPGIST_VACUUM_REDIRECT => spgRedoVacuumRedirect(record),
        other => panic!("spg_redo: unknown op code {other}"),
    }
}

pub fn spg_mask(pagedata: &mut [u8], _blkno: BlockNumber) -> PgResult<()> {
    bufmask::mask_page_lsn_and_checksum(pagedata);
    bufmask::mask_page_hint_bits(pagedata);
    if u16::from_ne_bytes([pagedata[12], pagedata[13]]) as usize >= SizeOfPageHeaderData {
        bufmask::mask_unused_space(pagedata);
    }
    Ok(())
}

