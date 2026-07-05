//! gistxlog.c — gist rmgr redo. Live arms: PAGE_UPDATE, DELETE, PAGE_SPLIT,
//! PAGE_DELETE, PAGE_REUSE (nop outside hot standby), ASSIGN_LSN (nop).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use types_core::{BlockNumber, Buffer, InvalidBlockNumber, OffsetNumber};
use types_error::PgResult;
use types_gist::{
    page_opaque_set, page_opaque_update, GISTPageOpaqueData, GistPageSetDeleted,
    GistxlogDelete, GistxlogPageDelete, GistxlogPageSplit, GistxlogPageUpdate, F_FOLLOW_RIGHT,
    F_HAS_GARBAGE, F_LEAF, F_TUPLES_DELETED, GIST_PAGE_ID, GIST_ROOT_BLKNO, SizeOfGistxlogDelete,
    XLOG_GIST_ASSIGN_LSN, XLOG_GIST_DELETE, XLOG_GIST_PAGE_DELETE, XLOG_GIST_PAGE_REUSE,
    XLOG_GIST_PAGE_SPLIT, XLOG_GIST_PAGE_UPDATE,
};
use types_storage::bufpage::{PageMut, PageRef, SizeOfPageHeaderData};

use xlogreader_seams::XLogReaderState;
use xlogutils::{XLogInitBufferForRedo, XLogReadBufferForRedo, BLK_NEEDS_REDO, BLK_RESTORED};

const XLR_INFO_MASK: u8 = 0x0F;
const FirstOffsetNumber: OffsetNumber = 1;

fn main_data<'a>(record: &'a XLogReaderState) -> &'a [u8] {
    let rec = record.record.as_ref().expect("gist redo with no decoded record");
    // SAFETY: points into the reader's decode buffer, valid for the redo
    // callback's duration.
    unsafe { rec.main_data_bytes() }
}

fn block_data<'a>(record: &'a XLogReaderState, block_id: u8) -> &'a [u8] {
    // SAFETY: same decode-buffer lifetime as main_data.
    unsafe { record.block(block_id).data_bytes() }
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

#[inline]
unsafe fn index_tuple_size(itup: *const u8) -> usize {
    (itup.add(6).cast::<u16>().read_unaligned() & 0x1FFF) as usize
}

fn page_is_leaf(page: &PageRef<'_>) -> bool {
    types_gist::GistPageIsLeaf(page)
}

fn add_item(pm: &mut PageMut<'_>, item: &[u8], off: OffsetNumber) {
    if pm.add_item(item, off, 0).is_none() {
        panic!(
            "failed to add item to GiST index page, size {} bytes",
            item.len()
        );
    }
}

fn gistRedoClearFollowRight(record: &XLogReaderState, block_id: u8) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let (action, buffer) = XLogReadBufferForRedo(record, block_id)?;
    if action == BLK_NEEDS_REDO || action == BLK_RESTORED {
        // SAFETY: redo pin+lock contract.
        let mut pm = unsafe { page_mut(buffer) };
        page_opaque_update(&mut pm, |op| {
            op.nsn = lsn;
            op.flags &= !F_FOLLOW_RIGHT;
        });
        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    }
    if buffer != 0 {
        unlock_release(buffer)?;
    }
    Ok(())
}

fn gistRedoPageUpdateRecord(record: &XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xldata = GistxlogPageUpdate::decode(main_data(record));

    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action == BLK_NEEDS_REDO {
        let data = block_data(record, 0);
        let mut off = 0usize;

        // SAFETY: redo pin+lock contract.
        let mut pm = unsafe { page_mut(buffer) };

        if xldata.ntodelete == 1 && xldata.ntoinsert == 1 {
            let offnum = OffsetNumber::from_ne_bytes([data[0], data[1]]);
            off += 2;
            // SAFETY: block data image.
            let sz = unsafe { index_tuple_size(data[off..].as_ptr()) };
            let itup = &data[off..off + sz];
            if !pm.index_tuple_overwrite(offnum, itup) {
                panic!("failed to add item to GiST index page, size {sz} bytes");
            }
            off += sz;
            debug_assert!(off == data.len());
        } else if xldata.ntodelete > 0 {
            let n = xldata.ntodelete as usize;
            let mut todelete: Vec<OffsetNumber> = Vec::with_capacity(n);
            for i in 0..n {
                todelete.push(OffsetNumber::from_ne_bytes([
                    data[i * 2],
                    data[i * 2 + 1],
                ]));
            }
            off += 2 * n;
            pm.index_multi_delete(&todelete);
            if page_is_leaf(&pm.as_ref()) {
                page_opaque_update(&mut pm, |op| op.flags |= F_TUPLES_DELETED);
            }
        }

        if off < data.len() {
            let mut insert_off = if pm.as_ref().pd_lower() as usize <= SizeOfPageHeaderData {
                FirstOffsetNumber
            } else {
                pm.as_ref().max_offset_number() + 1
            };
            while off < data.len() {
                // SAFETY: block data image.
                let sz = unsafe { index_tuple_size(data[off..].as_ptr()) };
                add_item(&mut pm, &data[off..off + sz], insert_off);
                off += sz;
                insert_off += 1;
            }
        }

        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    }

    if record.has_block_ref(1) {
        gistRedoClearFollowRight(record, 1)?;
    }

    if buffer != 0 {
        unlock_release(buffer)?;
    }
    Ok(())
}

fn gistRedoDeleteRecord(record: &XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let md = main_data(record);
    let xldata = GistxlogDelete::decode(md);

    if xlogutils::InHotStandby() {
        let (rlocator, _, _, _) =
            record.block_tag_extended(0).expect("gistRedoDeleteRecord: no block 0");
        standby::ResolveRecoveryConflictWithSnapshot(
            xldata.snapshotConflictHorizon,
            xldata.isCatalogRel,
            rlocator,
        )?;
    }

    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action == BLK_NEEDS_REDO {
        let n = xldata.ntodelete as usize;
        let mut todelete: Vec<OffsetNumber> = Vec::with_capacity(n);
        for i in 0..n {
            let base = SizeOfGistxlogDelete + i * 2;
            todelete.push(OffsetNumber::from_ne_bytes([md[base], md[base + 1]]));
        }

        // SAFETY: redo pin+lock contract.
        let mut pm = unsafe { page_mut(buffer) };
        pm.index_multi_delete(&todelete);
        page_opaque_update(&mut pm, |op| {
            op.flags &= !F_HAS_GARBAGE;
            op.flags |= F_TUPLES_DELETED;
        });
        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    }

    if buffer != 0 {
        unlock_release(buffer)?;
    }
    Ok(())
}

fn gistRedoPageSplitRecord(record: &XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xldata = GistxlogPageSplit::decode(main_data(record));
    let mut firstbuffer: Buffer = 0;
    let mut isrootsplit = false;

    for i in 0..xldata.npage as usize {
        let block_id = (i + 1) as u8;
        let blkno = record
            .block_tag_extended(block_id)
            .expect("split block ref")
            .2;
        if blkno == GIST_ROOT_BLKNO {
            debug_assert!(i == 0);
            isrootsplit = true;
        }

        let buffer = XLogInitBufferForRedo(record, block_id)?;
        let data = block_data(record, block_id);

        // decodePageSplitRecord: int num, then the tuple images
        let num = i32::from_ne_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let mut off = 4usize;

        let flags = if xldata.origleaf && blkno != GIST_ROOT_BLKNO {
            F_LEAF
        } else {
            0
        };

        // SAFETY: init-for-redo pin+lock contract.
        let mut pm = unsafe { page_mut(buffer) };
        pm.init(::types_gist::SizeOfGistPageOpaque);
        page_opaque_set(
            &mut pm,
            GISTPageOpaqueData {
                nsn: 0,
                rightlink: InvalidBlockNumber,
                flags,
                gist_page_id: GIST_PAGE_ID,
            },
        );

        let mut insert_off = FirstOffsetNumber;
        for _ in 0..num {
            // SAFETY: block data image.
            let sz = unsafe { index_tuple_size(data[off..].as_ptr()) };
            add_item(&mut pm, &data[off..off + sz], insert_off);
            insert_off += 1;
            off += sz;
        }
        debug_assert!(off == data.len());

        if blkno == GIST_ROOT_BLKNO {
            page_opaque_update(&mut pm, |op| {
                op.rightlink = InvalidBlockNumber;
                op.nsn = xldata.orignsn;
                op.flags &= !F_FOLLOW_RIGHT;
            });
        } else {
            let rightlink = if i < xldata.npage as usize - 1 {
                record
                    .block_tag_extended((i + 2) as u8)
                    .expect("next split block ref")
                    .2
            } else {
                xldata.origrlink
            };
            let markfr = i < xldata.npage as usize - 1 && !isrootsplit && xldata.markfollowright;
            page_opaque_update(&mut pm, |op| {
                op.rightlink = rightlink;
                op.nsn = xldata.orignsn;
                if markfr {
                    op.flags |= F_FOLLOW_RIGHT;
                } else {
                    op.flags &= !F_FOLLOW_RIGHT;
                }
            });
        }

        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;

        if i == 0 {
            firstbuffer = buffer;
        } else {
            unlock_release(buffer)?;
        }
    }

    if record.has_block_ref(0) {
        gistRedoClearFollowRight(record, 0)?;
    }

    unlock_release(firstbuffer)?;
    Ok(())
}

fn gistRedoPageDelete(record: &XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xldata = GistxlogPageDelete::decode(main_data(record));

    let (action, leaf_buffer) = XLogReadBufferForRedo(record, 0)?;
    if action == BLK_NEEDS_REDO {
        // SAFETY: redo pin+lock contract.
        let mut pm = unsafe { page_mut(leaf_buffer) };
        GistPageSetDeleted(&mut pm, xldata.deleteXid);
        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(leaf_buffer)?;
    }

    let (action, parent_buffer) = XLogReadBufferForRedo(record, 1)?;
    if action == BLK_NEEDS_REDO {
        // SAFETY: redo pin+lock contract.
        let mut pm = unsafe { page_mut(parent_buffer) };
        pm.index_tuple_delete(xldata.downlinkOffset);
        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(parent_buffer)?;
    }

    if parent_buffer != 0 {
        unlock_release(parent_buffer)?;
    }
    if leaf_buffer != 0 {
        unlock_release(leaf_buffer)?;
    }
    Ok(())
}

// gistRedoPageReuse: conflict point for hot standby only.
fn gistRedoPageReuse(record: &XLogReaderState) -> PgResult<()> {
    if xlogutils::InHotStandby() {
        let md = main_data(record);
        let locator = types_storage::RelFileLocator::new(
            u32::from_ne_bytes(md[0..4].try_into().unwrap()),
            u32::from_ne_bytes(md[4..8].try_into().unwrap()),
            u32::from_ne_bytes(md[8..12].try_into().unwrap()),
        );
        let horizon = types_core::FullTransactionId::from_u64(u64::from_ne_bytes(
            md[16..24].try_into().unwrap(),
        ));
        standby::ResolveRecoveryConflictWithSnapshotFullXid(horizon, md[24] != 0, locator)?;
    }
    Ok(())
}

/// gist_redo.
pub fn gist_redo(record: &mut XLogReaderState) -> PgResult<()> {
    let info = record
        .record
        .as_ref()
        .expect("gist_redo with no decoded record")
        .xl_info
        & !XLR_INFO_MASK;
    match info {
        XLOG_GIST_PAGE_UPDATE => gistRedoPageUpdateRecord(record),
        XLOG_GIST_DELETE => gistRedoDeleteRecord(record),
        XLOG_GIST_PAGE_REUSE => gistRedoPageReuse(record),
        XLOG_GIST_PAGE_SPLIT => gistRedoPageSplitRecord(record),
        XLOG_GIST_PAGE_DELETE => gistRedoPageDelete(record),
        XLOG_GIST_ASSIGN_LSN => Ok(()), // nop; see gistGetFakeLSN
        other => panic!("gist_redo: unknown op code {other}"),
    }
}

pub fn gist_mask(pagedata: &mut [u8], _blkno: BlockNumber) -> PgResult<()> {
    bufmask::mask_page_lsn_and_checksum(pagedata);
    bufmask::mask_page_hint_bits(pagedata);
    bufmask::mask_unused_space(pagedata);

    let ptr = core::ptr::NonNull::new(pagedata.as_mut_ptr()).unwrap();
    // SAFETY: pagedata is a full BLCKSZ page image, exclusively borrowed here.
    let mut pm = unsafe { PageMut::from_raw(ptr) };
    types_gist::page_opaque_update(&mut pm, |op| {
        op.nsn = 0;
        op.flags |= F_FOLLOW_RIGHT;
    });
    let is_leaf = page_is_leaf(&pm.as_ref());
    drop(pm);

    if is_leaf {
        bufmask::mask_lp_flags(pagedata);
    }

    let ptr = core::ptr::NonNull::new(pagedata.as_mut_ptr()).unwrap();
    // SAFETY: as above.
    let mut pm = unsafe { PageMut::from_raw(ptr) };
    types_gist::page_opaque_update(&mut pm, |op| op.flags &= !F_HAS_GARBAGE);
    Ok(())
}
