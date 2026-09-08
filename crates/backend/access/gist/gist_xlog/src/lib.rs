//! gistxlog.c — gist rmgr redo. Live arms: PAGE_UPDATE, DELETE, PAGE_SPLIT,
//! PAGE_DELETE, PAGE_REUSE (nop outside hot standby), ASSIGN_LSN (nop).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use types_core::{BlockNumber, Buffer, InvalidBlockNumber, OffsetNumber};
use types_error::{PgError, PgResult, ERRCODE_DATA_CORRUPTED, PANIC};
use types_gist::{
    page_opaque_set, page_opaque_update, GISTPageOpaqueData, GistPageSetDeleted,
    GistxlogDelete, GistxlogPageDelete, GistxlogPageSplit, GistxlogPageUpdate, F_FOLLOW_RIGHT,
    F_HAS_GARBAGE, F_LEAF, F_TUPLES_DELETED, GIST_PAGE_ID, GIST_ROOT_BLKNO, SizeOfGistxlogDelete,
    SizeOfGistxlogPageReuse, XLOG_GIST_ASSIGN_LSN, XLOG_GIST_DELETE, XLOG_GIST_PAGE_DELETE,
    XLOG_GIST_PAGE_REUSE, XLOG_GIST_PAGE_SPLIT, XLOG_GIST_PAGE_UPDATE,
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

/// sizeof(IndexTupleData): 6-byte ItemPointerData t_tid + 2-byte t_info. Every
/// index tuple is at least this large, and the size word lives in t_info.
const SIZE_OF_INDEX_TUPLE_DATA: usize = 8;

#[cold]
#[inline(never)]
fn corrupt_err(msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_DATA_CORRUPTED))
}

#[inline]
fn require(cond: bool, msg: impl FnOnce() -> String) -> PgResult<()> {
    if cond {
        Ok(())
    } else {
        Err(corrupt_err(msg()))
    }
}

// elog(ERROR) — a catchable XX000 (startup's redo error handling owns it),
// the level C's redo add-item / block-tag sites raise.
#[cold]
#[inline(never)]
fn redo_error(msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg))
}

// elog(PANIC) — an unrecoverable redo error (gist_redo's unknown op code).
#[cold]
#[inline(never)]
fn panic_err(msg: String) -> Box<PgError> {
    Box::new(PgError::new(PANIC, msg))
}

// XLogRecGetBlockTag (xlogreader.c:2001): elog(ERROR) if the referenced block
// is missing from the record, rather than a panic on the None. The shared
// port lives on XLogReaderState (xlogreader_seams::XLogReaderState::block_tag);
// this shim keeps the callers' 4-tuple shape (prefetch_buffer is never read).
fn block_tag(
    record: &XLogReaderState,
    block_id: u8,
) -> PgResult<(types_storage::RelFileLocator, types_core::ForkNumber, BlockNumber, Buffer)> {
    let (rlocator, forknum, blkno) = record.block_tag(block_id)?;
    Ok((rlocator, forknum, blkno, types_core::InvalidBuffer))
}

/// IndexTupleSize() over a block-data slice, validated against what remains.
///
/// C casts the block-data pointer to an IndexTuple and reads the size word out
/// of the decode buffer (gistxlog.c: `IndexTupleSize((IndexTuple) data)`); on a
/// truncated or size-inflated tuple stream that read — and the subsequent
/// `data[off..off+sz]` slice — would run past the buffer here. Confirm the
/// header is present and the declared size lies within the remaining payload
/// (and is at least a full header) before returning, so callers can slice
/// `data[..sz]` safely. Malformed input yields ERRCODE_DATA_CORRUPTED rather
/// than an out-of-bounds read or slice panic in the startup redo thread.
fn checked_index_tuple_size(data: &[u8]) -> PgResult<usize> {
    require(data.len() >= SIZE_OF_INDEX_TUPLE_DATA, || {
        format!(
            "GiST redo: block data too short for index tuple header: {} bytes",
            data.len()
        )
    })?;
    // t_info size bits: low 13 bits of the u16 at offset 6.
    let sz = (u16::from_ne_bytes([data[6], data[7]]) & 0x1FFF) as usize;
    require(sz >= SIZE_OF_INDEX_TUPLE_DATA && sz <= data.len(), || {
        format!(
            "GiST redo: invalid index tuple size {sz} in {} bytes of block data",
            data.len()
        )
    })?;
    Ok(sz)
}

fn page_is_leaf(page: &PageRef<'_>) -> bool {
    types_gist::GistPageIsLeaf(page)
}

// gistxlog.c:135 (gistRedoPageUpdateRecord insert loop): PageAddItem failure
// is elog(ERROR, "failed to add item to GiST index page, size %d bytes").
fn add_item(pm: &mut PageMut<'_>, item: &[u8], off: OffsetNumber) -> PgResult<()> {
    if pm.add_item(item, off, 0).is_none() {
        return Err(redo_error(format!(
            "failed to add item to GiST index page, size {} bytes",
            item.len()
        )));
    }
    Ok(())
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
    let xldata = GistxlogPageUpdate::decode(main_data(record))?;

    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action == BLK_NEEDS_REDO {
        let data = block_data(record, 0);
        let mut off = 0usize;

        // SAFETY: redo pin+lock contract.
        let mut pm = unsafe { page_mut(buffer) };

        if xldata.ntodelete == 1 && xldata.ntoinsert == 1 {
            require(data.len() >= 2, || {
                format!(
                    "GiST redo: PAGE_UPDATE block data too short for offset: {} bytes",
                    data.len()
                )
            })?;
            let offnum = OffsetNumber::from_ne_bytes([data[0], data[1]]);
            off += 2;
            let sz = checked_index_tuple_size(&data[off..])?;
            let itup = &data[off..off + sz];
            if !pm.index_tuple_overwrite(offnum, itup) {
                // gistxlog.c:102 elog(ERROR): catchable, not a panic.
                return Err(redo_error(format!(
                    "failed to add item to GiST index page, size {sz} bytes"
                )));
            }
            off += sz;
            debug_assert!(off == data.len());
        } else if xldata.ntodelete > 0 {
            let n = xldata.ntodelete as usize;
            require(data.len() >= 2 * n, || {
                format!(
                    "GiST redo: PAGE_UPDATE block data too short for {n} deletions: {} bytes",
                    data.len()
                )
            })?;
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
                let sz = checked_index_tuple_size(&data[off..])?;
                add_item(&mut pm, &data[off..off + sz], insert_off)?;
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
    let xldata = GistxlogDelete::decode(md)?;

    if xlogutils::InHotStandby() {
        let (rlocator, _, _, _) = block_tag(record, 0)?;
        standby::ResolveRecoveryConflictWithSnapshot(
            xldata.snapshotConflictHorizon,
            xldata.isCatalogRel,
            rlocator,
        )?;
    }

    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action == BLK_NEEDS_REDO {
        let n = xldata.ntodelete as usize;
        require(md.len() >= SizeOfGistxlogDelete + 2 * n, || {
            format!(
                "GiST redo: DELETE record too short for {n} offsets: {} bytes",
                md.len()
            )
        })?;
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
    let xldata = GistxlogPageSplit::decode(main_data(record))?;
    let mut firstbuffer: Buffer = 0;
    let mut isrootsplit = false;

    for i in 0..xldata.npage as usize {
        let block_id = (i + 1) as u8;
        let blkno = block_tag(record, block_id)?.2;
        if blkno == GIST_ROOT_BLKNO {
            debug_assert!(i == 0);
            isrootsplit = true;
        }

        let buffer = XLogInitBufferForRedo(record, block_id)?;
        let data = block_data(record, block_id);

        // decodePageSplitRecord: int num, then the tuple images
        require(data.len() >= 4, || {
            format!(
                "GiST redo: PAGE_SPLIT block data too short for tuple count: {} bytes",
                data.len()
            )
        })?;
        let num_raw = i32::from_ne_bytes([data[0], data[1], data[2], data[3]]);
        require(num_raw >= 0, || {
            format!("GiST redo: PAGE_SPLIT negative tuple count {num_raw}")
        })?;
        let num = num_raw as usize;
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
        for j in 0..num {
            let sz = checked_index_tuple_size(&data[off..])?;
            // gistfillbuffer (gistutil.c:49): the split redo fills via
            // gistfillbuffer, whose message names the item, not just its size.
            if pm.add_item(&data[off..off + sz], insert_off, 0).is_none() {
                return Err(redo_error(format!(
                    "failed to add item to GiST index page, item {j} out of {num}, size {sz} bytes"
                )));
            }
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
                block_tag(record, (i + 2) as u8)?.2
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
    let xldata = GistxlogPageDelete::decode(main_data(record))?;

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
        require(md.len() >= SizeOfGistxlogPageReuse, || {
            format!(
                "GiST redo: PAGE_REUSE record too short: need {SizeOfGistxlogPageReuse} bytes, got {}",
                md.len()
            )
        })?;
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
        // gistxlog.c:430 elog(PANIC): routed through the recovery error
        // callback ("WAL redo at ...") instead of an unhandled Rust panic.
        other => Err(panic_err(format!("gist_redo: unknown op code {other}"))),
    }
}

pub fn gist_mask(pagedata: &mut [u8], _blkno: BlockNumber) -> PgResult<()> {
    bufmask::mask_page_lsn_and_checksum(pagedata);
    bufmask::mask_page_hint_bits(pagedata);
    bufmask::mask_unused_space(pagedata)?;

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

#[cfg(test)]
mod redo_dispatch_tests {
    use super::*;

    // gistxlog.c:430: elog(PANIC, "gist_redo: unknown op code %u", info) prints
    // the whole info byte (xl_info & ~XLR_INFO_MASK). An unknown opcode must
    // surface as a PANIC-level PgError routed through the recovery error
    // callback, not an unhandled Rust panic (audit-18.6 b012 gistxlog-753671).
    #[test]
    fn unknown_op_code_is_a_panic_error_not_a_rust_panic() {
        let mut rec = xlogreader_seams::DecodedXLogRecord::default();
        rec.xl_info = 0xF0; // & !XLR_INFO_MASK == 0xF0, matches no gist opcode
        let mut record = xlogreader_seams::XLogReaderState {
            record: Some(rec),
            ..Default::default()
        };
        let err = gist_redo(&mut record).expect_err("unknown gist opcode must not redo silently");
        assert_eq!(err.message(), "gist_redo: unknown op code 240");
        assert_eq!(err.level(), PANIC);
    }

    // XLogRecGetBlockTag (xlogreader.c:2001) is elog(ERROR, "could not locate
    // backup block with ID %d in WAL record") when the block is absent; the
    // port panicked on the None (audit-18.6 b012 gistxlog-0396bd).
    #[test]
    fn missing_backup_block_is_a_catchable_error() {
        // A default record has no in-use blocks, so block 0 is absent.
        let record = xlogreader_seams::XLogReaderState {
            record: Some(xlogreader_seams::DecodedXLogRecord::default()),
            ..Default::default()
        };
        let err = block_tag(&record, 0).expect_err("absent block must be an error");
        assert_eq!(
            err.message(),
            "could not locate backup block with ID 0 in WAL record"
        );
        assert_eq!(err.level(), types_error::ERROR);
    }
}

#[cfg(test)]
mod redo_bounds_tests {
    use super::*;

    // A truncated block-data tuple stream (fewer than an IndexTupleData header)
    // must surface as ERRCODE_DATA_CORRUPTED, never an out-of-bounds read/slice
    // panic in the startup redo thread.
    #[test]
    fn short_tuple_header_is_data_corruption() {
        for len in 0..SIZE_OF_INDEX_TUPLE_DATA {
            let err = checked_index_tuple_size(&vec![0u8; len]).unwrap_err();
            assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);
        }
    }

    // A size word larger than the remaining payload (or smaller than a full
    // header) is rejected instead of driving a `data[..sz]` slice past the end.
    #[test]
    fn inflated_or_undersized_tuple_size_is_rejected() {
        // 16-byte buffer whose t_info size word claims 0x1FFF bytes.
        let mut data = vec![0u8; 16];
        data[6] = 0xFF;
        data[7] = 0x1F;
        let err = checked_index_tuple_size(&data).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);

        // Size word below the 8-byte header minimum.
        let mut small = vec![0u8; 16];
        small[6] = 4;
        small[7] = 0;
        let err = checked_index_tuple_size(&small).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);
    }

    // A well-formed tuple whose declared size fits the payload decodes cleanly.
    #[test]
    fn valid_tuple_size_is_accepted() {
        let mut data = vec![0u8; 24];
        data[6] = 16; // size = 16, fits within 24 bytes and >= 8-byte header
        data[7] = 0;
        assert_eq!(checked_index_tuple_size(&data).unwrap(), 16);
    }
}
