//! brin_xlog.c: BRIN rmgr redo. All six ops are live; brin_mask stays with
//! the wal-consistency lane (loud in rmgr, as every AM's).
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use types_brin::*;
use types_core::{BlockNumber, Buffer, InvalidBuffer};
use types_error::{PgError, PgResult, ERRCODE_DATA_CORRUPTED};
use types_storage::bufpage::{PageMut, SizeOfPageHeaderData};
use types_tuple::itemptr::ItemPointerData;
use xlogreader_seams::XLogReaderState;
use xlogutils::{XLogInitBufferForRedo, XLogReadBufferForRedo, BLK_NEEDS_REDO};

use brin_pageops::{brinSetHeapBlockItemptr, brin_metapage_init, brin_page_init};

const XLR_INFO_MASK: u8 = 0x0F;

/// DST fault-sweep RED hook (sim-cfg only, zero native surface): when armed,
/// `brin_xlog_samepage_update` keeps the OLD summary tuple instead of
/// applying the record's widened replacement — a deliberately weakened redo
/// that NARROWS a BRIN range relative to the heap. The crash sweep's brin
/// red leg arms this and must CATCH it through its query-level
/// consistent-or-wider coverage property (never through a replay failure).
#[cfg(pgrust_sim)]
pub mod sim_red {
    use core::sync::atomic::{AtomicBool, Ordering::Relaxed};
    pub static KEEP_STALE_SUMMARY: AtomicBool = AtomicBool::new(false);
    pub fn armed() -> bool {
        KEEP_STALE_SUMMARY.load(Relaxed)
    }
}

fn main_data<'a>(record: &'a XLogReaderState) -> &'a [u8] {
    let rec = record.record.as_ref().expect("brin redo with no decoded record");
    // SAFETY: points into the reader's decode buffer, valid for the redo
    // callback's duration.
    unsafe { rec.main_data_bytes() }
}

fn block_data<'a>(record: &'a XLogReaderState, block_id: u8) -> &'a [u8] {
    // SAFETY: same decode-buffer lifetime as main_data.
    unsafe { record.block(block_id).data_bytes() }
}

#[track_caller]
#[cold]
fn panic_err(msg: String) -> Box<PgError> {
    Box::new(PgError::new(types_error::PANIC, msg))
}

/// Upper bound on a BRIN index's pages_per_range: the reloption's max (see
/// reloptions.c, "pages_per_range" 1..131072). The minimum meaningful value
/// is 1 — 0 is never valid.
const BRIN_MAX_PAGES_PER_RANGE: BlockNumber = 131072;

/// Validate a `pagesPerRange` decoded from a BRIN WAL record before it is used
/// as a divisor in `HEAPBLK_TO_REVMAP_INDEX` (via `brinSetHeapBlockItemptr`).
///
/// C's BRIN code trusts the metapage-sourced `pagesPerRange` and never divides
/// by a validated value, but here a corrupt record carrying
/// `pagesPerRange == 0` would integer-divide-by-zero and panic the startup
/// redo thread — a crash that recovery re-hits on every restart. Treat 0 (and
/// out-of-supported-range) as data corruption and fail replay with a
/// diagnosable, catchable error rather than a panic.
fn validate_pages_per_range(pagesPerRange: BlockNumber) -> PgResult<()> {
    if pagesPerRange == 0 || pagesPerRange > BRIN_MAX_PAGES_PER_RANGE {
        return Err(Box::new(
            PgError::error(format!(
                "BRIN redo: invalid pagesPerRange {pagesPerRange} in WAL record"
            ))
            .with_sqlstate(ERRCODE_DATA_CORRUPTED),
        ));
    }
    Ok(())
}

// SAFETY contract shared by the redo arms: the buffer is pinned and
// exclusively locked (XLogReadBufferForRedo protocol).
unsafe fn page_mut<'p>(buffer: Buffer) -> PageMut<'p> {
    unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(buffer)) }
}

fn unlock_release(buffer: Buffer) -> PgResult<()> {
    bufmgr_seams::lock_buffer::call(buffer, bufmgr_seams::BUFFER_LOCK_UNLOCK)?;
    bufmgr_seams::release_buffer::call(buffer)
}

fn brin_xlog_createidx(record: &XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xlrec = decode_createidx(main_data(record))?;

    let buf = XLogInitBufferForRedo(record, 0)?;
    // SAFETY: pinned + exclusively locked (init-for-redo).
    let mut page = unsafe { page_mut(buf) };
    brin_metapage_init(&mut page, xlrec.pagesPerRange, xlrec.version);
    page.set_lsn(lsn);
    bufmgr_seams::mark_buffer_dirty::call(buf)?;
    unlock_release(buf)
}

fn brin_xlog_insert_update(
    record: &XLogReaderState,
    xlrec: &XlBrinInsert,
    init_page: bool,
) -> PgResult<()> {
    let lsn = record.EndRecPtr;

    // Reject a corrupt record before it reaches brinSetHeapBlockItemptr's
    // divide-by-pagesPerRange (below), which would otherwise panic redo.
    validate_pages_per_range(xlrec.pagesPerRange)?;

    let (action, buffer) = if init_page {
        let buffer = XLogInitBufferForRedo(record, 0)?;
        // SAFETY: pinned + exclusively locked.
        let mut page = unsafe { page_mut(buffer) };
        brin_page_init(&mut page, BRIN_PAGETYPE_REGULAR);
        (BLK_NEEDS_REDO, buffer)
    } else {
        XLogReadBufferForRedo(record, 0)?
    };

    let regpgno = bufmgr_seams::buffer_get_block_number::call(buffer);

    if action == BLK_NEEDS_REDO {
        let tuple = block_data(record, 0);
        debug_assert!(brin_tuple_blkno(tuple) == xlrec.heapBlk);

        // SAFETY: pinned + exclusively locked.
        let mut page = unsafe { page_mut(buffer) };
        let offnum = xlrec.offnum;
        if page.as_ref().max_offset_number() + 1 < offnum {
            return Err(panic_err(
                "brin_xlog_insert_update: invalid max offset number".into(),
            ));
        }
        let off = page.add_item(tuple, offnum, types_storage::bufpage::PAI_OVERWRITE);
        if off.is_none() {
            return Err(panic_err("brin_xlog_insert_update: failed to add tuple".into()));
        }
        page.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    }
    if buffer != InvalidBuffer {
        unlock_release(buffer)?;
    }

    let (action, buffer) = XLogReadBufferForRedo(record, 1)?;
    if action == BLK_NEEDS_REDO {
        let tid = ItemPointerData::new(regpgno, xlrec.offnum);
        brinSetHeapBlockItemptr(buffer, xlrec.pagesPerRange, xlrec.heapBlk, tid);
        // SAFETY: pinned + exclusively locked.
        unsafe { page_mut(buffer) }.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    }
    if buffer != InvalidBuffer {
        unlock_release(buffer)?;
    }

    Ok(())
}

fn brin_xlog_insert(record: &XLogReaderState, init_page: bool) -> PgResult<()> {
    let xlrec = decode_insert(main_data(record))?;
    brin_xlog_insert_update(record, &xlrec, init_page)
}

fn brin_xlog_update(record: &XLogReaderState, init_page: bool) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xlrec = decode_update(main_data(record))?;

    let (action, buffer) = XLogReadBufferForRedo(record, 2)?;
    if action == BLK_NEEDS_REDO {
        // SAFETY: pinned + exclusively locked.
        let mut page = unsafe { page_mut(buffer) };
        page.index_tuple_delete_no_compact(xlrec.oldOffnum);
        page.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    }

    brin_xlog_insert_update(record, &xlrec.insert, init_page)?;

    if buffer != InvalidBuffer {
        unlock_release(buffer)?;
    }
    Ok(())
}

fn brin_xlog_samepage_update(record: &XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let offnum = decode_samepage_update(main_data(record))?;

    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    // DST RED (sim-cfg only): the deliberately weakened redo — keep the OLD
    // (narrower) summary tuple, advance only the LSN. See sim_red.
    #[cfg(pgrust_sim)]
    if action == BLK_NEEDS_REDO && crate::sim_red::armed() {
        // SAFETY: pinned + exclusively locked.
        unsafe { page_mut(buffer) }.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
        if buffer != InvalidBuffer {
            unlock_release(buffer)?;
        }
        return Ok(());
    }
    if action == BLK_NEEDS_REDO {
        let brintuple = block_data(record, 0);
        // SAFETY: pinned + exclusively locked.
        let mut page = unsafe { page_mut(buffer) };
        if !page.index_tuple_overwrite(offnum, brintuple) {
            return Err(panic_err(
                "brin_xlog_samepage_update: failed to replace tuple".into(),
            ));
        }
        page.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    }
    if buffer != InvalidBuffer {
        unlock_release(buffer)?;
    }
    Ok(())
}

fn brin_xlog_revmap_extend(record: &XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let targetBlk = decode_revmap_extend(main_data(record))?;

    let (action, metabuf) = XLogReadBufferForRedo(record, 0)?;
    if action == BLK_NEEDS_REDO {
        // SAFETY: pinned + exclusively locked.
        let mut metapg = unsafe { page_mut(metabuf) };
        let mut metadata = brin_meta_read(&metapg.as_ref());
        debug_assert!(metadata.lastRevmapPage == targetBlk - 1);
        metadata.lastRevmapPage = targetBlk;
        brin_meta_write(&mut metapg, &metadata);
        metapg.set_lsn(lsn);
        // pd_lower past the metadata (see brin_metapage_init).
        metapg.set_pd_lower((SizeOfPageHeaderData + SizeOfBrinMetaPageData) as u16);
        bufmgr_seams::mark_buffer_dirty::call(metabuf)?;
    }

    // Re-init the target block as a revmap page (never a full-page image).
    let buf = XLogInitBufferForRedo(record, 1)?;
    // SAFETY: pinned + exclusively locked.
    let mut page = unsafe { page_mut(buf) };
    brin_page_init(&mut page, BRIN_PAGETYPE_REVMAP);
    page.set_lsn(lsn);
    bufmgr_seams::mark_buffer_dirty::call(buf)?;

    unlock_release(buf)?;
    if metabuf != InvalidBuffer {
        unlock_release(metabuf)?;
    }
    Ok(())
}

fn brin_xlog_desummarize_page(record: &XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xlrec = decode_desummarize(main_data(record))?;

    // Reject a corrupt record before it reaches brinSetHeapBlockItemptr's
    // divide-by-pagesPerRange (below), which would otherwise panic redo.
    validate_pages_per_range(xlrec.pagesPerRange)?;

    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action == BLK_NEEDS_REDO {
        brinSetHeapBlockItemptr(
            buffer,
            xlrec.pagesPerRange,
            xlrec.heapBlk,
            ItemPointerData::invalid(),
        );
        // SAFETY: pinned + exclusively locked.
        unsafe { page_mut(buffer) }.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    }
    if buffer != InvalidBuffer {
        unlock_release(buffer)?;
    }

    let (action, buffer) = XLogReadBufferForRedo(record, 1)?;
    if action == BLK_NEEDS_REDO {
        // SAFETY: pinned + exclusively locked.
        let mut page = unsafe { page_mut(buffer) };
        page.index_tuple_delete_no_compact(xlrec.regOffset);
        page.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    }
    if buffer != InvalidBuffer {
        unlock_release(buffer)?;
    }
    Ok(())
}

pub fn brin_redo(record: &mut XLogReaderState) -> PgResult<()> {
    let info =
        record.record.as_ref().expect("brin_redo with no decoded record").xl_info & !XLR_INFO_MASK;
    let init_page = info & XLOG_BRIN_INIT_PAGE != 0;
    match info & XLOG_BRIN_OPMASK {
        XLOG_BRIN_CREATE_INDEX => brin_xlog_createidx(record),
        XLOG_BRIN_INSERT => brin_xlog_insert(record, init_page),
        XLOG_BRIN_UPDATE => brin_xlog_update(record, init_page),
        XLOG_BRIN_SAMEPAGE_UPDATE => brin_xlog_samepage_update(record),
        XLOG_BRIN_REVMAP_EXTEND => brin_xlog_revmap_extend(record),
        XLOG_BRIN_DESUMMARIZE => brin_xlog_desummarize_page(record),
        // brin_xlog.c:334 prints the whole info byte (flags included).
        _ => Err(panic_err(format!("brin_redo: unknown op code {info}"))),
    }
}

/// brin_mask (brin_xlog.c) — mask a BRIN page's non-WAL-logged fields for
/// `wal_consistency_checking`. Faithful port of PostgreSQL REL_18_3.
pub fn brin_mask(pagedata: &mut [u8], _blkno: types_core::BlockNumber) -> PgResult<()> {
    bufmask::mask_page_lsn_and_checksum(pagedata);
    bufmask::mask_page_hint_bits(pagedata);

    let ptr = core::ptr::NonNull::new(pagedata.as_mut_ptr()).unwrap();
    // SAFETY: `pagedata` is a full BLCKSZ page image, exclusively borrowed here.
    let pm = unsafe { PageMut::from_raw(ptr) };
    let r = pm.as_ref();
    // Regular pages have real unused space; meta pages only when pd_lower was
    // set (revmap pages fill their "unused" region and must not be masked).
    let do_unused = BRIN_IS_REGULAR_PAGE(&r)
        || (BRIN_IS_META_PAGE(&r) && r.pd_lower() as usize > SizeOfPageHeaderData);
    drop(pm);

    if do_unused {
        bufmask::mask_unused_space(pagedata)?;
    }

    // BRIN_EVACUATE_PAGE is not WAL-logged; mask it.
    let ptr = core::ptr::NonNull::new(pagedata.as_mut_ptr()).unwrap();
    // SAFETY: as above.
    let mut pm = unsafe { PageMut::from_raw(ptr) };
    let flags = BrinPageFlags(&pm.as_ref());
    BrinSetPageFlags(&mut pm, flags & !BRIN_EVACUATE_PAGE);
    Ok(())
}

#[cfg(test)]
mod pages_per_range_tests {
    use super::*;

    #[test]
    fn zero_pages_per_range_is_data_corruption_not_panic() {
        let err = validate_pages_per_range(0).expect_err("pagesPerRange=0 must be rejected");
        assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);
    }

    #[test]
    fn out_of_range_pages_per_range_is_rejected() {
        let err = validate_pages_per_range(BRIN_MAX_PAGES_PER_RANGE + 1)
            .expect_err("pagesPerRange above the supported max must be rejected");
        assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);
    }

    #[test]
    fn valid_pages_per_range_is_accepted() {
        validate_pages_per_range(1).unwrap();
        validate_pages_per_range(128).unwrap();
        validate_pages_per_range(BRIN_MAX_PAGES_PER_RANGE).unwrap();
    }
}

#[cfg(test)]
mod redo_dispatch_tests {
    use super::*;

    // brin_xlog.c:334: elog(PANIC, "brin_redo: unknown op code %u", info) prints
    // the whole info byte (xl_info & ~XLR_INFO_MASK), flags included — an
    // unknown opcode carrying XLOG_BRIN_INIT_PAGE reports 0xE0 = 224, not the
    // XLOG_BRIN_OPMASK-stripped 96 (audit row b1-5cc21a49).
    #[test]
    fn unknown_opcode_reports_the_unmasked_info_byte() {
        let mut rec = xlogreader_seams::DecodedXLogRecord::default();
        rec.xl_info = 0xE0;
        let mut record = XLogReaderState { record: Some(rec), ..Default::default() };
        let err = brin_redo(&mut record).expect_err("unknown brin opcode must not redo silently");
        assert_eq!(err.message, "brin_redo: unknown op code 224");
        assert_eq!(err.level, types_error::PANIC);
    }
}

#[cfg(test)]
mod mask_tests {
    use super::*;
    use types_core::BLCKSZ;

    #[repr(align(8))]
    struct P([u8; BLCKSZ]);

    fn pm(p: &mut P) -> PageMut<'_> {
        let ptr = core::ptr::NonNull::new(p.0.as_mut_ptr()).unwrap();
        // SAFETY: owned MAXALIGNed BLCKSZ image, exclusively borrowed.
        unsafe { PageMut::from_raw(ptr) }
    }

    #[test]
    fn brin_mask_clears_evacuate_flag_and_lsn_idempotent() {
        let mut p = P([0u8; BLCKSZ]);
        {
            let mut page = pm(&mut p);
            brin_page_init(&mut page, BRIN_PAGETYPE_REGULAR);
            page.set_lsn(0x1111_2222_3333);
            let f = BrinPageFlags(&page.as_ref());
            BrinSetPageFlags(&mut page, f | BRIN_EVACUATE_PAGE);
        }
        brin_mask(&mut p.0, 0).unwrap();
        let masked = p.0;
        {
            let page = pm(&mut p);
            assert_eq!(page.as_ref().lsn(), 0);
            assert_eq!(BrinPageFlags(&page.as_ref()) & BRIN_EVACUATE_PAGE, 0);
        }
        let mut p2 = P(masked);
        brin_mask(&mut p2.0, 0).unwrap();
        assert_eq!(p2.0, masked);
    }
}
