//! xloginsert.c (PostgreSQL 18.3): WAL record assembly. C's stateful
//! XLogBeginInsert/XLogRegister*/XLogInsert protocol exists because its
//! working set is file-static; here every record arrives fully specified in
//! one call (the flat-fragment form xact's WAL builders proved out), so the
//! begin/register bookkeeping collapses into `insert_record` and only the
//! retained scratch (header image, fragment list, compression buffers)
//! survives between records — C's static registered_buffers, per rule 7.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::RefCell;

use elog::ereport;
use types_core::{BlockNumber, Buffer, ForkNumber, XLogRecPtr};
use types_error::{ErrorLocation, PgError, PgResult, ERROR, PANIC};
use types_storage::RelFileLocator;
use xloginsert_seams::{
    XLogRegBuf, REGBUF_FORCE_IMAGE, REGBUF_KEEP_DATA, REGBUF_NO_IMAGE, REGBUF_STANDARD,
    REGBUF_WILL_INIT,
};

#[cfg(test)]
mod tests;

const InvalidXLogRecPtr: XLogRecPtr = 0;
const BLCKSZ: usize = 8192;
const SizeOfXLogRecord: usize = 24;
const SizeOfXLogLongPHD: usize = 40;

// xlogrecord.h
pub const XLR_MAX_BLOCK_ID: usize = 32;
pub const XLR_NORMAL_MAX_BLOCK_ID: usize = 4;
pub const XLR_NORMAL_RDATAS: usize = 20;
pub const XLogRecordMaxSize: u64 = 1020 * 1024 * 1024;
const SizeOfXLogRecordBlockHeader: usize = 4;
const SizeOfXLogRecordBlockImageHeader: usize = 5;
const SizeOfXLogRecordBlockCompressHeader: usize = 2;
const SizeOfXLogRecordDataHeaderLong: usize = 5;
const SizeOfXlogOrigin: usize = 3;
const SizeOfXLogTransactionId: usize = 5;
const MaxSizeOfXLogRecordBlockHeader: usize = SizeOfXLogRecordBlockHeader
    + SizeOfXLogRecordBlockImageHeader
    + SizeOfXLogRecordBlockCompressHeader
    + 12
    + 4;
const HEADER_SCRATCH_SIZE: usize = SizeOfXLogRecord
    + MaxSizeOfXLogRecordBlockHeader * (XLR_MAX_BLOCK_ID + 1)
    + SizeOfXLogRecordDataHeaderLong
    + SizeOfXlogOrigin
    + SizeOfXLogTransactionId;

const XLR_BLOCK_ID_DATA_SHORT: u8 = 255;
const XLR_BLOCK_ID_DATA_LONG: u8 = 254;
const XLR_BLOCK_ID_ORIGIN: u8 = 253;
const XLR_BLOCK_ID_TOPLEVEL_XID: u8 = 252;

const XLR_RMGR_INFO_MASK: u8 = 0xF0;
pub const XLR_SPECIAL_REL_UPDATE: u8 = 0x01;
pub const XLR_CHECK_CONSISTENCY: u8 = 0x02;

use xlogreader_seams::{
    BKPBLOCK_HAS_DATA, BKPBLOCK_HAS_IMAGE, BKPBLOCK_SAME_REL, BKPBLOCK_WILL_INIT,
};
const BKPIMAGE_HAS_HOLE: u8 = 0x01;
const BKPIMAGE_APPLY: u8 = 0x02;
const BKPIMAGE_COMPRESS_PGLZ: u8 = 0x04;

const COMPRESS_BUFSIZE: usize = pglz::pglz_max_output(BLCKSZ);

const RM_XLOG_ID: u8 = 0;
const XLOG_FPI: u8 = 0xB0;
const XLOG_FPI_FOR_HINT: u8 = 0xA0;

const InvalidRepOriginId: u16 = 0;

#[track_caller]
fn loc(func: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, func)
}

#[track_caller]
#[cold]
fn too_much_wal_data(detail: String) -> Box<PgError> {
    Box::new(PgError::new(ERROR, format!("too much WAL data: {detail}")))
}

#[track_caller]
#[cold]
fn misordered_block_ids(prev: u8, cur: u8) -> Box<PgError> {
    Box::new(PgError::new(
        ERROR,
        format!("block IDs must be registered in ascending order: {cur} after {prev}"),
    ))
}

fn page_lsn(page: &[u8]) -> XLogRecPtr {
    // pd_lsn is two u32 halves (PageXLogRecPtr): xlogid @0, xrecoff @4.
    let hi = u32::from_ne_bytes(page[0..4].try_into().unwrap());
    let lo = u32::from_ne_bytes(page[4..8].try_into().unwrap());
    ((hi as u64) << 32) | lo as u64
}

fn page_is_new(page: &[u8]) -> bool {
    u16::from_ne_bytes(page[14..16].try_into().unwrap()) == 0
}

fn page_set_lsn(page: &mut [u8], lsn: XLogRecPtr) {
    page[0..4].copy_from_slice(&((lsn >> 32) as u32).to_ne_bytes());
    page[4..8].copy_from_slice(&(lsn as u32).to_ne_bytes());
}

/// One block reference, already resolved (C's registered_buffer essentials).
#[derive(Clone, Copy)]
pub struct RegBlock<'a> {
    pub block_id: u8,
    pub rlocator: RelFileLocator,
    pub forknum: ForkNumber,
    pub block: BlockNumber,
    pub page: &'a [u8],
    pub flags: u8,
    pub bufdata: &'a [&'a [u8]],
}

struct Scratch {
    hdr: Box<[u8; HEADER_SCRATCH_SIZE]>,
    // Fragment list handed to XLogInsertRecord. Lifetimes are erased so the
    // capacity is retained across records; entries only ever point at data
    // owned by the current call and are cleared before it returns.
    rdatas: Vec<&'static [u8]>,
    compressed: Vec<Box<[u8; COMPRESS_BUFSIZE]>>,
}

thread_local! {
    static SCRATCH: RefCell<Option<Scratch>> = const { RefCell::new(None) };
}

pub fn InitXLogInsert() -> PgResult<()> {
    SCRATCH.with(|s| {
        let mut s = s.borrow_mut();
        if s.is_none() {
            *s = Some(Scratch {
                hdr: Box::new([0u8; HEADER_SCRATCH_SIZE]),
                rdatas: Vec::with_capacity(XLR_NORMAL_RDATAS),
                compressed: Vec::new(),
            });
        }
    });
    Ok(())
}

// SAFETY contract carried by Scratch.rdatas: see its field comment.
fn erased(s: &[u8]) -> &'static [u8] {
    // SAFETY: the caller (assemble/insert path) clears rdatas before the
    // borrow that produced `s` ends; entries never escape the record call.
    unsafe { core::slice::from_raw_parts(s.as_ptr(), s.len()) }
}

struct Assembled {
    hdr_len: usize,
    fpw_lsn: XLogRecPtr,
    num_fpi: i32,
    topxid_included: bool,
}

// XLogRecordAssemble: fills scratch.hdr (record header + block/data headers)
// and scratch.rdatas[1..] (images, block data, main data); rdatas[0] is a
// placeholder for the header tail, patched by the caller after split.
#[allow(clippy::too_many_arguments)]
fn assemble(
    scratch: &mut Scratch,
    rmid: u8,
    info: u8,
    redo_rec_ptr: XLogRecPtr,
    do_page_writes: bool,
    record_flags: u8,
    main_data: &[&[u8]],
    blocks: &[RegBlock<'_>],
) -> PgResult<Assembled> {
    scratch.rdatas.clear();
    scratch.rdatas.push(&[]);

    let mut total_len: u64 = 0;
    let mut fpw_lsn = InvalidXLogRecPtr;
    let mut num_fpi = 0i32;
    let mut prev_rlocator: Option<RelFileLocator> = None;
    let mut sp = SizeOfXLogRecord;
    let hdr = &mut scratch.hdr[..];

    // wal_consistency_checking[] is pinned all-false (transam_xlog panics on
    // a non-empty setting), so no per-rmid probe here.

    for (i, blk) in blocks.iter().enumerate() {
        // C indexes registered_buffers by block_id; unchecked disorder here
        // would emit CRC-valid undecodable WAL.
        if i > 0 && blk.block_id <= blocks[i - 1].block_id {
            return Err(misordered_block_ids(blocks[i - 1].block_id, blk.block_id));
        }
        if blk.block_id as usize > XLR_MAX_BLOCK_ID {
            return Err(Box::new(PgError::new(
                ERROR,
                "too many registered buffers".to_string(),
            )));
        }
        debug_assert_eq!(blk.page.len(), BLCKSZ);

        let rdata_len: usize = blk.bufdata.iter().map(|d| d.len()).sum();
        if rdata_len > u16::MAX as usize {
            return Err(too_much_wal_data(format!(
                "registering more than maximum {} bytes allowed to block {}",
                u16::MAX,
                blk.block_id
            )));
        }

        let needs_backup = if blk.flags & REGBUF_FORCE_IMAGE != 0 {
            true
        } else if blk.flags & REGBUF_NO_IMAGE != 0 || !do_page_writes {
            false
        } else {
            let lsn = page_lsn(blk.page);
            let backup = lsn <= redo_rec_ptr;
            if !backup && (fpw_lsn == InvalidXLogRecPtr || lsn < fpw_lsn) {
                fpw_lsn = lsn;
            }
            backup
        };

        let needs_data = if rdata_len == 0 {
            false
        } else if blk.flags & REGBUF_KEEP_DATA != 0 {
            true
        } else {
            !needs_backup
        };

        let mut fork_flags: u8 = blk.forknum as i32 as u8;
        if blk.flags & REGBUF_WILL_INIT == REGBUF_WILL_INIT {
            fork_flags |= BKPBLOCK_WILL_INIT;
        }
        let include_image = needs_backup || info & XLR_CHECK_CONSISTENCY != 0;

        let mut bimg_length: u16 = 0;
        let mut bimg_hole_offset: u16 = 0;
        let mut bimg_info: u8 = 0;
        let mut cbimg_hole_length: u16 = 0;
        let mut is_compressed = false;

        if include_image {
            if blk.flags & REGBUF_STANDARD != 0 {
                let lower = u16::from_ne_bytes(blk.page[12..14].try_into().unwrap());
                let upper = u16::from_ne_bytes(blk.page[14..16].try_into().unwrap());
                // SizeOfPageHeaderData = 24.
                if lower >= 24 && upper > lower && upper as usize <= BLCKSZ {
                    bimg_hole_offset = lower;
                    cbimg_hole_length = upper - lower;
                }
            }

            let wal_compression = guc_tables::vars::wal_compression.read();
            if wal_compression != guc_tables::consts::WAL_COMPRESSION_NONE {
                if wal_compression != guc_tables::consts::WAL_COMPRESSION_PGLZ {
                    // The LZ4/zstd arms are the C #else branches (not built).
                    return Err(Box::new(PgError::new(
                        ERROR,
                        "LZ4/zstd wal_compression is not supported by this build".to_string(),
                    )));
                }
                while scratch.compressed.len() <= i {
                    scratch.compressed.push(Box::new([0u8; COMPRESS_BUFSIZE]));
                }
                if let Some(len) = compress_backup_block(
                    blk.page,
                    bimg_hole_offset,
                    cbimg_hole_length,
                    &mut scratch.compressed[i],
                ) {
                    is_compressed = true;
                    bimg_length = len as u16;
                }
            }

            fork_flags |= BKPBLOCK_HAS_IMAGE;
            num_fpi += 1;

            bimg_info = if cbimg_hole_length == 0 { 0 } else { BKPIMAGE_HAS_HOLE };
            if needs_backup {
                bimg_info |= BKPIMAGE_APPLY;
            }

            if is_compressed {
                bimg_info |= BKPIMAGE_COMPRESS_PGLZ;
                scratch
                    .rdatas
                    .push(erased(&scratch.compressed[i][..bimg_length as usize]));
            } else {
                bimg_length = (BLCKSZ - cbimg_hole_length as usize) as u16;
                if cbimg_hole_length == 0 {
                    scratch.rdatas.push(erased(blk.page));
                } else {
                    scratch.rdatas.push(erased(&blk.page[..bimg_hole_offset as usize]));
                    scratch.rdatas.push(erased(
                        &blk.page[(bimg_hole_offset + cbimg_hole_length) as usize..],
                    ));
                }
            }
            total_len += bimg_length as u64;
        }

        if needs_data {
            fork_flags |= BKPBLOCK_HAS_DATA;
            total_len += rdata_len as u64;
            for d in blk.bufdata {
                scratch.rdatas.push(erased(d));
            }
        }

        let samerel = prev_rlocator == Some(blk.rlocator);
        if samerel {
            fork_flags |= BKPBLOCK_SAME_REL;
        }
        prev_rlocator = Some(blk.rlocator);

        hdr[sp] = blk.block_id;
        hdr[sp + 1] = fork_flags;
        hdr[sp + 2..sp + 4]
            .copy_from_slice(&(if needs_data { rdata_len as u16 } else { 0 }).to_ne_bytes());
        sp += SizeOfXLogRecordBlockHeader;
        if include_image {
            hdr[sp..sp + 2].copy_from_slice(&bimg_length.to_ne_bytes());
            hdr[sp + 2..sp + 4].copy_from_slice(&bimg_hole_offset.to_ne_bytes());
            hdr[sp + 4] = bimg_info;
            sp += SizeOfXLogRecordBlockImageHeader;
            if cbimg_hole_length != 0 && is_compressed {
                hdr[sp..sp + 2].copy_from_slice(&cbimg_hole_length.to_ne_bytes());
                sp += SizeOfXLogRecordBlockCompressHeader;
            }
        }
        if !samerel {
            hdr[sp..sp + 4].copy_from_slice(&blk.rlocator.spcOid.to_ne_bytes());
            hdr[sp + 4..sp + 8].copy_from_slice(&blk.rlocator.dbOid.to_ne_bytes());
            hdr[sp + 8..sp + 12].copy_from_slice(&blk.rlocator.relNumber.to_ne_bytes());
            sp += 12;
        }
        hdr[sp..sp + 4].copy_from_slice(&blk.block.to_ne_bytes());
        sp += 4;
    }

    if record_flags & transam_xlog::XLOG_INCLUDE_ORIGIN != 0 {
        // Uninstalled seam = C default InvalidRepOriginId (origin.c).
        let origin = if origin_seams::replorigin_session_origin::is_installed() {
            origin_seams::replorigin_session_origin::call()
        } else {
            InvalidRepOriginId
        };
        if origin != InvalidRepOriginId {
            hdr[sp] = XLR_BLOCK_ID_ORIGIN;
            hdr[sp + 1..sp + 3].copy_from_slice(&origin.to_ne_bytes());
            sp += SizeOfXlogOrigin;
        }
    }

    let mut topxid_included = false;
    if xact::IsSubxactTopXidLogPending() {
        let xid = xact::GetTopTransactionIdIfAny();
        topxid_included = true;
        hdr[sp] = XLR_BLOCK_ID_TOPLEVEL_XID;
        hdr[sp + 1..sp + 5].copy_from_slice(&xid.to_ne_bytes());
        sp += SizeOfXLogTransactionId;
    }

    let mainrdata_len: u64 = main_data.iter().map(|d| d.len() as u64).sum();
    if mainrdata_len > 0 {
        if mainrdata_len > 255 {
            if mainrdata_len > u32::MAX as u64 {
                return Err(too_much_wal_data(format!(
                    "main data length is {mainrdata_len} bytes for a maximum of {}",
                    u32::MAX
                )));
            }
            hdr[sp] = XLR_BLOCK_ID_DATA_LONG;
            hdr[sp + 1..sp + 5].copy_from_slice(&(mainrdata_len as u32).to_ne_bytes());
            sp += 5;
        } else {
            hdr[sp] = XLR_BLOCK_ID_DATA_SHORT;
            hdr[sp + 1] = mainrdata_len as u8;
            sp += 2;
        }
        for d in main_data {
            scratch.rdatas.push(erased(d));
        }
        total_len += mainrdata_len;
    }

    let hdr_len = sp;
    total_len += hdr_len as u64;

    let mut crc = crc32c::pg_comp_crc32c(crc32c::CRC32C_INIT, &hdr[SizeOfXLogRecord..hdr_len]);
    for rd in &scratch.rdatas[1..] {
        crc = crc32c::pg_comp_crc32c(crc, rd);
    }

    if total_len > XLogRecordMaxSize {
        return Err(Box::new(PgError::new(
            ERROR,
            format!(
                "oversized WAL record: would be {total_len} bytes (of maximum {XLogRecordMaxSize}); rmid {rmid} flags {info}"
            ),
        )));
    }

    // XLogRecord header image; xl_prev is XLogInsertRecord's, xl_crc is the
    // body CRC it finalizes over bytes 0..20.
    hdr[0..4].copy_from_slice(&(total_len as u32).to_ne_bytes());
    hdr[4..8].copy_from_slice(&xact::GetCurrentTransactionIdIfAny().to_ne_bytes());
    hdr[8..16].copy_from_slice(&InvalidXLogRecPtr.to_ne_bytes());
    hdr[16] = info;
    hdr[17] = rmid;
    hdr[18] = 0;
    hdr[19] = 0;
    hdr[20..24].copy_from_slice(&crc.to_ne_bytes());

    Ok(Assembled {
        hdr_len,
        fpw_lsn,
        num_fpi,
        topxid_included,
    })
}

// XLogCompressBackupBlock, pglz arm.
fn compress_backup_block(
    page: &[u8],
    hole_offset: u16,
    hole_length: u16,
    dest: &mut [u8; COMPRESS_BUFSIZE],
) -> Option<usize> {
    let orig_len = BLCKSZ - hole_length as usize;
    let mut tmp = [0u8; BLCKSZ];
    let (source, extra_bytes): (&[u8], usize) = if hole_length != 0 {
        tmp[..hole_offset as usize].copy_from_slice(&page[..hole_offset as usize]);
        tmp[hole_offset as usize..orig_len]
            .copy_from_slice(&page[(hole_offset + hole_length) as usize..]);
        (&tmp[..orig_len], SizeOfXLogRecordBlockCompressHeader)
    } else {
        (page, 0)
    };

    // SAFETY: MaybeUninit<u8> view over an initialized buffer.
    let dest_uninit = unsafe {
        core::slice::from_raw_parts_mut(
            dest.as_mut_ptr().cast::<core::mem::MaybeUninit<u8>>(),
            COMPRESS_BUFSIZE,
        )
    };
    let len = pglz::pglz_compress_into(source, dest_uninit, &pglz::PGLZ_STRATEGY_DEFAULT)?;
    (len + extra_bytes < orig_len).then_some(len)
}

/// The whole C protocol in one call: XLogBeginInsert + XLogRegisterData(main
/// fragments) + per-block XLogRegisterBuffer/XLogRegisterBlock +
/// XLogRegisterBufData + XLogSetRecordFlags(record_flags) + XLogInsert.
pub fn insert_record(
    rmid: u8,
    info: u8,
    record_flags: u8,
    main_data: &[&[u8]],
    blocks: &[RegBlock<'_>],
) -> PgResult<XLogRecPtr> {
    if info & !(XLR_RMGR_INFO_MASK | XLR_SPECIAL_REL_UPDATE | XLR_CHECK_CONSISTENCY) != 0 {
        ereport(PANIC)
            .errmsg(format!("invalid xlog info mask {info:02X}"))
            .finish(loc("XLogInsert"))?;
    }
    if !transam_xlog::XLogInsertAllowed() {
        return Err(Box::new(PgError::new(
            ERROR,
            "cannot make new WAL entries during recovery".to_string(),
        )));
    }
    if miscinit::IsBootstrapProcessingMode() && rmid != RM_XLOG_ID {
        return Ok(SizeOfXLogLongPHD as XLogRecPtr);
    }

    SCRATCH.with(|cell| {
        let mut guard = cell.borrow_mut();
        if guard.is_none() {
            drop(guard);
            InitXLogInsert()?;
            guard = cell.borrow_mut();
        }
        let scratch = guard.as_mut().unwrap();

        loop {
            let (redo_rec_ptr, do_page_writes) = transam_xlog::GetFullPageWriteInfo();
            let asm = assemble(
                scratch,
                rmid,
                info,
                redo_rec_ptr,
                do_page_writes,
                record_flags,
                main_data,
                blocks,
            );
            let asm = match asm {
                Ok(a) => a,
                Err(e) => {
                    scratch.rdatas.clear();
                    return Err(e);
                }
            };

            let (hdr24, rest) = scratch.hdr.split_at_mut(SizeOfXLogRecord);
            let hdr24: &mut [u8; 24] = hdr24.try_into().unwrap();
            scratch.rdatas[0] = erased(&rest[..asm.hdr_len - SizeOfXLogRecord]);

            let end_pos = transam_xlog::XLogInsertRecord(
                hdr24,
                &scratch.rdatas,
                asm.fpw_lsn,
                record_flags,
                asm.num_fpi,
                asm.topxid_included,
            );
            scratch.rdatas.clear();
            let end_pos = end_pos?;
            if end_pos != InvalidXLogRecPtr {
                return Ok(end_pos);
            }
        }
    })
}

fn xlog_insert_record_seam(
    rmid: u8,
    info: u8,
    record_flags: u8,
    main_data: &[&[u8]],
    bufs: &[XLogRegBuf<'_>],
) -> PgResult<XLogRecPtr> {
    let n = bufs.len();
    if n > XLR_MAX_BLOCK_ID + 1 {
        return Err(Box::new(PgError::new(
            ERROR,
            "too many registered buffers".to_string(),
        )));
    }
    // Fixed stack array, only the live prefix written: the insert path
    // allocates nothing.
    let mut blocks: [core::mem::MaybeUninit<RegBlock<'_>>; XLR_MAX_BLOCK_ID + 1] =
        [const { core::mem::MaybeUninit::uninit() }; XLR_MAX_BLOCK_ID + 1];
    for (slot, b) in blocks.iter_mut().zip(bufs) {
        let tag = bufmgr::BufferGetTag(b.buffer);
        let page = bufmgr::BufferGetPagePtr(b.buffer).as_ptr() as *const u8;
        slot.write(RegBlock {
            block_id: b.block_id,
            rlocator: RelFileLocator::new(tag.spcOid, tag.dbOid, tag.relNumber),
            forknum: tag.forkNum,
            block: tag.blockNum,
            // SAFETY: caller holds the buffer pin+lock for the insert
            // (XLogRegisterBuffer contract); page is a BLCKSZ image.
            page: unsafe { core::slice::from_raw_parts(page, BLCKSZ) },
            flags: b.flags,
            bufdata: b.bufdata,
        });
    }
    // SAFETY: blocks[..n] initialized by the loop above (n <= array len).
    let blocks =
        unsafe { core::slice::from_raw_parts(blocks.as_ptr().cast::<RegBlock<'_>>(), n) };
    insert_record(rmid, info, record_flags, main_data, blocks)
}

fn xlog_insert_seam(rmid: u8, info: u8, fragments: &[&[u8]]) -> PgResult<XLogRecPtr> {
    insert_record(rmid, info, 0, fragments, &[])
}

fn xlog_insert_with_flags_seam(
    rmid: u8,
    info: u8,
    flags: u8,
    fragments: &[&[u8]],
) -> PgResult<XLogRecPtr> {
    insert_record(rmid, info, flags, fragments, &[])
}

// XLogResetInsertion: in the one-call form no partially-registered record can
// outlive its insert call, so abort-time cleanup has nothing to discard.
pub fn XLogResetInsertion() {
    SCRATCH.with(|cell| {
        if let Some(s) = cell.borrow_mut().as_mut() {
            s.rdatas.clear();
        }
    });
}

pub fn XLogCheckBufferNeedsBackup(buffer: Buffer) -> bool {
    let (redo_rec_ptr, do_page_writes) = transam_xlog::GetFullPageWriteInfo();
    do_page_writes && bufmgr::buffer_page_get_lsn(buffer) <= redo_rec_ptr
}

pub fn XLogSaveBufferForHint(buffer: Buffer, buffer_std: bool) -> PgResult<XLogRecPtr> {
    let redo_rec_ptr = transam_xlog::GetRedoRecPtr();
    let lsn = bufmgr::BufferGetLSNAtomic(buffer);
    if lsn > redo_rec_ptr {
        return Ok(InvalidXLogRecPtr);
    }

    let mut copied = [0u8; BLCKSZ];
    let orig = bufmgr::BufferGetPagePtr(buffer).as_ptr() as *const u8;
    // SAFETY: caller holds at least a share lock + pin; BLCKSZ readable.
    let orig = unsafe { core::slice::from_raw_parts(orig, BLCKSZ) };
    let mut flags = 0u8;
    if buffer_std {
        flags |= REGBUF_STANDARD;
        let lower = u16::from_ne_bytes(orig[12..14].try_into().unwrap()) as usize;
        let upper = u16::from_ne_bytes(orig[14..16].try_into().unwrap()) as usize;
        copied[..lower].copy_from_slice(&orig[..lower]);
        copied[upper..].copy_from_slice(&orig[upper..]);
    } else {
        copied.copy_from_slice(orig);
    }

    let tag = bufmgr::BufferGetTag(buffer);
    insert_record(
        RM_XLOG_ID,
        XLOG_FPI_FOR_HINT,
        0,
        &[],
        &[RegBlock {
            block_id: 0,
            rlocator: RelFileLocator::new(tag.spcOid, tag.dbOid, tag.relNumber),
            forknum: tag.forkNum,
            block: tag.blockNum,
            page: &copied,
            flags,
            bufdata: &[],
        }],
    )
}

pub fn log_newpage(
    rlocator: &RelFileLocator,
    forknum: ForkNumber,
    blkno: BlockNumber,
    page: &mut [u8],
    page_std: bool,
) -> PgResult<XLogRecPtr> {
    let mut flags = REGBUF_FORCE_IMAGE;
    if page_std {
        flags |= REGBUF_STANDARD;
    }
    let recptr = insert_record(
        RM_XLOG_ID,
        XLOG_FPI,
        0,
        &[],
        &[RegBlock {
            block_id: 0,
            rlocator: *rlocator,
            forknum,
            block: blkno,
            page,
            flags,
            bufdata: &[],
        }],
    )?;
    if !page_is_new(page) {
        page_set_lsn(page, recptr);
    }
    Ok(recptr)
}

pub fn log_newpages(
    rlocator: &RelFileLocator,
    forknum: ForkNumber,
    blknos: &[BlockNumber],
    pages: &mut [&mut [u8]],
    page_std: bool,
) -> PgResult<()> {
    debug_assert!(blknos.len() == pages.len());
    let mut flags = REGBUF_FORCE_IMAGE;
    if page_std {
        flags |= REGBUF_STANDARD;
    }
    let mut i = 0;
    while i < pages.len() {
        let nbatch = (pages.len() - i).min(XLR_MAX_BLOCK_ID);
        let mut blocks: Vec<RegBlock<'_>> = Vec::with_capacity(nbatch);
        for j in 0..nbatch {
            blocks.push(RegBlock {
                block_id: j as u8,
                rlocator: *rlocator,
                forknum,
                block: blknos[i + j],
                page: &pages[i + j],
                flags,
                bufdata: &[],
            });
        }
        let recptr = insert_record(RM_XLOG_ID, XLOG_FPI, 0, &[], &blocks)?;
        drop(blocks);
        for j in 0..nbatch {
            if !page_is_new(pages[i + j]) {
                page_set_lsn(pages[i + j], recptr);
            }
        }
        i += nbatch;
    }
    Ok(())
}

pub fn log_newpage_buffer(buffer: Buffer, page_std: bool) -> PgResult<XLogRecPtr> {
    debug_assert!(init_small::globals::CritSectionCount() > 0);
    let tag = bufmgr::BufferGetTag(buffer);
    let page = bufmgr::BufferGetPagePtr(buffer).as_ptr();
    // SAFETY: caller holds the exclusive lock + pin (log_newpage_buffer
    // contract); the LSN store below is the same PageSetLSN write.
    let page_ref = unsafe { core::slice::from_raw_parts(page, BLCKSZ) };
    let mut flags = REGBUF_FORCE_IMAGE;
    if page_std {
        flags |= REGBUF_STANDARD;
    }
    let recptr = insert_record(
        RM_XLOG_ID,
        XLOG_FPI,
        0,
        &[],
        &[RegBlock {
            block_id: 0,
            rlocator: RelFileLocator::new(tag.spcOid, tag.dbOid, tag.relNumber),
            forknum: tag.forkNum,
            block: tag.blockNum,
            page: page_ref,
            flags,
            bufdata: &[],
        }],
    )?;
    if !page_is_new(page_ref) {
        bufmgr::buffer_page_set_lsn(buffer, recptr);
    }
    Ok(recptr)
}

pub fn log_newpage_range(
    rel: &::types_rel::RelationData<'_>,
    forknum: ForkNumber,
    startblk: BlockNumber,
    endblk: BlockNumber,
    page_std: bool,
) -> PgResult<()> {
    use ::types_storage::ReadBufferMode;

    let mut flags = REGBUF_FORCE_IMAGE;
    if page_std {
        flags |= REGBUF_STANDARD;
    }

    let mut blkno = startblk;
    while blkno < endblk {
        postgres_seams::check_for_interrupts::call()?;

        let mut bufpack = [0 as Buffer; XLR_MAX_BLOCK_ID];
        let mut pages: [&[u8]; XLR_MAX_BLOCK_ID] = [&[]; XLR_MAX_BLOCK_ID];
        let mut nbufs = 0usize;
        while nbufs < XLR_MAX_BLOCK_ID && blkno < endblk {
            let buf =
                bufmgr::ReadBufferExtended(rel, forknum, blkno, ReadBufferMode::Normal, None)?;
            bufmgr::LockBuffer(buf, bufmgr::BUFFER_LOCK_EXCLUSIVE)?;
            let page = bufmgr::BufferGetPagePtr(buf).as_ptr() as *const u8;
            // SAFETY: exclusive lock + pin held for the batch; BLCKSZ readable.
            let page = unsafe { core::slice::from_raw_parts(page, BLCKSZ) };
            // Empty pages stay un-WAL-logged so their LSN stays zero.
            if !page_is_new(page) {
                bufpack[nbufs] = buf;
                pages[nbufs] = page;
                nbufs += 1;
            } else {
                bufmgr::UnlockReleaseBuffer(buf)?;
            }
            blkno += 1;
        }
        if nbufs == 0 {
            break;
        }

        let mut blocks = [RegBlock {
            block_id: 0,
            rlocator: RelFileLocator::new(0, 0, 0),
            forknum,
            block: 0,
            page: &[],
            flags,
            bufdata: &[],
        }; XLR_MAX_BLOCK_ID];
        for i in 0..nbufs {
            let tag = bufmgr::BufferGetTag(bufpack[i]);
            blocks[i] = RegBlock {
                block_id: i as u8,
                rlocator: RelFileLocator::new(tag.spcOid, tag.dbOid, tag.relNumber),
                forknum: tag.forkNum,
                block: tag.blockNum,
                page: pages[i],
                flags,
                bufdata: &[],
            };
        }

        init_small::globals::StartCriticalSection();
        for buf in &bufpack[..nbufs] {
            bufmgr::MarkBufferDirty(*buf)?;
        }
        let recptr = insert_record(RM_XLOG_ID, XLOG_FPI, 0, &[], &blocks[..nbufs])?;
        for buf in &bufpack[..nbufs] {
            bufmgr::buffer_page_set_lsn(*buf, recptr);
            bufmgr::UnlockReleaseBuffer(*buf)?;
        }
        init_small::globals::EndCriticalSection();
    }
    Ok(())
}

pub fn init_seams() {
    use xloginsert_seams as s;
    s::xlog_insert::set(xlog_insert_seam);
    s::xlog_insert_with_flags::set(xlog_insert_with_flags_seam);
    s::xlog_insert_record::set(xlog_insert_record_seam);
    s::xlog_reset_insertion::set(XLogResetInsertion);
    s::init_xlog_insert::set(InitXLogInsert);
    s::log_newpage_buffer::set(log_newpage_buffer);
    s::xlog_save_buffer_for_hint::set(XLogSaveBufferForHint);
    s::xlog_check_buffer_needs_backup::set(XLogCheckBufferNeedsBackup);
}
