//! `backend-access-transam-xlogreader` — `access/transam/xlogreader.c`, the
//! generic WAL record decoder.
//!
//! This is the self-contained machinery that, given a starting LSN and a
//! page-read callback, reads WAL pages (`ReadPageInternal`), validates page and
//! record headers (`XLogReaderValidatePageHeader` / `ValidXLogRecordHeader` /
//! `ValidXLogRecord` CRC), reassembles records that span page/segment
//! boundaries (`XLogDecodeNextRecord`), decodes the block references and main
//! data (`DecodeXLogRecord`), and exposes the decoded record through the
//! `XLogRecGetXXX` accessors and `RestoreBlockImage`.
//!
//! ## Memory model — arena + queue, *not* the src-idiomatic owned-Vec model
//!
//! The C decoder pallocs decoded records (and the circular `decode_buffer`) in
//! the reader's `MemoryContext` and casts `DecodedXLogRecord *` at offsets
//! inside that `char *` buffer, copying the record's payload bytes into the
//! same allocation. The decoded `main_data` / per-block `data` / `bkp_image`
//! are then `char *` into that buffer, *outliving* any single decode call.
//!
//! We preserve that borrowed-payload contract exactly: every decoded byte
//! payload is copied into the reader's external [`decode_arena`] (the C
//! `MemoryContext`), producing a `&'mcx [u8]` that borrows the arena — **not**
//! the reader struct — so the decode queue is not self-referential and the ~61
//! consumers that read `&'mcx [u8]` payloads keep working. The decode queue is
//! a FIFO of [`DecodedXLogRecord`]`<'mcx>` allocated in the arena (the C
//! `decode_queue_head..decode_queue_tail` linked list), with
//! `decode_queue_head` as the read cursor. The circular-decode-buffer
//! *occupancy accounting* (`decode_buffer_head`/`_tail`/`_size`, oversized
//! records) is reproduced faithfully on offsets so the
//! `XLREAD_WOULDBLOCK`-when-full / `allow_oversized` contract matches C
//! byte-for-byte; the actual decoded bytes live in the arena.
//!
//! [`decode_arena`]: ::wal::rmgr::XLogReaderState::decode_arena
//!
//! Block-image decompression uses the repo's `common-pglz`; LZ4/ZSTD are
//! compiled out exactly as the C `#ifdef USE_LZ4 / USE_ZSTD` defaults.
//! CRC-32C is the `port-crc32c` primitive. The page-read callback is the
//! reader's `routine.page_read` (the substrate is `xlogutils`'
//! `read_local_xlog_page`).

#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::needless_range_loop)]
#![allow(clippy::result_large_err)]
#![allow(clippy::manual_is_multiple_of)]
#![allow(clippy::nonminimal_bool)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::collapsible_else_if)]

extern crate alloc;
// The handle-based logical-decoding subset keeps a backend-local registry of
// live readers (`std::thread_local!`), mirroring the C per-backend reader. std
// is already in the dependency graph (the seam runtime uses it); pull it in
// explicitly for that one registry.
extern crate std;

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::ptr::NonNull;

use mcx::{Mcx, PgString, PgVec};

use ::types_core::primitive::{
    uint16, uint32, uint8, BlockNumber, Buffer, ForkNumber, RelFileNumber, RepOriginId, RmgrId,
    TimeLineID, XLogRecPtr, XLogSegNo,
};
use types_core::{
    InvalidRepOriginId, InvalidTransactionId, InvalidXLogRecPtr, TransactionId,
};
use ::types_error::PgResult;
use ::wal::rmgr::{XLogReaderState, RmgrIdIsValid, XLREAD_FAIL, XLREAD_SUCCESS, XLREAD_WOULDBLOCK};
use ::wal::wal::{DecodedBkpBlock, DecodedXLogRecord, RelFileLocator, XLogRecord};
use ::wal::xlog_consts::{SIZE_OF_XLOG_LONG_PHD, SIZE_OF_XLOG_SHORT_PHD, XLOG_BLCKSZ};

pub mod handle;
pub mod seams;
pub mod summarizer;

/// Install every inward seam this unit owns (the crate-root entry point
/// `seams-init`'s `init_all()` calls): the value-typed `XLogReaderState` seams
/// (`seams`) and the handle-based logical-decoding seams (`handle`).
pub fn init_seams() {
    seams::init_seams();
    handle::init_seams();
    summarizer::init_seams();
}

#[cfg(test)]
mod tests;

// ---------------------------------------------------------------------------
// Local constants (access/xlogreader.h, access/xlogrecord.h, access/xlog_internal.h,
// access/pg_lzcompress.h). The xlogrecord-level wire constants do not yet live
// in a shared types crate; reproduced here exactly as the C headers expand.
// ---------------------------------------------------------------------------

/// `MAX_ERRORMSG_LEN` (xlogreader.c:59).
const MAX_ERRORMSG_LEN: usize = 1000;
/// `DEFAULT_DECODE_BUFFER_SIZE` (xlogreader.c:65).
const DEFAULT_DECODE_BUFFER_SIZE: usize = 64 * 1024;
/// `MAXIMUM_ALIGNOF` — maximum scalar alignment on supported platforms.
const MAXIMUM_ALIGNOF: usize = 8;
/// `BLCKSZ` — the relation block size; equal to `XLOG_BLCKSZ` here.
const BLCKSZ: usize = 8192;

/// `XLR_INFO_MASK` (access/xlogrecord.h).
const XLR_INFO_MASK: uint8 = 0x0F;
/// `XLR_MAX_BLOCK_ID` (access/xlogrecord.h).
const XLR_MAX_BLOCK_ID: i32 = 32;
/// `XLR_BLOCK_ID_DATA_SHORT` (access/xlogrecord.h).
const XLR_BLOCK_ID_DATA_SHORT: uint8 = 255;
/// `XLR_BLOCK_ID_DATA_LONG` (access/xlogrecord.h).
const XLR_BLOCK_ID_DATA_LONG: uint8 = 254;
/// `XLR_BLOCK_ID_ORIGIN` (access/xlogrecord.h).
const XLR_BLOCK_ID_ORIGIN: uint8 = 253;
/// `XLR_BLOCK_ID_TOPLEVEL_XID` (access/xlogrecord.h).
const XLR_BLOCK_ID_TOPLEVEL_XID: uint8 = 252;

/// `BKPBLOCK_FORK_MASK` (access/xlogrecord.h).
const BKPBLOCK_FORK_MASK: uint8 = 0x0F;
/// `BKPBLOCK_HAS_IMAGE` (access/xlogrecord.h).
const BKPBLOCK_HAS_IMAGE: uint8 = 0x10;
/// `BKPBLOCK_HAS_DATA` (access/xlogrecord.h).
const BKPBLOCK_HAS_DATA: uint8 = 0x20;
/// `BKPBLOCK_SAME_REL` (access/xlogrecord.h).
const BKPBLOCK_SAME_REL: uint8 = 0x80;

/// `BKPIMAGE_HAS_HOLE` (access/xlogrecord.h).
const BKPIMAGE_HAS_HOLE: uint8 = 0x01;
/// `BKPIMAGE_APPLY` (access/xlogrecord.h) — page should be restored during replay.
const BKPIMAGE_APPLY: uint8 = 0x02;
/// `BKPIMAGE_COMPRESS_PGLZ` (access/xlogrecord.h).
const BKPIMAGE_COMPRESS_PGLZ: uint8 = 0x04;
/// `BKPIMAGE_COMPRESS_LZ4` (access/xlogrecord.h).
const BKPIMAGE_COMPRESS_LZ4: uint8 = 0x08;
/// `BKPIMAGE_COMPRESS_ZSTD` (access/xlogrecord.h).
const BKPIMAGE_COMPRESS_ZSTD: uint8 = 0x10;

/// `BKPIMAGE_COMPRESSED(info)` (access/xlogrecord.h).
const fn BKPIMAGE_COMPRESSED(info: uint8) -> bool {
    (info & (BKPIMAGE_COMPRESS_PGLZ | BKPIMAGE_COMPRESS_LZ4 | BKPIMAGE_COMPRESS_ZSTD)) != 0
}

/// `XLP_FIRST_IS_CONTRECORD` (access/xlog_internal.h).
const XLP_FIRST_IS_CONTRECORD: uint16 = 0x0001;
/// `XLP_LONG_HEADER` (access/xlog_internal.h).
const XLP_LONG_HEADER: uint16 = 0x0002;
// `XLP_BKP_REMOVABLE` (access/xlog_internal.h) = 0x0004 — set on most pages and
// benign to the reader; no named constant, but the bit must NOT be conflated
// with XLP_FIRST_IS_OVERWRITE_CONTRECORD (0x0008), which was the page-crossing
// recovery bug.
/// `XLP_FIRST_IS_OVERWRITE_CONTRECORD` (access/xlog_internal.h).
const XLP_FIRST_IS_OVERWRITE_CONTRECORD: uint16 = 0x0008;
/// `XLP_ALL_FLAGS` (access/xlog_internal.h).
const XLP_ALL_FLAGS: uint16 = 0x000F;
/// `XLOG_PAGE_MAGIC` (access/xlog_internal.h, PG 18).
const XLOG_PAGE_MAGIC: uint16 = 0xD118;

/// `XLOG_SWITCH` (access/xlog.h) — rmgr opcode for an xlog switch record.
const XLOG_SWITCH: uint8 = 0x40;
/// `RM_XLOG_ID` (access/rmgrlist.h entry 0).
const RM_XLOG_ID: RmgrId = 0;

/// `SizeOfXLogRecord` — `MAXALIGN(sizeof(XLogRecord))`. `XLogRecord` is
/// xl_tot_len(4) xl_xid(4) xl_prev(8) xl_info(1) xl_rmid(1) pad(2) xl_crc(4) =
/// 24 bytes, already 8-aligned.
const SIZE_OF_XLOG_RECORD: usize = 24;
/// `offsetof(XLogRecord, xl_crc)` — header bytes the CRC covers last.
const OFFSETOF_XLOG_RECORD_XL_CRC: usize = 20;
/// `sizeof(RelFileLocator)` on the WAL wire — spcOid + dbOid + relNumber, three
/// 4-byte fields, unpadded.
const SIZEOF_REL_FILE_LOCATOR: usize = 12;

/// `MAXALIGN(len)` (c.h).
const fn MAXALIGN(len: usize) -> usize {
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// `LSN_FORMAT_ARGS(lsn)` — `(high32, low32)` for the `%X/%X` format.
pub(crate) fn lsn_fmt(lsn: XLogRecPtr) -> (u32, u32) {
    ((lsn >> 32) as u32, lsn as u32)
}

// ---------------------------------------------------------------------------
// Segment-arithmetic helpers (access/xlog_internal.h macros).
// ---------------------------------------------------------------------------

/// `XLByteToSeg(xlrp, logSegNo, wal_segsz_bytes)`.
pub(crate) fn XLByteToSeg(xlrp: XLogRecPtr, wal_segsz_bytes: i32) -> XLogSegNo {
    xlrp / wal_segsz_bytes as u64
}

/// `XLogSegmentOffset(xlogptr, wal_segsz_bytes)`.
pub(crate) fn XLogSegmentOffset(xlogptr: XLogRecPtr, wal_segsz_bytes: i32) -> u32 {
    (xlogptr & (wal_segsz_bytes as u64 - 1)) as u32
}

/// `XLogRecPtrIsInvalid(r)`.
fn XLogRecPtrIsInvalid(r: XLogRecPtr) -> bool {
    r == InvalidXLogRecPtr
}

/// `XRecOffIsValid(xlrp)` (access/xlog_internal.h).
fn XRecOffIsValid(xlrp: XLogRecPtr) -> bool {
    let offset = (xlrp % XLOG_BLCKSZ as u64) as usize;
    offset >= SIZE_OF_XLOG_SHORT_PHD
        && (offset <= XLOG_BLCKSZ - SIZE_OF_XLOG_RECORD || offset >= SIZE_OF_XLOG_LONG_PHD)
}

/// `XLogFileName(fname, tli, logSegNo, wal_segsz_bytes)` (access/xlog_internal.h).
pub(crate) fn XLogFileName(tli: TimeLineID, log_seg_no: XLogSegNo, wal_segsz_bytes: i32) -> String {
    let segs_per_id: u64 = 0x1_0000_0000u64 / wal_segsz_bytes as u64;
    let hi = log_seg_no / segs_per_id;
    let lo = log_seg_no % segs_per_id;
    format!("{:08X}{:08X}{:08X}", tli, hi, lo)
}

/// `imin` — `Min` macro on `int`.
fn imin(a: i32, b: i32) -> i32 {
    if a < b { a } else { b }
}
/// `imax` — `Max` macro on `int`.
fn imax(a: i32, b: i32) -> i32 {
    if a > b { a } else { b }
}

// ---------------------------------------------------------------------------
// CRC32C (port/pg_crc32c.h). INIT/FIN/EQ trivial; COMP is the port primitive.
// ---------------------------------------------------------------------------

const fn INIT_CRC32C() -> u32 {
    0xFFFF_FFFF
}
fn COMP_CRC32C(crc: u32, data: &[u8]) -> u32 {
    crc32c::pg_comp_crc32c_sb8(crc, data)
}
const fn FIN_CRC32C(crc: u32) -> u32 {
    crc ^ 0xFFFF_FFFF
}
const fn EQ_CRC32C(c1: u32, c2: u32) -> bool {
    c1 == c2
}

// ---------------------------------------------------------------------------
// Wire-format header parsing (native order == the C struct memcpy; WAL is not
// endianness-portable).
// ---------------------------------------------------------------------------

/// `XLogPageHeaderSize(hdr)` — short/long header size from `xlp_info`.
fn XLogPageHeaderSize(info: uint16) -> usize {
    if (info & XLP_LONG_HEADER) != 0 {
        SIZE_OF_XLOG_LONG_PHD
    } else {
        SIZE_OF_XLOG_SHORT_PHD
    }
}

/// In-memory view of a parsed `XLogPageHeaderData` / `XLogLongPageHeaderData`.
#[derive(Clone, Copy, Debug, Default)]
struct PageHeaderView {
    xlp_magic: uint16,
    xlp_info: uint16,
    xlp_tli: TimeLineID,
    xlp_pageaddr: XLogRecPtr,
    xlp_rem_len: uint32,
    xlp_sysid: u64,
    xlp_seg_size: uint32,
    xlp_xlog_blcksz: uint32,
}

/// Parse the short page header from the start of `buf` (>= SizeOfXLogShortPHD).
/// Offsets: xlp_magic(2) xlp_info(2) xlp_tli(4) xlp_pageaddr(8) xlp_rem_len(4).
fn parse_short_page_header(buf: &[u8]) -> PageHeaderView {
    PageHeaderView {
        xlp_magic: u16::from_ne_bytes([buf[0], buf[1]]),
        xlp_info: u16::from_ne_bytes([buf[2], buf[3]]),
        xlp_tli: u32::from_ne_bytes([buf[4], buf[5], buf[6], buf[7]]),
        xlp_pageaddr: u64::from_ne_bytes([
            buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
        ]),
        xlp_rem_len: u32::from_ne_bytes([buf[16], buf[17], buf[18], buf[19]]),
        ..Default::default()
    }
}

/// Parse the long-header extension fields (XLogLongPageHeaderData: sysid@24,
/// seg_size@32, xlog_blcksz@36).
fn parse_long_page_header(buf: &[u8]) -> PageHeaderView {
    let mut v = parse_short_page_header(buf);
    v.xlp_sysid = u64::from_ne_bytes([
        buf[24], buf[25], buf[26], buf[27], buf[28], buf[29], buf[30], buf[31],
    ]);
    v.xlp_seg_size = u32::from_ne_bytes([buf[32], buf[33], buf[34], buf[35]]);
    v.xlp_xlog_blcksz = u32::from_ne_bytes([buf[36], buf[37], buf[38], buf[39]]);
    v
}

/// Parse the page header at `buf`, including long fields when the flag is set.
fn parse_page_header(buf: &[u8]) -> PageHeaderView {
    let short = parse_short_page_header(buf);
    if (short.xlp_info & XLP_LONG_HEADER) != 0 && buf.len() >= SIZE_OF_XLOG_LONG_PHD {
        parse_long_page_header(buf)
    } else {
        short
    }
}

/// Parse the fixed `XLogRecord` header from the start of `buf`.
fn parse_xlog_record(buf: &[u8]) -> XLogRecord {
    XLogRecord::new(
        u32::from_ne_bytes([buf[0], buf[1], buf[2], buf[3]]), // xl_tot_len
        u32::from_ne_bytes([buf[4], buf[5], buf[6], buf[7]]), // xl_xid
        u64::from_ne_bytes([
            buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
        ]), // xl_prev
        buf[16],                                              // xl_info
        buf[17],                                              // xl_rmid
        // bytes 18,19 are padding
        u32::from_ne_bytes([buf[20], buf[21], buf[22], buf[23]]), // xl_crc
    )
}

/// Read only `xl_tot_len` (first field; always on the first page).
fn read_xl_tot_len(buf: &[u8]) -> uint32 {
    u32::from_ne_bytes([buf[0], buf[1], buf[2], buf[3]])
}

/// Parse a `RelFileLocator` from `b` (spcOid, dbOid, relNumber; each 4 bytes).
fn parse_rel_file_locator(b: &[u8]) -> RelFileLocator {
    let spc = u32::from_ne_bytes([b[0], b[1], b[2], b[3]]);
    let db = u32::from_ne_bytes([b[4], b[5], b[6], b[7]]);
    let rel = u32::from_ne_bytes([b[8], b[9], b[10], b[11]]);
    RelFileLocator::new(spc, db, rel as RelFileNumber)
}

// ---------------------------------------------------------------------------
// Arena allocation: copy bytes into the reader's `decode_arena` and hand back a
// `&'mcx [u8]` that borrows the arena (the bump/malloc block lives for `'mcx`,
// the arena's lifetime), reproducing the C `char *` payload into the reader
// context.
// ---------------------------------------------------------------------------

/// Copy `src` into `arena`, returning a `&'mcx [u8]` borrowing the arena.
///
/// SAFETY: `Mcx::allocate` returns a block valid until the context is
/// reset/dropped; the `Mcx<'mcx>` handle borrows the context for `'mcx`, so the
/// returned slice's `'mcx` lifetime cannot outlive the backing memory. The
/// bytes are initialized by `copy_from_slice` before the slice is formed.
fn arena_copy<'mcx>(arena: Mcx<'mcx>, src: &[u8]) -> PgResult<&'mcx [u8]> {
    use core::alloc::Layout;
    use ::mcx::Allocator;
    if src.is_empty() {
        return Ok(&[]);
    }
    ::mcx::check_alloc_size(src.len())?;
    let layout = Layout::from_size_align(src.len(), 1).map_err(|_| arena.oom(src.len()))?;
    let ptr: NonNull<[u8]> = arena.allocate(layout).map_err(|_| arena.oom(src.len()))?;
    // SAFETY: `ptr` points to `src.len()` freshly-allocated bytes in the arena.
    let dst: &'mcx mut [u8] =
        unsafe { core::slice::from_raw_parts_mut(ptr.as_ptr() as *mut u8, src.len()) };
    dst.copy_from_slice(src);
    Ok(&*dst)
}

// ---------------------------------------------------------------------------
// Error reporting (xlogreader.c report_invalid_record).
// ---------------------------------------------------------------------------

/// `report_invalid_record(state, fmt, ...)` (xlogreader.c:71). The variadic
/// `fmt` is replaced by an already-formatted Rust message; the gettext `_()`
/// wrapper is a project-wide deferral, so the message is stored verbatim,
/// truncated at `MAX_ERRORMSG_LEN` as `vsnprintf` would.
fn report_invalid_record(state: &mut XLogReaderState<'_>, mut msg: String) {
    if msg.len() > MAX_ERRORMSG_LEN {
        msg.truncate(MAX_ERRORMSG_LEN);
    }
    // A new decode error supersedes any prior RestoreBlockImage message.
    state.restore_errmsg.get_mut().clear();
    match state.decode_arena {
        Some(arena) => {
            // from_str_in copies into the arena (try_reserve internally); on OOM
            // leave the buffer empty (the C path uses a fixed buffer).
            state.errormsg_buf = PgString::from_str_in(&msg, arena).ok();
        }
        None => {
            // No arena wired yet (pre-allocate): drop the text but keep the flag.
            state.errormsg_buf = None;
        }
    }
    state.errormsg_deferred = true;
}

// ===========================================================================
// Allocation / lifecycle (XLogReaderAllocate / Free / SetDecodeBuffer).
// ===========================================================================

/// `XLogReaderSetDecodeBuffer` (xlogreader.c:90). Set the decode-buffer size
/// (the bytes themselves are accounted on offsets; the decoded payload lives in
/// the arena).
pub fn XLogReaderSetDecodeBuffer(state: &mut XLogReaderState<'_>, size: usize) {
    debug_assert!(state.decode_buffer.is_none());
    state.decode_buffer_size = size;
    state.decode_buffer_tail = 0;
    state.decode_buffer_head = 0;
}

/// `allocate_recordbuf` (xlogreader.c:190). Allocate `readRecordBuf` to fit a
/// record of at least `reclength`, rounded to a `XLOG_BLCKSZ` multiple and at
/// least `5*Max(BLCKSZ, XLOG_BLCKSZ)`.
pub fn allocate_recordbuf<'mcx>(state: &mut XLogReaderState<'mcx>, reclength: uint32) -> PgResult<()> {
    let arena = state.decode_arena.expect("reader has no allocator context");
    let mut new_size: uint32 = reclength;
    new_size = new_size.wrapping_add(XLOG_BLCKSZ as u32 - (new_size % XLOG_BLCKSZ as u32));
    new_size = new_size.max(5 * (BLCKSZ as u32).max(XLOG_BLCKSZ as u32));

    let mut buf: PgVec<'mcx, u8> = ::mcx::vec_with_capacity_in(arena, new_size as usize)?;
    buf.resize(new_size as usize, 0);
    state.readRecordBuf = Some(buf);
    state.readRecordBufSize = new_size;
    Ok(())
}

// ===========================================================================
// Positioning (XLogBeginRead / ResetDecoder).
// ===========================================================================

/// `XLogBeginRead` (xlogreader.c:231). Position the reader at `rec_ptr`,
/// forgetting any queued records. Infallible.
pub fn XLogBeginRead(state: &mut XLogReaderState<'_>, rec_ptr: XLogRecPtr) {
    debug_assert!(!XLogRecPtrIsInvalid(rec_ptr));
    ResetDecoder(state);
    state.EndRecPtr = rec_ptr;
    state.NextRecPtr = rec_ptr;
    state.ReadRecPtr = InvalidXLogRecPtr;
    state.DecodeRecPtr = InvalidXLogRecPtr;
}

/// `ResetDecoder` (xlogreader.c:1614). Forget all decoded records and reset the
/// decode buffer/error state.
fn ResetDecoder(state: &mut XLogReaderState<'_>) {
    // Reset the decoded-record queue (the arena reclaims the records on reset).
    if let Some(q) = state.decode_queue.as_mut() {
        q.clear();
    }
    state.decode_queue_head = 0;
    state.record = None;

    // Reset the decode buffer to empty.
    state.decode_buffer_tail = 0;
    state.decode_buffer_head = 0;

    // Clear error state.
    state.errormsg_buf = None;
    state.errormsg_deferred = false;
}

// ===========================================================================
// Queue consumption (XLogReleasePreviousRecord / XLogNextRecord /
// XLogReadRecord / XLogReaderHasQueuedRecordOrError / XLogReadAhead).
// ===========================================================================

/// `XLogReaderHasQueuedRecordOrError` (xlogreader.h inline).
///
/// The decode queue holds the records decoded ahead but not yet consumed (the
/// head is always index 0; `decode_queue_head` stays 0 for C struct parity).
pub fn XLogReaderHasQueuedRecordOrError(state: &XLogReaderState<'_>) -> bool {
    let has_queued = state.decode_queue.as_ref().is_some_and(|q| !q.is_empty());
    has_queued || state.errormsg_deferred
}

/// The oldest still-queued record (the C `decode_queue_head`), if any.
fn queue_head<'a, 'mcx>(state: &'a XLogReaderState<'mcx>) -> Option<&'a DecodedXLogRecord<'mcx>> {
    state.decode_queue.as_ref()?.first()
}

/// The newest still-queued record (the C `decode_queue_tail`), if any.
fn queue_tail<'a, 'mcx>(state: &'a XLogReaderState<'mcx>) -> Option<&'a DecodedXLogRecord<'mcx>> {
    state.decode_queue.as_ref()?.last()
}

/// `XLogReleasePreviousRecord` (xlogreader.c:249). Release the last record
/// returned by `XLogNextRecord`; returns the LSN past its end (or
/// `InvalidXLogRecPtr` when there was none). Infallible.
pub fn XLogReleasePreviousRecord(state: &mut XLogReaderState<'_>) -> XLogRecPtr {
    let rec = match state.record.take() {
        Some(r) => r,
        None => return InvalidXLogRecPtr,
    };

    // It must be the oldest item decoded (decode_queue_head). The record was
    // already moved out of the queue by XLogNextRecord; dropping `rec` here is
    // the C `pfree` for an oversized record / decode-buffer reclaim.
    let next_lsn = rec.next_lsn();
    let oversized = rec.oversized();

    // Release decode-buffer space (offset accounting only; the C ring math).
    if oversized {
        // Not in the decode buffer; nothing to release (dropping `rec` is the
        // C `pfree(record)`).
    } else {
        // Advance head to the next non-oversized still-queued record, else
        // reset both cursors (the queue is empty).
        let next_off = next_non_oversized_offset(state);
        match next_off {
            Some(off) => state.decode_buffer_head = off,
            None => {
                state.decode_buffer_head = 0;
                state.decode_buffer_tail = 0;
            }
        }
    }

    next_lsn
}

/// The decode-buffer offset of the next non-oversized still-queued record (the
/// C "skip oversized ones" loop in `XLogReleasePreviousRecord`).
fn next_non_oversized_offset(state: &XLogReaderState<'_>) -> Option<usize> {
    let q = state.decode_queue.as_ref()?;
    for r in q.iter() {
        if !r.oversized() {
            return Some(r.buffer_offset());
        }
    }
    None
}

/// `XLogNextRecord(reader, &errmsg)` (xlogreader.c:325). Consume the next
/// record off the decode queue (it becomes `reader->record`); returns its start
/// LSN, or `None` (with a deferred error readable via `xlog_reader_deferred_errmsg`).
pub fn XLogNextRecord(state: &mut XLogReaderState<'_>) -> Option<XLogRecPtr> {
    // Release the last record returned by XLogNextRecord().
    XLogReleasePreviousRecord(state);

    if queue_head(state).is_none() {
        if state.errormsg_deferred {
            // The deferred error (if any) is readable through errormsg_buf;
            // clear the deferred flag as C does after exposing *errmsg.
            state.errormsg_deferred = false;
        }
        debug_assert!(!XLogRecPtrIsInvalid(state.EndRecPtr));
        return None;
    }

    // Move the head out of the queue into `reader->record` (C aliases the
    // pointer; we own the value). Expose its LSN/EndRecPtr for the historical
    // XLogRecXXX(xlogreader) accessors.
    let head = state.decode_queue.as_mut().unwrap().remove(0);
    let lsn = head.lsn();
    let next_lsn = head.next_lsn();
    state.record = Some(head);
    state.ReadRecPtr = lsn;
    state.EndRecPtr = next_lsn;
    Some(lsn)
}

/// The reader's deferred error message (`reader->errormsg_deferred ?
/// errormsg_buf : NULL`).
pub fn xlog_reader_deferred_errmsg(state: &XLogReaderState<'_>) -> Option<String> {
    state
        .errormsg_buf
        .as_ref()
        .map(|s| String::from(s.as_str()))
        .filter(|s| !s.is_empty())
}

/// `XLogReadAhead` (xlogreader.c:976). Try to decode one more record ahead;
/// returns the newly decoded record (now the decode-queue tail), or `None`
/// (error deferred or no data in nonblocking mode). `Err` is an
/// `ereport(ERROR)` from the page-read callback.
pub fn XLogReadAhead<'mcx>(
    state: &mut XLogReaderState<'mcx>,
    nonblocking: bool,
) -> PgResult<Option<()>> {
    if state.errormsg_deferred {
        return Ok(None);
    }
    let result = XLogDecodeNextRecord(state, nonblocking)?;
    if result == XLREAD_SUCCESS {
        debug_assert!(queue_tail(state).is_some());
        return Ok(Some(()));
    }
    Ok(None)
}

// ===========================================================================
// XLogReadRecordAlloc — circular-decode-buffer occupancy accounting.
// ===========================================================================

/// Result of reserving a decode-buffer slot: the placement offset (and whether
/// the record had to be oversized).
struct AllocSlot {
    /// The decode-buffer offset where a non-oversized record is placed.
    offset: usize,
    oversized: bool,
}

/// `XLogReadRecordAlloc` (xlogreader.c:438). Reserve space for a record of
/// `xl_tot_len`, reproducing the circular decode-buffer occupancy logic.
/// Returns the placement (offset + oversized flag) or `None` when there is no
/// space and `allow_oversized` is false.
fn XLogReadRecordAlloc(
    state: &mut XLogReaderState<'_>,
    xl_tot_len: usize,
    allow_oversized: bool,
) -> Option<AllocSlot> {
    let required_space = DecodeXLogRecordRequiredSpace(xl_tot_len);

    // Allocate a circular decode buffer if we don't have one already (here:
    // mark it present and seed its size; the bytes are offset-accounted).
    if state.decode_buffer.is_none() {
        if state.decode_buffer_size == 0 {
            state.decode_buffer_size = DEFAULT_DECODE_BUFFER_SIZE;
        }
        // Mark the buffer as present with a zero-length placeholder Vec (the
        // real decoded bytes live in the arena; this just records presence and
        // ownership for the C `free_decode_buffer` accounting).
        if let Some(arena) = state.decode_arena {
            state.decode_buffer = Some(PgVec::new_in(arena));
        }
        state.decode_buffer_head = 0;
        state.decode_buffer_tail = 0;
        state.free_decode_buffer = true;
    }

    // Try to allocate space in the circular decode buffer (C pointer math
    // expressed on offsets, where decode_buffer base == 0).
    if state.decode_buffer_tail >= state.decode_buffer_head {
        // Empty, or tail is to the right of head.
        if required_space <= state.decode_buffer_size - state.decode_buffer_tail {
            // Space between tail and end.
            return Some(AllocSlot {
                offset: state.decode_buffer_tail,
                oversized: false,
            });
        } else if required_space < state.decode_buffer_head {
            // Space between start and head.
            return Some(AllocSlot {
                offset: 0,
                oversized: false,
            });
        }
    } else {
        // Tail is to the left of head.
        if required_space < state.decode_buffer_head - state.decode_buffer_tail {
            // Space between tail and head.
            return Some(AllocSlot {
                offset: state.decode_buffer_tail,
                oversized: false,
            });
        }
    }

    // Not enough space. Are we allowed to allocate an oversized record?
    if allow_oversized {
        return Some(AllocSlot {
            offset: 0,
            oversized: true,
        });
    }

    None
}

/// `DecodeXLogRecordRequiredSpace` (xlogreader.c:1648). The maximum footprint
/// to decode a record given `xl_tot_len`, used to drive the ring accounting.
pub fn DecodeXLogRecordRequiredSpace(xl_tot_len: usize) -> usize {
    let mut size: usize = 0;
    // Fixed-size part of the decoded record struct (offsetof(.., blocks[0])).
    size += SIZEOF_DECODED_XLOG_RECORD_FIXED;
    // Flexible blocks array of maximum possible size.
    size += SIZEOF_DECODED_BKP_BLOCK * (XLR_MAX_BLOCK_ID as usize + 1);
    // All the raw main and block data.
    size += xl_tot_len;
    // Padding before main_data.
    size += MAXIMUM_ALIGNOF - 1;
    // Padding before each block's data.
    size += (MAXIMUM_ALIGNOF - 1) * (XLR_MAX_BLOCK_ID as usize + 1);
    // Padding at the end.
    size += MAXIMUM_ALIGNOF - 1;
    size
}

// `#[repr(C)]` mirrors of the C `DecodedBkpBlock`/`DecodedXLogRecord` layouts
// (xlogreader.h), used *only* to derive `offsetof(.., blocks[0])` and
// `sizeof(DecodedBkpBlock)` for the ring-buffer footprint math, so the
// oversized/WOULDBLOCK behavior matches C byte-for-byte. The decoded data lives
// in the safe arena structs.
mod layout {
    use super::*;

    #[repr(C)]
    #[allow(dead_code)]
    struct RelFileLocatorLayout {
        spc_oid: ::types_core::primitive::Oid,
        db_oid: ::types_core::primitive::Oid,
        rel_number: RelFileNumber,
    }

    #[repr(C)]
    #[allow(dead_code)]
    pub struct DecodedBkpBlockLayout {
        in_use: bool,
        rlocator: RelFileLocatorLayout,
        forknum: ForkNumber,
        blkno: BlockNumber,
        prefetch_buffer: Buffer,
        flags: u8,
        has_image: bool,
        apply_image: bool,
        bkp_image: *mut u8,
        hole_offset: u16,
        hole_length: u16,
        bimg_len: u16,
        bimg_info: u8,
        has_data: bool,
        data: *mut u8,
        data_len: u16,
        data_bufsz: u16,
    }

    #[repr(C)]
    #[allow(dead_code)]
    pub struct XLogRecordLayout {
        xl_tot_len: u32,
        xl_xid: TransactionId,
        xl_prev: XLogRecPtr,
        xl_info: u8,
        xl_rmid: RmgrId,
        xl_crc: u32,
    }

    #[repr(C)]
    #[allow(dead_code)]
    pub struct DecodedXLogRecordLayout {
        size: usize,
        oversized: bool,
        next: *mut DecodedXLogRecordLayout,
        lsn: XLogRecPtr,
        next_lsn: XLogRecPtr,
        header: XLogRecordLayout,
        record_origin: RepOriginId,
        toplevel_xid: TransactionId,
        main_data: *mut u8,
        main_data_len: u32,
        max_block_id: i32,
        pub blocks: [DecodedBkpBlockLayout; 0],
    }
}

const SIZEOF_DECODED_XLOG_RECORD_FIXED: usize =
    core::mem::offset_of!(layout::DecodedXLogRecordLayout, blocks);
const SIZEOF_DECODED_BKP_BLOCK: usize = core::mem::size_of::<layout::DecodedBkpBlockLayout>();

// ===========================================================================
// XLogDecodeNextRecord — the page/segment-spanning record reader.
// ===========================================================================

/// `XLogDecodeNextRecord` (xlogreader.c:528). Decode the next available record
/// into the queue. Returns `XLREAD_SUCCESS` / `XLREAD_FAIL` / `XLREAD_WOULDBLOCK`;
/// `Err` carries an `ereport(ERROR)` from the page-read callback.
#[allow(unused_assignments)]
fn XLogDecodeNextRecord<'mcx>(
    state: &mut XLogReaderState<'mcx>,
    nonblocking: bool,
) -> PgResult<i32> {
    let mut rand_access = false;

    // Reset error state.
    state.errormsg_buf = None;
    state.abortedRecPtr = InvalidXLogRecPtr;
    state.missingContrecPtr = InvalidXLogRecPtr;

    let mut rec_ptr: XLogRecPtr = state.NextRecPtr;

    if state.DecodeRecPtr != InvalidXLogRecPtr {
        // read the record after the one we just read
    } else {
        // Caller supplied a position to start at.
        debug_assert!(rec_ptr % XLOG_BLCKSZ as u64 == 0 || XRecOffIsValid(rec_ptr));
        rand_access = true;
    }

    let mut target_page_ptr: XLogRecPtr;
    let mut target_rec_off: u32;
    let mut page_header_size: usize;
    let mut assembled: bool;
    let mut got_header: bool;
    let mut read_off: i32;
    let mut total_len: uint32;
    let mut record_hdr: XLogRecord;
    let mut last_cont_rem_len: uint32 = 0;

    'restart: loop {
        state.nonblocking = nonblocking;
        state.currRecPtr = rec_ptr;
        assembled = false;

        target_page_ptr = rec_ptr - (rec_ptr % XLOG_BLCKSZ as u64);
        target_rec_off = (rec_ptr % XLOG_BLCKSZ as u64) as u32;

        // Read the page containing the record into readBuf.
        read_off = ReadPageInternal(
            state,
            target_page_ptr,
            imin(
                target_rec_off as i32 + SIZE_OF_XLOG_RECORD as i32,
                XLOG_BLCKSZ as i32,
            ),
        )?;
        if read_off == XLREAD_WOULDBLOCK {
            return Ok(XLREAD_WOULDBLOCK);
        } else if read_off < 0 {
            return Ok(decode_err(state, assembled, rec_ptr, target_page_ptr));
        }

        // ReadPageInternal always returns at least the page header.
        let phdr = parse_page_header(read_buf(state));
        page_header_size = XLogPageHeaderSize(phdr.xlp_info);
        if target_rec_off == 0 {
            // At page start, skip over page header.
            rec_ptr += page_header_size as u64;
            target_rec_off = page_header_size as u32;
        } else if (target_rec_off as usize) < page_header_size {
            let (h, l) = lsn_fmt(rec_ptr);
            report_invalid_record(
                state,
                format!(
                    "invalid record offset at {:X}/{:X}: expected at least {}, got {}",
                    h, l, page_header_size, target_rec_off
                ),
            );
            return Ok(decode_err(state, assembled, rec_ptr, target_page_ptr));
        }

        if (phdr.xlp_info & XLP_FIRST_IS_CONTRECORD) != 0
            && target_rec_off as usize == page_header_size
        {
            let (h, l) = lsn_fmt(rec_ptr);
            report_invalid_record(state, format!("contrecord is requested by {:X}/{:X}", h, l));
            return Ok(decode_err(state, assembled, rec_ptr, target_page_ptr));
        }

        debug_assert!(page_header_size <= read_off as usize);

        // Read the record length (xl_tot_len is first field, always on-page).
        let rec_off_in_page = (rec_ptr % XLOG_BLCKSZ as u64) as usize;
        total_len = read_xl_tot_len(&read_buf(state)[rec_off_in_page..]);

        if target_rec_off as usize <= XLOG_BLCKSZ - SIZE_OF_XLOG_RECORD {
            record_hdr = parse_xlog_record(&read_buf(state)[rec_off_in_page..]);
            if !ValidXLogRecordHeader(state, rec_ptr, state.DecodeRecPtr, &record_hdr, rand_access) {
                return Ok(decode_err(state, assembled, rec_ptr, target_page_ptr));
            }
            got_header = true;
        } else {
            if (total_len as usize) < SIZE_OF_XLOG_RECORD {
                let (h, l) = lsn_fmt(rec_ptr);
                report_invalid_record(
                    state,
                    format!(
                        "invalid record length at {:X}/{:X}: expected at least {}, got {}",
                        h, l, SIZE_OF_XLOG_RECORD as u32, total_len
                    ),
                );
                return Ok(decode_err(state, assembled, rec_ptr, target_page_ptr));
            }
            got_header = false;
            // record_hdr is parsed below once the whole header is assembled.
            record_hdr = XLogRecord::new(0, 0, 0, 0, 0, 0);
        }

        // Try to find space without an arena allocation.
        let mut slot = XLogReadRecordAlloc(state, total_len as usize, false /* allow_oversized */);
        if slot.is_none() && nonblocking {
            return Ok(XLREAD_WOULDBLOCK);
        }

        let len: u32 = XLOG_BLCKSZ as u32 - (rec_ptr % XLOG_BLCKSZ as u64) as u32;

        // The fully-assembled record bytes (== xl_tot_len bytes), to decode.
        let record_bytes: Vec<u8>;

        if total_len > len {
            // Need to reassemble the record across pages.
            assembled = true;
            debug_assert!(state.readRecordBufSize as usize >= XLOG_BLCKSZ * 2);
            debug_assert!(state.readRecordBufSize >= len);

            // Copy the first fragment from the first page.
            let first_off = (rec_ptr % XLOG_BLCKSZ as u64) as usize;
            {
                let rb = read_buf(state);
                let frag = rb[first_off..first_off + len as usize].to_vec();
                write_record_buf(state, 0, &frag);
            }
            let mut buffer: usize = len as usize; // index into readRecordBuf
            let mut gotlen: u32 = len;

            loop {
                // Beginning of next page.
                target_page_ptr += XLOG_BLCKSZ as u64;

                // Read the page header first.
                read_off = ReadPageInternal(state, target_page_ptr, SIZE_OF_XLOG_SHORT_PHD as i32)?;
                if read_off == XLREAD_WOULDBLOCK {
                    return Ok(XLREAD_WOULDBLOCK);
                } else if read_off < 0 {
                    return Ok(decode_err(state, assembled, rec_ptr, target_page_ptr));
                }
                debug_assert!(SIZE_OF_XLOG_SHORT_PHD <= read_off as usize);

                let cont_phdr = parse_page_header(read_buf(state));

                // Overwrite-contrecord: restart at this flag's location.
                if (cont_phdr.xlp_info & XLP_FIRST_IS_OVERWRITE_CONTRECORD) != 0 {
                    state.overwrittenRecPtr = rec_ptr;
                    rec_ptr = target_page_ptr;
                    continue 'restart;
                }

                // Check that the continuation looks valid.
                if (cont_phdr.xlp_info & XLP_FIRST_IS_CONTRECORD) == 0 {
                    let (h, l) = lsn_fmt(rec_ptr);
                    report_invalid_record(
                        state,
                        format!("there is no contrecord flag at {:X}/{:X}", h, l),
                    );
                    return Ok(decode_err(state, assembled, rec_ptr, target_page_ptr));
                }

                // Cross-check xlp_rem_len.
                if cont_phdr.xlp_rem_len == 0 || total_len != cont_phdr.xlp_rem_len + gotlen {
                    let (h, l) = lsn_fmt(rec_ptr);
                    report_invalid_record(
                        state,
                        format!(
                            "invalid contrecord length {} (expected {}) at {:X}/{:X}",
                            cont_phdr.xlp_rem_len,
                            (total_len as i64) - gotlen as i64,
                            h,
                            l
                        ),
                    );
                    return Ok(decode_err(state, assembled, rec_ptr, target_page_ptr));
                }

                // Wait for the next page to become available.
                read_off = ReadPageInternal(
                    state,
                    target_page_ptr,
                    imin(
                        (total_len - gotlen) as i32 + SIZE_OF_XLOG_SHORT_PHD as i32,
                        XLOG_BLCKSZ as i32,
                    ),
                )?;
                if read_off == XLREAD_WOULDBLOCK {
                    return Ok(XLREAD_WOULDBLOCK);
                } else if read_off < 0 {
                    return Ok(decode_err(state, assembled, rec_ptr, target_page_ptr));
                }

                // Append the continuation from this page to the buffer.
                page_header_size = XLogPageHeaderSize(cont_phdr.xlp_info);

                if (read_off as usize) < page_header_size {
                    read_off = ReadPageInternal(state, target_page_ptr, page_header_size as i32)?;
                    if read_off == XLREAD_WOULDBLOCK {
                        return Ok(XLREAD_WOULDBLOCK);
                    } else if read_off < 0 {
                        return Ok(decode_err(state, assembled, rec_ptr, target_page_ptr));
                    }
                }
                debug_assert!(page_header_size <= read_off as usize);

                let mut cont_len = XLOG_BLCKSZ - page_header_size;
                if (cont_phdr.xlp_rem_len as usize) < cont_len {
                    cont_len = cont_phdr.xlp_rem_len as usize;
                }

                if (read_off as usize) < page_header_size + cont_len {
                    read_off =
                        ReadPageInternal(state, target_page_ptr, (page_header_size + cont_len) as i32)?;
                    if read_off == XLREAD_WOULDBLOCK {
                        return Ok(XLREAD_WOULDBLOCK);
                    } else if read_off < 0 {
                        return Ok(decode_err(state, assembled, rec_ptr, target_page_ptr));
                    }
                }

                {
                    let rb = read_buf(state);
                    let frag = rb[page_header_size..page_header_size + cont_len].to_vec();
                    write_record_buf(state, buffer, &frag);
                }
                buffer += cont_len;
                gotlen += cont_len as u32;
                last_cont_rem_len = cont_phdr.xlp_rem_len;

                // If we just reassembled the record header, validate it.
                if !got_header {
                    record_hdr = parse_xlog_record(record_buf(state));
                    if !ValidXLogRecordHeader(
                        state,
                        rec_ptr,
                        state.DecodeRecPtr,
                        &record_hdr,
                        rand_access,
                    ) {
                        return Ok(decode_err(state, assembled, rec_ptr, target_page_ptr));
                    }
                    got_header = true;
                }

                // We might need a bigger buffer.
                if total_len > state.readRecordBufSize {
                    debug_assert!(gotlen as usize <= XLOG_BLCKSZ * 2);
                    debug_assert!(gotlen <= state.readRecordBufSize);
                    let save_copy: Vec<u8> = record_buf(state)[..gotlen as usize].to_vec();
                    if allocate_recordbuf(state, total_len).is_err() {
                        let (h, l) = lsn_fmt(rec_ptr);
                        report_invalid_record(
                            state,
                            format!("out of memory while reading WAL record at {:X}/{:X}", h, l),
                        );
                        return Ok(decode_err(state, assembled, rec_ptr, target_page_ptr));
                    }
                    write_record_buf(state, 0, &save_copy);
                    buffer = gotlen as usize;
                }

                if gotlen >= total_len {
                    break;
                }
            }
            debug_assert!(got_header);

            record_hdr = parse_xlog_record(record_buf(state));
            record_bytes = record_buf(state)[..total_len as usize].to_vec();
            if !ValidXLogRecord(state, &record_hdr, &record_bytes, rec_ptr) {
                return Ok(decode_err(state, assembled, rec_ptr, target_page_ptr));
            }

            page_header_size = XLogPageHeaderSize(parse_page_header(read_buf(state)).xlp_info);
            state.DecodeRecPtr = rec_ptr;
            state.NextRecPtr = target_page_ptr
                + page_header_size as u64
                + MAXALIGN(last_cont_rem_len as usize) as u64;
        } else {
            // Record does not cross a page boundary.
            read_off = ReadPageInternal(
                state,
                target_page_ptr,
                imin(target_rec_off as i32 + total_len as i32, XLOG_BLCKSZ as i32),
            )?;
            if read_off == XLREAD_WOULDBLOCK {
                return Ok(XLREAD_WOULDBLOCK);
            } else if read_off < 0 {
                return Ok(decode_err(state, assembled, rec_ptr, target_page_ptr));
            }

            let rec_off = (rec_ptr % XLOG_BLCKSZ as u64) as usize;
            record_bytes = read_buf(state)[rec_off..rec_off + total_len as usize].to_vec();
            record_hdr = parse_xlog_record(&record_bytes);
            if !ValidXLogRecord(state, &record_hdr, &record_bytes, rec_ptr) {
                return Ok(decode_err(state, assembled, rec_ptr, target_page_ptr));
            }

            state.NextRecPtr = rec_ptr + MAXALIGN(total_len as usize) as u64;
            state.DecodeRecPtr = rec_ptr;
        }

        // ---- finish_decode (xlogreader.c:873-925) ----

        // Special processing if it's an XLOG SWITCH record.
        if record_hdr.rmid() == RM_XLOG_ID && (record_hdr.info() & !XLR_INFO_MASK) == XLOG_SWITCH {
            // Pretend it extends to end of segment.
            state.NextRecPtr += state.segcxt.ws_segsize as u64 - 1;
            state.NextRecPtr -= XLogSegmentOffset(state.NextRecPtr, state.segcxt.ws_segsize) as u64;
        }

        // If we got here without a slot, validate total_len before trusting it.
        if slot.is_none() {
            debug_assert!(!nonblocking);
            slot = XLogReadRecordAlloc(state, total_len as usize, true /* allow_oversized */);
            debug_assert!(slot.is_some());
        }
        let slot = slot.unwrap();

        match DecodeXLogRecord(state, &slot, &record_hdr, &record_bytes, rec_ptr) {
            Ok(decoded) => {
                let size = decoded.size();
                let oversized = decoded.oversized();
                let next_lsn = state.NextRecPtr;
                let decoded = decoded.with_lsns(rec_ptr, next_lsn);

                // If it's in the decode buffer, mark the space as occupied.
                if !oversized {
                    debug_assert_eq!(size, MAXALIGN(size));
                    if slot.offset == 0 {
                        state.decode_buffer_tail = size;
                    } else {
                        state.decode_buffer_tail = slot.offset + size;
                    }
                }

                // Insert it into the queue of decoded records.
                push_queue(state, decoded);
                return Ok(XLREAD_SUCCESS);
            }
            Err(()) => {
                return Ok(decode_err(state, assembled, rec_ptr, target_page_ptr));
            }
        }
    }
}

/// Push a freshly decoded record onto the tail of the decode queue (the C
/// `decode_queue_tail->next = decoded; decode_queue_tail = decoded`).
fn push_queue<'mcx>(state: &mut XLogReaderState<'mcx>, decoded: DecodedXLogRecord<'mcx>) {
    let arena = state.decode_arena.expect("reader has no allocator context");
    if state.decode_queue.is_none() {
        state.decode_queue = Some(PgVec::new_in(arena));
    }
    state.decode_queue_head = 0;
    let q = state.decode_queue.as_mut().unwrap();
    q.push(decoded);
}

/// The `err:` label of `XLogDecodeNextRecord` (xlogreader.c:927-965).
fn decode_err(
    state: &mut XLogReaderState<'_>,
    assembled: bool,
    rec_ptr: XLogRecPtr,
    target_page_ptr: XLogRecPtr,
) -> i32 {
    if assembled {
        state.abortedRecPtr = rec_ptr;
        state.missingContrecPtr = target_page_ptr;
        // Make sure an error is queued so the prefetcher doesn't loop back.
        state.errormsg_deferred = true;
    }

    // (Oversized speculative records are never committed to the queue here, so
    // there is nothing to free — the arena reclaims any scratch on reset.)

    // Invalidate the read state.
    XLogReaderInvalReadState(state);

    XLREAD_FAIL
}

// ===========================================================================
// ReadPageInternal — page fetch through the reader's page_read callback.
// ===========================================================================

/// Read-only view of the reader's `readBuf` (the current page).
fn read_buf<'a>(state: &'a XLogReaderState<'_>) -> &'a [u8] {
    state
        .readBuf
        .as_ref()
        .map(|v| v.as_slice())
        .unwrap_or(&[])
}

/// Read-only view of the reader's `readRecordBuf`.
fn record_buf<'a>(state: &'a XLogReaderState<'_>) -> &'a [u8] {
    state
        .readRecordBuf
        .as_ref()
        .map(|v| v.as_slice())
        .unwrap_or(&[])
}

/// Write `src` into `readRecordBuf` starting at `off`.
fn write_record_buf(state: &mut XLogReaderState<'_>, off: usize, src: &[u8]) {
    if let Some(rb) = state.readRecordBuf.as_mut() {
        rb[off..off + src.len()].copy_from_slice(src);
    }
}

/// `ReadPageInternal` (xlogreader.c:1010). Read a single xlog page covering at
/// least `[pageptr, reqLen]` via the reader's page-read callback. Returns the
/// valid byte count, `XLREAD_FAIL`, or `XLREAD_WOULDBLOCK`; `Err` is an
/// `ereport(ERROR)` raised inside the callback.
fn ReadPageInternal(
    state: &mut XLogReaderState<'_>,
    pageptr: XLogRecPtr,
    req_len: i32,
) -> PgResult<i32> {
    debug_assert!(pageptr % XLOG_BLCKSZ as u64 == 0);

    let target_seg_no = XLByteToSeg(pageptr, state.segcxt.ws_segsize);
    let target_page_off = XLogSegmentOffset(pageptr, state.segcxt.ws_segsize);

    // Check whether we have all the requested data already.
    if target_seg_no == state.seg.ws_segno
        && target_page_off == state.segoff
        && req_len <= state.readLen as i32
    {
        return Ok(state.readLen as i32);
    }

    // Invalidate contents of the internal buffer before the read attempt.
    state.readLen = 0;

    // When switching to a new WAL segment, read + validate the first page.
    if target_seg_no != state.seg.ws_segno && target_page_off != 0 {
        let target_segment_ptr = pageptr - target_page_off as u64;

        let read_len = call_page_read(state, target_segment_ptr, XLOG_BLCKSZ as i32)?;
        if read_len == XLREAD_WOULDBLOCK {
            return Ok(XLREAD_WOULDBLOCK);
        } else if read_len < 0 {
            XLogReaderInvalReadState(state);
            return Ok(XLREAD_FAIL);
        }
        debug_assert_eq!(read_len, XLOG_BLCKSZ as i32);

        if !validate_page_header_from_read_buf(state, target_segment_ptr) {
            XLogReaderInvalReadState(state);
            return Ok(XLREAD_FAIL);
        }
    }

    // First, read the requested length, but at least a short page header.
    let mut read_len = call_page_read(state, pageptr, imax(req_len, SIZE_OF_XLOG_SHORT_PHD as i32))?;
    if read_len == XLREAD_WOULDBLOCK {
        return Ok(XLREAD_WOULDBLOCK);
    } else if read_len < 0 {
        XLogReaderInvalReadState(state);
        return Ok(XLREAD_FAIL);
    }
    debug_assert!(read_len <= XLOG_BLCKSZ as i32);

    // Do we have enough data to check the header length?
    if read_len <= SIZE_OF_XLOG_SHORT_PHD as i32 {
        XLogReaderInvalReadState(state);
        return Ok(XLREAD_FAIL);
    }
    debug_assert!(read_len >= req_len);

    let hdr_info = {
        let rb = read_buf(state);
        u16::from_ne_bytes([rb[2], rb[3]])
    };
    let hdr_size = XLogPageHeaderSize(hdr_info);

    // Still not enough?
    if (read_len as usize) < hdr_size {
        read_len = call_page_read(state, pageptr, hdr_size as i32)?;
        if read_len == XLREAD_WOULDBLOCK {
            return Ok(XLREAD_WOULDBLOCK);
        } else if read_len < 0 {
            XLogReaderInvalReadState(state);
            return Ok(XLREAD_FAIL);
        }
    }

    // Now that we have the full header, validate it.
    if !validate_page_header_from_read_buf(state, pageptr) {
        XLogReaderInvalReadState(state);
        return Ok(XLREAD_FAIL);
    }

    // Update read state information.
    state.seg.ws_segno = target_seg_no;
    state.segoff = target_page_off;
    state.readLen = read_len as u32;

    Ok(read_len)
}

/// Invoke `state->routine.page_read(state, pageptr, req_len, currRecPtr,
/// readBuf)`. The callback reads into the reader's `readBuf` and returns the
/// byte count (or a negative `XLREAD_*` sentinel). The callback may
/// `ereport(ERROR)` (carried on `Err`).
fn call_page_read(
    state: &mut XLogReaderState<'_>,
    target_page_ptr: XLogRecPtr,
    req_len: i32,
) -> PgResult<i32> {
    let cb = match state.routine.page_read {
        Some(cb) => cb,
        // No callback installed: behave as the C all-NULL routine would — a
        // segfault in C, here a hard failure (the reader is misconfigured).
        None => return Ok(XLREAD_FAIL),
    };
    cb(state, target_page_ptr, req_len, state.currRecPtr)
}

/// `XLogReaderInvalReadState` (xlogreader.c:1123).
fn XLogReaderInvalReadState(state: &mut XLogReaderState<'_>) {
    state.seg.ws_segno = 0;
    state.segoff = 0;
    state.readLen = 0;
}

// ===========================================================================
// Header / record validation + CRC.
// ===========================================================================

/// `ValidXLogRecordHeader` (xlogreader.c:1137).
fn ValidXLogRecordHeader(
    state: &mut XLogReaderState<'_>,
    rec_ptr: XLogRecPtr,
    prev_rec_ptr: XLogRecPtr,
    record: &XLogRecord,
    rand_access: bool,
) -> bool {
    if (record.total_len() as usize) < SIZE_OF_XLOG_RECORD {
        let (h, l) = lsn_fmt(rec_ptr);
        report_invalid_record(
            state,
            format!(
                "invalid record length at {:X}/{:X}: expected at least {}, got {}",
                h, l, SIZE_OF_XLOG_RECORD as u32, record.total_len()
            ),
        );
        return false;
    }
    if !RmgrIdIsValid(record.rmid() as i32) {
        let (h, l) = lsn_fmt(rec_ptr);
        report_invalid_record(
            state,
            format!(
                "invalid resource manager ID {} at {:X}/{:X}",
                record.rmid(), h, l
            ),
        );
        return false;
    }
    if rand_access {
        if !(record.prev() < rec_ptr) {
            let (ph, pl) = lsn_fmt(record.prev());
            let (h, l) = lsn_fmt(rec_ptr);
            report_invalid_record(
                state,
                format!(
                    "record with incorrect prev-link {:X}/{:X} at {:X}/{:X}",
                    ph, pl, h, l
                ),
            );
            return false;
        }
    } else {
        if record.prev() != prev_rec_ptr {
            let (ph, pl) = lsn_fmt(record.prev());
            let (h, l) = lsn_fmt(rec_ptr);
            report_invalid_record(
                state,
                format!(
                    "record with incorrect prev-link {:X}/{:X} at {:X}/{:X}",
                    ph, pl, h, l
                ),
            );
            return false;
        }
    }
    true
}

/// `ValidXLogRecord` (xlogreader.c:1203). CRC-check the record (over its whole
/// `xl_tot_len` body, then the header up to `xl_crc`).
fn ValidXLogRecord(
    state: &mut XLogReaderState<'_>,
    record: &XLogRecord,
    record_bytes: &[u8],
    recptr: XLogRecPtr,
) -> bool {
    debug_assert!(record.total_len() as usize >= SIZE_OF_XLOG_RECORD);

    let mut crc = INIT_CRC32C();
    crc = COMP_CRC32C(
        crc,
        &record_bytes[SIZE_OF_XLOG_RECORD..record.total_len() as usize],
    );
    crc = COMP_CRC32C(crc, &record_bytes[..OFFSETOF_XLOG_RECORD_XL_CRC]);
    crc = FIN_CRC32C(crc);

    if !EQ_CRC32C(record.crc(), crc) {
        let (h, l) = lsn_fmt(recptr);
        report_invalid_record(
            state,
            format!(
                "incorrect resource manager data checksum in record at {:X}/{:X}",
                h, l
            ),
        );
        return false;
    }
    true
}

/// `validate_page_header_from_read_buf` — `XLogReaderValidatePageHeader` reading
/// the page from `state.readBuf`.
fn validate_page_header_from_read_buf(state: &mut XLogReaderState<'_>, recptr: XLogRecPtr) -> bool {
    let buf = read_buf(state).to_vec();
    XLogReaderValidatePageHeader(state, recptr, &buf)
}

/// `XLogReaderValidatePageHeader` (xlogreader.c:1234).
pub fn XLogReaderValidatePageHeader(
    state: &mut XLogReaderState<'_>,
    recptr: XLogRecPtr,
    phdr: &[u8],
) -> bool {
    debug_assert!(recptr % XLOG_BLCKSZ as u64 == 0);

    let segno = XLByteToSeg(recptr, state.segcxt.ws_segsize);
    let offset = XLogSegmentOffset(recptr, state.segcxt.ws_segsize) as i32;
    let hdr = parse_page_header(phdr);

    if hdr.xlp_magic != XLOG_PAGE_MAGIC {
        let fname = XLogFileName(state.seg.ws_tli, segno, state.segcxt.ws_segsize);
        let (h, l) = lsn_fmt(recptr);
        report_invalid_record(
            state,
            format!(
                "invalid magic number {:04X} in WAL segment {}, LSN {:X}/{:X}, offset {}",
                hdr.xlp_magic, fname, h, l, offset
            ),
        );
        return false;
    }

    if (hdr.xlp_info & !XLP_ALL_FLAGS) != 0 {
        let fname = XLogFileName(state.seg.ws_tli, segno, state.segcxt.ws_segsize);
        let (h, l) = lsn_fmt(recptr);
        report_invalid_record(
            state,
            format!(
                "invalid info bits {:04X} in WAL segment {}, LSN {:X}/{:X}, offset {}",
                hdr.xlp_info, fname, h, l, offset
            ),
        );
        return false;
    }

    if (hdr.xlp_info & XLP_LONG_HEADER) != 0 {
        if state.system_identifier != 0 && hdr.xlp_sysid != state.system_identifier {
            report_invalid_record(
                state,
                format!(
                    "WAL file is from different database system: WAL file database system identifier is {}, pg_control database system identifier is {}",
                    hdr.xlp_sysid, state.system_identifier
                ),
            );
            return false;
        } else if hdr.xlp_seg_size != state.segcxt.ws_segsize as u32 {
            report_invalid_record(
                state,
                String::from(
                    "WAL file is from different database system: incorrect segment size in page header",
                ),
            );
            return false;
        } else if hdr.xlp_xlog_blcksz != XLOG_BLCKSZ as u32 {
            report_invalid_record(
                state,
                String::from(
                    "WAL file is from different database system: incorrect XLOG_BLCKSZ in page header",
                ),
            );
            return false;
        }
    } else if offset == 0 {
        let fname = XLogFileName(state.seg.ws_tli, segno, state.segcxt.ws_segsize);
        let (h, l) = lsn_fmt(recptr);
        report_invalid_record(
            state,
            format!(
                "invalid info bits {:04X} in WAL segment {}, LSN {:X}/{:X}, offset {}",
                hdr.xlp_info, fname, h, l, offset
            ),
        );
        return false;
    }

    // Check the page address matches what we expected.
    if hdr.xlp_pageaddr != recptr {
        let fname = XLogFileName(state.seg.ws_tli, segno, state.segcxt.ws_segsize);
        let (ph, pl) = lsn_fmt(hdr.xlp_pageaddr);
        let (h, l) = lsn_fmt(recptr);
        report_invalid_record(
            state,
            format!(
                "unexpected pageaddr {:X}/{:X} in WAL segment {}, LSN {:X}/{:X}, offset {}",
                ph, pl, fname, h, l, offset
            ),
        );
        return false;
    }

    // TLI should never go backwards across successive pages.
    if recptr > state.latestPagePtr {
        if hdr.xlp_tli < state.latestPageTLI {
            let fname = XLogFileName(state.seg.ws_tli, segno, state.segcxt.ws_segsize);
            let (h, l) = lsn_fmt(recptr);
            report_invalid_record(
                state,
                format!(
                    "out-of-sequence timeline ID {} (after {}) in WAL segment {}, LSN {:X}/{:X}, offset {}",
                    hdr.xlp_tli, state.latestPageTLI, fname, h, l, offset
                ),
            );
            return false;
        }
    }
    state.latestPagePtr = recptr;
    state.latestPageTLI = hdr.xlp_tli;

    true
}

/// `XLogReaderResetError` (xlogreader.c:1375).
pub fn XLogReaderResetError(state: &mut XLogReaderState<'_>) {
    state.errormsg_buf = None;
    state.errormsg_deferred = false;
}

// ===========================================================================
// XLogFindNextRecord — find the first record at/after a position.
// ===========================================================================

/// `XLogFindNextRecord` (xlogreader.c:1393). Find the first record with an
/// lsn >= `rec_ptr`, positioning the reader for the next `XLogReadRecord`.
/// Returns the found LSN, or `InvalidXLogRecPtr`. `Err` is an `ereport(ERROR)`
/// from the page-read callback.
pub fn XLogFindNextRecord<'mcx>(
    state: &mut XLogReaderState<'mcx>,
    rec_ptr: XLogRecPtr,
) -> PgResult<XLogRecPtr> {
    debug_assert!(!XLogRecPtrIsInvalid(rec_ptr));

    // Make sure ReadPageInternal() can't return XLREAD_WOULDBLOCK.
    state.nonblocking = false;

    let mut tmp_rec_ptr = rec_ptr;

    'outer: loop {
        let target_rec_off = (tmp_rec_ptr % XLOG_BLCKSZ as u64) as i32;
        let target_page_ptr = tmp_rec_ptr - target_rec_off as u64;

        let mut read_len = ReadPageInternal(state, target_page_ptr, target_rec_off)?;
        if read_len < 0 {
            XLogReaderInvalReadState(state);
            return Ok(InvalidXLogRecPtr);
        }

        let header = parse_page_header(read_buf(state));
        let page_header_size = XLogPageHeaderSize(header.xlp_info);

        read_len = ReadPageInternal(state, target_page_ptr, page_header_size as i32)?;
        if read_len < 0 {
            XLogReaderInvalReadState(state);
            return Ok(InvalidXLogRecPtr);
        }

        // Skip over potential continuation data.
        if (header.xlp_info & XLP_FIRST_IS_CONTRECORD) != 0 {
            if MAXALIGN(header.xlp_rem_len as usize) >= (XLOG_BLCKSZ - page_header_size) {
                tmp_rec_ptr = target_page_ptr + XLOG_BLCKSZ as u64;
            } else {
                tmp_rec_ptr = target_page_ptr
                    + page_header_size as u64
                    + MAXALIGN(header.xlp_rem_len as usize) as u64;
                break 'outer;
            }
        } else {
            tmp_rec_ptr = target_page_ptr + page_header_size as u64;
            break 'outer;
        }
    }

    // tmp_rec_ptr now points to a valid XLogRecord.
    XLogBeginRead(state, tmp_rec_ptr);
    loop {
        let decoded = read_record(state)?;
        if decoded.is_none() {
            break;
        }
        if rec_ptr <= state.ReadRecPtr {
            let found = state.ReadRecPtr;
            XLogBeginRead(state, found);
            return Ok(found);
        }
    }

    XLogReaderInvalReadState(state);
    Ok(InvalidXLogRecPtr)
}

/// `XLogReadRecord` (xlogreader.c:389) — internal driver used by
/// `XLogFindNextRecord`. Ensures the queue has a record (blocking), then
/// consumes the head; `Some(())` on success.
fn read_record<'mcx>(state: &mut XLogReaderState<'mcx>) -> PgResult<Option<()>> {
    XLogReleasePreviousRecord(state);
    if !XLogReaderHasQueuedRecordOrError(state) {
        XLogReadAhead(state, false)?;
    }
    Ok(XLogNextRecord(state).map(|_| ()))
}

// ===========================================================================
// DecodeXLogRecord — decode the block references and payload into the arena.
// ===========================================================================

/// `DecodeXLogRecord` (xlogreader.c:1681). Decode `record_bytes` (the whole
/// record) into a fresh [`DecodedXLogRecord`] whose payload byte slices borrow
/// the reader's `decode_arena`. Sets `size`/`oversized` per the slot. Returns
/// `Err(())` on a short/invalid record (with errormsg set).
fn DecodeXLogRecord<'mcx>(
    state: &mut XLogReaderState<'mcx>,
    slot: &AllocSlot,
    record_hdr: &XLogRecord,
    record_bytes: &[u8],
    lsn: XLogRecPtr,
) -> Result<DecodedXLogRecord<'mcx>, ()> {
    let arena = state.decode_arena.expect("reader has no allocator context");

    // ptr walks the record after the fixed header.
    let mut ptr: usize = SIZE_OF_XLOG_RECORD;
    let mut remaining: u32 = record_hdr.total_len() - SIZE_OF_XLOG_RECORD as u32;

    let mut record_origin: RepOriginId = InvalidRepOriginId;
    let mut toplevel_xid: TransactionId = InvalidTransactionId;
    let mut main_data_len: u32 = 0;
    let mut datatotal: u32 = 0;
    let mut max_block_id: i32 = -1;

    // Working block descriptors (raw, decoded from the headers; the byte
    // payloads are filled in the second pass below). Index == block_id.
    let mut blks: Vec<WorkingBlock> = Vec::new();
    let mut rlocator: Option<RelFileLocator> = None;

    // COPY_HEADER_FIELD: bounds-check then return the field slice.
    macro_rules! copy_header_field {
        ($size:expr) => {{
            let sz = $size;
            if (remaining as usize) < sz {
                shortdata_err(state);
                return Err(());
            }
            let s = ptr;
            ptr += sz;
            remaining -= sz as u32;
            &record_bytes[s..s + sz]
        }};
    }

    while remaining > datatotal {
        let block_id = copy_header_field!(1)[0];

        if block_id == XLR_BLOCK_ID_DATA_SHORT {
            let b = copy_header_field!(1);
            let mdl = b[0] as u32;
            main_data_len = mdl;
            datatotal += mdl;
            break; // main data fragment is always last
        } else if block_id == XLR_BLOCK_ID_DATA_LONG {
            let b = copy_header_field!(4);
            let mdl = u32::from_ne_bytes([b[0], b[1], b[2], b[3]]);
            main_data_len = mdl;
            datatotal += mdl;
            break; // main data fragment is always last
        } else if block_id == XLR_BLOCK_ID_ORIGIN {
            let b = copy_header_field!(core::mem::size_of::<RepOriginId>());
            record_origin = u16::from_ne_bytes([b[0], b[1]]);
        } else if block_id == XLR_BLOCK_ID_TOPLEVEL_XID {
            let b = copy_header_field!(core::mem::size_of::<TransactionId>());
            toplevel_xid = u32::from_ne_bytes([b[0], b[1], b[2], b[3]]);
        } else if (block_id as i32) <= XLR_MAX_BLOCK_ID {
            // Mark any intervening block IDs as not in use.
            for i in (max_block_id + 1)..(block_id as i32) {
                while blks.len() <= i as usize {
                    blks.push(WorkingBlock::default());
                }
                blks[i as usize].in_use = false;
            }

            if (block_id as i32) <= max_block_id {
                let (h, l) = lsn_fmt(state.ReadRecPtr);
                report_invalid_record(
                    state,
                    format!("out-of-order block_id {} at {:X}/{:X}", block_id, h, l),
                );
                return Err(());
            }
            max_block_id = block_id as i32;

            while blks.len() <= block_id as usize {
                blks.push(WorkingBlock::default());
            }
            let mut blk = WorkingBlock {
                in_use: true,
                apply_image: false,
                ..Default::default()
            };

            let fork_flags = copy_header_field!(1)[0];
            blk.forknum = ForkNumber::from_i32((fork_flags & BKPBLOCK_FORK_MASK) as i32)
                .unwrap_or(ForkNumber::MAIN_FORKNUM);
            blk.flags = fork_flags;
            blk.has_image = (fork_flags & BKPBLOCK_HAS_IMAGE) != 0;
            blk.has_data = (fork_flags & BKPBLOCK_HAS_DATA) != 0;

            let b = copy_header_field!(2);
            blk.data_len = u16::from_ne_bytes([b[0], b[1]]);
            if blk.has_data && blk.data_len == 0 {
                let (h, l) = lsn_fmt(state.ReadRecPtr);
                report_invalid_record(
                    state,
                    format!("BKPBLOCK_HAS_DATA set, but no data included at {:X}/{:X}", h, l),
                );
                return Err(());
            }
            if !blk.has_data && blk.data_len != 0 {
                let (h, l) = lsn_fmt(state.ReadRecPtr);
                report_invalid_record(
                    state,
                    format!(
                        "BKPBLOCK_HAS_DATA not set, but data length is {} at {:X}/{:X}",
                        blk.data_len, h, l
                    ),
                );
                return Err(());
            }
            datatotal += blk.data_len as u32;

            if blk.has_image {
                let b = copy_header_field!(2);
                blk.bimg_len = u16::from_ne_bytes([b[0], b[1]]);
                let b = copy_header_field!(2);
                blk.hole_offset = u16::from_ne_bytes([b[0], b[1]]);
                let b = copy_header_field!(1);
                blk.bimg_info = b[0];

                blk.apply_image = (blk.bimg_info & BKPIMAGE_APPLY) != 0;

                if BKPIMAGE_COMPRESSED(blk.bimg_info) {
                    if (blk.bimg_info & BKPIMAGE_HAS_HOLE) != 0 {
                        let b = copy_header_field!(2);
                        blk.hole_length = u16::from_ne_bytes([b[0], b[1]]);
                    } else {
                        blk.hole_length = 0;
                    }
                } else {
                    blk.hole_length = BLCKSZ as u16 - blk.bimg_len;
                }
                datatotal += blk.bimg_len as u32;

                if (blk.bimg_info & BKPIMAGE_HAS_HOLE) != 0
                    && (blk.hole_offset == 0
                        || blk.hole_length == 0
                        || blk.bimg_len as usize == BLCKSZ)
                {
                    let (h, l) = lsn_fmt(state.ReadRecPtr);
                    report_invalid_record(
                        state,
                        format!(
                            "BKPIMAGE_HAS_HOLE set, but hole offset {} length {} block image length {} at {:X}/{:X}",
                            blk.hole_offset, blk.hole_length, blk.bimg_len, h, l
                        ),
                    );
                    return Err(());
                }
                if (blk.bimg_info & BKPIMAGE_HAS_HOLE) == 0
                    && (blk.hole_offset != 0 || blk.hole_length != 0)
                {
                    let (h, l) = lsn_fmt(state.ReadRecPtr);
                    report_invalid_record(
                        state,
                        format!(
                            "BKPIMAGE_HAS_HOLE not set, but hole offset {} length {} at {:X}/{:X}",
                            blk.hole_offset, blk.hole_length, h, l
                        ),
                    );
                    return Err(());
                }
                if BKPIMAGE_COMPRESSED(blk.bimg_info) && blk.bimg_len as usize == BLCKSZ {
                    let (h, l) = lsn_fmt(state.ReadRecPtr);
                    report_invalid_record(
                        state,
                        format!(
                            "BKPIMAGE_COMPRESSED set, but block image length {} at {:X}/{:X}",
                            blk.bimg_len, h, l
                        ),
                    );
                    return Err(());
                }
                if (blk.bimg_info & BKPIMAGE_HAS_HOLE) == 0
                    && !BKPIMAGE_COMPRESSED(blk.bimg_info)
                    && blk.bimg_len as usize != BLCKSZ
                {
                    let (h, l) = lsn_fmt(state.ReadRecPtr);
                    report_invalid_record(
                        state,
                        format!(
                            "neither BKPIMAGE_HAS_HOLE nor BKPIMAGE_COMPRESSED set, but block image length is {} at {:X}/{:X}",
                            blk.data_len, h, l
                        ),
                    );
                    return Err(());
                }
            }

            if (fork_flags & BKPBLOCK_SAME_REL) == 0 {
                let b = copy_header_field!(SIZEOF_REL_FILE_LOCATOR);
                let loc = parse_rel_file_locator(b);
                blk.rlocator = loc;
                rlocator = Some(loc);
            } else {
                match rlocator {
                    None => {
                        let (h, l) = lsn_fmt(state.ReadRecPtr);
                        report_invalid_record(
                            state,
                            format!("BKPBLOCK_SAME_REL set but no previous rel at {:X}/{:X}", h, l),
                        );
                        return Err(());
                    }
                    Some(loc) => blk.rlocator = loc,
                }
            }
            let b = copy_header_field!(core::mem::size_of::<BlockNumber>());
            blk.blkno = u32::from_ne_bytes([b[0], b[1], b[2], b[3]]);

            blks[block_id as usize] = blk;
        } else {
            let (h, l) = lsn_fmt(state.ReadRecPtr);
            report_invalid_record(
                state,
                format!("invalid block_id {} at {:X}/{:X}", block_id, h, l),
            );
            return Err(());
        }
    }

    if remaining != datatotal {
        shortdata_err(state);
        return Err(());
    }

    // Make sure the blocks vec covers 0..=max_block_id.
    while (blks.len() as i32) < max_block_id + 1 {
        blks.push(WorkingBlock::default());
    }

    // `out` tracks the C output cursor for the `size` footprint (measured from
    // offsetof(.., blocks), i.e. after the (max_block_id+1) block array).
    let mut out: usize =
        SIZEOF_DECODED_XLOG_RECORD_FIXED + SIZEOF_DECODED_BKP_BLOCK * (max_block_id + 1) as usize;

    // Second pass: copy each fragment's bytes into the decode arena (the C
    // `memcpy` into the decode buffer; here a `&'mcx [u8]` borrowing the arena),
    // and build the final DecodedBkpBlock array.
    let mut blocks: PgVec<'mcx, DecodedBkpBlock<'mcx>> =
        ::mcx::vec_with_capacity_in(arena, (max_block_id + 1) as usize).map_err(|_| ())?;

    for block_id in 0..=max_block_id {
        let blk = &blks[block_id as usize];
        if !blk.in_use {
            blocks.push(DecodedBkpBlock::default());
            continue;
        }
        debug_assert!(blk.has_image || !blk.apply_image);

        let mut bkp_image: Option<&'mcx [u8]> = None;
        let mut data: Option<&'mcx [u8]> = None;

        if blk.has_image {
            // no need to align image
            let src = &record_bytes[ptr..ptr + blk.bimg_len as usize];
            bkp_image = Some(arena_copy(arena, src).map_err(|_| ())?);
            ptr += blk.bimg_len as usize;
            out += blk.bimg_len as usize;
        }
        if blk.has_data {
            out = MAXALIGN(out);
            let src = &record_bytes[ptr..ptr + blk.data_len as usize];
            data = Some(arena_copy(arena, src).map_err(|_| ())?);
            ptr += blk.data_len as usize;
            out += blk.data_len as usize;
        }

        blocks.push(DecodedBkpBlock::decoded(
            blk.rlocator,
            blk.forknum,
            blk.blkno,
            blk.flags,
            blk.has_image,
            blk.apply_image,
            bkp_image,
            blk.hole_offset,
            blk.hole_length,
            blk.bimg_len,
            blk.bimg_info,
            blk.has_data,
            data,
            blk.data_len,
        ));
    }

    // and finally, the main data
    let main_data: &'mcx [u8];
    if main_data_len > 0 {
        out = MAXALIGN(out);
        let src = &record_bytes[ptr..ptr + main_data_len as usize];
        main_data = arena_copy(arena, src).map_err(|_| ())?;
        out += main_data_len as usize;
    } else {
        main_data = &[];
    }

    // Report the actual size used.
    let size = MAXALIGN(out);
    debug_assert!(DecodeXLogRecordRequiredSpace(record_hdr.total_len() as usize) >= size);

    let decoded = DecodedXLogRecord::new(*record_hdr, main_data, blocks)
        .with_origin(record_origin)
        .with_toplevel_xid(toplevel_xid)
        .with_size(size, slot.oversized)
        .with_lsn(lsn)
        .with_buffer_offset(slot.offset);

    Ok(decoded)
}

/// Working (pre-payload) block descriptor decoded from the block headers, with
/// the byte-payload borrows filled in the second pass.
#[derive(Clone, Copy, Default)]
struct WorkingBlock {
    in_use: bool,
    rlocator: RelFileLocator,
    forknum: ForkNumber,
    blkno: BlockNumber,
    flags: uint8,
    has_image: bool,
    apply_image: bool,
    hole_offset: uint16,
    hole_length: uint16,
    bimg_len: uint16,
    bimg_info: uint8,
    has_data: bool,
    data_len: uint16,
}

/// `shortdata_err:` label of `DecodeXLogRecord`. Records the error; the caller
/// returns `Err(())`.
fn shortdata_err(state: &mut XLogReaderState<'_>) {
    let (h, l) = lsn_fmt(state.ReadRecPtr);
    report_invalid_record(state, format!("record with invalid length at {:X}/{:X}", h, l));
}

// ===========================================================================
// Decoded-record accessors (XLogRecGetBlockTagExtended / BlockData /
// HasBlockImage / BlockImageApply / GetBlockFlags / RestoreBlockImage / errmsg).
// All operate on the reader's current record (`reader->record`).
// ===========================================================================

/// The reader's current record (`reader->record`), or `None`.
fn current<'a, 'mcx>(state: &'a XLogReaderState<'mcx>) -> Option<&'a DecodedXLogRecord<'mcx>> {
    state.record.as_ref()
}

/// `XLogRecGetRmid(decoder)` (xlogreader.h) — `(decoder)->record->header.xl_rmid`,
/// the resource-manager id of the reader's current record. The caller (the
/// recovery driver's `ReadCheckpointRecord`) guarantees a decoded current
/// record, exactly as the C macro dereferences `record->record` unconditionally.
pub fn XLogRecGetRmid(state: &XLogReaderState<'_>) -> RmgrId {
    current(state)
        .expect("XLogRecGetRmid requires a decoded current record")
        .header()
        .rmid()
}

/// `XLogRecGetInfo(decoder)` (xlogreader.h) — `(decoder)->record->header.xl_info`.
pub fn XLogRecGetInfo(state: &XLogReaderState<'_>) -> uint8 {
    current(state)
        .expect("XLogRecGetInfo requires a decoded current record")
        .info()
}

/// `XLogRecGetTotalLen(decoder)` (xlogreader.h) —
/// `(decoder)->record->header.xl_tot_len`.
pub fn XLogRecGetTotalLen(state: &XLogReaderState<'_>) -> uint32 {
    current(state)
        .expect("XLogRecGetTotalLen requires a decoded current record")
        .header()
        .total_len()
}

/// `XLogRecGetData(decoder)` (xlogreader.h) — `(decoder)->record->main_data`,
/// the record's main data area. The caller (the recovery redo driver)
/// guarantees a decoded current record.
pub fn XLogRecGetData<'a>(state: &'a XLogReaderState<'_>) -> &'a [u8] {
    current(state)
        .expect("XLogRecGetData requires a decoded current record")
        .main_data()
}

/// `XLogRecGetXid(decoder)` (xlogreader.h) — `(decoder)->record->header.xl_xid`,
/// the (bare, epoch-less) transaction id of the reader's current record.
pub fn XLogRecGetXid(state: &XLogReaderState<'_>) -> TransactionId {
    current(state)
        .expect("XLogRecGetXid requires a decoded current record")
        .xid()
}

/// `XLogRecGetPrev(decoder)` (xlogreader.h) — `(decoder)->record->header.xl_prev`,
/// the LSN of the previous record in the WAL chain.
pub fn XLogRecGetPrev(state: &XLogReaderState<'_>) -> XLogRecPtr {
    current(state)
        .expect("XLogRecGetPrev requires a decoded current record")
        .header()
        .prev()
}

/// `XLogRecGetDataLen(decoder)` (xlogreader.h) —
/// `(decoder)->record->main_data_len`, the length of the record's main data area.
pub fn XLogRecGetDataLen(state: &XLogReaderState<'_>) -> uint32 {
    current(state)
        .expect("XLogRecGetDataLen requires a decoded current record")
        .main_data_len()
}

/// `XLogRecMaxBlockId(decoder)` (xlogreader.h) —
/// `(decoder)->record->max_block_id`, the highest block id registered in the
/// reader's current record (`-1` when none). `XLogRecHasAnyBlockRefs` is
/// `max_block_id >= 0`.
pub fn reader_max_block_id(state: &XLogReaderState<'_>) -> i32 {
    current(state)
        .expect("XLogRecMaxBlockId requires a decoded current record")
        .max_block_id()
}

/// `XLogRecHasBlockRef(record, block_id)` (xlogreader.h inline).
fn has_block_ref(state: &XLogReaderState<'_>, block_id: u8) -> bool {
    match current(state) {
        Some(d) => (block_id as i32) <= d.max_block_id() && d.has_block_ref(block_id as usize),
        None => false,
    }
}

/// `XLogRecGetBlockTagExtended` (xlogreader.c:2016). `Some(tag)` when the block
/// reference exists, else `None` (the C `false`).
pub fn xlog_rec_get_block_tag_extended(
    state: &XLogReaderState<'_>,
    block_id: u8,
) -> Option<seams::XLogBlockTag> {
    if !has_block_ref(state, block_id) {
        return None;
    }
    let d = current(state)?;
    let blk = &d.blocks()[block_id as usize];
    Some(seams::XLogBlockTag {
        rlocator: to_storage_locator(blk.rlocator()),
        forknum: blk.forknum(),
        blkno: blk.blkno(),
        prefetch_buffer: blk.prefetch_buffer(),
    })
}

/// `XLogRecGetBlock(record, block_id)->flags` (xlogreader.h).
pub fn xlog_rec_get_block_flags(state: &XLogReaderState<'_>, block_id: u8) -> u8 {
    if !has_block_ref(state, block_id) {
        return 0;
    }
    current(state)
        .map(|d| d.blocks()[block_id as usize].flags())
        .unwrap_or(0)
}

/// `XLogRecHasBlockImage(record, block_id)` (xlogreader.h).
pub fn xlog_rec_has_block_image(state: &XLogReaderState<'_>, block_id: u8) -> bool {
    match current(state) {
        Some(d) => d.has_block_image(block_id as usize),
        None => false,
    }
}

/// `XLogRecBlockImageApply(record, block_id)` (xlogreader.h).
pub fn xlog_rec_block_image_apply(state: &XLogReaderState<'_>, block_id: u8) -> bool {
    match current(state) {
        Some(d) => d.block_image_apply(block_id as usize),
        None => false,
    }
}

/// `state->errormsg_buf` (xlogreader.c). Prefers the `RestoreBlockImage`
/// failure message (written through the `&`-seam interior-mutable slot) when
/// present, else the deferred-decode `errormsg_buf`.
pub fn reader_errormsg_buf(state: &XLogReaderState<'_>) -> String {
    let restore = state.restore_errmsg.borrow();
    if !restore.is_empty() {
        return String::from(restore.as_str());
    }
    state
        .errormsg_buf
        .as_ref()
        .map(|s| String::from(s.as_str()))
        .unwrap_or_default()
}

/// Store a `RestoreBlockImage` failure message through the reader's
/// interior-mutable slot (the `&`-seam analogue of C's
/// `record->errormsg_buf` write). Truncated at `MAX_ERRORMSG_LEN` like
/// `report_invalid_record`.
fn set_restore_error(state: &XLogReaderState<'_>, mut msg: String) {
    if msg.len() > MAX_ERRORMSG_LEN {
        msg.truncate(MAX_ERRORMSG_LEN);
    }
    *state.restore_errmsg.borrow_mut() = msg;
}

/// `RestoreBlockImage` (xlogreader.c:2075). Restore the full-page image of
/// `block_id` of the current record onto `buf`'s page (via the bufmgr seam).
/// Returns `false` on failure (with `errormsg_buf` populated). `Err` carries an
/// `ereport(ERROR)` from the buffer access.
///
/// The seam contract takes `&XLogReaderState` (the redo consumer holds the
/// reader shared); the failure-message text is stored through the reader's
/// interior-mutable error slot so the consumer's follow-up
/// `reader_errormsg_buf` sees it, exactly as C's `RestoreBlockImage` populates
/// `record->errormsg_buf` through the shared decoder pointer.
pub fn restore_block_image(
    state: &XLogReaderState<'_>,
    block_id: u8,
    buf: Buffer,
) -> PgResult<bool> {
    // Decode the block image into a full `BLCKSZ` page (hole re-zeroed), then
    // copy it onto the buffer's live page bytes through the bufmgr seam. On a
    // decode failure the error text is already recorded and we return false.
    let page = match decode_block_image_page(state, block_id)? {
        Some(p) => p,
        None => return Ok(false),
    };

    // The bufmgr owns the page; the seam runs our copy over it.
    seams::with_buffer_page::call(buf, &mut |dst: &mut [u8]| {
        dst[..BLCKSZ].copy_from_slice(&page[..BLCKSZ]);
        Ok(())
    })?;

    Ok(true)
}

/// `RestoreBlockImage(record, block_id, page)` (xlogreader.c:2075) — the
/// faithful C signature, restoring the full-page image of `block_id` of the
/// current record onto a caller-provided `BLCKSZ` byte buffer (C's `char *page`).
///
/// This is the form `verifyBackupPageConsistency` uses: it restores into a
/// plain scratch page (`primary_image_masked`) to mask + `memcmp`, never
/// touching a shared buffer. Returns `false` on failure (with the reader's
/// error slot populated); `Err` carries a decode `ereport(ERROR)`.
pub fn restore_block_image_bytes(
    state: &XLogReaderState<'_>,
    block_id: u8,
    page: &mut [u8],
) -> PgResult<bool> {
    match decode_block_image_page(state, block_id)? {
        Some(p) => {
            page[..BLCKSZ].copy_from_slice(&p[..BLCKSZ]);
            Ok(true)
        }
        None => Ok(false),
    }
}

/// Shared decode core for `RestoreBlockImage`: validate the block reference,
/// decompress if needed, and re-insert the page "hole" as zeroes, yielding the
/// reconstructed `BLCKSZ` page. Returns `Ok(None)` when a failure message has
/// been recorded on the reader (the public wrappers then return `Ok(false)`),
/// `Err` for a hard `ereport(ERROR)`.
fn decode_block_image_page(
    state: &XLogReaderState<'_>,
    block_id: u8,
) -> PgResult<Option<Vec<u8>>> {
    // Validate the block reference + image presence (reads on the current rec).
    let d = match current(state) {
        Some(d) => d,
        None => {
            let (h, l) = lsn_fmt(state.ReadRecPtr);
            set_restore_error(
                state,
                format!(
                    "could not restore image at {:X}/{:X} with invalid block {} specified",
                    h, l, block_id
                ),
            );
            return Ok(None);
        }
    };
    if (block_id as i32) > d.max_block_id() || !d.has_block_ref(block_id as usize) {
        let (h, l) = lsn_fmt(state.ReadRecPtr);
        set_restore_error(
            state,
            format!(
                "could not restore image at {:X}/{:X} with invalid block {} specified",
                h, l, block_id
            ),
        );
        return Ok(None);
    }
    if !d.blocks()[block_id as usize].has_image() {
        let (h, l) = lsn_fmt(state.ReadRecPtr);
        set_restore_error(
            state,
            format!(
                "could not restore image at {:X}/{:X} with invalid state, block {}",
                h, l, block_id
            ),
        );
        return Ok(None);
    }

    // Snapshot the block-image inputs.
    let (bimg_info, bimg_len, hole_offset, hole_length, raw) = {
        let blk = &d.blocks()[block_id as usize];
        (
            blk.bimg_info(),
            blk.bimg_len() as usize,
            blk.hole_offset() as usize,
            blk.hole_length() as usize,
            blk.bkp_image().unwrap_or(&[]).to_vec(),
        )
    };

    let mut tmp = alloc::vec![0u8; BLCKSZ];
    let ptr: Vec<u8>;

    if BKPIMAGE_COMPRESSED(bimg_info) {
        if (bimg_info & BKPIMAGE_COMPRESS_PGLZ) != 0 {
            match pglz::pglz_decompress_to_slice(
                &raw[..bimg_len],
                &mut tmp[..BLCKSZ - hole_length],
                true,
            ) {
                Ok(_) => {}
                Err(_) => {
                    let (h, l) = lsn_fmt(state.ReadRecPtr);
                    set_restore_error(
                        state,
                        format!("could not decompress image at {:X}/{:X}, block {}", h, l, block_id),
                    );
                    return Ok(None);
                }
            }
        } else if (bimg_info & BKPIMAGE_COMPRESS_LZ4) != 0 {
            // USE_LZ4 not defined in this build (mirrors the C #else).
            let (h, l) = lsn_fmt(state.ReadRecPtr);
            set_restore_error(
                state,
                format!(
                    "could not restore image at {:X}/{:X} compressed with {} not supported by build, block {}",
                    h, l, "LZ4", block_id
                ),
            );
            return Ok(None);
        } else if (bimg_info & BKPIMAGE_COMPRESS_ZSTD) != 0 {
            // USE_ZSTD not defined in this build (mirrors the C #else).
            let (h, l) = lsn_fmt(state.ReadRecPtr);
            set_restore_error(
                state,
                format!(
                    "could not restore image at {:X}/{:X} compressed with {} not supported by build, block {}",
                    h, l, "zstd", block_id
                ),
            );
            return Ok(None);
        } else {
            let (h, l) = lsn_fmt(state.ReadRecPtr);
            set_restore_error(
                state,
                format!(
                    "could not restore image at {:X}/{:X} compressed with unknown method, block {}",
                    h, l, block_id
                ),
            );
            return Ok(None);
        }
        ptr = tmp;
    } else {
        ptr = raw;
    }

    // Generate the reconstructed page, re-inserting the hole as zeroes.
    let mut page = alloc::vec![0u8; BLCKSZ];
    if hole_length == 0 {
        page[..BLCKSZ].copy_from_slice(&ptr[..BLCKSZ]);
    } else {
        page[..hole_offset].copy_from_slice(&ptr[..hole_offset]);
        // bytes [hole_offset, hole_offset+hole_length) stay zero
        page[hole_offset + hole_length..BLCKSZ].copy_from_slice(
            &ptr[hole_offset..hole_offset + (BLCKSZ - (hole_offset + hole_length))],
        );
    }

    Ok(Some(page))
}

// ===========================================================================
// Reader field accessors + setters (xlogutils consumes these).
// ===========================================================================

/// `state->readLen`.
pub fn reader_read_len(state: &XLogReaderState<'_>) -> u32 {
    state.readLen
}
/// `state->seg.ws_segno`.
pub fn reader_seg_segno(state: &XLogReaderState<'_>) -> XLogSegNo {
    state.seg.ws_segno
}
/// `state->segoff`.
pub fn reader_segoff(state: &XLogReaderState<'_>) -> u32 {
    state.segoff
}
/// `state->segcxt.ws_segsize`.
pub fn reader_seg_size(state: &XLogReaderState<'_>) -> i32 {
    state.segcxt.ws_segsize
}
/// `state->currTLI`.
pub fn reader_curr_tli(state: &XLogReaderState<'_>) -> TimeLineID {
    state.currTLI
}
/// `state->currTLIValidUntil`.
pub fn reader_curr_tli_valid_until(state: &XLogReaderState<'_>) -> XLogRecPtr {
    state.currTLIValidUntil
}
/// `state->currTLI = tli`.
pub fn reader_set_curr_tli(state: &mut XLogReaderState<'_>, tli: TimeLineID) {
    state.currTLI = tli;
}
/// `state->currTLIValidUntil = lsn`.
pub fn reader_set_curr_tli_valid_until(state: &mut XLogReaderState<'_>, lsn: XLogRecPtr) {
    state.currTLIValidUntil = lsn;
}
/// `state->nextTLI = tli`.
pub fn reader_set_next_tli(state: &mut XLogReaderState<'_>, tli: TimeLineID) {
    state.nextTLI = tli;
}
/// `state->seg.ws_file = fd`.
pub fn reader_set_ws_file(state: &mut XLogReaderState<'_>, fd: i32) {
    state.seg.ws_file = fd;
}
/// `close(state->seg.ws_file); state->seg.ws_file = -1`. The actual `close()`
/// is performed by the segment_close callback / OS layer; here we reset the fd
/// to the C negative-fd convention (the file-descriptor lifecycle is owned by
/// the fd layer, the reader only tracks the number).
pub fn reader_close_ws_file(state: &mut XLogReaderState<'_>) {
    state.seg.ws_file = -1;
}

/// `((ReadLocalXLogPageNoWaitPrivate *) state->private_data)->end_of_wal = true`.
/// Flags the no-wait page-read caller that end of WAL was reached. The concrete
/// private struct is owned by the allocator; here we set the shared end-of-wal
/// flag on the private block the owner downcasts.
pub fn reader_set_private_end_of_wal(state: &mut XLogReaderState<'_>) {
    if let Some(pd) = state.private_data.as_mut() {
        if let Some(p) = pd.downcast_mut::<ReadLocalXLogPageNoWaitPrivate>() {
            p.end_of_wal = true;
        }
    }
}

/// `ReadLocalXLogPageNoWaitPrivate` (xlogutils.h) — the private block for the
/// no-wait local page-read callback. The reader stores it as the type-erased
/// `private_data`; the no-wait caller reads `end_of_wal` back.
#[derive(Clone, Copy, Debug, Default)]
pub struct ReadLocalXLogPageNoWaitPrivate {
    /// `end_of_wal` — set by `reader_set_private_end_of_wal`.
    pub end_of_wal: bool,
}

// ===========================================================================
// Decode-queue LSN accessors + read-ahead record projections (prefetcher).
// ===========================================================================

/// `reader->decode_queue_head->lsn` — start LSN of the oldest decoded record.
pub fn decode_queue_head_lsn(state: &XLogReaderState<'_>) -> Option<XLogRecPtr> {
    queue_head(state).map(|r| r.lsn())
}

/// `reader->decode_queue_tail->lsn` — start LSN of the newest decoded record.
pub fn decode_queue_tail_lsn(state: &XLogReaderState<'_>) -> Option<XLogRecPtr> {
    queue_tail(state).map(|r| r.lsn())
}

/// `&record->blocks[block_id]` of the decode-queue tail (the record
/// `XLogReadAhead` just returned): a copy of the decoded block reference.
pub fn read_ahead_record_block<'r>(
    state: &'r XLogReaderState<'_>,
    block_id: i32,
) -> DecodedBkpBlock<'r> {
    match queue_tail(state) {
        Some(r) if block_id >= 0 && (block_id as usize) < r.blocks().len() => {
            r.blocks()[block_id as usize]
        }
        _ => DecodedBkpBlock::default(),
    }
}

/// `record->main_data` of the decode-queue tail.
pub fn read_ahead_record_main_data<'r>(state: &'r XLogReaderState<'_>) -> &'r [u8] {
    queue_tail(state).map(|r| r.main_data()).unwrap_or(&[])
}

/// `record->blocks[block_id].prefetch_buffer = buffer` on the decode-queue tail.
pub fn set_read_ahead_record_prefetch_buffer(
    state: &mut XLogReaderState<'_>,
    block_id: i32,
    buffer: Buffer,
) {
    if let Some(q) = state.decode_queue.as_mut() {
        if let Some(r) = q.last_mut() {
            r.set_block_prefetch_buffer(block_id, buffer);
        }
    }
}

/// `XLogReadAhead` header projection of the decode-queue tail for the
/// prefetcher (the `ReadAheadRecordInfo` Copy facts).
pub fn read_ahead_record_info(
    state: &XLogReaderState<'_>,
) -> Option<::wal::ReadAheadRecordInfo> {
    let r = queue_tail(state)?;
    Some(::wal::ReadAheadRecordInfo {
        lsn: r.lsn(),
        xl_rmid: r.header().rmid(),
        xl_info: r.header().info(),
        max_block_id: r.max_block_id(),
    })
}

/// `XLogRecGetFullXid(record)` (xlogreader.c:2187) — the `FullTransactionId`
/// of the reader's current record. It is only safe during replay because it
/// depends on the replay state (`TransamVariables->nextXid`); see
/// `AdvanceNextFullTransactionIdPastXid` for more.
///
/// `FullTransactionIdFromAllowableAt(TransamVariables->nextXid,
/// XLogRecGetXid(record))` (transam.h:380): recover the epoch for the record's
/// bare `xl_xid`, which is known to precede-or-equal the next full xid. The
/// `TransamVariables->nextXid` read is the varsup-owned
/// `read_next_full_transaction_id` seam. The `current(state)` borrow mirrors the
/// C `record->record->header.xl_xid` dereference (the caller guarantees a
/// decoded current record during replay).
pub fn XLogRecGetFullXid(state: &XLogReaderState<'_>) -> ::types_core::FullTransactionId {
    use varsup_seams as varsup;
    use ::types_core::xact::TransactionIdIsNormal;
    use ::types_core::FullTransactionId;

    let xid = current(state)
        .expect("XLogRecGetFullXid requires a decoded current record")
        .xid();

    // FullTransactionIdFromAllowableAt(TransamVariables->nextXid, xid):

    // Special transaction ID.
    if !TransactionIdIsNormal(xid) {
        return FullTransactionId::from_epoch_and_xid(0, xid);
    }

    let next_full_xid = varsup::read_next_full_transaction_id::call();

    // The 64-bit result must be <= nextFullXid, so xid is from the epoch of
    // nextFullXid or the epoch before.
    let mut epoch = next_full_xid.epoch();
    if xid > next_full_xid.xid() {
        debug_assert!(epoch != 0);
        epoch -= 1;
    }
    FullTransactionId::from_epoch_and_xid(epoch, xid)
}

// ---------------------------------------------------------------------------
// RelFileLocator conversion: the seam's `XLogBlockTag` uses
// `types_storage::RelFileLocator`; the decoded block carries
// `::wal::RelFileLocator`. They are the same ABI (three Oids).
// ---------------------------------------------------------------------------

fn to_storage_locator(loc: RelFileLocator) -> types_storage::RelFileLocator {
    types_storage::RelFileLocator {
        spcOid: loc.spc_oid(),
        dbOid: loc.db_oid(),
        relNumber: loc.rel_number(),
    }
}
