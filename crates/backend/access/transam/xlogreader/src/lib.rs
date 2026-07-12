//! xlogreader.c — WAL page fetch, per-record header walk, CRC validation,
//! cross-page reassembly, and block-reference decode.
//!
//! Divergences from the C shape, per the day-one rules: the circular decode
//! buffer is reader-owned retained scratch holding only payload bytes (the C
//! ring math is reproduced on offsets so oversized/WOULDBLOCK behavior
//! matches); decoded records are index ranges into that scratch, and every
//! consumer read is a borrowed `&[u8]` view resolved at access time; the C
//! `XLogReaderRoutine` fn-pointer table is a generic trait over the closed
//! reader set (M1: the startup/local reader; walsender and debug readers
//! implement the same trait later, each call site monomorphizing).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::manual_range_patterns)]
#![allow(clippy::result_unit_err)]

use core::fmt::{self, Write as _};

use mcx::{vec_append_bytes, vec_with_capacity_in, Mcx, PgVec};
use types_core::{
    BlockNumber, Buffer, ForkNumber, InvalidRepOriginId, InvalidTransactionId, InvalidXLogRecPtr,
    RelFileNumber, RepOriginId, RmgrId, TimeLineID, TransactionId, XLogRecPtr, XLogSegNo,
};
use types_error::PgResult;
use types_storage::RelFileLocator;
use xlogreader_seams::{
    DecodedXLogRecord as ViewRecord, WALOpenSegment, WALReadError, WALSegmentContext,
    XLogReaderState as ReaderView, BKPBLOCK_FORK_MASK, BKPBLOCK_HAS_DATA, BKPBLOCK_HAS_IMAGE,
    BKPBLOCK_SAME_REL, XLOG_BLCKSZ, XLR_MAX_BLOCK_ID,
};

#[cfg(test)]
mod tests;

pub const XLREAD_SUCCESS: i32 = 0;
pub const XLREAD_FAIL: i32 = -1;
pub const XLREAD_WOULDBLOCK: i32 = -2;

const MAX_ERRORMSG_LEN: usize = 1000;
const DEFAULT_DECODE_BUFFER_SIZE: usize = 64 * 1024;
const MAXIMUM_ALIGNOF: usize = 8;
const BLCKSZ: usize = 8192;

const XLR_INFO_MASK: u8 = 0x0F;
const XLR_BLOCK_ID_DATA_SHORT: u8 = 255;
const XLR_BLOCK_ID_DATA_LONG: u8 = 254;
const XLR_BLOCK_ID_ORIGIN: u8 = 253;
const XLR_BLOCK_ID_TOPLEVEL_XID: u8 = 252;

const BKPIMAGE_HAS_HOLE: u8 = 0x01;
const BKPIMAGE_APPLY: u8 = 0x02;
const BKPIMAGE_COMPRESS_PGLZ: u8 = 0x04;
const BKPIMAGE_COMPRESS_LZ4: u8 = 0x08;
const BKPIMAGE_COMPRESS_ZSTD: u8 = 0x10;

const fn BKPIMAGE_COMPRESSED(info: u8) -> bool {
    info & (BKPIMAGE_COMPRESS_PGLZ | BKPIMAGE_COMPRESS_LZ4 | BKPIMAGE_COMPRESS_ZSTD) != 0
}

const XLP_FIRST_IS_CONTRECORD: u16 = 0x0001;
const XLP_LONG_HEADER: u16 = 0x0002;
const XLP_FIRST_IS_OVERWRITE_CONTRECORD: u16 = 0x0008;
const XLP_ALL_FLAGS: u16 = 0x000F;
pub const XLOG_PAGE_MAGIC: u16 = 0xD118;

const XLOG_SWITCH: u8 = 0x40;
const RM_XLOG_ID: RmgrId = 0;

pub const SIZE_OF_XLOG_RECORD: usize = 24;
const OFFSETOF_XLOG_RECORD_XL_CRC: usize = 20;
const SIZEOF_REL_FILE_LOCATOR: usize = 12;
pub const SIZE_OF_XLOG_SHORT_PHD: usize = 24;
pub const SIZE_OF_XLOG_LONG_PHD: usize = 40;

const fn MAXALIGN(len: usize) -> usize {
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

pub fn XLByteToSeg(xlrp: XLogRecPtr, wal_segsz_bytes: i32) -> XLogSegNo {
    xlrp / wal_segsz_bytes as u64
}

pub fn XLogSegmentOffset(xlogptr: XLogRecPtr, wal_segsz_bytes: i32) -> u32 {
    (xlogptr & (wal_segsz_bytes as u64 - 1)) as u32
}

fn XLByteInSeg(xlrp: XLogRecPtr, log_seg_no: XLogSegNo, wal_segsz_bytes: i32) -> bool {
    xlrp / wal_segsz_bytes as u64 == log_seg_no
}

fn XRecOffIsValid(xlrp: XLogRecPtr) -> bool {
    let offset = (xlrp % XLOG_BLCKSZ as u64) as usize;
    offset >= SIZE_OF_XLOG_SHORT_PHD
        && (offset <= XLOG_BLCKSZ - SIZE_OF_XLOG_RECORD || offset >= SIZE_OF_XLOG_LONG_PHD)
}

fn lsn_fmt(lsn: XLogRecPtr) -> (u32, u32) {
    ((lsn >> 32) as u32, lsn as u32)
}

pub fn XLogFileName(tli: TimeLineID, log_seg_no: XLogSegNo, wal_segsz_bytes: i32) -> String {
    let segs_per_id: u64 = 0x1_0000_0000u64 / wal_segsz_bytes as u64;
    format!(
        "{:08X}{:08X}{:08X}",
        tli,
        log_seg_no / segs_per_id,
        log_seg_no % segs_per_id
    )
}

fn crc_init() -> u32 {
    0xFFFF_FFFF
}
fn crc_comp(crc: u32, data: &[u8]) -> u32 {
    crc32c::pg_comp_crc32c(crc, data)
}
fn crc_fin(crc: u32) -> u32 {
    crc ^ 0xFFFF_FFFF
}

// Wire parsing is native-order field reads (the C struct memcpy; WAL is not
// endianness-portable).
fn XLogPageHeaderSize(info: u16) -> usize {
    if info & XLP_LONG_HEADER != 0 {
        SIZE_OF_XLOG_LONG_PHD
    } else {
        SIZE_OF_XLOG_SHORT_PHD
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct PageHeaderView {
    xlp_magic: u16,
    xlp_info: u16,
    xlp_tli: TimeLineID,
    xlp_pageaddr: XLogRecPtr,
    xlp_rem_len: u32,
    xlp_sysid: u64,
    xlp_seg_size: u32,
    xlp_xlog_blcksz: u32,
}

fn read_u16(b: &[u8], off: usize) -> u16 {
    u16::from_ne_bytes([b[off], b[off + 1]])
}
fn read_u32(b: &[u8], off: usize) -> u32 {
    u32::from_ne_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]])
}
fn read_u64(b: &[u8], off: usize) -> u64 {
    u64::from_ne_bytes([
        b[off],
        b[off + 1],
        b[off + 2],
        b[off + 3],
        b[off + 4],
        b[off + 5],
        b[off + 6],
        b[off + 7],
    ])
}

fn parse_page_header(buf: &[u8]) -> PageHeaderView {
    let mut v = PageHeaderView {
        xlp_magic: read_u16(buf, 0),
        xlp_info: read_u16(buf, 2),
        xlp_tli: read_u32(buf, 4),
        xlp_pageaddr: read_u64(buf, 8),
        xlp_rem_len: read_u32(buf, 16),
        ..Default::default()
    };
    if v.xlp_info & XLP_LONG_HEADER != 0 && buf.len() >= SIZE_OF_XLOG_LONG_PHD {
        v.xlp_sysid = read_u64(buf, 24);
        v.xlp_seg_size = read_u32(buf, 32);
        v.xlp_xlog_blcksz = read_u32(buf, 36);
    }
    v
}

#[derive(Clone, Copy, Debug, Default)]
pub struct XLogRecord {
    pub xl_tot_len: u32,
    pub xl_xid: TransactionId,
    pub xl_prev: XLogRecPtr,
    pub xl_info: u8,
    pub xl_rmid: RmgrId,
    pub xl_crc: u32,
}

fn parse_xlog_record(buf: &[u8]) -> XLogRecord {
    XLogRecord {
        xl_tot_len: read_u32(buf, 0),
        xl_xid: read_u32(buf, 4),
        xl_prev: read_u64(buf, 8),
        xl_info: buf[16],
        xl_rmid: buf[17],
        xl_crc: read_u32(buf, 20),
    }
}

fn parse_rel_file_locator(b: &[u8]) -> RelFileLocator {
    RelFileLocator {
        spcOid: read_u32(b, 0),
        dbOid: read_u32(b, 4),
        relNumber: read_u32(b, 8) as RelFileNumber,
    }
}

fn record_crc(hdr: &XLogRecord, record_bytes: &[u8]) -> u32 {
    debug_assert!(hdr.xl_tot_len as usize >= SIZE_OF_XLOG_RECORD);
    let mut crc = crc_init();
    crc = crc_comp(crc, &record_bytes[SIZE_OF_XLOG_RECORD..hdr.xl_tot_len as usize]);
    crc = crc_comp(crc, &record_bytes[..OFFSETOF_XLOG_RECORD_XL_CRC]);
    crc_fin(crc)
}

// The C DecodedXLogRecord/DecodedBkpBlock layouts, used only for the ring
// footprint math so oversized/WOULDBLOCK accounting matches C byte-for-byte.
mod layout {
    use super::*;
    #[repr(C)]
    pub struct DecodedBkpBlockLayout {
        in_use: bool,
        rlocator: RelFileLocator,
        forknum: i32,
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
    struct XLogRecordLayout {
        xl_tot_len: u32,
        xl_xid: TransactionId,
        xl_prev: XLogRecPtr,
        xl_info: u8,
        xl_rmid: RmgrId,
        xl_crc: u32,
    }
    #[repr(C)]
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

pub fn DecodeXLogRecordRequiredSpace(xl_tot_len: usize) -> usize {
    SIZEOF_DECODED_XLOG_RECORD_FIXED
        + SIZEOF_DECODED_BKP_BLOCK * (XLR_MAX_BLOCK_ID + 1)
        + xl_tot_len
        + (MAXIMUM_ALIGNOF - 1)
        + (MAXIMUM_ALIGNOF - 1) * (XLR_MAX_BLOCK_ID + 1)
        + (MAXIMUM_ALIGNOF - 1)
}

// The C XLogReaderRoutine, as a generic over the closed set of readers.
pub trait XLogSegmentRoutine {
    fn segment_open(
        &mut self,
        v: &mut ReaderView,
        next_seg_no: XLogSegNo,
        tli: &mut TimeLineID,
    ) -> PgResult<()>;
    fn segment_close(&mut self, v: &mut ReaderView);
}

pub trait XLogReaderRoutine: XLogSegmentRoutine {
    fn page_read(
        &mut self,
        v: &mut ReaderView,
        target_page_ptr: XLogRecPtr,
        req_len: i32,
        target_rec_ptr: XLogRecPtr,
        cur_page: &mut [u8],
    ) -> PgResult<i32>;
}

// The local (this server's pg_wal) reader: xlogutils' callbacks. Startup
// recovery brings its own XLogPageRead impl in xlogrecovery.
pub struct LocalPageRead {
    pub wait_for_wal: bool,
}

impl XLogSegmentRoutine for LocalPageRead {
    fn segment_open(
        &mut self,
        v: &mut ReaderView,
        next_seg_no: XLogSegNo,
        tli: &mut TimeLineID,
    ) -> PgResult<()> {
        xlogutils::wal_segment_open(v, next_seg_no, tli)
    }
    fn segment_close(&mut self, v: &mut ReaderView) {
        xlogutils::wal_segment_close(v);
    }
}

impl XLogReaderRoutine for LocalPageRead {
    fn page_read(
        &mut self,
        v: &mut ReaderView,
        target_page_ptr: XLogRecPtr,
        req_len: i32,
        target_rec_ptr: XLogRecPtr,
        cur_page: &mut [u8],
    ) -> PgResult<i32> {
        if self.wait_for_wal {
            xlogutils::read_local_xlog_page(v, target_page_ptr, req_len, target_rec_ptr, cur_page)
        } else {
            xlogutils::read_local_xlog_page_no_wait(
                v,
                target_page_ptr,
                req_len,
                target_rec_ptr,
                cur_page,
            )
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct PayloadRange {
    off: u32,
    len: u32,
}

#[derive(Clone, Copy, Debug, Default)]
struct BlockRef {
    in_use: bool,
    rlocator: RelFileLocator,
    forknum: ForkNumber,
    blkno: BlockNumber,
    prefetch_buffer: Buffer,
    flags: u8,
    has_image: bool,
    apply_image: bool,
    hole_offset: u16,
    hole_length: u16,
    bimg_len: u16,
    bimg_info: u8,
    has_data: bool,
    data_len: u16,
    bkp_image: PayloadRange,
    data: PayloadRange,
}

pub struct DecodedXLogRecord<'mcx> {
    size: usize,
    oversized: bool,
    buffer_offset: usize,
    pub lsn: XLogRecPtr,
    pub next_lsn: XLogRecPtr,
    pub header: XLogRecord,
    pub record_origin: RepOriginId,
    pub toplevel_xid: TransactionId,
    main_data: PayloadRange,
    pub max_block_id: i32,
    blocks_start: u32,
    nblocks: u32,
    overflow: Option<PgVec<'mcx, u8>>,
}

struct ErrSink<'a, 'mcx> {
    buf: &'a mut PgVec<'mcx, u8>,
    deferred: &'a mut bool,
}

impl ErrSink<'_, '_> {
    #[cold]
    fn report(&mut self, args: fmt::Arguments<'_>) {
        self.buf.clear();
        let mut w = CapWriter { buf: self.buf };
        let _ = w.write_fmt(args);
        *self.deferred = true;
    }
}

struct CapWriter<'a, 'mcx> {
    buf: &'a mut PgVec<'mcx, u8>,
}

impl fmt::Write for CapWriter<'_, '_> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        let room = MAX_ERRORMSG_LEN.saturating_sub(self.buf.len());
        let take = s.len().min(room);
        vec_append_bytes(self.buf, &s.as_bytes()[..take]).map_err(|_| fmt::Error)
    }
}

macro_rules! report_invalid {
    ($sink:expr, $($arg:tt)*) => {{ $sink.report(format_args!($($arg)*)); }};
}

struct AllocSlot {
    offset: usize,
    oversized: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct ReadAheadRecordInfo {
    pub lsn: XLogRecPtr,
    pub xl_rmid: RmgrId,
    pub xl_info: u8,
    pub max_block_id: i32,
}

#[derive(Clone, Copy, Debug)]
pub struct ReadAheadBlock {
    pub in_use: bool,
    pub rlocator: RelFileLocator,
    pub forknum: ForkNumber,
    pub blkno: BlockNumber,
    pub flags: u8,
    pub has_image: bool,
    pub prefetch_buffer: Buffer,
}

pub struct XLogReaderState<'mcx> {
    /// The consumer-facing projection (redo/xlogutils vocabulary); page-read
    /// callbacks receive `&mut v` and may set seg/timeline/end-of-wal fields.
    pub v: ReaderView,
    pub system_identifier: u64,
    pub nonblocking: bool,
    pub abortedRecPtr: XLogRecPtr,
    pub missingContrecPtr: XLogRecPtr,
    pub overwrittenRecPtr: XLogRecPtr,
    currRecPtr: XLogRecPtr,
    latestPagePtr: XLogRecPtr,
    latestPageTLI: TimeLineID,
    pub DecodeRecPtr: XLogRecPtr,
    pub NextRecPtr: XLogRecPtr,
    errormsg_buf: PgVec<'mcx, u8>,
    pub errormsg_deferred: bool,
    errormsg_exposed: bool,
    read_buf: PgVec<'mcx, u8>,
    read_record_buf: PgVec<'mcx, u8>,
    read_record_buf_size: u32,
    decode_buffer: PgVec<'mcx, u8>,
    decode_buffer_size: usize,
    decode_buffer_head: usize,
    decode_buffer_tail: usize,
    queue: PgVec<'mcx, DecodedXLogRecord<'mcx>>,
    blocks_pool: PgVec<'mcx, BlockRef>,
    current: Option<DecodedXLogRecord<'mcx>>,
    mcx: Mcx<'mcx>,
}

fn allocate_recordbuf(state: &mut XLogReaderState<'_>, reclength: u32) -> PgResult<()> {
    let mut new_size = reclength;
    new_size = new_size.wrapping_add(XLOG_BLCKSZ as u32 - (new_size % XLOG_BLCKSZ as u32));
    new_size = new_size.max(5 * (BLCKSZ as u32).max(XLOG_BLCKSZ as u32));

    let mut buf: PgVec<'_, u8> = vec_with_capacity_in(state.mcx, new_size as usize)?;
    buf.resize(new_size as usize, 0);
    state.read_record_buf = buf;
    state.read_record_buf_size = new_size;
    Ok(())
}

impl<'mcx> XLogReaderState<'mcx> {
    /// `XLogReaderAllocate` (no waldir: the segcxt vocabulary carries only
    /// ws_segsize; path construction lives in the segment_open callbacks).
    pub fn allocate(mcx: Mcx<'mcx>, wal_segment_size: i32) -> PgResult<Self> {
        let mut read_buf: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, XLOG_BLCKSZ)?;
        read_buf.resize(XLOG_BLCKSZ, 0);
        let mut state = XLogReaderState {
            v: ReaderView {
                seg: WALOpenSegment::default(),
                segcxt: WALSegmentContext {
                    ws_segsize: wal_segment_size,
                },
                ..Default::default()
            },
            system_identifier: 0,
            nonblocking: false,
            abortedRecPtr: InvalidXLogRecPtr,
            missingContrecPtr: InvalidXLogRecPtr,
            overwrittenRecPtr: InvalidXLogRecPtr,
            currRecPtr: InvalidXLogRecPtr,
            latestPagePtr: InvalidXLogRecPtr,
            latestPageTLI: 0,
            DecodeRecPtr: InvalidXLogRecPtr,
            NextRecPtr: InvalidXLogRecPtr,
            errormsg_buf: vec_with_capacity_in(mcx, MAX_ERRORMSG_LEN)?,
            errormsg_deferred: false,
            errormsg_exposed: false,
            read_buf,
            read_record_buf: PgVec::new_in(mcx),
            read_record_buf_size: 0,
            decode_buffer: PgVec::new_in(mcx),
            decode_buffer_size: 0,
            decode_buffer_head: 0,
            decode_buffer_tail: 0,
            queue: PgVec::new_in(mcx),
            blocks_pool: PgVec::new_in(mcx),
            current: None,
            mcx,
        };
        allocate_recordbuf(&mut state, 0)?;
        Ok(state)
    }

    pub fn XLogReaderSetDecodeBuffer(&mut self, size: usize) {
        debug_assert!(self.decode_buffer.is_empty());
        self.decode_buffer_size = size;
        self.decode_buffer_head = 0;
        self.decode_buffer_tail = 0;
    }

    fn err(&mut self) -> ErrSink<'_, 'mcx> {
        ErrSink {
            buf: &mut self.errormsg_buf,
            deferred: &mut self.errormsg_deferred,
        }
    }

    /// The message exposed by the last consuming call that reported a
    /// deferred error (the C `*errormsg` out-parameter).
    pub fn errormsg(&self) -> Option<&str> {
        if !self.errormsg_exposed || self.errormsg_buf.is_empty() {
            return None;
        }
        core::str::from_utf8(&self.errormsg_buf).ok()
    }

    /// The raw error buffer, ignoring the exposure flag — C consumers
    /// (pg_walinspect) read `record->errormsg_buf` directly after a failed
    /// `RestoreBlockImage`, which never goes through the out-parameter path.
    pub fn errormsg_buf_raw(&self) -> Option<&str> {
        if self.errormsg_buf.is_empty() {
            return None;
        }
        core::str::from_utf8(&self.errormsg_buf).ok()
    }

    pub fn XLogReaderResetError(&mut self) {
        self.errormsg_buf.clear();
        self.errormsg_deferred = false;
        self.errormsg_exposed = false;
    }

    pub fn XLogBeginRead(&mut self, rec_ptr: XLogRecPtr) {
        debug_assert!(rec_ptr != InvalidXLogRecPtr);
        self.ResetDecoder();
        self.v.EndRecPtr = rec_ptr;
        self.NextRecPtr = rec_ptr;
        self.v.ReadRecPtr = InvalidXLogRecPtr;
        self.DecodeRecPtr = InvalidXLogRecPtr;
    }

    fn ResetDecoder(&mut self) {
        self.queue.clear();
        self.blocks_pool.clear();
        self.current = None;
        self.v.record = None;
        self.decode_buffer_head = 0;
        self.decode_buffer_tail = 0;
        self.errormsg_buf.clear();
        self.errormsg_deferred = false;
        self.errormsg_exposed = false;
    }

    pub fn XLogReaderHasQueuedRecordOrError(&self) -> bool {
        !self.queue.is_empty() || self.errormsg_deferred
    }

    pub fn XLogReleasePreviousRecord(&mut self) -> XLogRecPtr {
        let Some(rec) = self.current.take() else {
            return InvalidXLogRecPtr;
        };
        self.v.record = None;
        let next_lsn = rec.next_lsn;
        if !rec.oversized {
            debug_assert!(self.decode_buffer_head == rec.buffer_offset);
            match self
                .queue
                .iter()
                .find(|r| !r.oversized)
                .map(|r| r.buffer_offset)
            {
                Some(off) => self.decode_buffer_head = off,
                None => {
                    self.decode_buffer_head = 0;
                    self.decode_buffer_tail = 0;
                }
            }
        }
        if self.queue.is_empty() {
            self.blocks_pool.clear();
        }
        next_lsn
    }

    pub fn XLogNextRecord(&mut self) -> Option<XLogRecPtr> {
        self.XLogReleasePreviousRecord();
        if self.queue.is_empty() {
            self.errormsg_exposed = self.errormsg_deferred && !self.errormsg_buf.is_empty();
            self.errormsg_deferred = false;
            debug_assert!(self.v.EndRecPtr != InvalidXLogRecPtr);
            return None;
        }
        let head = self.queue.remove(0);
        let lsn = head.lsn;
        self.v.ReadRecPtr = head.lsn;
        self.v.EndRecPtr = head.next_lsn;
        self.errormsg_exposed = false;
        self.current = Some(head);
        self.marshal_view_record();
        Some(lsn)
    }

    /// `XLogReadRecord`: block until one record is decoded and consume it.
    /// `Ok(None)` = failure; `self.errormsg()` has the deferred message when
    /// the error wasn't already reported by the page-read callback.
    pub fn XLogReadRecord<R: XLogReaderRoutine>(
        &mut self,
        routine: &mut R,
    ) -> PgResult<Option<XLogRecPtr>> {
        self.XLogReleasePreviousRecord();
        if !self.XLogReaderHasQueuedRecordOrError() {
            self.XLogReadAhead(routine, false)?;
        }
        Ok(self.XLogNextRecord())
    }

    /// `Ok(Some(lsn))` = a new record is at the decode-queue tail.
    pub fn XLogReadAhead<R: XLogReaderRoutine>(
        &mut self,
        routine: &mut R,
        nonblocking: bool,
    ) -> PgResult<Option<XLogRecPtr>> {
        if self.errormsg_deferred {
            return Ok(None);
        }
        let result = self.XLogDecodeNextRecord(routine, nonblocking)?;
        if result == XLREAD_SUCCESS {
            let tail = self.queue.last().expect("decoded record queued");
            return Ok(Some(tail.lsn));
        }
        Ok(None)
    }

    // C decode_queue_head includes the record NextRecord handed out (it stays
    // queued until released); here that record lives in `current`.
    pub fn decode_queue_head_lsn(&self) -> Option<XLogRecPtr> {
        self.current.as_ref().or_else(|| self.queue.first()).map(|r| r.lsn)
    }

    pub fn decode_queue_tail_lsn(&self) -> Option<XLogRecPtr> {
        self.queue.last().or(self.current.as_ref()).map(|r| r.lsn)
    }

    pub fn read_ahead_record_info(&self) -> Option<ReadAheadRecordInfo> {
        self.queue.last().map(|r| ReadAheadRecordInfo {
            lsn: r.lsn,
            xl_rmid: r.header.xl_rmid,
            xl_info: r.header.xl_info,
            max_block_id: r.max_block_id,
        })
    }

    pub fn read_ahead_main_data(&self) -> &[u8] {
        let rec = self.queue.last().expect("read-ahead accessors require a queued record");
        self.payload(rec, rec.main_data)
    }

    pub fn read_ahead_block(&self, block_id: i32) -> ReadAheadBlock {
        let rec = self.queue.last().expect("read-ahead accessors require a queued record");
        let blk = self.block(rec, block_id as usize);
        ReadAheadBlock {
            in_use: blk.in_use,
            rlocator: blk.rlocator,
            forknum: blk.forknum,
            blkno: blk.blkno,
            flags: blk.flags,
            has_image: blk.has_image,
            prefetch_buffer: blk.prefetch_buffer,
        }
    }

    pub fn set_read_ahead_block_prefetch_buffer(&mut self, block_id: i32, buffer: Buffer) {
        let rec = self.queue.last().expect("read-ahead accessors require a queued record");
        debug_assert!((block_id as u32) < rec.nblocks);
        let idx = rec.blocks_start as usize + block_id as usize;
        self.blocks_pool[idx].prefetch_buffer = buffer;
    }

    fn XLogReadRecordAlloc(&mut self, xl_tot_len: usize, allow_oversized: bool) -> Option<AllocSlot> {
        let required_space = DecodeXLogRecordRequiredSpace(xl_tot_len);

        if self.decode_buffer.is_empty() {
            if self.decode_buffer_size == 0 {
                self.decode_buffer_size = DEFAULT_DECODE_BUFFER_SIZE;
            }
            self.decode_buffer.resize(self.decode_buffer_size, 0);
            self.decode_buffer_head = 0;
            self.decode_buffer_tail = 0;
        }

        if self.decode_buffer_tail >= self.decode_buffer_head {
            if required_space <= self.decode_buffer_size - self.decode_buffer_tail {
                return Some(AllocSlot {
                    offset: self.decode_buffer_tail,
                    oversized: false,
                });
            } else if required_space < self.decode_buffer_head {
                return Some(AllocSlot {
                    offset: 0,
                    oversized: false,
                });
            }
        } else if required_space < self.decode_buffer_head - self.decode_buffer_tail {
            return Some(AllocSlot {
                offset: self.decode_buffer_tail,
                oversized: false,
            });
        }

        if allow_oversized {
            return Some(AllocSlot {
                offset: 0,
                oversized: true,
            });
        }
        None
    }

    #[allow(unused_assignments)]
    fn XLogDecodeNextRecord<R: XLogReaderRoutine>(
        &mut self,
        routine: &mut R,
        nonblocking: bool,
    ) -> PgResult<i32> {
        let mut rand_access = false;

        self.errormsg_buf.clear();
        self.abortedRecPtr = InvalidXLogRecPtr;
        self.missingContrecPtr = InvalidXLogRecPtr;

        let mut rec_ptr: XLogRecPtr = self.NextRecPtr;
        if self.DecodeRecPtr != InvalidXLogRecPtr {
            // read the record after the one we just read
        } else {
            debug_assert!(rec_ptr % XLOG_BLCKSZ as u64 == 0 || XRecOffIsValid(rec_ptr));
            rand_access = true;
        }

        'restart: loop {
            self.nonblocking = nonblocking;
            self.v.nonblocking = nonblocking;
            self.currRecPtr = rec_ptr;
            let mut assembled = false;

            let mut target_page_ptr = rec_ptr - (rec_ptr % XLOG_BLCKSZ as u64);
            let mut target_rec_off = (rec_ptr % XLOG_BLCKSZ as u64) as u32;

            let mut read_off = self.ReadPageInternal(
                routine,
                target_page_ptr,
                (target_rec_off as i32 + SIZE_OF_XLOG_RECORD as i32).min(XLOG_BLCKSZ as i32),
            )?;
            if read_off == XLREAD_WOULDBLOCK {
                return Ok(XLREAD_WOULDBLOCK);
            } else if read_off < 0 {
                return Ok(self.decode_err(assembled, rec_ptr, target_page_ptr));
            }

            let phdr = parse_page_header(&self.read_buf);
            let mut page_header_size = XLogPageHeaderSize(phdr.xlp_info);
            if target_rec_off == 0 {
                rec_ptr += page_header_size as u64;
                target_rec_off = page_header_size as u32;
            } else if (target_rec_off as usize) < page_header_size {
                let (h, l) = lsn_fmt(rec_ptr);
                report_invalid!(
                    self.err(),
                    "invalid record offset at {:X}/{:X}: expected at least {}, got {}",
                    h,
                    l,
                    page_header_size,
                    target_rec_off
                );
                return Ok(self.decode_err(assembled, rec_ptr, target_page_ptr));
            }

            if phdr.xlp_info & XLP_FIRST_IS_CONTRECORD != 0
                && target_rec_off as usize == page_header_size
            {
                let (h, l) = lsn_fmt(rec_ptr);
                report_invalid!(self.err(), "contrecord is requested by {:X}/{:X}", h, l);
                return Ok(self.decode_err(assembled, rec_ptr, target_page_ptr));
            }

            debug_assert!(page_header_size <= read_off as usize);

            let rec_off_in_page = (rec_ptr % XLOG_BLCKSZ as u64) as usize;
            let total_len = read_u32(&self.read_buf, rec_off_in_page);

            let mut record_hdr = XLogRecord::default();
            let mut got_header = false;
            if target_rec_off as usize <= XLOG_BLCKSZ - SIZE_OF_XLOG_RECORD {
                record_hdr = parse_xlog_record(&self.read_buf[rec_off_in_page..]);
                if !self.ValidXLogRecordHeader(rec_ptr, self.DecodeRecPtr, &record_hdr, rand_access)
                {
                    return Ok(self.decode_err(assembled, rec_ptr, target_page_ptr));
                }
                got_header = true;
            } else if (total_len as usize) < SIZE_OF_XLOG_RECORD {
                let (h, l) = lsn_fmt(rec_ptr);
                report_invalid!(
                    self.err(),
                    "invalid record length at {:X}/{:X}: expected at least {}, got {}",
                    h,
                    l,
                    SIZE_OF_XLOG_RECORD,
                    total_len
                );
                return Ok(self.decode_err(assembled, rec_ptr, target_page_ptr));
            }

            let mut slot = self.XLogReadRecordAlloc(total_len as usize, false);
            if slot.is_none() && nonblocking {
                return Ok(XLREAD_WOULDBLOCK);
            }

            let len = XLOG_BLCKSZ as u32 - (rec_ptr % XLOG_BLCKSZ as u64) as u32;
            let from_record_buf: bool;

            if total_len > len {
                // Reassemble the record across pages into read_record_buf.
                assembled = true;
                debug_assert!(self.read_record_buf_size as usize >= XLOG_BLCKSZ * 2);
                debug_assert!(self.read_record_buf_size >= len);

                {
                    let XLogReaderState {
                        ref read_buf,
                        ref mut read_record_buf,
                        ..
                    } = *self;
                    read_record_buf[..len as usize]
                        .copy_from_slice(&read_buf[rec_off_in_page..rec_off_in_page + len as usize]);
                }
                let mut buffer = len as usize;
                let mut gotlen = len;
                let mut last_cont_rem_len = 0u32;

                loop {
                    target_page_ptr += XLOG_BLCKSZ as u64;

                    read_off =
                        self.ReadPageInternal(routine, target_page_ptr, SIZE_OF_XLOG_SHORT_PHD as i32)?;
                    if read_off == XLREAD_WOULDBLOCK {
                        return Ok(XLREAD_WOULDBLOCK);
                    } else if read_off < 0 {
                        return Ok(self.decode_err(assembled, rec_ptr, target_page_ptr));
                    }
                    debug_assert!(SIZE_OF_XLOG_SHORT_PHD <= read_off as usize);

                    let cont = parse_page_header(&self.read_buf);

                    if cont.xlp_info & XLP_FIRST_IS_OVERWRITE_CONTRECORD != 0 {
                        self.overwrittenRecPtr = rec_ptr;
                        rec_ptr = target_page_ptr;
                        continue 'restart;
                    }

                    if cont.xlp_info & XLP_FIRST_IS_CONTRECORD == 0 {
                        let (h, l) = lsn_fmt(rec_ptr);
                        report_invalid!(self.err(), "there is no contrecord flag at {:X}/{:X}", h, l);
                        return Ok(self.decode_err(assembled, rec_ptr, target_page_ptr));
                    }

                    if cont.xlp_rem_len == 0 || total_len != cont.xlp_rem_len + gotlen {
                        let (h, l) = lsn_fmt(rec_ptr);
                        report_invalid!(
                            self.err(),
                            "invalid contrecord length {} (expected {}) at {:X}/{:X}",
                            cont.xlp_rem_len,
                            total_len as i64 - gotlen as i64,
                            h,
                            l
                        );
                        return Ok(self.decode_err(assembled, rec_ptr, target_page_ptr));
                    }

                    read_off = self.ReadPageInternal(
                        routine,
                        target_page_ptr,
                        ((total_len - gotlen) as i32 + SIZE_OF_XLOG_SHORT_PHD as i32)
                            .min(XLOG_BLCKSZ as i32),
                    )?;
                    if read_off == XLREAD_WOULDBLOCK {
                        return Ok(XLREAD_WOULDBLOCK);
                    } else if read_off < 0 {
                        return Ok(self.decode_err(assembled, rec_ptr, target_page_ptr));
                    }

                    page_header_size = XLogPageHeaderSize(cont.xlp_info);

                    if (read_off as usize) < page_header_size {
                        read_off =
                            self.ReadPageInternal(routine, target_page_ptr, page_header_size as i32)?;
                        if read_off == XLREAD_WOULDBLOCK {
                            return Ok(XLREAD_WOULDBLOCK);
                        } else if read_off < 0 {
                            return Ok(self.decode_err(assembled, rec_ptr, target_page_ptr));
                        }
                    }
                    debug_assert!(page_header_size <= read_off as usize);

                    let mut cont_len = XLOG_BLCKSZ - page_header_size;
                    if (cont.xlp_rem_len as usize) < cont_len {
                        cont_len = cont.xlp_rem_len as usize;
                    }

                    if (read_off as usize) < page_header_size + cont_len {
                        read_off = self.ReadPageInternal(
                            routine,
                            target_page_ptr,
                            (page_header_size + cont_len) as i32,
                        )?;
                        if read_off == XLREAD_WOULDBLOCK {
                            return Ok(XLREAD_WOULDBLOCK);
                        } else if read_off < 0 {
                            return Ok(self.decode_err(assembled, rec_ptr, target_page_ptr));
                        }
                    }

                    {
                        let XLogReaderState {
                            ref read_buf,
                            ref mut read_record_buf,
                            ..
                        } = *self;
                        read_record_buf[buffer..buffer + cont_len]
                            .copy_from_slice(&read_buf[page_header_size..page_header_size + cont_len]);
                    }
                    buffer += cont_len;
                    gotlen += cont_len as u32;
                    last_cont_rem_len = cont.xlp_rem_len;

                    if !got_header {
                        record_hdr = parse_xlog_record(&self.read_record_buf);
                        if !self.ValidXLogRecordHeader(
                            rec_ptr,
                            self.DecodeRecPtr,
                            &record_hdr,
                            rand_access,
                        ) {
                            return Ok(self.decode_err(assembled, rec_ptr, target_page_ptr));
                        }
                        got_header = true;
                    }

                    if total_len > self.read_record_buf_size {
                        debug_assert!(gotlen as usize <= XLOG_BLCKSZ * 2);
                        debug_assert!(gotlen <= self.read_record_buf_size);
                        let mut save = [0u8; XLOG_BLCKSZ * 2];
                        save[..gotlen as usize]
                            .copy_from_slice(&self.read_record_buf[..gotlen as usize]);
                        if allocate_recordbuf(self, total_len).is_err() {
                            let (h, l) = lsn_fmt(rec_ptr);
                            report_invalid!(
                                self.err(),
                                "out of memory while reading WAL record at {:X}/{:X}",
                                h,
                                l
                            );
                            return Ok(self.decode_err(assembled, rec_ptr, target_page_ptr));
                        }
                        self.read_record_buf[..gotlen as usize].copy_from_slice(&save[..gotlen as usize]);
                        buffer = gotlen as usize;
                    }

                    if gotlen >= total_len {
                        break;
                    }
                }
                debug_assert!(got_header);
                let _ = buffer;

                record_hdr = parse_xlog_record(&self.read_record_buf);
                let crc_ok = record_crc(&record_hdr, &self.read_record_buf[..total_len as usize])
                    == record_hdr.xl_crc;
                if !crc_ok {
                    let (h, l) = lsn_fmt(rec_ptr);
                    report_invalid!(
                        self.err(),
                        "incorrect resource manager data checksum in record at {:X}/{:X}",
                        h,
                        l
                    );
                    return Ok(self.decode_err(assembled, rec_ptr, target_page_ptr));
                }

                page_header_size = XLogPageHeaderSize(parse_page_header(&self.read_buf).xlp_info);
                self.DecodeRecPtr = rec_ptr;
                self.NextRecPtr = target_page_ptr
                    + page_header_size as u64
                    + MAXALIGN(last_cont_rem_len as usize) as u64;
                from_record_buf = true;
            } else {
                // Record does not cross a page boundary.
                read_off = self.ReadPageInternal(
                    routine,
                    target_page_ptr,
                    (target_rec_off as i32 + total_len as i32).min(XLOG_BLCKSZ as i32),
                )?;
                if read_off == XLREAD_WOULDBLOCK {
                    return Ok(XLREAD_WOULDBLOCK);
                } else if read_off < 0 {
                    return Ok(self.decode_err(assembled, rec_ptr, target_page_ptr));
                }

                record_hdr = parse_xlog_record(&self.read_buf[rec_off_in_page..]);
                let crc_ok = record_crc(
                    &record_hdr,
                    &self.read_buf[rec_off_in_page..rec_off_in_page + total_len as usize],
                ) == record_hdr.xl_crc;
                if !crc_ok {
                    let (h, l) = lsn_fmt(rec_ptr);
                    report_invalid!(
                        self.err(),
                        "incorrect resource manager data checksum in record at {:X}/{:X}",
                        h,
                        l
                    );
                    return Ok(self.decode_err(assembled, rec_ptr, target_page_ptr));
                }

                self.NextRecPtr = rec_ptr + MAXALIGN(total_len as usize) as u64;
                self.DecodeRecPtr = rec_ptr;
                from_record_buf = false;
            }

            // Special processing if it's an XLOG SWITCH record.
            if record_hdr.xl_rmid == RM_XLOG_ID
                && (record_hdr.xl_info & !XLR_INFO_MASK) == XLOG_SWITCH
            {
                self.NextRecPtr += self.v.segcxt.ws_segsize as u64 - 1;
                self.NextRecPtr -=
                    XLogSegmentOffset(self.NextRecPtr, self.v.segcxt.ws_segsize) as u64;
            }

            if slot.is_none() {
                debug_assert!(!nonblocking);
                slot = self.XLogReadRecordAlloc(total_len as usize, true);
                debug_assert!(slot.is_some());
            }
            let slot = slot.unwrap();

            let mcx = self.mcx;
            let read_rec_ptr = self.v.ReadRecPtr;
            let next_lsn = self.NextRecPtr;
            let decoded = {
                let XLogReaderState {
                    ref read_buf,
                    ref read_record_buf,
                    ref mut decode_buffer,
                    ref mut blocks_pool,
                    ref mut errormsg_buf,
                    ref mut errormsg_deferred,
                    ..
                } = *self;
                let src: &[u8] = if from_record_buf {
                    &read_record_buf[..total_len as usize]
                } else {
                    &read_buf[rec_off_in_page..rec_off_in_page + total_len as usize]
                };
                let mut err = ErrSink {
                    buf: errormsg_buf,
                    deferred: errormsg_deferred,
                };
                decode_record(
                    mcx,
                    decode_buffer,
                    blocks_pool,
                    &slot,
                    &record_hdr,
                    src,
                    read_rec_ptr,
                    &mut err,
                )
            };

            match decoded {
                Some(mut decoded) => {
                    decoded.lsn = rec_ptr;
                    decoded.next_lsn = next_lsn;
                    if !decoded.oversized {
                        debug_assert_eq!(decoded.size, MAXALIGN(decoded.size));
                        self.decode_buffer_tail = slot.offset + decoded.size;
                    }
                    self.queue.push(decoded);
                    return Ok(XLREAD_SUCCESS);
                }
                None => return Ok(self.decode_err(assembled, rec_ptr, target_page_ptr)),
            }
        }
    }

    #[cold]
    fn decode_err(&mut self, assembled: bool, rec_ptr: XLogRecPtr, target_page_ptr: XLogRecPtr) -> i32 {
        if assembled {
            self.abortedRecPtr = rec_ptr;
            self.missingContrecPtr = target_page_ptr;
            self.errormsg_deferred = true;
        }
        self.XLogReaderInvalReadState();
        XLREAD_FAIL
    }

    fn ReadPageInternal<R: XLogReaderRoutine>(
        &mut self,
        routine: &mut R,
        pageptr: XLogRecPtr,
        req_len: i32,
    ) -> PgResult<i32> {
        debug_assert!(pageptr % XLOG_BLCKSZ as u64 == 0);

        let target_seg_no = XLByteToSeg(pageptr, self.v.segcxt.ws_segsize);
        let target_page_off = XLogSegmentOffset(pageptr, self.v.segcxt.ws_segsize);

        if target_seg_no == self.v.seg.ws_segno
            && target_page_off == self.v.segoff
            && req_len <= self.v.readLen as i32
        {
            return Ok(self.v.readLen as i32);
        }

        self.v.readLen = 0;

        if target_seg_no != self.v.seg.ws_segno && target_page_off != 0 {
            let target_segment_ptr = pageptr - target_page_off as u64;

            let read_len = self.call_page_read(routine, target_segment_ptr, XLOG_BLCKSZ as i32)?;
            if read_len == XLREAD_WOULDBLOCK {
                return Ok(XLREAD_WOULDBLOCK);
            } else if read_len < 0 {
                self.XLogReaderInvalReadState();
                return Ok(XLREAD_FAIL);
            }
            debug_assert_eq!(read_len, XLOG_BLCKSZ as i32);

            if !self.validate_page_header_from_read_buf(target_segment_ptr) {
                self.XLogReaderInvalReadState();
                return Ok(XLREAD_FAIL);
            }
        }

        let mut read_len =
            self.call_page_read(routine, pageptr, req_len.max(SIZE_OF_XLOG_SHORT_PHD as i32))?;
        if read_len == XLREAD_WOULDBLOCK {
            return Ok(XLREAD_WOULDBLOCK);
        } else if read_len < 0 {
            self.XLogReaderInvalReadState();
            return Ok(XLREAD_FAIL);
        }
        debug_assert!(read_len <= XLOG_BLCKSZ as i32);

        if read_len <= SIZE_OF_XLOG_SHORT_PHD as i32 {
            self.XLogReaderInvalReadState();
            return Ok(XLREAD_FAIL);
        }
        debug_assert!(read_len >= req_len);

        let hdr_size = XLogPageHeaderSize(read_u16(&self.read_buf, 2));

        if (read_len as usize) < hdr_size {
            read_len = self.call_page_read(routine, pageptr, hdr_size as i32)?;
            if read_len == XLREAD_WOULDBLOCK {
                return Ok(XLREAD_WOULDBLOCK);
            } else if read_len < 0 {
                self.XLogReaderInvalReadState();
                return Ok(XLREAD_FAIL);
            }
        }

        if !self.validate_page_header_from_read_buf(pageptr) {
            self.XLogReaderInvalReadState();
            return Ok(XLREAD_FAIL);
        }

        self.v.seg.ws_segno = target_seg_no;
        self.v.segoff = target_page_off;
        self.v.readLen = read_len as u32;
        Ok(read_len)
    }

    fn call_page_read<R: XLogReaderRoutine>(
        &mut self,
        routine: &mut R,
        target_page_ptr: XLogRecPtr,
        req_len: i32,
    ) -> PgResult<i32> {
        let curr_rec_ptr = self.currRecPtr;
        let XLogReaderState {
            ref mut v,
            ref mut read_buf,
            ..
        } = *self;
        routine.page_read(v, target_page_ptr, req_len, curr_rec_ptr, read_buf)
    }

    pub fn XLogReaderInvalReadState(&mut self) {
        self.v.seg.ws_segno = 0;
        self.v.segoff = 0;
        self.v.readLen = 0;
    }

    fn ValidXLogRecordHeader(
        &mut self,
        rec_ptr: XLogRecPtr,
        prev_rec_ptr: XLogRecPtr,
        record: &XLogRecord,
        rand_access: bool,
    ) -> bool {
        if (record.xl_tot_len as usize) < SIZE_OF_XLOG_RECORD {
            let (h, l) = lsn_fmt(rec_ptr);
            report_invalid!(
                self.err(),
                "invalid record length at {:X}/{:X}: expected at least {}, got {}",
                h,
                l,
                SIZE_OF_XLOG_RECORD,
                record.xl_tot_len
            );
            return false;
        }
        if !rmgr::RmgrIdIsValid(record.xl_rmid as i32) {
            let (h, l) = lsn_fmt(rec_ptr);
            report_invalid!(
                self.err(),
                "invalid resource manager ID {} at {:X}/{:X}",
                record.xl_rmid,
                h,
                l
            );
            return false;
        }
        if rand_access {
            if record.xl_prev >= rec_ptr {
                let (ph, pl) = lsn_fmt(record.xl_prev);
                let (h, l) = lsn_fmt(rec_ptr);
                report_invalid!(
                    self.err(),
                    "record with incorrect prev-link {:X}/{:X} at {:X}/{:X}",
                    ph,
                    pl,
                    h,
                    l
                );
                return false;
            }
        } else if record.xl_prev != prev_rec_ptr {
            let (ph, pl) = lsn_fmt(record.xl_prev);
            let (h, l) = lsn_fmt(rec_ptr);
            report_invalid!(
                self.err(),
                "record with incorrect prev-link {:X}/{:X} at {:X}/{:X}",
                ph,
                pl,
                h,
                l
            );
            return false;
        }
        true
    }

    fn validate_page_header_from_read_buf(&mut self, recptr: XLogRecPtr) -> bool {
        let XLogReaderState {
            ref read_buf,
            ref mut errormsg_buf,
            ref mut errormsg_deferred,
            ref mut latestPagePtr,
            ref mut latestPageTLI,
            ref v,
            system_identifier,
            ..
        } = *self;
        let mut err = ErrSink {
            buf: errormsg_buf,
            deferred: errormsg_deferred,
        };
        validate_page_header(
            v.seg.ws_tli,
            v.segcxt.ws_segsize,
            system_identifier,
            latestPagePtr,
            latestPageTLI,
            &mut err,
            recptr,
            read_buf,
        )
    }

    pub fn XLogReaderValidatePageHeader(&mut self, recptr: XLogRecPtr, phdr: &[u8]) -> bool {
        let XLogReaderState {
            ref mut errormsg_buf,
            ref mut errormsg_deferred,
            ref mut latestPagePtr,
            ref mut latestPageTLI,
            ref v,
            system_identifier,
            ..
        } = *self;
        let mut err = ErrSink {
            buf: errormsg_buf,
            deferred: errormsg_deferred,
        };
        validate_page_header(
            v.seg.ws_tli,
            v.segcxt.ws_segsize,
            system_identifier,
            latestPagePtr,
            latestPageTLI,
            &mut err,
            recptr,
            phdr,
        )
    }

    pub fn XLogFindNextRecord<R: XLogReaderRoutine>(
        &mut self,
        routine: &mut R,
        rec_ptr: XLogRecPtr,
    ) -> PgResult<XLogRecPtr> {
        debug_assert!(rec_ptr != InvalidXLogRecPtr);

        self.nonblocking = false;
        self.v.nonblocking = false;

        let mut tmp_rec_ptr = rec_ptr;
        loop {
            let target_rec_off = (tmp_rec_ptr % XLOG_BLCKSZ as u64) as i32;
            let target_page_ptr = tmp_rec_ptr - target_rec_off as u64;

            let read_len = self.ReadPageInternal(routine, target_page_ptr, target_rec_off)?;
            if read_len < 0 {
                self.XLogReaderInvalReadState();
                return Ok(InvalidXLogRecPtr);
            }

            let header = parse_page_header(&self.read_buf);
            let page_header_size = XLogPageHeaderSize(header.xlp_info);

            let read_len = self.ReadPageInternal(routine, target_page_ptr, page_header_size as i32)?;
            if read_len < 0 {
                self.XLogReaderInvalReadState();
                return Ok(InvalidXLogRecPtr);
            }

            if header.xlp_info & XLP_FIRST_IS_CONTRECORD != 0 {
                if MAXALIGN(header.xlp_rem_len as usize) >= XLOG_BLCKSZ - page_header_size {
                    tmp_rec_ptr = target_page_ptr + XLOG_BLCKSZ as u64;
                } else {
                    tmp_rec_ptr = target_page_ptr
                        + page_header_size as u64
                        + MAXALIGN(header.xlp_rem_len as usize) as u64;
                    break;
                }
            } else {
                tmp_rec_ptr = target_page_ptr + page_header_size as u64;
                break;
            }
        }

        self.XLogBeginRead(tmp_rec_ptr);
        while self.XLogReadRecord(routine)?.is_some() {
            if rec_ptr <= self.v.ReadRecPtr {
                let found = self.v.ReadRecPtr;
                self.XLogBeginRead(found);
                return Ok(found);
            }
        }

        self.XLogReaderInvalReadState();
        Ok(InvalidXLogRecPtr)
    }

    fn payload<'a>(&'a self, rec: &'a DecodedXLogRecord<'mcx>, r: PayloadRange) -> &'a [u8] {
        if r.len == 0 {
            return &[];
        }
        let buf: &[u8] = match &rec.overflow {
            Some(v) => v,
            None => &self.decode_buffer,
        };
        &buf[r.off as usize..(r.off + r.len) as usize]
    }

    fn block(&self, rec: &DecodedXLogRecord<'mcx>, block_id: usize) -> &BlockRef {
        debug_assert!((block_id as u32) < rec.nblocks);
        &self.blocks_pool[rec.blocks_start as usize + block_id]
    }

    fn current(&self) -> &DecodedXLogRecord<'mcx> {
        self.current
            .as_ref()
            .expect("XLogRecGetXXX requires a decoded current record")
    }

    pub fn XLogRecGetTotalLen(&self) -> u32 {
        self.current().header.xl_tot_len
    }
    pub fn XLogRecGetPrev(&self) -> XLogRecPtr {
        self.current().header.xl_prev
    }
    pub fn XLogRecGetInfo(&self) -> u8 {
        self.current().header.xl_info
    }
    pub fn XLogRecGetRmid(&self) -> RmgrId {
        self.current().header.xl_rmid
    }
    pub fn XLogRecGetXid(&self) -> TransactionId {
        self.current().header.xl_xid
    }
    pub fn XLogRecGetOrigin(&self) -> RepOriginId {
        self.current().record_origin
    }
    pub fn XLogRecGetTopXid(&self) -> TransactionId {
        self.current().toplevel_xid
    }
    pub fn XLogRecGetData(&self) -> &[u8] {
        let rec = self.current();
        self.payload(rec, rec.main_data)
    }
    pub fn XLogRecGetDataLen(&self) -> u32 {
        self.current().main_data.len
    }
    pub fn XLogRecMaxBlockId(&self) -> i32 {
        self.current().max_block_id
    }
    pub fn XLogRecHasAnyBlockRefs(&self) -> bool {
        self.XLogRecMaxBlockId() >= 0
    }

    pub fn XLogRecHasBlockRef(&self, block_id: u8) -> bool {
        match self.current.as_ref() {
            Some(rec) => {
                (block_id as i32) <= rec.max_block_id && self.block(rec, block_id as usize).in_use
            }
            None => false,
        }
    }

    pub fn XLogRecGetBlockTagExtended(
        &self,
        block_id: u8,
    ) -> Option<(RelFileLocator, ForkNumber, BlockNumber, Buffer)> {
        if !self.XLogRecHasBlockRef(block_id) {
            return None;
        }
        let blk = self.block(self.current(), block_id as usize);
        Some((blk.rlocator, blk.forknum, blk.blkno, blk.prefetch_buffer))
    }

    pub fn XLogRecGetBlockFlags(&self, block_id: u8) -> u8 {
        if !self.XLogRecHasBlockRef(block_id) {
            return 0;
        }
        self.block(self.current(), block_id as usize).flags
    }

    pub fn XLogRecHasBlockImage(&self, block_id: u8) -> bool {
        self.XLogRecHasBlockRef(block_id) && self.block(self.current(), block_id as usize).has_image
    }

    pub fn XLogRecBlockImageApply(&self, block_id: u8) -> bool {
        self.XLogRecHasBlockRef(block_id)
            && self.block(self.current(), block_id as usize).apply_image
    }

    /// `XLogRecGetBlockData`: `None` mirrors the C NULL returns (no such
    /// block reference, or no data attached).
    pub fn XLogRecGetBlockData(&self, block_id: u8) -> Option<&[u8]> {
        if !self.XLogRecHasBlockRef(block_id) {
            return None;
        }
        let rec = self.current();
        let blk = self.block(rec, block_id as usize);
        if !blk.has_data {
            return None;
        }
        Some(self.payload(rec, blk.data))
    }

    /// `RestoreBlockImage` restoring into a caller page buffer; `false` leaves
    /// the failure message in `errormsg_buf` (readable via a following
    /// `errormsg` exposure), matching the C bool + errormsg contract.
    pub fn RestoreBlockImage(&mut self, block_id: u8, page: &mut [u8]) -> bool {
        let read_rec_ptr = self.v.ReadRecPtr;
        let bad = |state: &mut Self, msg: String| -> bool {
            let mut err = state.err();
            report_invalid!(err, "{}", msg);
            false
        };

        let Some(rec) = self.current.as_ref() else {
            let msg = restore_err_msg(RestoreErr::InvalidBlock, read_rec_ptr, block_id);
            return bad(self, msg);
        };
        if (block_id as i32) > rec.max_block_id || !self.block(rec, block_id as usize).in_use {
            let msg = restore_err_msg(RestoreErr::InvalidBlock, read_rec_ptr, block_id);
            return bad(self, msg);
        }
        let blk = *self.block(rec, block_id as usize);
        if !blk.has_image {
            let msg = restore_err_msg(RestoreErr::InvalidState, read_rec_ptr, block_id);
            return bad(self, msg);
        }
        let image_range = blk.bkp_image;
        let rec = self.current.as_ref().unwrap();
        // Payload borrow ends before the error path re-borrows self.
        let result = {
            let image = self.payload(rec, image_range);
            restore_image_core(
                image,
                blk.bimg_info,
                blk.hole_offset as usize,
                blk.hole_length as usize,
                page,
            )
        };
        match result {
            Ok(()) => true,
            Err(e) => {
                let msg = restore_err_msg(e, read_rec_ptr, block_id);
                bad(self, msg)
            }
        }
    }

    /// The page buffer holding the last page read (C `state->readBuf`);
    /// FinishWalRecovery copies the last partial block out of it.
    pub fn read_buf(&self) -> &[u8] {
        &self.read_buf
    }

    /// (latestPagePtr, latestPageTLI): the last page-header-validated page.
    pub fn latest_page(&self) -> (XLogRecPtr, TimeLineID) {
        (self.latestPagePtr, self.latestPageTLI)
    }

    fn marshal_view_record(&mut self) {
        let rec = self.current.as_ref().expect("current record");
        let mut vr = ViewRecord {
            lsn: rec.lsn,
            next_lsn: rec.next_lsn,
            xl_tot_len: rec.header.xl_tot_len,
            xl_xid: rec.header.xl_xid,
            xl_prev: rec.header.xl_prev,
            xl_info: rec.header.xl_info,
            xl_rmid: rec.header.xl_rmid,
            record_origin: rec.record_origin,
            toplevel_xid: rec.toplevel_xid,
            max_block_id: rec.max_block_id as i8,
            ..Default::default()
        };
        let main = self.payload(rec, rec.main_data);
        if !main.is_empty() {
            vr.main_data = main.as_ptr();
            vr.main_data_len = main.len() as u32;
        }
        for id in 0..rec.nblocks as usize {
            let blk = self.block(rec, id);
            let out = &mut vr.blocks[id];
            out.in_use = blk.in_use;
            if !blk.in_use {
                continue;
            }
            out.rlocator = blk.rlocator;
            out.forknum = blk.forknum;
            out.blkno = blk.blkno;
            out.prefetch_buffer = blk.prefetch_buffer;
            out.flags = blk.flags;
            out.has_image = blk.has_image;
            out.apply_image = blk.apply_image;
            out.hole_offset = blk.hole_offset;
            out.hole_length = blk.hole_length;
            out.bimg_len = blk.bimg_len;
            out.bimg_info = blk.bimg_info;
            out.has_data = blk.has_data;
            out.data_len = blk.data_len;
            let image = self.payload(rec, blk.bkp_image);
            if !image.is_empty() {
                out.bkp_image = image.as_ptr();
            }
            let data = self.payload(rec, blk.data);
            if !data.is_empty() {
                out.data = data.as_ptr();
            }
        }
        self.v.record = Some(vr);
    }
}

#[allow(clippy::too_many_arguments)]
fn validate_page_header(
    ws_tli: TimeLineID,
    ws_segsize: i32,
    system_identifier: u64,
    latest_page_ptr: &mut XLogRecPtr,
    latest_page_tli: &mut TimeLineID,
    err: &mut ErrSink<'_, '_>,
    recptr: XLogRecPtr,
    phdr: &[u8],
) -> bool {
    debug_assert!(recptr % XLOG_BLCKSZ as u64 == 0);

    let segno = XLByteToSeg(recptr, ws_segsize);
    let offset = XLogSegmentOffset(recptr, ws_segsize);
    let hdr = parse_page_header(phdr);

    if hdr.xlp_magic != XLOG_PAGE_MAGIC {
        let fname = XLogFileName(ws_tli, segno, ws_segsize);
        let (h, l) = lsn_fmt(recptr);
        report_invalid!(
            err,
            "invalid magic number {:04X} in WAL segment {}, LSN {:X}/{:X}, offset {}",
            hdr.xlp_magic,
            fname,
            h,
            l,
            offset
        );
        return false;
    }

    if hdr.xlp_info & !XLP_ALL_FLAGS != 0 {
        let fname = XLogFileName(ws_tli, segno, ws_segsize);
        let (h, l) = lsn_fmt(recptr);
        report_invalid!(
            err,
            "invalid info bits {:04X} in WAL segment {}, LSN {:X}/{:X}, offset {}",
            hdr.xlp_info,
            fname,
            h,
            l,
            offset
        );
        return false;
    }

    if hdr.xlp_info & XLP_LONG_HEADER != 0 {
        if system_identifier != 0 && hdr.xlp_sysid != system_identifier {
            report_invalid!(
                err,
                "WAL file is from different database system: WAL file database system identifier is {}, pg_control database system identifier is {}",
                hdr.xlp_sysid,
                system_identifier
            );
            return false;
        } else if hdr.xlp_seg_size != ws_segsize as u32 {
            report_invalid!(
                err,
                "WAL file is from different database system: incorrect segment size in page header"
            );
            return false;
        } else if hdr.xlp_xlog_blcksz != XLOG_BLCKSZ as u32 {
            report_invalid!(
                err,
                "WAL file is from different database system: incorrect XLOG_BLCKSZ in page header"
            );
            return false;
        }
    } else if offset == 0 {
        let fname = XLogFileName(ws_tli, segno, ws_segsize);
        let (h, l) = lsn_fmt(recptr);
        report_invalid!(
            err,
            "invalid info bits {:04X} in WAL segment {}, LSN {:X}/{:X}, offset {}",
            hdr.xlp_info,
            fname,
            h,
            l,
            offset
        );
        return false;
    }

    if hdr.xlp_pageaddr != recptr {
        let fname = XLogFileName(ws_tli, segno, ws_segsize);
        let (ph, pl) = lsn_fmt(hdr.xlp_pageaddr);
        let (h, l) = lsn_fmt(recptr);
        report_invalid!(
            err,
            "unexpected pageaddr {:X}/{:X} in WAL segment {}, LSN {:X}/{:X}, offset {}",
            ph,
            pl,
            fname,
            h,
            l,
            offset
        );
        return false;
    }

    if recptr > *latest_page_ptr && hdr.xlp_tli < *latest_page_tli {
        let fname = XLogFileName(ws_tli, segno, ws_segsize);
        let (h, l) = lsn_fmt(recptr);
        report_invalid!(
            err,
            "out-of-sequence timeline ID {} (after {}) in WAL segment {}, LSN {:X}/{:X}, offset {}",
            hdr.xlp_tli,
            *latest_page_tli,
            fname,
            h,
            l,
            offset
        );
        return false;
    }
    *latest_page_ptr = recptr;
    *latest_page_tli = hdr.xlp_tli;
    true
}

#[derive(Clone, Copy, Default)]
struct WorkingBlock {
    in_use: bool,
    rlocator: RelFileLocator,
    forknum: ForkNumber,
    blkno: BlockNumber,
    flags: u8,
    has_image: bool,
    apply_image: bool,
    hole_offset: u16,
    hole_length: u16,
    bimg_len: u16,
    bimg_info: u8,
    has_data: bool,
    data_len: u16,
}

struct PayloadSink<'a, 'mcx> {
    ring: &'a mut PgVec<'mcx, u8>,
    overflow: Option<PgVec<'mcx, u8>>,
    cursor: usize,
}

impl PayloadSink<'_, '_> {
    fn put(&mut self, src: &[u8]) -> PayloadRange {
        let len = src.len() as u32;
        match self.overflow.as_mut() {
            Some(ovf) => {
                let off = ovf.len() as u32;
                vec_append_bytes(ovf, src).expect("overflow buffer preallocated");
                PayloadRange { off, len }
            }
            None => {
                let off = self.cursor;
                self.ring[off..off + src.len()].copy_from_slice(src);
                self.cursor += src.len();
                PayloadRange {
                    off: off as u32,
                    len,
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn decode_record<'mcx>(
    mcx: Mcx<'mcx>,
    decode_buffer: &mut PgVec<'mcx, u8>,
    blocks_pool: &mut PgVec<'mcx, BlockRef>,
    slot: &AllocSlot,
    record_hdr: &XLogRecord,
    record_bytes: &[u8],
    read_rec_ptr: XLogRecPtr,
    err: &mut ErrSink<'_, '_>,
) -> Option<DecodedXLogRecord<'mcx>> {
    let mut ptr: usize = SIZE_OF_XLOG_RECORD;
    let mut remaining: u32 = record_hdr.xl_tot_len - SIZE_OF_XLOG_RECORD as u32;

    let mut record_origin: RepOriginId = InvalidRepOriginId;
    let mut toplevel_xid: TransactionId = InvalidTransactionId;
    let mut main_data_len: u32 = 0;
    let mut datatotal: u32 = 0;
    let mut max_block_id: i32 = -1;

    let mut blks = [WorkingBlock::default(); XLR_MAX_BLOCK_ID + 1];
    let mut rlocator: Option<RelFileLocator> = None;

    macro_rules! header_field {
        ($size:expr) => {{
            let sz = $size;
            if (remaining as usize) < sz {
                let (h, l) = lsn_fmt(read_rec_ptr);
                report_invalid!(err, "record with invalid length at {:X}/{:X}", h, l);
                return None;
            }
            let s = ptr;
            ptr += sz;
            remaining -= sz as u32;
            &record_bytes[s..s + sz]
        }};
    }

    while remaining > datatotal {
        let block_id = header_field!(1)[0];

        if block_id == XLR_BLOCK_ID_DATA_SHORT {
            main_data_len = header_field!(1)[0] as u32;
            datatotal = datatotal.wrapping_add(main_data_len);
            break;
        } else if block_id == XLR_BLOCK_ID_DATA_LONG {
            main_data_len = read_u32(header_field!(4), 0);
            datatotal = datatotal.wrapping_add(main_data_len);
            break;
        } else if block_id == XLR_BLOCK_ID_ORIGIN {
            record_origin = read_u16(header_field!(2), 0);
        } else if block_id == XLR_BLOCK_ID_TOPLEVEL_XID {
            toplevel_xid = read_u32(header_field!(4), 0);
        } else if (block_id as usize) <= XLR_MAX_BLOCK_ID {
            if (block_id as i32) <= max_block_id {
                let (h, l) = lsn_fmt(read_rec_ptr);
                report_invalid!(err, "out-of-order block_id {} at {:X}/{:X}", block_id, h, l);
                return None;
            }
            max_block_id = block_id as i32;

            let blk = &mut blks[block_id as usize];
            blk.in_use = true;
            blk.apply_image = false;

            let fork_flags = header_field!(1)[0];
            blk.forknum = ForkNumber::from_i32((fork_flags & BKPBLOCK_FORK_MASK) as i32)
                .unwrap_or(ForkNumber::MAIN_FORKNUM);
            blk.flags = fork_flags;
            blk.has_image = fork_flags & BKPBLOCK_HAS_IMAGE != 0;
            blk.has_data = fork_flags & BKPBLOCK_HAS_DATA != 0;

            blk.data_len = read_u16(header_field!(2), 0);
            if blk.has_data && blk.data_len == 0 {
                let (h, l) = lsn_fmt(read_rec_ptr);
                report_invalid!(
                    err,
                    "BKPBLOCK_HAS_DATA set, but no data included at {:X}/{:X}",
                    h,
                    l
                );
                return None;
            }
            if !blk.has_data && blk.data_len != 0 {
                let (h, l) = lsn_fmt(read_rec_ptr);
                report_invalid!(
                    err,
                    "BKPBLOCK_HAS_DATA not set, but data length is {} at {:X}/{:X}",
                    blk.data_len,
                    h,
                    l
                );
                return None;
            }
            datatotal = datatotal.wrapping_add(blk.data_len as u32);

            if blk.has_image {
                blk.bimg_len = read_u16(header_field!(2), 0);
                blk.hole_offset = read_u16(header_field!(2), 0);
                blk.bimg_info = header_field!(1)[0];

                blk.apply_image = blk.bimg_info & BKPIMAGE_APPLY != 0;

                if BKPIMAGE_COMPRESSED(blk.bimg_info) {
                    if blk.bimg_info & BKPIMAGE_HAS_HOLE != 0 {
                        blk.hole_length = read_u16(header_field!(2), 0);
                    } else {
                        blk.hole_length = 0;
                    }
                } else {
                    // C computes this in uint16 arithmetic; a corrupt bimg_len
                    // > BLCKSZ wraps (defined in C) and is caught by the
                    // cross-checks below. wrapping_sub keeps that behavior
                    // identical across profiles (debug overflow-checks would
                    // otherwise abort the decoder on adversarial input).
                    blk.hole_length = (BLCKSZ as u16).wrapping_sub(blk.bimg_len);
                }
                datatotal = datatotal.wrapping_add(blk.bimg_len as u32);

                if blk.bimg_info & BKPIMAGE_HAS_HOLE != 0
                    && (blk.hole_offset == 0
                        || blk.hole_length == 0
                        || blk.bimg_len as usize == BLCKSZ)
                {
                    let (h, l) = lsn_fmt(read_rec_ptr);
                    report_invalid!(
                        err,
                        "BKPIMAGE_HAS_HOLE set, but hole offset {} length {} block image length {} at {:X}/{:X}",
                        blk.hole_offset,
                        blk.hole_length,
                        blk.bimg_len,
                        h,
                        l
                    );
                    return None;
                }
                if blk.bimg_info & BKPIMAGE_HAS_HOLE == 0
                    && (blk.hole_offset != 0 || blk.hole_length != 0)
                {
                    let (h, l) = lsn_fmt(read_rec_ptr);
                    report_invalid!(
                        err,
                        "BKPIMAGE_HAS_HOLE not set, but hole offset {} length {} at {:X}/{:X}",
                        blk.hole_offset,
                        blk.hole_length,
                        h,
                        l
                    );
                    return None;
                }
                if BKPIMAGE_COMPRESSED(blk.bimg_info) && blk.bimg_len as usize == BLCKSZ {
                    let (h, l) = lsn_fmt(read_rec_ptr);
                    report_invalid!(
                        err,
                        "BKPIMAGE_COMPRESSED set, but block image length {} at {:X}/{:X}",
                        blk.bimg_len,
                        h,
                        l
                    );
                    return None;
                }
                if blk.bimg_info & BKPIMAGE_HAS_HOLE == 0
                    && !BKPIMAGE_COMPRESSED(blk.bimg_info)
                    && blk.bimg_len as usize != BLCKSZ
                {
                    let (h, l) = lsn_fmt(read_rec_ptr);
                    report_invalid!(
                        err,
                        "neither BKPIMAGE_HAS_HOLE nor BKPIMAGE_COMPRESSED set, but block image length is {} at {:X}/{:X}",
                        blk.data_len,
                        h,
                        l
                    );
                    return None;
                }
            }

            if fork_flags & BKPBLOCK_SAME_REL == 0 {
                let loc = parse_rel_file_locator(header_field!(SIZEOF_REL_FILE_LOCATOR));
                blk.rlocator = loc;
                rlocator = Some(loc);
            } else {
                match rlocator {
                    None => {
                        let (h, l) = lsn_fmt(read_rec_ptr);
                        report_invalid!(
                            err,
                            "BKPBLOCK_SAME_REL set but no previous rel at {:X}/{:X}",
                            h,
                            l
                        );
                        return None;
                    }
                    Some(loc) => blk.rlocator = loc,
                }
            }
            blk.blkno = read_u32(header_field!(4), 0);
        } else {
            let (h, l) = lsn_fmt(read_rec_ptr);
            report_invalid!(err, "invalid block_id {} at {:X}/{:X}", block_id, h, l);
            return None;
        }
    }

    if remaining != datatotal {
        let (h, l) = lsn_fmt(read_rec_ptr);
        report_invalid!(err, "record with invalid length at {:X}/{:X}", h, l);
        return None;
    }

    // Re-validate each fragment against the bytes actually left as we copy: a
    // u32-wrapped datatotal satisfies the aggregate gate above yet leaves a
    // fragment length exceeding the record. C's uint32 datatotal wraps the
    // same way and C then silently overreads the heap (UB) on such a record;
    // we deliberately diverge to a clean invalid-length rejection (the ruled
    // never-replicate-C-UB exception).
    macro_rules! payload_field {
        ($len:expr) => {{
            let len = $len as usize;
            if (remaining as usize) < len {
                let (h, l) = lsn_fmt(read_rec_ptr);
                report_invalid!(err, "record with invalid length at {:X}/{:X}", h, l);
                return None;
            }
            let s = ptr;
            ptr += len;
            remaining -= len as u32;
            &record_bytes[s..s + len]
        }};
    }

    let overflow = if slot.oversized {
        match vec_with_capacity_in(mcx, record_hdr.xl_tot_len as usize) {
            Ok(v) => Some(v),
            Err(_) => {
                report_invalid!(
                    err,
                    "out of memory while trying to decode a record of length {}",
                    record_hdr.xl_tot_len
                );
                return None;
            }
        }
    } else {
        None
    };
    let mut sink = PayloadSink {
        ring: decode_buffer,
        overflow,
        cursor: slot.offset,
    };

    // `out` tracks the C output cursor purely for the ring `size` footprint.
    let mut out: usize =
        SIZEOF_DECODED_XLOG_RECORD_FIXED + SIZEOF_DECODED_BKP_BLOCK * (max_block_id + 1) as usize;

    let blocks_start = blocks_pool.len() as u32;
    for blk in blks.iter().take((max_block_id + 1) as usize) {
        if !blk.in_use {
            blocks_pool.push(BlockRef::default());
            continue;
        }
        debug_assert!(blk.has_image || !blk.apply_image);

        let mut out_blk = BlockRef {
            in_use: true,
            rlocator: blk.rlocator,
            forknum: blk.forknum,
            blkno: blk.blkno,
            prefetch_buffer: 0,
            flags: blk.flags,
            has_image: blk.has_image,
            apply_image: blk.apply_image,
            hole_offset: blk.hole_offset,
            hole_length: blk.hole_length,
            bimg_len: blk.bimg_len,
            bimg_info: blk.bimg_info,
            has_data: blk.has_data,
            data_len: blk.data_len,
            bkp_image: PayloadRange::default(),
            data: PayloadRange::default(),
        };

        if blk.has_image {
            out_blk.bkp_image = sink.put(payload_field!(blk.bimg_len));
            out += blk.bimg_len as usize;
        }
        if blk.has_data {
            out = MAXALIGN(out);
            out_blk.data = sink.put(payload_field!(blk.data_len));
            out += blk.data_len as usize;
        }
        blocks_pool.push(out_blk);
    }

    let mut main_data = PayloadRange::default();
    if main_data_len > 0 {
        out = MAXALIGN(out);
        main_data = sink.put(payload_field!(main_data_len));
        out += main_data_len as usize;
    }

    let size = MAXALIGN(out);
    debug_assert!(DecodeXLogRecordRequiredSpace(record_hdr.xl_tot_len as usize) >= size);
    let overflow = sink.overflow;

    Some(DecodedXLogRecord {
        size,
        oversized: slot.oversized,
        buffer_offset: slot.offset,
        lsn: InvalidXLogRecPtr,
        next_lsn: InvalidXLogRecPtr,
        header: *record_hdr,
        record_origin,
        toplevel_xid,
        main_data,
        max_block_id,
        blocks_start,
        nblocks: (max_block_id + 1).max(0) as u32,
        overflow,
    })
}

#[cfg_attr(test, derive(Debug))]
enum RestoreErr {
    InvalidBlock,
    InvalidState,
    NotSupportedByBuild(&'static str),
    UnknownMethod,
    DecompressFailure,
}

#[cold]
fn restore_err_msg(e: RestoreErr, lsn: XLogRecPtr, block_id: u8) -> String {
    let (h, l) = lsn_fmt(lsn);
    match e {
        RestoreErr::InvalidBlock => format!(
            "could not restore image at {:X}/{:X} with invalid block {} specified",
            h, l, block_id
        ),
        RestoreErr::InvalidState => format!(
            "could not restore image at {:X}/{:X} with invalid state, block {}",
            h, l, block_id
        ),
        RestoreErr::NotSupportedByBuild(alg) => format!(
            "could not restore image at {:X}/{:X} compressed with {} not supported by build, block {}",
            h, l, alg, block_id
        ),
        RestoreErr::UnknownMethod => format!(
            "could not restore image at {:X}/{:X} compressed with unknown method, block {}",
            h, l, block_id
        ),
        RestoreErr::DecompressFailure => format!(
            "could not decompress image at {:X}/{:X}, block {}",
            h, l, block_id
        ),
    }
}

fn restore_image_core(
    image: &[u8],
    bimg_info: u8,
    hole_offset: usize,
    hole_length: usize,
    page: &mut [u8],
) -> Result<(), RestoreErr> {
    // hole_length is `BLCKSZ - bimg_len` (u16-wrapping) or a raw field; a
    // corrupt bimg_len > BLCKSZ / oversized hole wraps it. C computes
    // `BLCKSZ - (hole_offset + hole_length)` and heap-overreads (UB) on such a
    // record; we diverge to a clean invalid-state rejection (never replicate C
    // UB) instead of panicking on the slices below.
    if hole_offset > BLCKSZ || hole_length > BLCKSZ || hole_offset + hole_length > BLCKSZ {
        return Err(RestoreErr::InvalidState);
    }
    let mut tmp = [core::mem::MaybeUninit::<u8>::uninit(); BLCKSZ];
    let src: &[u8] = if BKPIMAGE_COMPRESSED(bimg_info) {
        if bimg_info & BKPIMAGE_COMPRESS_PGLZ != 0 {
            match pglz::pglz_decompress(image, &mut tmp[..BLCKSZ - hole_length], true) {
                // SAFETY: the kernel initialized the first n bytes of tmp.
                Some(n) => unsafe { core::slice::from_raw_parts(tmp.as_ptr().cast::<u8>(), n) },
                None => return Err(RestoreErr::DecompressFailure),
            }
        } else if bimg_info & BKPIMAGE_COMPRESS_LZ4 != 0 {
            // USE_LZ4 not defined in this build (the C #else branch).
            return Err(RestoreErr::NotSupportedByBuild("LZ4"));
        } else if bimg_info & BKPIMAGE_COMPRESS_ZSTD != 0 {
            // USE_ZSTD not defined in this build (the C #else branch).
            return Err(RestoreErr::NotSupportedByBuild("zstd"));
        } else {
            return Err(RestoreErr::UnknownMethod);
        }
    } else {
        image
    };

    if hole_length == 0 {
        page[..BLCKSZ].copy_from_slice(&src[..BLCKSZ]);
    } else {
        page[..hole_offset].copy_from_slice(&src[..hole_offset]);
        page[hole_offset..hole_offset + hole_length].fill(0);
        page[hole_offset + hole_length..BLCKSZ]
            .copy_from_slice(&src[hole_offset..BLCKSZ - hole_length]);
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn current_errno() -> i32 {
    // SAFETY: libc returns this thread's errno slot.
    unsafe { *libc::__error() }
}
#[cfg(not(target_os = "macos"))]
fn current_errno() -> i32 {
    // SAFETY: libc returns this thread's errno slot.
    unsafe { *libc::__errno_location() }
}
#[cfg(target_os = "macos")]
fn clear_errno() {
    // SAFETY: libc returns this thread's errno slot.
    unsafe { *libc::__error() = 0 };
}
#[cfg(not(target_os = "macos"))]
fn clear_errno() {
    // SAFETY: libc returns this thread's errno slot.
    unsafe { *libc::__errno_location() = 0 };
}

/// `WALRead`: `Ok(Err(_))` is the C `false` + errinfo; the outer `Err` is the
/// segment_open callback's ereport surface.
pub fn WALRead<R: XLogSegmentRoutine>(
    v: &mut ReaderView,
    routine: &mut R,
    buf: &mut [u8],
    startptr: XLogRecPtr,
    count: usize,
    mut tli: TimeLineID,
) -> PgResult<Result<(), WALReadError>> {
    debug_assert!(buf.len() >= count);
    let mut recptr = startptr;
    let mut nbytes = count;
    let mut p: usize = 0;

    while nbytes > 0 {
        let startoff = XLogSegmentOffset(recptr, v.segcxt.ws_segsize) as usize;

        if v.seg.ws_file < 0
            || !XLByteInSeg(recptr, v.seg.ws_segno, v.segcxt.ws_segsize)
            || tli != v.seg.ws_tli
        {
            if v.seg.ws_file >= 0 {
                routine.segment_close(v);
            }
            let next_seg_no = XLByteToSeg(recptr, v.segcxt.ws_segsize);
            routine.segment_open(v, next_seg_no, &mut tli)?;
            debug_assert!(v.seg.ws_file >= 0);
            v.seg.ws_tli = tli;
            v.seg.ws_segno = next_seg_no;
        }

        let segbytes = nbytes.min(v.segcxt.ws_segsize as usize - startoff);

        let io_start = if pgstat_seams::pgstat_prepare_io_time::is_installed() {
            pgstat_seams::pgstat_prepare_io_time::call(
                guc_tables::vars::track_wal_io_timing.read(),
            )
        } else {
            0
        };
        clear_errno();
        // SAFETY: buf[p..p+segbytes] is in-bounds writable memory
        // (segbytes <= nbytes <= buf.len() - p).
        let readbytes = unsafe {
            libc::pread(
                v.seg.ws_file,
                buf[p..].as_mut_ptr() as *mut libc::c_void,
                segbytes,
                startoff as libc::off_t,
            )
        };
        if pgstat_seams::pgstat_count_io_op_time::is_installed() {
            pgstat_seams::pgstat_count_io_op_time::call(
                pgstat_seams::IOOBJECT_WAL,
                pgstat_seams::IOCONTEXT_NORMAL,
                pgstat_seams::IOOP_READ,
                io_start,
                1,
                readbytes.max(0) as u64,
            );
        }
        if readbytes <= 0 {
            return Ok(Err(WALReadError {
                wre_errno: current_errno(),
                wre_off: startoff as i32,
                wre_req: segbytes as i32,
                wre_read: readbytes as i32,
                wre_seg: v.seg,
            }));
        }

        recptr += readbytes as u64;
        nbytes -= readbytes as usize;
        p += readbytes as usize;
    }
    Ok(Ok(()))
}

fn wal_read_seam<'a>(
    state: &'a mut ReaderView,
    buf: &'a mut [u8],
    startptr: XLogRecPtr,
    count: usize,
    tli: TimeLineID,
) -> PgResult<Result<(), WALReadError>> {
    WALRead(
        state,
        &mut LocalPageRead { wait_for_wal: true },
        buf,
        startptr,
        count,
        tli,
    )
}

fn restore_block_image_seam(
    record: &ReaderView,
    block_id: u8,
    buf: Buffer,
) -> PgResult<Result<(), String>> {
    let Some(rec) = record.record.as_ref() else {
        return Ok(Err(restore_err_msg(
            RestoreErr::InvalidBlock,
            record.ReadRecPtr,
            block_id,
        )));
    };
    if (block_id as i8) > rec.max_block_id || !rec.blocks[block_id as usize].in_use {
        return Ok(Err(restore_err_msg(
            RestoreErr::InvalidBlock,
            record.ReadRecPtr,
            block_id,
        )));
    }
    let blk = &rec.blocks[block_id as usize];
    if !blk.has_image {
        return Ok(Err(restore_err_msg(
            RestoreErr::InvalidState,
            record.ReadRecPtr,
            block_id,
        )));
    }
    // SAFETY: the caller (redo via xlogutils) holds the marshaled view of the
    // owning reader's *current* record, so the payload pointers target live
    // decode-buffer bytes (the marshal invariant in `marshal_view_record`).
    let image = unsafe { blk.bkp_image_bytes() };

    let mut page = [0u8; BLCKSZ];
    match restore_image_core(
        image,
        blk.bimg_info,
        blk.hole_offset as usize,
        blk.hole_length as usize,
        &mut page,
    ) {
        Ok(()) => {
            bufmgr_seams::overwrite_buffer_page::call(buf, &page);
            Ok(Ok(()))
        }
        Err(e) => Ok(Err(restore_err_msg(e, record.ReadRecPtr, block_id))),
    }
}

pub fn init_seams() {
    xlogreader_seams::wal_read::set(wal_read_seam);
    xlogreader_seams::restore_block_image::set(restore_block_image_seam);
}
