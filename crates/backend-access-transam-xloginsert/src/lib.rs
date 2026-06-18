//! Port of `src/backend/access/transam/xloginsert.c` (PostgreSQL 18.3).
//!
//! xloginsert.c is the WAL-record construction API used by every resource
//! manager: `XLogBeginInsert`, `XLogSetRecordFlags`, `XLogEnsureRecordSpace`,
//! `XLogRegisterData`, `XLogRegisterBuffer`, `XLogRegisterBlock`,
//! `XLogRegisterBufData`, `XLogResetInsertion`, `XLogInsert`, plus the
//! `log_newpage*` / `XLogSaveBufferForHint` / `XLogCheckBufferNeedsBackup`
//! helpers and `InitXLogInsert`. Its core is `XLogRecordAssemble`, which packs
//! the registered buffers and data chunks into a WAL record (with
//! full-page-image hole removal and optional compression) and computes the
//! record CRC, then hands the assembled record to xlog.c's `XLogInsertRecord`
//! (the `xlog_insert_record` boundary on `backend-access-transam-xlog-seams`).
//!
//! ## Backend-local working area
//!
//! C keeps the record-construction working set in file-static mutable arrays
//! (`registered_buffers`, `mainrdata_*`, `hdr_scratch`, ...) lazily palloc'd by
//! `InitXLogInsert` into `xloginsert_cxt`. This port mirrors that exactly: the
//! working set lives in a backend-local [`XLogInsertState`] held in a
//! `thread_local`, lazily created on first use (`InitXLogInsert`). The
//! public-facing functions are stateless (they reach the thread-local through
//! [`with_state`]), matching the inward seam contract that consumers call.
//!
//! Registered data chunks and page images are owned `Vec<u8>` instead of C's
//! intrusive `XLogRecData` pointer chain; `XLogRecordAssemble` collects the
//! body spans in chain order and hands them, with the header span, to the
//! `xlog_insert_record` boundary as a slice of byte fragments.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

extern crate alloc;

use alloc::format;
use alloc::vec;
use alloc::vec::Vec;
use core::cell::RefCell;

use backend_utils_error::{PgError, PgResult};
use common_pglz::{pglz_compress, PGLZ_strategy_default, PglzError};
use types_core::primitive::{
    BlockNumber, Buffer, ForkNumber, RmgrId, TransactionId, XLogRecPtr, BLCKSZ,
};
use types_storage::bufpage::SizeOfPageHeaderData;
use types_storage::storage::{RelFileLocator, RelFileLocatorEquals};
use types_wal::{
    BKPBLOCK_HAS_DATA, BKPBLOCK_HAS_IMAGE, BKPBLOCK_SAME_REL, BKPBLOCK_WILL_INIT, BKPIMAGE_APPLY,
    BKPIMAGE_COMPRESS_LZ4, BKPIMAGE_COMPRESS_PGLZ, BKPIMAGE_COMPRESS_ZSTD, BKPIMAGE_HAS_HOLE,
    MAX_SIZE_OF_XLOG_RECORD_BLOCK_HEADER, REGBUF_FORCE_IMAGE, REGBUF_KEEP_DATA, REGBUF_NO_CHANGE,
    REGBUF_NO_IMAGE, REGBUF_STANDARD, REGBUF_WILL_INIT, RM_XLOG_ID, SIZE_OF_XLOG_LONG_PHD,
    SIZE_OF_XLOG_RECORD, SIZE_OF_XLOG_RECORD_BLOCK_COMPRESS_HEADER,
    SIZE_OF_XLOG_RECORD_BLOCK_HEADER, SIZE_OF_XLOG_RECORD_BLOCK_IMAGE_HEADER,
    SIZE_OF_XLOG_RECORD_DATA_HEADER_LONG, WAL_COMPRESSION_LZ4, WAL_COMPRESSION_NONE,
    WAL_COMPRESSION_PGLZ, WAL_COMPRESSION_ZSTD, XLOG_FPI, XLOG_FPI_FOR_HINT, XLOG_INCLUDE_ORIGIN,
    XLOG_RECORD_MAX_SIZE, XLR_BLOCK_ID_DATA_LONG, XLR_BLOCK_ID_DATA_SHORT, XLR_BLOCK_ID_ORIGIN,
    XLR_BLOCK_ID_TOPLEVEL_XID, XLR_CHECK_CONSISTENCY, XLR_MAX_BLOCK_ID, XLR_NORMAL_MAX_BLOCK_ID,
    XLR_NORMAL_RDATAS, XLR_RMGR_INFO_MASK, XLR_SPECIAL_REL_UPDATE,
};

use backend_access_transam_xlog_seams as xlog_seam;
use backend_access_transam_xact_seams as xact_seam;
use backend_replication_logical_origin_seams as origin_seam;
use backend_storage_buffer_bufmgr_seams as bufmgr_seam;
use backend_utils_init_miscinit_seams as miscinit_seam;

// ---------------------------------------------------------------------------
// Local constants matching the C macros / globals.
// ---------------------------------------------------------------------------

/// `InvalidXLogRecPtr` (`#define InvalidXLogRecPtr 0`).
const InvalidXLogRecPtr: XLogRecPtr = 0;

/// `InvalidRepOriginId`.
const InvalidRepOriginId: u16 = 0;

/// `SizeOfXLogLongPHD` — start of the first checkpoint record, used as the
/// phony end-position in bootstrap mode.
const SizeOfXLogLongPHD: XLogRecPtr = SIZE_OF_XLOG_LONG_PHD as XLogRecPtr;

const UINT16_MAX: u32 = 0xFFFF;
const PG_UINT32_MAX: u64 = 0xFFFF_FFFF;

/// `XLogRecordMaxSize` — the maximum allowed length of a WAL record.
const XLogRecordMaxSize: u64 = XLOG_RECORD_MAX_SIZE;

/// `HEADER_SCRATCH_SIZE` — size of the header scratch buffer.
/// `SizeOfXLogRecord +
///  MaxSizeOfXLogRecordBlockHeader * (XLR_MAX_BLOCK_ID + 1) +
///  SizeOfXLogRecordDataHeaderLong + SizeOfXlogOrigin + SizeOfXLogTransactionId`.
const SizeOfXlogOrigin: usize = core::mem::size_of::<u16>() + core::mem::size_of::<u8>();
const SizeOfXLogTransactionId: usize =
    core::mem::size_of::<TransactionId>() + core::mem::size_of::<u8>();
const HEADER_SCRATCH_SIZE: usize = SIZE_OF_XLOG_RECORD
    + MAX_SIZE_OF_XLOG_RECORD_BLOCK_HEADER * (XLR_MAX_BLOCK_ID as usize + 1)
    + SIZE_OF_XLOG_RECORD_DATA_HEADER_LONG
    + SizeOfXlogOrigin
    + SizeOfXLogTransactionId;

// Page-header field byte offsets within a page image (native-endian layout of
// the C `PageHeaderData`). pd_lsn is a PageXLogRecPtr (xlogid@0, xrecoff@4) at
// offset 0; pd_lower@12, pd_upper@14.
const OFF_PD_LSN: usize = 0;
const OFF_PD_LOWER: usize = 12;
const OFF_PD_UPPER: usize = 14;

// ---------------------------------------------------------------------------
// Page helpers (pure; read directly from the owned page bytes).
// ---------------------------------------------------------------------------

/// `PageGetLSN(page)` — read the 8-byte pd_lsn. C stores the LSN as two 32-bit
/// halves (`xlogid` high, `xrecoff` low); reconstruct the 64-bit value.
#[inline]
fn PageGetLSN(page: &[u8]) -> XLogRecPtr {
    let xlogid = u32::from_ne_bytes([
        page[OFF_PD_LSN],
        page[OFF_PD_LSN + 1],
        page[OFF_PD_LSN + 2],
        page[OFF_PD_LSN + 3],
    ]);
    let xrecoff = u32::from_ne_bytes([
        page[OFF_PD_LSN + 4],
        page[OFF_PD_LSN + 5],
        page[OFF_PD_LSN + 6],
        page[OFF_PD_LSN + 7],
    ]);
    ((xlogid as u64) << 32) | (xrecoff as u64)
}

/// `((PageHeader) page)->pd_lower`.
#[inline]
fn page_pd_lower(page: &[u8]) -> u16 {
    u16::from_ne_bytes([page[OFF_PD_LOWER], page[OFF_PD_LOWER + 1]])
}

/// `((PageHeader) page)->pd_upper`.
#[inline]
fn page_pd_upper(page: &[u8]) -> u16 {
    u16::from_ne_bytes([page[OFF_PD_UPPER], page[OFF_PD_UPPER + 1]])
}

/// `PageIsNew(page)` — the page is new iff pd_upper == 0.
#[inline]
fn PageIsNew(page: &[u8]) -> bool {
    page_pd_upper(page) == 0
}

/// `PageSetLSN(page, lsn)` — write the 8-byte pd_lsn (xlogid high, xrecoff low).
#[inline]
fn PageSetLSN(page: &mut [u8], lsn: XLogRecPtr) {
    let xlogid = (lsn >> 32) as u32;
    let xrecoff = (lsn & 0xFFFF_FFFF) as u32;
    page[OFF_PD_LSN..OFF_PD_LSN + 4].copy_from_slice(&xlogid.to_ne_bytes());
    page[OFF_PD_LSN + 4..OFF_PD_LSN + 8].copy_from_slice(&xrecoff.to_ne_bytes());
}

/// `errmsg_internal(msg) + errdetail_internal(detail)` carrier.
fn err_internal_detail(
    msg: impl Into<alloc::string::String>,
    detail: impl Into<alloc::string::String>,
) -> PgError {
    PgError::error(msg).with_detail(detail)
}

/// Allocate a zeroed BLCKSZ buffer, OOM-safely.
fn alloc_block() -> PgResult<Vec<u8>> {
    let mut v: Vec<u8> = Vec::new();
    v.try_reserve_exact(BLCKSZ)
        .map_err(|_| PgError::error("out of memory allocating WAL page buffer"))?;
    v.resize(BLCKSZ, 0);
    Ok(v)
}

// ---------------------------------------------------------------------------
// Working-area structs (xloginsert.c file-statics -> backend-local state).
// ---------------------------------------------------------------------------

/// `registered_buffer` (file-static in xloginsert.c): the per-block-id working
/// slot. The intrusive `rdata_head`/`rdata_tail` chain and `bkp_rdatas[2]`
/// temporaries become owned data: `rdata` collects the per-buffer data chunks
/// in order, and the backup-block image spans are produced during assembly.
#[derive(Clone, Debug)]
struct RegBuf {
    /// `regbuf->in_use`.
    in_use: bool,
    /// `regbuf->flags` (REGBUF_*).
    flags: u8,
    /// `regbuf->rlocator`.
    rlocator: RelFileLocator,
    /// `regbuf->forkno`.
    forkno: ForkNumber,
    /// `regbuf->block`.
    block: BlockNumber,
    /// `regbuf->page` — the page image (owned BLCKSZ copy), or empty if unset.
    page: Vec<u8>,
    /// `regbuf->rdata_len` — total length of the per-buffer data chunks.
    rdata_len: u32,
    /// The per-buffer data chain (C's `rdata_head`..`rdata_tail`), owned in
    /// order. Each entry is one `XLogRegisterBufData` chunk.
    rdata: Vec<Vec<u8>>,
}

impl RegBuf {
    fn zeroed() -> Self {
        Self {
            in_use: false,
            flags: 0,
            rlocator: RelFileLocator {
                spcOid: 0,
                dbOid: 0,
                relNumber: 0,
            },
            forkno: ForkNumber::MAIN_FORKNUM,
            block: 0,
            page: Vec::new(),
            rdata_len: 0,
            rdata: Vec::new(),
        }
    }

    /// Reset the per-buffer data chain (C: `rdata_tail = &rdata_head; rdata_len = 0`).
    fn reset_rdata(&mut self) {
        self.rdata.clear();
        self.rdata_len = 0;
    }
}

/// xloginsert.c's file-static WAL-record construction working area, held
/// backend-local. Lazily created by [`InitXLogInsert`].
#[derive(Clone, Debug)]
struct XLogInsertState {
    /// `registered_buffers` + `max_registered_buffers` (its `len()`).
    registered_buffers: Vec<RegBuf>,
    /// `max_registered_block_id` — highest block_id + 1 currently registered.
    max_registered_block_id: i32,

    /// `mainrdata_*` — the main-data chain, owned in order.
    mainrdata: Vec<Vec<u8>>,
    /// `mainrdata_len` — total # of bytes in the main-data chain.
    mainrdata_len: u64,
    /// `num_rdatas` + `max_rdatas` (allocated cap) — C bounds the number of
    /// `XLogRecData` segments.
    num_rdatas: i32,
    max_rdatas: i32,

    /// `curinsert_flags` — flags for the in-progress insertion.
    curinsert_flags: u8,

    /// `hdr_scratch` — the header scratch buffer (zeroed, MAXALIGNed in C).
    hdr_scratch: Vec<u8>,

    /// `begininsert_called`.
    begininsert_called: bool,
}

impl XLogInsertState {
    #[inline]
    fn max_registered_buffers(&self) -> i32 {
        self.registered_buffers.len() as i32
    }
}

thread_local! {
    /// xloginsert.c's file-static working area (`xloginsert_cxt` contents), per
    /// backend. `None` until `InitXLogInsert` allocates it.
    static XLOG_INSERT_STATE: RefCell<Option<XLogInsertState>> = const { RefCell::new(None) };
}

/// Run `f` with a mutable borrow of the backend-local working area, lazily
/// allocating it (C's `InitXLogInsert` is called once at backend start; ports
/// that have not pre-initialized still get the allocation on demand).
fn with_state<R>(f: impl FnOnce(&mut XLogInsertState) -> R) -> R {
    XLOG_INSERT_STATE.with(|cell| {
        let mut slot = cell.borrow_mut();
        if slot.is_none() {
            *slot = Some(new_insert_state());
        }
        f(slot.as_mut().unwrap())
    })
}

/// Construct a fresh working area with the C initial sizes
/// (`XLR_NORMAL_MAX_BLOCK_ID + 1` registered buffers, `XLR_NORMAL_RDATAS` rdata
/// cap, a zeroed `HEADER_SCRATCH_SIZE` header scratch).
fn new_insert_state() -> XLogInsertState {
    let registered_buffers: Vec<RegBuf> =
        vec![RegBuf::zeroed(); (XLR_NORMAL_MAX_BLOCK_ID + 1) as usize];
    let hdr_scratch: Vec<u8> = vec![0u8; HEADER_SCRATCH_SIZE];
    XLogInsertState {
        registered_buffers,
        max_registered_block_id: 0,
        mainrdata: Vec::new(),
        mainrdata_len: 0,
        num_rdatas: 0,
        max_rdatas: XLR_NORMAL_RDATAS,
        curinsert_flags: 0,
        hdr_scratch,
        begininsert_called: false,
    }
}

// ---------------------------------------------------------------------------
// xloginsert.c:148  XLogBeginInsert
// ---------------------------------------------------------------------------

/// Begin constructing a WAL record. This must be called before the
/// XLogRegister* functions and XLogInsert().
pub fn XLogBeginInsert() -> PgResult<()> {
    with_state(|state| {
        debug_assert!(state.max_registered_block_id == 0);
        debug_assert!(state.mainrdata.is_empty());
        debug_assert!(state.mainrdata_len == 0);

        /* cross-check on whether we should be here or not */
        if !xlog_seam::xlog_insert_allowed::call() {
            return Err(PgError::error("cannot make new WAL entries during recovery"));
        }

        if state.begininsert_called {
            return Err(PgError::error("XLogBeginInsert was already called"));
        }

        state.begininsert_called = true;
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// xloginsert.c:174  XLogEnsureRecordSpace
// ---------------------------------------------------------------------------

/// Ensure that there are enough buffer and data slots in the working area, for
/// subsequent XLogRegisterBuffer, XLogRegisterData and XLogRegisterBufData calls.
pub fn XLogEnsureRecordSpace(mut max_block_id: i32, mut ndatas: i32) -> PgResult<()> {
    with_state(|state| {
        /*
         * This must be called before entering a critical section, because
         * allocating memory inside a critical section can fail.
         */
        debug_assert!(backend_utils_error::config::crit_section_count() == 0);

        /* the minimum values can't be decreased */
        if max_block_id < XLR_NORMAL_MAX_BLOCK_ID {
            max_block_id = XLR_NORMAL_MAX_BLOCK_ID;
        }
        if ndatas < XLR_NORMAL_RDATAS {
            ndatas = XLR_NORMAL_RDATAS;
        }

        if max_block_id > XLR_MAX_BLOCK_ID {
            return Err(PgError::error(
                "maximum number of WAL record block references exceeded",
            ));
        }
        let nbuffers = max_block_id + 1;

        if nbuffers > state.max_registered_buffers() {
            /*
             * repalloc keeps existing entries; the freshly grown tail must be
             * zeroed because padding bytes are included in WAL data.
             */
            state
                .registered_buffers
                .try_reserve(nbuffers as usize - state.registered_buffers.len())
                .map_err(|_| PgError::error("out of memory growing WAL registered buffers"))?;
            state
                .registered_buffers
                .resize_with(nbuffers as usize, RegBuf::zeroed);
        }

        if ndatas > state.max_rdatas {
            state.max_rdatas = ndatas;
        }

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// xloginsert.c:221  XLogResetInsertion
// ---------------------------------------------------------------------------

/// Reset WAL record construction buffers.
pub fn XLogResetInsertion() {
    with_state(reset_insertion)
}

fn reset_insertion(state: &mut XLogInsertState) {
    for i in 0..state.max_registered_block_id as usize {
        state.registered_buffers[i].in_use = false;
    }

    state.num_rdatas = 0;
    state.max_registered_block_id = 0;
    state.mainrdata.clear();
    state.mainrdata_len = 0;
    state.curinsert_flags = 0;
    state.begininsert_called = false;
}

// ---------------------------------------------------------------------------
// xloginsert.c:241  XLogRegisterBuffer
// ---------------------------------------------------------------------------

/// Register a reference to a buffer with the WAL record being constructed.
/// This must be called for every page that the WAL-logged operation modifies.
pub fn XLogRegisterBuffer(block_id: u8, buffer: Buffer, flags: u8) -> PgResult<()> {
    /* NO_IMAGE doesn't make sense with FORCE_IMAGE */
    debug_assert!(!((flags & REGBUF_FORCE_IMAGE) != 0 && (flags & REGBUF_NO_IMAGE) != 0));

    /*
     * Ordinarily, buffer should be exclusive-locked and marked dirty before we
     * get here. REGBUF_NO_CHANGE bypasses these checks. The
     * BufferIsExclusiveLocked/BufferIsDirty Assert()s are USE_ASSERT_CHECKING
     * only and are not modeled here (no debug-only buffer-state seam exists).
     */
    let _ = REGBUF_NO_CHANGE;

    // The page bytes and tag are read from the buffer manager before we take
    // the working-area borrow.
    let (rlocator, forkno, block) = bufmgr_seam::buffer_get_tag::call(buffer)?;
    let mut page = alloc_block()?;
    bufmgr_seam::with_buffer_page::call(buffer, &mut |p: &mut [u8]| {
        debug_assert!(p.len() == BLCKSZ);
        page.copy_from_slice(&p[..BLCKSZ]);
        Ok(())
    })?;

    with_state(|state| {
        debug_assert!(state.begininsert_called);

        if block_id as i32 >= state.max_registered_block_id {
            if block_id as i32 >= state.max_registered_buffers() {
                return Err(PgError::error("too many registered buffers"));
            }
            state.max_registered_block_id = block_id as i32 + 1;
        }

        {
            let regbuf = &mut state.registered_buffers[block_id as usize];
            regbuf.rlocator = rlocator;
            regbuf.forkno = forkno;
            regbuf.block = block;
            regbuf.page = page;
            regbuf.flags = flags;
            regbuf.reset_rdata();
        }

        check_no_duplicate_page(state, block_id);

        state.registered_buffers[block_id as usize].in_use = true;
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// xloginsert.c:308  XLogRegisterBlock
// ---------------------------------------------------------------------------

/// Like XLogRegisterBuffer, but for registering a block that's not in the
/// shared buffer pool (i.e. when you don't have a Buffer for it). `page` is the
/// page image bytes (caller-supplied, as the C `const PageData *page`).
pub fn XLogRegisterBlock(
    block_id: u8,
    rlocator: &RelFileLocator,
    forknum: ForkNumber,
    blknum: BlockNumber,
    page: &[u8],
    flags: u8,
) -> PgResult<()> {
    // Copy the caller's page image into the owned working slot (C keeps the
    // caller's pointer alive until XLogInsert; we copy eagerly).
    let mut page_copy = alloc_block()?;
    debug_assert!(page.len() == BLCKSZ);
    page_copy.copy_from_slice(&page[..BLCKSZ]);

    with_state(|state| {
        debug_assert!(state.begininsert_called);

        if block_id as i32 >= state.max_registered_block_id {
            state.max_registered_block_id = block_id as i32 + 1;
        }

        if block_id as i32 >= state.max_registered_buffers() {
            return Err(PgError::error("too many registered buffers"));
        }

        {
            let regbuf = &mut state.registered_buffers[block_id as usize];
            regbuf.rlocator = *rlocator;
            regbuf.forkno = forknum;
            regbuf.block = blknum;
            regbuf.page = page_copy;
            regbuf.flags = flags;
            regbuf.reset_rdata();
        }

        check_no_duplicate_page(state, block_id);

        state.registered_buffers[block_id as usize].in_use = true;
        Ok(())
    })
}

/// `USE_ASSERT_CHECKING` duplicate-page check shared by the two register-block
/// entry points: no two in-use block_ids may name the same rlocator/fork/block.
#[inline]
fn check_no_duplicate_page(state: &XLogInsertState, block_id: u8) {
    #[cfg(debug_assertions)]
    {
        let cur = &state.registered_buffers[block_id as usize];
        for i in 0..state.max_registered_block_id as usize {
            let regbuf_old = &state.registered_buffers[i];
            if i == block_id as usize || !regbuf_old.in_use {
                continue;
            }
            debug_assert!(
                !RelFileLocatorEquals(&regbuf_old.rlocator, &cur.rlocator)
                    || regbuf_old.forkno != cur.forkno
                    || regbuf_old.block != cur.block
            );
        }
    }
    let _ = (state, block_id);
}

// ---------------------------------------------------------------------------
// xloginsert.c:363  XLogRegisterData
// ---------------------------------------------------------------------------

/// Add data to the WAL record that's being constructed. The data is appended to
/// the "main chunk", available at replay with XLogRecGetData(). `data` is the
/// bytes to copy (C's `const void *data, uint32 len`).
pub fn XLogRegisterData(data: &[u8]) -> PgResult<()> {
    let len = data.len() as u32;
    with_state(|state| {
        debug_assert!(state.begininsert_called);

        if state.num_rdatas >= state.max_rdatas {
            return Err(err_internal_detail(
                "too much WAL data",
                format!(
                    "{} out of {} data segments are already in use.",
                    state.num_rdatas, state.max_rdatas
                ),
            ));
        }
        state.num_rdatas += 1;

        state
            .mainrdata
            .try_reserve(1)
            .map_err(|_| PgError::error("out of memory registering WAL data"))?;
        let mut owned: Vec<u8> = Vec::new();
        owned
            .try_reserve_exact(data.len())
            .map_err(|_| PgError::error("out of memory registering WAL data"))?;
        owned.extend_from_slice(data);
        state.mainrdata.push(owned);
        state.mainrdata_len += len as u64;
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// xloginsert.c:404  XLogRegisterBufData
// ---------------------------------------------------------------------------

/// Add buffer-specific data to the WAL record that's being constructed.
/// `block_id` must reference a block previously registered with
/// XLogRegisterBuffer(); repeated calls for the same block append.
pub fn XLogRegisterBufData(block_id: u8, data: &[u8]) -> PgResult<()> {
    let len = data.len() as u32;
    with_state(|state| {
        debug_assert!(state.begininsert_called);

        /* find the registered buffer struct */
        {
            let regbuf = &state.registered_buffers[block_id as usize];
            if !regbuf.in_use {
                return Err(PgError::error(format!(
                    "no block with id {block_id} registered with WAL insertion"
                )));
            }

            /*
             * Check against max_rdatas and ensure we do not register more data
             * per buffer than can be handled by the physical data format.
             */
            if state.num_rdatas >= state.max_rdatas {
                return Err(err_internal_detail(
                    "too much WAL data",
                    format!(
                        "{} out of {} data segments are already in use.",
                        state.num_rdatas, state.max_rdatas
                    ),
                ));
            }
            if regbuf.rdata_len + len > UINT16_MAX || len > UINT16_MAX {
                return Err(err_internal_detail(
                    "too much WAL data",
                    format!(
                        "Registering more than maximum {UINT16_MAX} bytes allowed to block {block_id}: current {} bytes, adding {len} bytes.",
                        regbuf.rdata_len
                    ),
                ));
            }
        }

        state.num_rdatas += 1;

        let regbuf = &mut state.registered_buffers[block_id as usize];
        regbuf
            .rdata
            .try_reserve(1)
            .map_err(|_| PgError::error("out of memory registering WAL buffer data"))?;
        let mut owned: Vec<u8> = Vec::new();
        owned
            .try_reserve_exact(data.len())
            .map_err(|_| PgError::error("out of memory registering WAL buffer data"))?;
        owned.extend_from_slice(data);
        regbuf.rdata.push(owned);
        regbuf.rdata_len += len;
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// xloginsert.c:455  XLogSetRecordFlags
// ---------------------------------------------------------------------------

/// Set insert status flags for the upcoming WAL record.
pub fn XLogSetRecordFlags(flags: u8) {
    with_state(|state| {
        debug_assert!(state.begininsert_called);
        state.curinsert_flags |= flags;
    })
}

// ---------------------------------------------------------------------------
// xloginsert.c:473  XLogInsert
// ---------------------------------------------------------------------------

/// Insert an XLOG record having the specified RMID and info bytes, with the body
/// of the record being the data and buffer references registered earlier with
/// XLogRegister* calls. Returns XLOG pointer to end of record.
pub fn XLogInsert(rmid: RmgrId, info: u8) -> PgResult<XLogRecPtr> {
    /* XLogBeginInsert() must have been called. */
    if !with_state(|state| state.begininsert_called) {
        return Err(PgError::error("XLogBeginInsert was not called"));
    }

    /*
     * The caller can set rmgr bits, XLR_SPECIAL_REL_UPDATE and
     * XLR_CHECK_CONSISTENCY; the rest are reserved for use by me.
     */
    if (info & !(XLR_RMGR_INFO_MASK | XLR_SPECIAL_REL_UPDATE | XLR_CHECK_CONSISTENCY)) != 0 {
        return Err(PgError::error(format!("invalid xlog info mask {info:02X}")));
    }

    /* TRACE_POSTGRESQL_WAL_INSERT(rmid, info) — trace probe, no-op. */

    /*
     * In bootstrap mode, we don't actually log anything but XLOG resources;
     * return a phony record pointer.
     */
    if miscinit_seam::is_bootstrap_processing_mode::call() && rmid != RM_XLOG_ID {
        XLogResetInsertion();
        return Ok(SizeOfXLogLongPHD); /* start of 1st chkpt record */
    }

    let curinsert_flags = with_state(|state| state.curinsert_flags);

    let EndPos: XLogRecPtr = loop {
        /*
         * Get values needed to decide whether to do full-page writes. Since we
         * don't yet have an insertion lock, these could change under us, but
         * XLogInsertRecord will recheck them once it has a lock.
         */
        let (RedoRecPtr, doPageWrites) = xlog_seam::get_full_page_write_info::call();

        let record = with_state(|state| {
            XLogRecordAssemble(state, rmid, info, RedoRecPtr, doPageWrites)
        })?;

        // Hand the assembled record to xlog.c's XLogInsertRecord. The body
        // spans are passed in chain order as a slice of byte fragments
        // (rdata[0] is the fixed XLogRecord header carrying the running CRC).
        let span_refs: Vec<&[u8]> = record.spans.iter().map(|s| s.as_slice()).collect();
        let end = xlog_seam::xlog_insert_record::call(
            &span_refs,
            record.fpw_lsn,
            curinsert_flags,
            record.num_fpi,
            record.topxid_included,
        )?;

        if end != InvalidXLogRecPtr {
            break end;
        }
    };

    XLogResetInsertion();

    Ok(EndPos)
}

// ---------------------------------------------------------------------------
// xloginsert.c:547  XLogRecordAssemble
// ---------------------------------------------------------------------------

/// The fully-assembled WAL record handed to `xlog_insert_record`. `spans[0]` is
/// the fixed record header + block/data sub-headers (C's `hdr_rdt`); the
/// remaining spans are the backup-block images and registered data in chain
/// order. `fpw_lsn`, `num_fpi`, and `topxid_included` are the by-reference
/// out-params `XLogRecordAssemble` fills for `XLogInsertRecord`.
struct AssembledRecord {
    spans: Vec<Vec<u8>>,
    fpw_lsn: XLogRecPtr,
    num_fpi: i32,
    topxid_included: bool,
}

/// Assemble a WAL record from the registered data and buffers into an owned
/// [`AssembledRecord`] (the equivalent of C's `XLogRecData` chain), ready for
/// insertion with `XLogInsertRecord()`.
///
/// The record header fields are filled in, except `xl_prev` (set by the WAL
/// engine once the position is known). The calculated CRC does not include the
/// record header yet.
fn XLogRecordAssemble(
    state: &mut XLogInsertState,
    rmid: RmgrId,
    mut info: u8,
    RedoRecPtr: XLogRecPtr,
    doPageWrites: bool,
) -> PgResult<AssembledRecord> {
    let mut total_len: u64 = 0;
    let mut prev_regbuf: i32 = -1;
    let mut num_fpi: i32 = 0;
    let mut topxid_included = false;
    let mut fpw_lsn: XLogRecPtr = InvalidXLogRecPtr;

    // The header scratch builds the fixed record header followed by the block
    // and data sub-headers. `scratch_off` is the write cursor; the first
    // SizeOfXLogRecord bytes are the XLogRecord header (filled at the end).
    let scratch = &mut state.hdr_scratch;
    for b in scratch.iter_mut() {
        *b = 0;
    }
    let mut scratch_off: usize = SIZE_OF_XLOG_RECORD;

    // The assembled record's body spans, in chain order (everything after the
    // header).
    let mut body: Vec<Vec<u8>> = Vec::new();

    /*
     * Enforce consistency checks for this record if user is looking for it.
     */
    if xlog_seam::wal_consistency_checking::call(rmid) {
        info |= XLR_CHECK_CONSISTENCY;
    }

    let wal_compression = xlog_seam::wal_compression::call();

    /*
     * Make the data portions of all block references. This includes the data
     * for full-page images. Also append the headers for the block references in
     * the scratch buffer.
     */
    for block_id in 0..state.max_registered_block_id {
        if !state.registered_buffers[block_id as usize].in_use {
            continue;
        }

        // Snapshot the regbuf fields we need.
        let flags = state.registered_buffers[block_id as usize].flags;
        let regbuf_rdata_len = state.registered_buffers[block_id as usize].rdata_len;
        let regbuf_rlocator = state.registered_buffers[block_id as usize].rlocator;
        let regbuf_forkno = state.registered_buffers[block_id as usize].forkno;
        let regbuf_block = state.registered_buffers[block_id as usize].block;

        let needs_backup: bool;
        let needs_data: bool;

        // XLogRecordBlockHeader bkpb.
        let bkpb_id: u8 = block_id as u8;
        let mut bkpb_fork_flags: u8 = regbuf_forkno as u8;
        let mut bkpb_data_length: u16 = 0;
        // XLogRecordBlockImageHeader bimg.
        let mut bimg_length: u16 = 0;
        let mut bimg_hole_offset: u16 = 0;
        let mut bimg_bimg_info: u8 = 0;
        // XLogRecordBlockCompressHeader cbimg = {0}.
        let mut cbimg_hole_length: u16 = 0;
        let samerel: bool;
        let mut is_compressed = false;
        let include_image: bool;

        /* Determine if this block needs to be backed up */
        if flags & REGBUF_FORCE_IMAGE != 0 {
            needs_backup = true;
        } else if flags & REGBUF_NO_IMAGE != 0 {
            needs_backup = false;
        } else if !doPageWrites {
            needs_backup = false;
        } else {
            /*
             * We assume page LSN is first data on *every* page that can be
             * passed to XLogInsert.
             */
            let page = &state.registered_buffers[block_id as usize].page;
            let page_lsn = PageGetLSN(page);

            let nb = page_lsn <= RedoRecPtr;
            needs_backup = nb;
            if !nb && (fpw_lsn == InvalidXLogRecPtr || page_lsn < fpw_lsn) {
                fpw_lsn = page_lsn;
            }
        }

        /* Determine if the buffer data needs to be included */
        if regbuf_rdata_len == 0 {
            needs_data = false;
        } else if (flags & REGBUF_KEEP_DATA) != 0 {
            needs_data = true;
        } else {
            needs_data = !needs_backup;
        }

        if (flags & REGBUF_WILL_INIT) == REGBUF_WILL_INIT {
            bkpb_fork_flags |= BKPBLOCK_WILL_INIT;
        }

        /*
         * If needs_backup is true or WAL checking is enabled for current
         * resource manager, log a full-page write for the current block.
         */
        include_image = needs_backup || (info & XLR_CHECK_CONSISTENCY) != 0;

        if include_image {
            // The page bytes for this block (owned copy snapshot).
            let page: Vec<u8> = state.registered_buffers[block_id as usize].page.clone();
            debug_assert!(page.len() == BLCKSZ);

            /*
             * The page needs to be backed up, so calculate its hole length and
             * offset.
             */
            if flags & REGBUF_STANDARD != 0 {
                /* Assume we can omit data between pd_lower and pd_upper */
                let lower = page_pd_lower(&page);
                let upper = page_pd_upper(&page);

                if lower as usize >= SizeOfPageHeaderData
                    && upper > lower
                    && upper as usize <= BLCKSZ
                {
                    bimg_hole_offset = lower;
                    cbimg_hole_length = upper - lower;
                } else {
                    /* No "hole" to remove */
                    bimg_hole_offset = 0;
                    cbimg_hole_length = 0;
                }
            } else {
                /* Not a standard page header, don't try to eliminate "hole" */
                bimg_hole_offset = 0;
                cbimg_hole_length = 0;
            }

            /*
             * Try to compress a block image if wal_compression is enabled.
             */
            let mut compressed_page: Vec<u8> = Vec::new();
            let mut compressed_len: u16 = 0;
            if wal_compression != WAL_COMPRESSION_NONE {
                if let Some((cp, clen)) = XLogCompressBackupBlock(
                    wal_compression,
                    &page,
                    bimg_hole_offset,
                    cbimg_hole_length,
                )? {
                    compressed_page = cp;
                    compressed_len = clen;
                    is_compressed = true;
                }
            }

            /* Fill in the remaining fields in the XLogRecordBlockHeader struct */
            bkpb_fork_flags |= BKPBLOCK_HAS_IMAGE;

            /* Report a full page image constructed for the WAL record */
            num_fpi += 1;

            bimg_bimg_info = if cbimg_hole_length == 0 {
                0
            } else {
                BKPIMAGE_HAS_HOLE
            };

            /*
             * If WAL consistency checking is enabled for the resource manager of
             * this WAL record, a full-page image is included in the record for
             * the block modified. During redo, the full-page is replayed only if
             * BKPIMAGE_APPLY is set.
             */
            if needs_backup {
                bimg_bimg_info |= BKPIMAGE_APPLY;
            }

            if is_compressed {
                /* The current compression is stored in the WAL record */
                bimg_length = compressed_len;

                /* Set the compression method used for this block */
                match wal_compression {
                    WAL_COMPRESSION_PGLZ => {
                        bimg_bimg_info |= BKPIMAGE_COMPRESS_PGLZ;
                    }
                    WAL_COMPRESSION_LZ4 => {
                        // USE_LZ4 not defined in this build.
                        let _ = BKPIMAGE_COMPRESS_LZ4;
                        return Err(PgError::error("LZ4 is not supported by this build"));
                    }
                    WAL_COMPRESSION_ZSTD => {
                        // USE_ZSTD not defined in this build.
                        let _ = BKPIMAGE_COMPRESS_ZSTD;
                        return Err(PgError::error("zstd is not supported by this build"));
                    }
                    WAL_COMPRESSION_NONE => {
                        debug_assert!(false); /* cannot happen */
                    }
                    _ => {}
                }

                compressed_page.truncate(compressed_len as usize);
                body.push(compressed_page);
            } else {
                bimg_length = (BLCKSZ as u16) - cbimg_hole_length;

                if cbimg_hole_length == 0 {
                    body.push(page);
                } else {
                    /* must skip the hole */
                    let off = bimg_hole_offset as usize;
                    let holelen = cbimg_hole_length as usize;
                    let first = page[..off].to_vec();
                    let second = page[off + holelen..BLCKSZ].to_vec();
                    body.push(first);
                    body.push(second);
                }
            }

            total_len += bimg_length as u64;
        }

        if needs_data {
            /*
             * When copying to XLogRecordBlockHeader, the length is narrowed to
             * an uint16. Double-check that it is still correct.
             */
            debug_assert!(regbuf_rdata_len <= UINT16_MAX);

            /*
             * Link the caller-supplied rdata chain for this buffer to the
             * overall list.
             */
            bkpb_fork_flags |= BKPBLOCK_HAS_DATA;
            bkpb_data_length = regbuf_rdata_len as u16;
            total_len += regbuf_rdata_len as u64;

            // Copy the per-buffer data chunks into the body chain in order. As
            // with the main-data chunks below, do NOT consume the registered
            // chain — XLogInsert may re-assemble the same record on the
            // full-page-writes retry, and C keeps the chain intact until
            // XLogResetInsertion().
            for c in &state.registered_buffers[block_id as usize].rdata {
                body.push(c.clone());
            }
        }

        if prev_regbuf >= 0
            && RelFileLocatorEquals(
                &regbuf_rlocator,
                &state.registered_buffers[prev_regbuf as usize].rlocator,
            )
        {
            samerel = true;
            bkpb_fork_flags |= BKPBLOCK_SAME_REL;
        } else {
            samerel = false;
        }
        prev_regbuf = block_id;

        /* Ok, copy the header to the scratch buffer */
        // XLogRecordBlockHeader { id: u8, fork_flags: u8, data_length: u16 }
        scratch[scratch_off] = bkpb_id;
        scratch[scratch_off + 1] = bkpb_fork_flags;
        scratch[scratch_off + 2..scratch_off + 4].copy_from_slice(&bkpb_data_length.to_ne_bytes());
        scratch_off += SIZE_OF_XLOG_RECORD_BLOCK_HEADER;

        if include_image {
            // XLogRecordBlockImageHeader { length: u16, hole_offset: u16, bimg_info: u8 }
            scratch[scratch_off..scratch_off + 2].copy_from_slice(&bimg_length.to_ne_bytes());
            scratch[scratch_off + 2..scratch_off + 4]
                .copy_from_slice(&bimg_hole_offset.to_ne_bytes());
            scratch[scratch_off + 4] = bimg_bimg_info;
            scratch_off += SIZE_OF_XLOG_RECORD_BLOCK_IMAGE_HEADER;
            if cbimg_hole_length != 0 && is_compressed {
                // XLogRecordBlockCompressHeader { hole_length: u16 }
                scratch[scratch_off..scratch_off + 2]
                    .copy_from_slice(&cbimg_hole_length.to_ne_bytes());
                scratch_off += SIZE_OF_XLOG_RECORD_BLOCK_COMPRESS_HEADER;
            }
        }
        if !samerel {
            // memcpy(scratch, &regbuf->rlocator, sizeof(RelFileLocator)).
            let rl = rel_file_locator_bytes(&regbuf_rlocator);
            scratch[scratch_off..scratch_off + rl.len()].copy_from_slice(&rl);
            scratch_off += rl.len();
        }
        // memcpy(scratch, &regbuf->block, sizeof(BlockNumber)).
        let blk = regbuf_block.to_ne_bytes();
        scratch[scratch_off..scratch_off + blk.len()].copy_from_slice(&blk);
        scratch_off += blk.len();
    }

    /* followed by the record's origin, if any */
    let replorigin = origin_seam::replorigin_session_origin::call();
    if (state.curinsert_flags & XLOG_INCLUDE_ORIGIN) != 0 && replorigin != InvalidRepOriginId {
        scratch[scratch_off] = XLR_BLOCK_ID_ORIGIN;
        scratch[scratch_off + 1..scratch_off + 3].copy_from_slice(&replorigin.to_ne_bytes());
        scratch_off += 1 + core::mem::size_of::<u16>();
    }

    /* followed by toplevel XID, if not already included in previous record */
    if xact_seam::is_subxact_top_xid_log_pending::call() {
        let xid = xact_seam::get_top_transaction_id_if_any::call();

        /* Set the flag that the top xid is included in the WAL */
        topxid_included = true;

        scratch[scratch_off] = XLR_BLOCK_ID_TOPLEVEL_XID;
        scratch[scratch_off + 1..scratch_off + 1 + core::mem::size_of::<TransactionId>()]
            .copy_from_slice(&xid.to_ne_bytes());
        scratch_off += 1 + core::mem::size_of::<TransactionId>();
    }

    /* followed by main data, if any */
    if state.mainrdata_len > 0 {
        if state.mainrdata_len > 255 {
            if state.mainrdata_len > PG_UINT32_MAX {
                return Err(err_internal_detail(
                    "too much WAL data",
                    format!(
                        "Main data length is {} bytes for a maximum of {} bytes.",
                        state.mainrdata_len, PG_UINT32_MAX
                    ),
                ));
            }

            let mainrdata_len_4b = state.mainrdata_len as u32;
            scratch[scratch_off] = XLR_BLOCK_ID_DATA_LONG;
            scratch[scratch_off + 1..scratch_off + 1 + core::mem::size_of::<u32>()]
                .copy_from_slice(&mainrdata_len_4b.to_ne_bytes());
            scratch_off += 1 + core::mem::size_of::<u32>();
        } else {
            scratch[scratch_off] = XLR_BLOCK_ID_DATA_SHORT;
            scratch[scratch_off + 1] = state.mainrdata_len as u8;
            scratch_off += 2;
        }

        // Copy the main-data chunks into the body chain in order.
        //
        // NOTE: do NOT consume state.mainrdata here. XLogInsert may call
        // XLogRecordAssemble more than once for the same record (the
        // full-page-writes retry loop, xloginsert.c: when XLogInsertRecord
        // returns InvalidXLogRecPtr because the caller must back up a buffer it
        // didn't). In C the registered rdata chain persists across retries and
        // is only cleared by XLogResetInsertion() after a successful insert; a
        // re-assembly walks the same intact chain. Taking the chunks here left
        // mainrdata empty on the second pass while mainrdata_len stayed nonzero,
        // so total_len (-> xl_tot_len) still counted the main data but no body
        // span carried it -> CopyXLogRecordToWAL's `written != write_len`.
        for c in &state.mainrdata {
            body.push(c.clone());
        }
        total_len += state.mainrdata_len;
    }

    let hdr_len = scratch_off; // (scratch - hdr_scratch)
    total_len += hdr_len as u64;

    /*
     * Calculate CRC of the data.
     *
     * Note that the record header isn't added into the CRC initially since we
     * don't know the prev-link yet. Thus, the CRC will represent the CRC of the
     * whole record in the order: rdata, then backup blocks, then record header.
     */
    let mut crc = INIT_CRC32C();
    // COMP_CRC32C(crc, hdr_scratch + SizeOfXLogRecord, hdr_len - SizeOfXLogRecord)
    crc = COMP_CRC32C(crc, &scratch[SIZE_OF_XLOG_RECORD..hdr_len]);
    for span in &body {
        if !span.is_empty() {
            crc = COMP_CRC32C(crc, span);
        }
    }
    let rdata_crc = crc;

    /*
     * Ensure that the XLogRecord is not too large.
     */
    if total_len > XLogRecordMaxSize {
        return Err(err_internal_detail(
            "oversized WAL record",
            format!(
                "WAL record would be {total_len} bytes (of maximum {XLogRecordMaxSize} bytes); rmid {rmid} flags {info}."
            ),
        ));
    }

    /*
     * Fill in the fields in the record header. Prev-link is filled in later,
     * once we know where in the WAL the record will be inserted. The CRC does
     * not include the record header yet.
     *
     * XLogRecord { xl_tot_len:u32@0, xl_xid:u32@4, xl_prev:u64@8, xl_info:u8@16,
     *              xl_rmid:u8@17, [pad @18..20], xl_crc:u32@20 }
     */
    let xl_xid = xact_seam::get_current_transaction_id_if_any::call();
    scratch[0..4].copy_from_slice(&(total_len as u32).to_ne_bytes());
    scratch[4..8].copy_from_slice(&xl_xid.to_ne_bytes());
    scratch[8..16].copy_from_slice(&InvalidXLogRecPtr.to_ne_bytes());
    scratch[16] = info;
    scratch[17] = rmid;
    // padding bytes 18..20 are already zeroed.
    scratch[20..24].copy_from_slice(&rdata_crc.to_ne_bytes());

    // hdr_rdt: the header span is the scratch bytes [0 .. hdr_len].
    let mut spans: Vec<Vec<u8>> = Vec::new();
    spans
        .try_reserve(1 + body.len())
        .map_err(|_| PgError::error("out of memory assembling WAL record"))?;
    spans.push(scratch[..hdr_len].to_vec());
    spans.extend(body);

    Ok(AssembledRecord {
        spans,
        fpw_lsn,
        num_fpi,
        topxid_included,
    })
}

/// `memcpy(scratch, &regbuf->rlocator, sizeof(RelFileLocator))` — the on-WAL
/// byte layout of a `RelFileLocator` is `{ spcOid:u32, dbOid:u32, relNumber:u32 }`
/// (12 bytes), native-endian, exactly as the C struct.
#[inline]
fn rel_file_locator_bytes(rl: &RelFileLocator) -> [u8; 12] {
    let mut out = [0u8; 12];
    out[0..4].copy_from_slice(&rl.spcOid.to_ne_bytes());
    out[4..8].copy_from_slice(&rl.dbOid.to_ne_bytes());
    out[8..12].copy_from_slice(&rl.relNumber.to_ne_bytes());
    out
}

// ---------------------------------------------------------------------------
// CRC32C macros (port/pg_crc32c.h). INIT/FIN/EQ are trivial; COMP is the prim.
// ---------------------------------------------------------------------------

/// `INIT_CRC32C(crc)`.
#[inline]
const fn INIT_CRC32C() -> u32 {
    0xFFFF_FFFF
}

/// `COMP_CRC32C(crc, data, len)` — the CRC32C accumulation primitive
/// (port/pg_crc32c). `FIN_CRC32C` (xor 0xffffffff) is applied by the WAL engine
/// when finalizing; the assembler stores the un-finalized accumulator in
/// `xl_crc`, matching C's `XLogRecordAssemble` (which leaves FIN to the
/// inserter).
#[inline]
fn COMP_CRC32C(crc: u32, data: &[u8]) -> u32 {
    port_crc32c::pg_comp_crc32c_sb8(crc, data)
}

// ---------------------------------------------------------------------------
// xloginsert.c:943  XLogCompressBackupBlock
// ---------------------------------------------------------------------------

/// Create a compressed version of a backup block image.
///
/// Returns `None` if compression fails (i.e. the compressed result is actually
/// bigger than the original). Otherwise returns `Some((compressed_bytes, len))`.
fn XLogCompressBackupBlock(
    wal_compression: i32,
    page: &[u8],
    hole_offset: u16,
    hole_length: u16,
) -> PgResult<Option<(Vec<u8>, u16)>> {
    let orig_len: i32 = BLCKSZ as i32 - hole_length as i32;
    let mut len: i32 = -1;
    let mut extra_bytes: i32 = 0;

    // PGAlignedBlock tmp + `source`.
    let tmp: Vec<u8>;
    let source: &[u8];

    if hole_length != 0 {
        /* must skip the hole */
        let off = hole_offset as usize;
        let holelen = hole_length as usize;
        let mut t = alloc_block()?;
        t[..off].copy_from_slice(&page[..off]);
        t[off..off + (BLCKSZ - (holelen + off))].copy_from_slice(&page[off + holelen..BLCKSZ]);
        t.truncate(orig_len as usize);
        tmp = t;
        source = &tmp;

        /*
         * Extra data needs to be stored in WAL record for the compressed
         * version of block image if the hole exists.
         */
        extra_bytes = SIZE_OF_XLOG_RECORD_BLOCK_COMPRESS_HEADER as i32;
    } else {
        source = &page[..orig_len as usize];
    }

    let mut dest: Vec<u8> = Vec::new();

    match wal_compression {
        WAL_COMPRESSION_PGLZ => {
            // len = pglz_compress(source, orig_len, dest, PGLZ_strategy_default).
            //
            // pglz_compress charges the compressed bytes to the supplied mcx
            // and returns Err(PglzError) on failure (output not smaller, or
            // strategy refuses) — matching pg_lzcompress.c's -1-on-failure
            // contract. The Ok(Err(..)) inner failure maps to len = -1. A
            // transient bump context backs the compression scratch (C uses a
            // PGAlignedBlock on the stack); the bytes are copied out before it
            // drops.
            let scratch_cx = mcx::MemoryContext::new_bump("XLogCompressBackupBlock");
            let res = pglz_compress(scratch_cx.mcx(), source, Some(PGLZ_strategy_default()))?;
            match res {
                Ok(out) => {
                    len = out.len() as i32;
                    dest = out[..].to_vec();
                }
                Err(_e) => {
                    let _: PglzError = _e;
                    len = -1; /* failure */
                }
            }
        }
        WAL_COMPRESSION_LZ4 => {
            // USE_LZ4 not defined in this build.
            return Err(PgError::error("LZ4 is not supported by this build"));
        }
        WAL_COMPRESSION_ZSTD => {
            // USE_ZSTD not defined in this build.
            return Err(PgError::error("zstd is not supported by this build"));
        }
        WAL_COMPRESSION_NONE => {
            debug_assert!(false); /* cannot happen */
        }
        _ => {}
    }

    /*
     * We recheck the actual size even if compression reports success and see if
     * the number of bytes saved by compression is larger than the length of
     * extra data needed for the compressed version of block image.
     */
    if len >= 0 && len + extra_bytes < orig_len {
        return Ok(Some((dest, len as u16))); /* successful compression */
    }
    Ok(None)
}

// ---------------------------------------------------------------------------
// xloginsert.c:1026  XLogCheckBufferNeedsBackup
// ---------------------------------------------------------------------------

/// Determine whether the buffer referenced has to be backed up.
///
/// Since we don't yet have the insert lock, fullPageWrites and runningBackups
/// could change later, so the result should be used for optimization only.
pub fn XLogCheckBufferNeedsBackup(buffer: Buffer) -> PgResult<bool> {
    let (RedoRecPtr, doPageWrites) = xlog_seam::get_full_page_write_info::call();

    let mut page_lsn: XLogRecPtr = InvalidXLogRecPtr;
    bufmgr_seam::with_buffer_page::call(buffer, &mut |p: &mut [u8]| {
        page_lsn = PageGetLSN(p);
        Ok(())
    })?;

    if doPageWrites && page_lsn <= RedoRecPtr {
        return Ok(true); /* buffer requires backup */
    }

    Ok(false) /* buffer does not need to be backed up */
}

// ---------------------------------------------------------------------------
// xloginsert.c:1064  XLogSaveBufferForHint
// ---------------------------------------------------------------------------

/// Write a backup block if needed when we are setting a hint. Callable while
/// holding just a share lock on the buffer content.
pub fn XLogSaveBufferForHint(buffer: Buffer, buffer_std: bool) -> PgResult<XLogRecPtr> {
    let mut recptr: XLogRecPtr = InvalidXLogRecPtr;

    /*
     * Ensure no checkpoint can change our view of RedoRecPtr. The C
     * `Assert(MyProc->delayChkptFlags & DELAY_CHKPT_START)` is USE_ASSERT_
     * CHECKING only and is not modeled here.
     */

    /* Update RedoRecPtr so that we can make the right decision */
    let RedoRecPtr = xlog_seam::get_redo_rec_ptr::call();

    /*
     * We assume page LSN is first data on *every* page that can be passed to
     * XLogInsert. Since we're only holding a share-lock on the page, we must
     * take the buffer header lock when we look at the LSN.
     */
    let lsn = bufmgr_seam::buffer_get_lsn_atomic::call(buffer)?;

    if lsn <= RedoRecPtr {
        let mut flags: u8 = 0;
        let mut copied_buffer = alloc_block()?;

        /*
         * Copy buffer so we don't have to worry about concurrent hint bit or
         * lsn updates. We assume pd_lower/upper cannot be changed without an
         * exclusive lock, so the contents bkp are not racy.
         */
        if buffer_std {
            /* Assume we can omit data between pd_lower and pd_upper */
            bufmgr_seam::with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
                let lower = page_pd_lower(page) as usize;
                let upper = page_pd_upper(page) as usize;
                copied_buffer[..lower].copy_from_slice(&page[..lower]);
                copied_buffer[upper..BLCKSZ].copy_from_slice(&page[upper..BLCKSZ]);
                Ok(())
            })?;
        } else {
            bufmgr_seam::with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
                copied_buffer[..BLCKSZ].copy_from_slice(&page[..BLCKSZ]);
                Ok(())
            })?;
        }

        XLogBeginInsert()?;

        if buffer_std {
            flags |= REGBUF_STANDARD;
        }

        let (rlocator, forkno, block) = bufmgr_seam::buffer_get_tag::call(buffer)?;
        XLogRegisterBlock(0, &rlocator, forkno, block, &copied_buffer, flags)?;

        recptr = XLogInsert(RM_XLOG_ID, XLOG_FPI_FOR_HINT)?;
    }

    Ok(recptr)
}

// ---------------------------------------------------------------------------
// xloginsert.c:1142  log_newpage
// ---------------------------------------------------------------------------

/// Write a WAL record containing a full image of a page. Caller is responsible
/// for writing the page to disk after calling this routine.
///
/// Returns `(recptr, new_page_bytes)`: the record LSN and the page with its LSN
/// set (unless the page is new). C mutates the caller's page in place via
/// `PageSetLSN`; this port returns the updated page so the caller can store it
/// back.
pub fn log_newpage(
    rlocator: &RelFileLocator,
    forknum: ForkNumber,
    blkno: BlockNumber,
    mut page: Vec<u8>,
    page_std: bool,
) -> PgResult<(XLogRecPtr, Vec<u8>)> {
    let mut flags: u8 = REGBUF_FORCE_IMAGE;
    if page_std {
        flags |= REGBUF_STANDARD;
    }

    XLogBeginInsert()?;
    XLogRegisterBlock(0, rlocator, forknum, blkno, &page, flags)?;
    let recptr = XLogInsert(RM_XLOG_ID, XLOG_FPI)?;

    /*
     * The page may be uninitialized. If so, we can't set the LSN because that
     * would corrupt the page.
     */
    if !PageIsNew(&page) {
        PageSetLSN(&mut page, recptr);
    }

    Ok((recptr, page))
}

// ---------------------------------------------------------------------------
// xloginsert.c:1174  log_newpages
// ---------------------------------------------------------------------------

/// Like `log_newpage`, but allows logging multiple pages in one operation.
///
/// Takes the pages by value and returns them with their LSNs set (unless new),
/// mirroring `log_newpage`'s owned-page convention.
#[allow(clippy::too_many_arguments)]
pub fn log_newpages(
    rlocator: &RelFileLocator,
    forknum: ForkNumber,
    num_pages: i32,
    blknos: &[BlockNumber],
    mut pages: Vec<Vec<u8>>,
    page_std: bool,
) -> PgResult<Vec<Vec<u8>>> {
    let mut flags: u8 = REGBUF_FORCE_IMAGE;
    if page_std {
        flags |= REGBUF_STANDARD;
    }

    /*
     * Iterate over all the pages. They are collected into batches of
     * XLR_MAX_BLOCK_ID pages, and a single WAL-record is written for each batch.
     */
    XLogEnsureRecordSpace(XLR_MAX_BLOCK_ID - 1, 0)?;

    let mut i: i32 = 0;
    while i < num_pages {
        let batch_start = i;

        XLogBeginInsert()?;

        let mut nbatch: i32 = 0;
        while nbatch < XLR_MAX_BLOCK_ID && i < num_pages {
            XLogRegisterBlock(
                nbatch as u8,
                rlocator,
                forknum,
                blknos[i as usize],
                &pages[i as usize],
                flags,
            )?;
            i += 1;
            nbatch += 1;
        }

        let recptr = XLogInsert(RM_XLOG_ID, XLOG_FPI)?;

        let mut j = batch_start;
        while j < i {
            /*
             * The page may be uninitialized. If so, we can't set the LSN because
             * that would corrupt the page.
             */
            if !PageIsNew(&pages[j as usize]) {
                PageSetLSN(&mut pages[j as usize], recptr);
            }
            j += 1;
        }
    }

    Ok(pages)
}

// ---------------------------------------------------------------------------
// xloginsert.c:1236  log_newpage_buffer
// ---------------------------------------------------------------------------

/// Write a WAL record containing a full image of a page. Caller should
/// initialize the buffer and mark it dirty before calling. Sets the page LSN
/// (via the buffer manager seam).
pub fn log_newpage_buffer(buffer: Buffer, page_std: bool) -> PgResult<XLogRecPtr> {
    let mut page = alloc_block()?;
    bufmgr_seam::with_buffer_page::call(buffer, &mut |p: &mut [u8]| {
        page.copy_from_slice(&p[..BLCKSZ]);
        Ok(())
    })?;

    /* Shared buffers should be modified in a critical section. */
    debug_assert!(backend_utils_error::config::crit_section_count() > 0);

    let (rlocator, forkno, block) = bufmgr_seam::buffer_get_tag::call(buffer)?;

    let (recptr, new_page) = log_newpage(&rlocator, forkno, block, page, page_std)?;
    // C's log_newpage writes the LSN into the shared-buffer page in place;
    // mirror that by writing the updated page LSN back through the buffer
    // manager.
    if !PageIsNew(&new_page) {
        bufmgr_seam::page_set_lsn::call(buffer, recptr)?;
    }
    Ok(recptr)
}

// ---------------------------------------------------------------------------
// xloginsert.c:1269  log_newpage_range
// ---------------------------------------------------------------------------

/// WAL-log a range of blocks in a relation. An image of all pages with block
/// numbers `startblk <= X < endblk` is written to the WAL (in multiple records
/// if the range is large).
pub fn log_newpage_range(
    rel: &types_rel::Relation<'_>,
    forknum: ForkNumber,
    startblk: BlockNumber,
    endblk: BlockNumber,
    page_std: bool,
) -> PgResult<()> {
    let mut flags: u8 = REGBUF_FORCE_IMAGE;
    if page_std {
        flags |= REGBUF_STANDARD;
    }

    /*
     * Iterate over all the pages in the range. They are collected into batches
     * of XLR_MAX_BLOCK_ID pages, and a single WAL-record is written for each
     * batch.
     */
    XLogEnsureRecordSpace(XLR_MAX_BLOCK_ID - 1, 0)?;

    let mut blkno = startblk;
    while blkno < endblk {
        // Buffer bufpack[XLR_MAX_BLOCK_ID].
        let mut bufpack: Vec<Buffer> = vec![0; XLR_MAX_BLOCK_ID as usize];
        let mut nbufs: i32 = 0;

        miscinit_seam::check_for_interrupts::call()?;

        /* Collect a batch of blocks. */
        while nbufs < XLR_MAX_BLOCK_ID && blkno < endblk {
            let buf = bufmgr_seam::read_buffer_extended_fork::call(rel, forknum, blkno)?;

            bufmgr_seam::lock_buffer_exclusive::call(buf)?;

            /*
             * Completely empty pages are not WAL-logged. Writing a WAL record
             * would change the LSN, and we don't want that. We want the page to
             * stay empty.
             */
            let mut is_new = false;
            bufmgr_seam::with_buffer_page::call(buf, &mut |page: &mut [u8]| {
                is_new = PageIsNew(page);
                Ok(())
            })?;
            if !is_new {
                bufpack[nbufs as usize] = buf;
                nbufs += 1;
            } else {
                bufmgr_seam::unlock_release_buffer::call(buf);
            }
            blkno += 1;
        }

        /* Nothing more to do if all remaining blocks were empty. */
        if nbufs == 0 {
            break;
        }

        /* Write WAL record for this batch. */
        XLogBeginInsert()?;

        miscinit_seam::start_crit_section::call();
        let mut i: i32 = 0;
        while i < nbufs {
            bufmgr_seam::mark_buffer_dirty::call(bufpack[i as usize]);
            XLogRegisterBuffer(i as u8, bufpack[i as usize], flags)?;
            i += 1;
        }

        let recptr = XLogInsert(RM_XLOG_ID, XLOG_FPI)?;

        i = 0;
        while i < nbufs {
            bufmgr_seam::page_set_lsn::call(bufpack[i as usize], recptr)?;
            bufmgr_seam::unlock_release_buffer::call(bufpack[i as usize]);
            i += 1;
        }
        miscinit_seam::end_crit_section::call();
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// xloginsert.c:1347  InitXLogInsert
// ---------------------------------------------------------------------------

/// Allocate working buffers needed for WAL record construction.
///
/// In C this lazily palloc's the file-static arrays into `xloginsert_cxt`. This
/// port mirrors that: it (re)initializes the backend-local working area to the
/// same initial sizes. `Err` carries its OOM surface (the seam contract).
pub fn InitXLogInsert() -> PgResult<()> {
    XLOG_INSERT_STATE.with(|cell| {
        let mut slot = cell.borrow_mut();
        if slot.is_none() {
            *slot = Some(new_insert_state());
        }
    });
    Ok(())
}

// ---------------------------------------------------------------------------
// Combined convenience entry consumed across cycle boundaries.
// ---------------------------------------------------------------------------

/// `XLogBeginInsert()`, one `XLogRegisterData(fragment)` per `fragments` entry
/// (in order), `XLogSetRecordFlags(flags)` (skipped when `flags == 0`), then
/// `XLogInsert(rmid, info)`. The high-level all-in-one used by callers (xact.c,
/// clog.c, etc.) that have no per-buffer registration to do.
fn xlog_insert(rmid: RmgrId, info: u8, flags: u8, fragments: &[&[u8]]) -> PgResult<XLogRecPtr> {
    XLogBeginInsert()?;
    for frag in fragments {
        XLogRegisterData(frag)?;
    }
    if flags != 0 {
        XLogSetRecordFlags(flags);
    }
    XLogInsert(rmid, info)
}

// ---------------------------------------------------------------------------
// Seam installation.
// ---------------------------------------------------------------------------

/// Install this unit's inward seams (the WAL-record construction API that other
/// resource-manager crates call across a dependency cycle).
pub fn init_seams() {
    use backend_access_transam_xloginsert_seams as s;

    s::xlog_insert::set(xlog_insert);
    s::xlog_begin_insert::set(XLogBeginInsert);
    s::xlog_register_data::set(XLogRegisterData);
    s::xlog_register_buffer::set(XLogRegisterBuffer);
    xlog_seam::xlog_ensure_record_space::set(XLogEnsureRecordSpace);
    // `EndPrepare` (twophase.c) emits the 2PC PREPARE WAL record via the
    // standard XLogInsert path. The chunk-assembly + crit-section/DELAY_CHKPT
    // bracket stay in twophase's `end_prepare`; this seam is the
    // `XLogBeginInsert(); XLogRegisterData(body); XLogSetRecordFlags(
    // XLOG_INCLUDE_ORIGIN); XLogInsert(RM_XACT_ID, XLOG_XACT_PREPARE)` tail.
    xlog_seam::xlog_insert_prepare::set(|body: &[u8]| -> PgResult<XLogRecPtr> {
        XLogBeginInsert()?;
        XLogRegisterData(body)?;
        XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);
        XLogInsert(types_wal::RM_XACT_ID, types_wal::XLOG_XACT_PREPARE)
    });
    s::xlog_register_block::set(|block_id, rlocator, forknum, blknum, page, flags| {
        XLogRegisterBlock(block_id, &rlocator, forknum, blknum, page, flags)
    });
    s::xlog_register_buf_data::set(XLogRegisterBufData);
    s::xlog_insert_record::set(XLogInsert);
    s::xlog_set_record_flags::set(XLogSetRecordFlags);
    s::xlog_reset_insertion::set(XLogResetInsertion);
    s::init_xlog_insert::set(InitXLogInsert);
    s::log_newpage_buffer::set(log_newpage_buffer);
    s::log_newpage_range::set(log_newpage_range);
    s::xlog_save_buffer_for_hint::set(XLogSaveBufferForHint);

    // `log_newpage` (xloginsert.c) is declared as a bufmgr seam (consumed by the
    // hash AM); xloginsert OWNS the C function, so it installs it here. The seam
    // hands the page image in as a slice and only wants the record LSN back; the
    // owner takes/returns the page by value (it stamps the LSN into a NON-new
    // page), so adapt: copy in, drop the returned image.
    backend_storage_buffer_bufmgr_seams::log_newpage::set(
        |rlocator, forknum, blkno, page, page_std| {
            let (recptr, _page) = log_newpage(&rlocator, forknum, blkno, page.to_vec(), page_std)?;
            Ok(recptr)
        },
    );

    // --- lazy-vacuum driver's log_newpage_buffer (vacuumlazy.c
    //     lazy_vacuum_heap_page WAL-logging of a no-longer-all-visible page).
    //     Homes in vacuumlazy-seams; xloginsert.c owns it. The driver discards
    //     the returned record LSN. ---
    backend_access_heap_vacuumlazy_seams::log_newpage_buffer::set(|buffer, page_std| {
        log_newpage_buffer(buffer, page_std).map(|_| ())
    });
}

#[cfg(test)]
mod tests;
