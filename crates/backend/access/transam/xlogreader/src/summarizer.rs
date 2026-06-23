//! Handle-based private-reader API for the WAL summarizer
//! (`walsummarizer.c` `SummarizeWAL`).
//!
//! The summarizer allocates its own [`XLogReaderState`] for one summary pass
//! and drives it through the handle-based `summarizer_*` seams (declared in
//! `backend-access-transam-xlogreader-seams`, consumed by the walsummarizer
//! unit). Because the summarizer's reader carries a per-pass
//! `SummarizerReadLocalXLogPrivate` private-data block whose shape the shared
//! [`XLogReaderState`] does not model, the reader is named by an opaque
//! [`XLogReaderHandle`] token; the summarizer keeps its own private-data keyed
//! by the same token.
//!
//! # Reader registry (C-faithful raw-pointer)
//!
//! In C the reader is a heap pointer (`XLogReaderState *`) passed freely to
//! `XLogReadRecord`, `WALRead`, `XLogRecGet*`, etc.; the page-read callback
//! re-enters `WALRead` on the *same* pointer while a record read is in flight.
//! We mirror that exactly with a per-backend registry keyed by handle. Each
//! entry boxes the reader (so its address is stable) alongside the leaked
//! [`MemoryContext`] that backs its `decode_arena` (the C reader
//! `MemoryContext`) and the summarizer's page-read callback. Seams resolve a
//! handle to the boxed reader's raw pointer and reborrow `&mut` only for their
//! own scope; the re-entrant `summarizer_wal_read` call from inside the
//! page-read callback reborrows the same pointer to reach `seg`/`segcxt`
//! (disjoint from the `readBuf` slice the callback fills), exactly as the C
//! single-threaded reader does.
//!
//! `XLogReaderFree` drops the boxed reader and then the leaked context (the C
//! `pfree(state)` + context teardown).

extern crate alloc;

use alloc::boxed::Box;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use mcx::MemoryContext;

use types_core::{TimeLineID, XLogRecPtr, XLogSegNo};
use types_error::{PgError, PgResult};
use types_storage::RelFileLocator;
use wal::rmgr::{XLogReaderRoutine, XLogReaderState, XLREAD_FAIL};
use wal::xlog_consts::{XLOGDIR, XLOG_BLCKSZ};

use types_walsummarizer::{BlockTag, ReadRecordResult, XLogReaderHandle};

use xlogreader_seams as seam;
use xlogreader_seams::SummarizerPageReadCB;
use fd_seams as fd;

// ===========================================================================
// Registry
// ===========================================================================

/// One live summarizer reader: the boxed [`XLogReaderState`] (stable address),
/// the leaked [`MemoryContext`] backing its `decode_arena`, and the
/// summarizer's page-read callback (the `routine.page_read` analogue that the
/// value-typed trampoline forwards to).
///
/// `'static` because the registry outlives any single seam call; the leaked
/// context gives the reader a `'static` decode arena (the C reader owns its
/// `MemoryContext` for the pass).
struct ReaderEntry {
    reader: Box<XLogReaderState<'static>>,
    /// The owning context for `decode_arena` (and the reader's allocations).
    /// Boxed + leaked so the `Mcx<'static>` inside `reader.decode_arena` stays
    /// valid; reclaimed by [`free_reader`].
    ctx: *mut MemoryContext,
    /// `summarizer_read_local_xlog_page` — the summarizer's page-read callback.
    page_read: SummarizerPageReadCB,
}

struct Registry {
    /// handle id -> entry. A `Vec` of `Option` slot is enough: the summarizer
    /// allocates one reader at a time per pass, and the id is monotonic.
    slots: Vec<Option<ReaderEntry>>,
    next_id: u64,
}

impl Registry {
    const fn new() -> Self {
        Registry { slots: Vec::new(), next_id: 1 }
    }
}

// The crate is `#![no_std]`; the registry needs interior-mutable per-backend
// storage. PG is single-threaded per backend, so a non-thread-local static
// behind a `RefCell` matches the C `XLogReaderState *` global-free model.
// `RefCell` (not a raw `static mut`) keeps the borrow discipline checked.
//
// We deliberately do not require `std`: a `RefCell<Registry>` in a
// `#[no_std]` crate is fine (it is in `core`); the only constraint is
// `!Sync`, satisfied because the registry is only ever touched from the one
// backend thread (as in C).
struct BackendCell(core::cell::RefCell<Registry>);
// SAFETY: a backend is single-threaded; the registry is never shared across
// threads (the summarizer runs in one auxiliary process). Mirrors the C
// per-process reader pointers.
unsafe impl Sync for BackendCell {}

static REGISTRY_CELL: BackendCell = BackendCell(core::cell::RefCell::new(Registry::new()));

fn with_registry<R>(f: impl FnOnce(&mut Registry) -> R) -> R {
    f(&mut REGISTRY_CELL.0.borrow_mut())
}

/// Resolve a handle to the boxed reader's raw pointer (the C `XLogReaderState
/// *`). Panics on a stale/unknown handle (a use-after-free in C). The pointer
/// is valid until [`free_reader`]; callers reborrow `&mut` for their own scope
/// only, mirroring C's single-threaded re-entrant reader access.
fn reader_ptr(handle: XLogReaderHandle) -> *mut XLogReaderState<'static> {
    with_registry(|reg| {
        let idx = (handle.0 as usize).wrapping_sub(1);
        let entry = reg
            .slots
            .get_mut(idx)
            .and_then(|s| s.as_mut())
            .unwrap_or_else(|| panic!("summarizer reader handle {} is not live", handle.0));
        entry.reader.as_mut() as *mut XLogReaderState<'static>
    })
}

/// The summarizer page-read callback registered for `handle`.
fn page_read_cb(handle: XLogReaderHandle) -> SummarizerPageReadCB {
    with_registry(|reg| {
        let idx = (handle.0 as usize).wrapping_sub(1);
        reg.slots
            .get(idx)
            .and_then(|s| s.as_ref())
            .unwrap_or_else(|| panic!("summarizer reader handle {} is not live", handle.0))
            .page_read
    })
}

/// Run `f` with an exclusive `&mut` to the reader behind `handle`. The borrow
/// lasts only for `f`; re-entrant seam calls (e.g. `summarizer_wal_read` from
/// inside the page-read callback) reborrow the same pointer afresh, which is
/// sound because each access touches disjoint reader state, exactly as the C
/// single-threaded reader does.
fn with_reader<R>(handle: XLogReaderHandle, f: impl FnOnce(&mut XLogReaderState<'static>) -> R) -> R {
    let ptr = reader_ptr(handle);
    // SAFETY: `ptr` points at a boxed reader live in the registry until
    // `free_reader`; PG backends are single-threaded so there is no concurrent
    // access. Re-entrancy reborrows disjoint fields (the page-read callback
    // fills `readBuf`; the nested `WALRead` touches `seg`/`segcxt`).
    f(unsafe { &mut *ptr })
}

// ===========================================================================
// Page-read trampoline
// ===========================================================================

/// The value-typed `routine.page_read` installed on every summarizer reader.
/// It recovers the reader's handle from `private_data` (the C
/// `state->private_data` analogue), fetches the summarizer's handle-based
/// callback, and forwards the read into the reader's `readBuf` (the C
/// `cur_page`).
fn page_read_trampoline(
    state: &mut XLogReaderState<'_>,
    target_page_ptr: XLogRecPtr,
    req_len: i32,
    _target_rec_ptr: XLogRecPtr,
) -> PgResult<i32> {
    let handle = reader_handle(state);
    let cb = page_read_cb(handle);

    // Ensure readBuf exists (allocated in XLogReaderAllocate); the callback
    // fills it as the C `cur_page`.
    if state.readBuf.is_none() {
        return Ok(XLREAD_FAIL);
    }
    // Borrow the readBuf out so the callback (which re-enters the registry via
    // summarizer_wal_read) does not alias it through the reader pointer.
    let mut buf = state.readBuf.take().unwrap();
    let result = cb(handle, target_page_ptr, req_len, buf.as_mut_slice());
    state.readBuf = Some(buf);
    result
}

/// Recover a reader's handle by reverse-looking-up its address in the
/// registry. The page-read trampoline holds a `&mut XLogReaderState` that is
/// the boxed reader living in exactly one registry slot; matching the boxed
/// reader's address recovers its handle (the C `state->private_data` token).
fn reader_handle(state: &XLogReaderState<'_>) -> XLogReaderHandle {
    let target = state as *const XLogReaderState<'_> as *const ();
    with_registry(|reg| {
        for (i, slot) in reg.slots.iter().enumerate() {
            if let Some(entry) = slot {
                let addr = entry.reader.as_ref() as *const XLogReaderState<'static> as *const ();
                if addr == target {
                    return XLogReaderHandle((i as u64) + 1);
                }
            }
        }
        panic!("summarizer page-read callback fired on an unregistered reader")
    })
}

// ===========================================================================
// Lifecycle (XLogReaderAllocate / XLogReaderFree)
// ===========================================================================

/// `XLogReaderAllocate(wal_segment_size, NULL, XL_ROUTINE(.page_read = ...,
/// .segment_open = wal_segment_open, .segment_close = wal_segment_close),
/// NULL)` for the summarizer's private reader. Returns the reader's registry
/// handle.
fn allocate_reader(
    wal_segment_size: i32,
    page_read: SummarizerPageReadCB,
) -> PgResult<XLogReaderHandle> {
    // Leak the reader's MemoryContext so its Mcx<'static> backs the boxed
    // reader's decode_arena for the lifetime of the pass (the C reader owns
    // its MemoryContext; freed in free_reader).
    let ctx_box = Box::new(MemoryContext::new("xlog reader (summarizer)"));
    let ctx_ptr: *mut MemoryContext = Box::into_raw(ctx_box);
    // SAFETY: ctx_ptr is a freshly leaked Box, alive until free_reader.
    let arena = unsafe { (*ctx_ptr).mcx() };

    let mut reader = Box::new(XLogReaderState {
        decode_arena: Some(arena),
        ..Default::default()
    });

    // initialize caller-provided support functions
    reader.routine = XLogReaderRoutine {
        page_read: Some(page_read_trampoline),
        // segment_open / segment_close are driven in-crate by WALRead (the
        // summarizer's stock wal_segment_open / wal_segment_close); the
        // value-typed routine slots are unused for this reader.
        segment_open: None,
        segment_close: None,
    };

    // Permanently allocate readBuf (XLOG_BLCKSZ), MAXALIGN'd by the arena.
    let mut read_buf = match mcx::vec_with_capacity_in(arena, XLOG_BLCKSZ) {
        Ok(v) => v,
        Err(_) => {
            // pfree(state) path; reclaim the leaked context.
            free_ctx(ctx_ptr);
            return Err(oom());
        }
    };
    read_buf.resize(XLOG_BLCKSZ, 0);
    reader.readBuf = Some(read_buf);

    // WALOpenSegmentInit(&seg, &segcxt, wal_segment_size, NULL): seg.ws_file =
    // -1 (Default), segcxt.ws_segsize = wal_segment_size.
    reader.segcxt.ws_segsize = wal_segment_size;

    // errormsg_buf is the optional arena string in this model (None == "\0").
    reader.errormsg_buf = None;

    // Allocate an initial readRecordBuf of minimal size.
    if crate::allocate_recordbuf(&mut reader, 0).is_err() {
        free_ctx(ctx_ptr);
        return Err(oom());
    }

    with_registry(|reg| {
        let id = reg.next_id;
        reg.next_id += 1;
        let handle = XLogReaderHandle(id);
        let idx = (id as usize) - 1;
        debug_assert_eq!(idx, reg.slots.len());
        // state->private_data = private_data: the summarizer keeps its own
        // SummarizerReadLocalXLogPrivate keyed by this same handle. The owner
        // recovers the handle from the boxed reader's address (reader_handle),
        // so no opaque token need be stored in `private_data` here.
        reg.slots.push(Some(ReaderEntry { reader, ctx: ctx_ptr, page_read }));
        Ok(handle)
    })
}

fn free_reader(handle: XLogReaderHandle) {
    let entry = with_registry(|reg| {
        let idx = (handle.0 as usize).wrapping_sub(1);
        reg.slots.get_mut(idx).and_then(|s| s.take())
    });
    if let Some(entry) = entry {
        let ctx = entry.ctx;
        // Drop the boxed reader first (the C `pfree(state)` and its arena
        // contents), then the context (its allocations are reclaimed wholesale).
        drop(entry.reader);
        free_ctx(ctx);
    }
}

fn free_ctx(ctx: *mut MemoryContext) {
    // SAFETY: ctx came from Box::into_raw in allocate_reader and is freed once.
    drop(unsafe { Box::from_raw(ctx) });
}

fn oom() -> PgError {
    PgError::error(String::from("out of memory"))
}

// ===========================================================================
// XLogReadRecord (the summarizer's blocking record read)
// ===========================================================================

/// `XLogReadRecord(reader, &errormsg)` (xlogreader.c:389) discriminated into
/// [`ReadRecordResult`]. Ensures the queue has a record (blocking) then
/// consumes the head. NULL with an error => `Error`; NULL with the
/// summarizer's `private_data->end_of_wal` (surfaced through the page-read
/// callback returning `-1`) => `EndOfWal`.
fn read_record(state: &mut XLogReaderState<'static>) -> PgResult<ReadRecordResult> {
    crate::XLogReleasePreviousRecord(state);
    if !crate::XLogReaderHasQueuedRecordOrError(state) {
        crate::XLogReadAhead(state, false)?;
    }
    match crate::XLogNextRecord(state) {
        Some(_) => Ok(ReadRecordResult::Record),
        None => {
            // NULL return: distinguish a deferred error from end-of-WAL. The
            // summarizer's page-read callback sets end_of_wal and returns -1 on
            // a historic timeline; that surfaces here as no queued record and
            // no deferred error message.
            let errmsg = crate::xlog_reader_deferred_errmsg(state);
            if errmsg.is_some() {
                Ok(ReadRecordResult::Error { errormsg: errmsg })
            } else {
                Ok(ReadRecordResult::EndOfWal)
            }
        }
    }
}

// ===========================================================================
// WALRead (xlogreader.c) — driven by the summarizer's page-read callback.
// ===========================================================================

/// `XLogFilePath(path, tli, logSegNo, wal_segsz_bytes)` -> `XLOGDIR "/" name`.
fn xlog_file_path(tli: TimeLineID, log_seg_no: XLogSegNo, wal_segsz_bytes: i32) -> String {
    format!("{}/{}", XLOGDIR, crate::XLogFileName(tli, log_seg_no, wal_segsz_bytes))
}

/// Stock `XLogReaderRoutine->segment_open` for the summarizer reader
/// (`wal_segment_open`): open the WAL segment file and store its bare fd in
/// `seg.ws_file`.
fn wal_segment_open(state: &mut XLogReaderState<'_>, next_seg_no: XLogSegNo, tli: TimeLineID) -> PgResult<()> {
    let path = xlog_file_path(tli, next_seg_no, state.segcxt.ws_segsize);
    match fd::basic_open_file::call(&path) {
        Ok(fd_value) => {
            state.seg.ws_file = fd_value;
            Ok(())
        }
        Err(err_errno) => {
            // ENOENT == 2: the segment has already been removed.
            if err_errno == 2 {
                Err(PgError::error(format!(
                    "requested WAL segment {path} has already been removed"
                )))
            } else {
                Err(PgError::error(format!("could not open file \"{path}\"")))
            }
        }
    }
}

/// Stock `XLogReaderRoutine->segment_close` (`wal_segment_close`):
/// `close(seg.ws_file); seg.ws_file = -1`.
fn wal_segment_close(state: &mut XLogReaderState<'_>) {
    if state.seg.ws_file >= 0 {
        // CloseTransientFile / close(2) on a bare fd. The summarizer uses
        // BasicOpenFile fds; close them directly via the fd-seams transient
        // close (it tolerates a bare kernel fd value).
        let _ = fd::close_transient_file::call(state.seg.ws_file);
        state.seg.ws_file = -1;
    }
}

/// `WALRead(state, buf, startptr, count, tli, &errinfo)` (xlogreader.c). Reads
/// `count` bytes of WAL at `startptr` on timeline `tli` into `buf`, opening
/// segments via [`wal_segment_open`] / [`wal_segment_close`]. On a read failure
/// raises the C `WALReadRaiseError` `ereport(ERROR)` (carried on `Err`).
fn wal_read(
    state: &mut XLogReaderState<'_>,
    buf: &mut [u8],
    startptr: XLogRecPtr,
    count: i32,
    mut tli: TimeLineID,
) -> PgResult<()> {
    let mut recptr = startptr;
    let mut nbytes = count as i64;
    let mut p: usize = 0; // offset into `buf`

    while nbytes > 0 {
        let startoff = crate::XLogSegmentOffset(recptr, state.segcxt.ws_segsize);

        // If the data we want is not in a segment we have open, close what we
        // have (if anything) and open the next one.
        if state.seg.ws_file < 0
            || !xl_byte_in_seg(recptr, state.seg.ws_segno, state.segcxt.ws_segsize)
            || tli != state.seg.ws_tli
        {
            if state.seg.ws_file >= 0 {
                wal_segment_close(state);
            }

            let next_seg_no = crate::XLByteToSeg(recptr, state.segcxt.ws_segsize);
            wal_segment_open(state, next_seg_no, tli)?;

            debug_assert!(state.seg.ws_file >= 0);

            // Update the current segment info.
            state.seg.ws_tli = tli;
            state.seg.ws_segno = next_seg_no;
        }

        // How many bytes are within this segment?
        let segbytes: i64 = if nbytes > (state.segcxt.ws_segsize as i64 - startoff as i64) {
            state.segcxt.ws_segsize as i64 - startoff as i64
        } else {
            nbytes
        };

        // pg_pread(state->seg.ws_file, p, segbytes, (off_t) startoff)
        let readbytes = fd::pg_pread::call(
            state.seg.ws_file,
            &mut buf[p..p + segbytes as usize],
            startoff as i64,
        );

        if readbytes <= 0 {
            // errinfo populated; WALReadRaiseError(&errinfo) ereport(ERROR).
            let (hi, lo) = crate::lsn_fmt(recptr);
            let req = segbytes;
            return Err(PgError::error(if readbytes < 0 {
                format!(
                    "could not read from WAL segment {}, LSN {:X}/{:X}, offset {}",
                    state.seg.ws_segno, hi, lo, startoff
                )
            } else {
                format!(
                    "could not read from WAL segment {}, LSN {:X}/{:X}, offset {}: read {} of {}",
                    state.seg.ws_segno, hi, lo, startoff, readbytes, req
                )
            }));
        }

        // Update state for read.
        recptr += readbytes as u64;
        nbytes -= readbytes as i64;
        p += readbytes as usize;
        // tli is constant across this WALRead call (C threads it by value).
        let _ = &mut tli;
    }

    Ok(())
}

/// `XLByteInSeg(xlrp, segno, wal_segsz_bytes)`.
fn xl_byte_in_seg(xlrp: XLogRecPtr, segno: XLogSegNo, wal_segsz_bytes: i32) -> bool {
    crate::XLByteToSeg(xlrp, wal_segsz_bytes) == segno
}

// ===========================================================================
// Seam adapters (resolve handle -> reader, delegate to the F1 reader).
// ===========================================================================

fn summarizer_xlogreader_allocate(
    wal_segment_size: i32,
    page_read: SummarizerPageReadCB,
) -> PgResult<XLogReaderHandle> {
    allocate_reader(wal_segment_size, page_read)
}

fn summarizer_xlogreader_free(reader: XLogReaderHandle) {
    free_reader(reader)
}

fn summarizer_xlog_begin_read(reader: XLogReaderHandle, start_lsn: XLogRecPtr) {
    with_reader(reader, |r| crate::XLogBeginRead(r, start_lsn))
}

fn summarizer_xlog_find_next_record(
    reader: XLogReaderHandle,
    start_lsn: XLogRecPtr,
) -> PgResult<XLogRecPtr> {
    with_reader(reader, |r| crate::XLogFindNextRecord(r, start_lsn))
}

fn summarizer_xlog_read_record(reader: XLogReaderHandle) -> PgResult<ReadRecordResult> {
    with_reader(reader, read_record)
}

fn summarizer_wal_read(
    reader: XLogReaderHandle,
    buf: &mut [u8],
    startptr: XLogRecPtr,
    count: i32,
    tli: TimeLineID,
) -> PgResult<()> {
    with_reader(reader, |r| wal_read(r, buf, startptr, count, tli))
}

fn summarizer_reader_end_rec_ptr(reader: XLogReaderHandle) -> XLogRecPtr {
    with_reader(reader, |r| r.EndRecPtr)
}

fn summarizer_reader_read_rec_ptr(reader: XLogReaderHandle) -> XLogRecPtr {
    with_reader(reader, |r| r.ReadRecPtr)
}

fn summarizer_rec_get_rmid(reader: XLogReaderHandle) -> u8 {
    with_reader(reader, |r| {
        r.record
            .as_ref()
            .map(|d| d.header().rmid())
            .expect("XLogRecGetRmid with no current record")
    })
}

fn summarizer_rec_get_info(reader: XLogReaderHandle) -> u8 {
    with_reader(reader, |r| {
        r.record
            .as_ref()
            .map(|d| d.info())
            .expect("XLogRecGetInfo with no current record")
    })
}

fn summarizer_rec_get_data(reader: XLogReaderHandle) -> Vec<u8> {
    with_reader(reader, |r| {
        r.record
            .as_ref()
            .map(|d| d.main_data().to_vec())
            .unwrap_or_default()
    })
}

fn summarizer_rec_max_block_id(reader: XLogReaderHandle) -> i32 {
    with_reader(reader, |r| {
        r.record.as_ref().map(|d| d.max_block_id()).unwrap_or(-1)
    })
}

fn summarizer_rec_get_block_tag_extended(
    reader: XLogReaderHandle,
    block_id: i32,
) -> Option<BlockTag> {
    with_reader(reader, |r| {
        if block_id < 0 || block_id > u8::MAX as i32 {
            return None;
        }
        let tag = crate::xlog_rec_get_block_tag_extended(r, block_id as u8)?;
        Some(BlockTag {
            rlocator: relfilelocator(tag.rlocator),
            forknum: tag.forknum,
            blocknum: tag.blkno,
        })
    })
}

/// The seam `XLogBlockTag.rlocator` and the summarizer `BlockTag.rlocator` are
/// both `types_storage::RelFileLocator`; an identity copy.
fn relfilelocator(loc: RelFileLocator) -> RelFileLocator {
    loc
}

// ===========================================================================
// Install
// ===========================================================================

/// Install every summarizer (`SummarizeWAL`) reader seam this unit owns.
pub fn init_seams() {
    seam::summarizer_xlogreader_allocate::set(summarizer_xlogreader_allocate);
    seam::summarizer_xlogreader_free::set(summarizer_xlogreader_free);
    seam::summarizer_xlog_begin_read::set(summarizer_xlog_begin_read);
    seam::summarizer_xlog_find_next_record::set(summarizer_xlog_find_next_record);
    seam::summarizer_xlog_read_record::set(summarizer_xlog_read_record);
    seam::summarizer_wal_read::set(summarizer_wal_read);
    seam::summarizer_reader_end_rec_ptr::set(summarizer_reader_end_rec_ptr);
    seam::summarizer_reader_read_rec_ptr::set(summarizer_reader_read_rec_ptr);
    seam::summarizer_rec_get_rmid::set(summarizer_rec_get_rmid);
    seam::summarizer_rec_get_info::set(summarizer_rec_get_info);
    seam::summarizer_rec_get_data::set(summarizer_rec_get_data);
    seam::summarizer_rec_max_block_id::set(summarizer_rec_max_block_id);
    seam::summarizer_rec_get_block_tag_extended::set(summarizer_rec_get_block_tag_extended);
}
