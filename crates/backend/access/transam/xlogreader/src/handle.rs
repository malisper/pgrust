//! Handle-based logical-decoding subset of `access/transam/xlogreader.c`.
//!
//! `replication/logical/logical.c` holds an `XLogReaderState *` as an opaque
//! pointer (`ctx->reader`) it never dereferences itself — it only forwards it
//! through `XLogReaderAllocate` / `XLogReaderFree` / `XLogBeginRead` /
//! `XLogReadRecord` and reads `reader->EndRecPtr`. Those five entry points are
//! modeled as handle seams keyed by an opaque [`XLogReaderHandle`]; this module
//! is their owner.
//!
//! ## Backing store (the F1 value-typed reader)
//!
//! Each live handle owns a real [`XLogReaderState`] — the in-crate value-typed
//! reader the F1 decode core drives — plus the reader's `MemoryContext`
//! (`decode_arena`). In C the reader and everything it pallocs live in the
//! caller's context for the lifetime between `XLogReaderAllocate` and
//! `XLogReaderFree`; here a per-handle `MemoryContext` plays that role. The
//! context is heap-allocated and leaked to `'static` while the handle is live
//! (so the reader's decoded payload may borrow `decode_arena` as the borrowed
//! `&'mcx [u8]` contract the 61 consumers read), and reclaimed when
//! `XLogReaderFree` drops the slot. The registry is backend-local
//! (`thread_local!`), matching the C per-backend reader.
//!
//! ## Routine resolution
//!
//! `XLogReaderAllocate`'s `routine` argument arrives as the opaque
//! [`XLogReaderRoutineHandle`]; logical.c only ever forwards the default handle
//! (the local-xlog routine that lives in `xlogutils`). The concrete
//! `XLogReaderRoutine` crosses the outward
//! [`xlog_reader_routine_for_handle`](seam::xlog_reader_routine_for_handle)
//! seam from that downstream owner and is stored verbatim into
//! `state.routine`, exactly as the C `state->routine = *routine`.

extern crate alloc;

use alloc::boxed::Box;
use alloc::string::ToString;
use core::cell::RefCell;

use mcx::MemoryContext;
use types_core::primitive::XLogRecPtr;
use types_logical::{XLogReadResult, XLogReaderHandle, XLogReaderRoutineHandle};
use wal::rmgr::XLogReaderState;
use wal::xlog_consts::XLOG_BLCKSZ;

use xlogreader_seams as seam;

use crate::{
    allocate_recordbuf, xlog_reader_deferred_errmsg, XLogBeginRead as XLogBeginReadValue,
    XLogNextRecord, XLogReadAhead, XLogReaderHasQueuedRecordOrError, XLogReleasePreviousRecord,
    DEFAULT_DECODE_BUFFER_SIZE, MAX_ERRORMSG_LEN,
};

/// One live reader: its decode-arena context (kept alive for the borrowed-slice
/// contract) and the value-typed [`XLogReaderState`] that borrows it.
///
/// `ctx` is leaked to `'static` for the duration of the slot so the reader's
/// `'mcx` decode payload can outlive any single seam call; `XLogReaderFree`
/// reconstitutes the `Box` and drops it, reclaiming the arena (the C
/// `pfree(state)` + context teardown).
struct ReaderSlot {
    /// The reader's `MemoryContext` (the C reader context the decode buffer /
    /// oversized records / `errormsg_buf` are allocated in). `'static` for the
    /// slot's lifetime; freed in [`free`].
    ctx: &'static MemoryContext,
    /// The value-typed reader. Borrows `ctx` (its `decode_arena`).
    state: XLogReaderState<'static>,
}

::std::thread_local! {
    /// Backend-local table of live readers. A handle is `1 + slot index`
    /// (`0` is the C `NULL`, never handed out); freed slots become `None` and
    /// are reused, mirroring the C per-backend reader allocation.
    static READERS: RefCell<alloc::vec::Vec<Option<ReaderSlot>>> =
        const { RefCell::new(alloc::vec::Vec::new()) };
}

/// `XLogReaderHandle` <-> slot index. Handle `0` is reserved for `NULL`.
fn handle_to_index(h: XLogReaderHandle) -> usize {
    debug_assert!(h.0 != 0, "NULL XLogReaderHandle");
    h.0 - 1
}
fn index_to_handle(i: usize) -> XLogReaderHandle {
    XLogReaderHandle(i + 1)
}

/// Run `f` against the live reader behind `handle`. The reader lives in the
/// backend-local registry; `f` borrows it mutably for the duration of one seam
/// call (the C dereferences `reader` for the same span).
fn with_reader<R>(handle: XLogReaderHandle, f: impl FnOnce(&mut XLogReaderState<'static>) -> R) -> R {
    READERS.with(|r| {
        let mut tab = r.borrow_mut();
        let slot = tab
            .get_mut(handle_to_index(handle))
            .and_then(|s| s.as_mut())
            .expect("XLogReaderHandle refers to a freed/unknown reader");
        f(&mut slot.state)
    })
}

/// `XLogReaderAllocate(wal_segment_size, NULL, routine, NULL)`
/// (xlogreader.c:104). Allocate a new reader in a fresh context, wire its
/// resolved routine, and register it. Returns `None` on OOM (the C
/// `palloc_extended(..., MCXT_ALLOC_NO_OOM)` NULL return). `private_data` is
/// always `NULL` for the logical-decoding caller.
pub fn XLogReaderAllocate(
    wal_segment_size: i32,
    xl_routine: XLogReaderRoutineHandle,
) -> Option<XLogReaderHandle> {
    // The reader's own MemoryContext: heap-allocated and leaked to 'static for
    // the slot's lifetime so the reader's 'mcx decode payload can borrow it.
    let ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("xlogreader")));
    let arena = ctx.mcx();

    let mut state = XLogReaderState {
        decode_arena: Some(arena),
        ..Default::default()
    };

    // state->routine = *routine; — resolve the opaque routine handle into the
    // concrete callbacks (the downstream xlogutils owner supplies them).
    state.routine = seam::xlog_reader_routine_for_handle::call(xl_routine);

    // Permanently allocate readBuf (XLOG_BLCKSZ, MAXALIGN'd by the arena).
    state.readBuf = match mcx::vec_with_capacity_in(arena, XLOG_BLCKSZ) {
        Ok(mut v) => {
            v.resize(XLOG_BLCKSZ, 0);
            Some(v)
        }
        Err(_) => return free_partial(ctx),
    };

    // WALOpenSegmentInit(&state->seg, &state->segcxt, wal_segment_size, NULL):
    // seg defaults to ws_file = -1 already; set the segment size, no waldir.
    state.segcxt.ws_segsize = wal_segment_size;

    // system_identifier / ReadRecPtr / EndRecPtr / readLen are zero already.
    // private_data stays None (the logical-decoding caller passes NULL).

    // errormsg_buf = palloc(MAX_ERRORMSG_LEN + 1); start empty.
    state.errormsg_buf = match mcx::vec_with_capacity_in::<u8>(arena, MAX_ERRORMSG_LEN + 1) {
        Ok(_) => Some(mcx::PgString::new_in(arena)),
        Err(_) => return free_partial(ctx),
    };
    state.errormsg_deferred = false;

    // Set the default decode buffer size (the C XLogReaderSetDecodeBuffer is
    // not called by XLogReaderAllocate, but the decode core sizes the ring from
    // decode_buffer_size; mirror DEFAULT_DECODE_BUFFER_SIZE so the first
    // XLogReadRecordAlloc has a ring to place records in).
    if state.decode_buffer_size == 0 {
        state.decode_buffer_size = DEFAULT_DECODE_BUFFER_SIZE;
    }

    // Allocate an initial readRecordBuf of minimal size.
    if allocate_recordbuf(&mut state, 0).is_err() {
        return free_partial(ctx);
    }

    // Register the live reader and hand back its handle.
    let handle = READERS.with(|r| {
        let mut tab = r.borrow_mut();
        // Reuse a freed slot if one exists.
        if let Some(i) = tab.iter().position(|s| s.is_none()) {
            tab[i] = Some(ReaderSlot { ctx, state });
            index_to_handle(i)
        } else {
            tab.push(Some(ReaderSlot { ctx, state }));
            index_to_handle(tab.len() - 1)
        }
    });
    Some(handle)
}

/// Reclaim a partially-built reader's context on an allocation failure
/// mid-`XLogReaderAllocate` (the C `pfree` cascade before `return NULL`).
fn free_partial(ctx: &'static MemoryContext) -> Option<XLogReaderHandle> {
    // SAFETY: `ctx` was just produced by `Box::leak(Box::new(..))` above and is
    // not registered in any slot, so this is its sole owner. Reconstituting and
    // dropping the Box reclaims the arena (no reader borrows it yet).
    drop(unsafe { Box::from_raw(ctx as *const MemoryContext as *mut MemoryContext) });
    None
}

/// `XLogReaderFree(reader)` (xlogreader.c:165). Close any open segment via the
/// routine, then free the reader and its context.
pub fn XLogReaderFree(handle: XLogReaderHandle) {
    let slot = READERS.with(|r| {
        let mut tab = r.borrow_mut();
        tab.get_mut(handle_to_index(handle)).and_then(|s| s.take())
    });
    let mut slot = match slot {
        Some(s) => s,
        // Double free / unknown handle: the C would crash dereferencing a freed
        // pointer; here it is a no-op (the slot is already gone).
        None => return,
    };

    // if (state->seg.ws_file != -1) state->routine.segment_close(state);
    if slot.state.seg.ws_file != -1 {
        if let Some(close) = slot.state.routine.segment_close {
            close(&mut slot.state);
        }
    }

    // Drop the reader (and everything it borrows from the arena) BEFORE the
    // context, so no live borrow outlives the arena. Then reclaim the context.
    let ctx = slot.ctx;
    drop(slot);
    // SAFETY: `ctx` was leaked by `Box::leak` in `XLogReaderAllocate` and lived
    // only in this slot, which we just removed; the reader (sole borrower) is
    // dropped above, so this is the unique owner.
    drop(unsafe { Box::from_raw(ctx as *const MemoryContext as *mut MemoryContext) });
}

/// `XLogBeginRead(reader, RecPtr)` (xlogreader.c:231) on the handle reader.
pub fn XLogBeginRead(handle: XLogReaderHandle, rec_ptr: XLogRecPtr) {
    with_reader(handle, |state| XLogBeginReadValue(state, rec_ptr));
}

/// `XLogReadRecord(reader, &errormsg)` (xlogreader.c:389). Release the previous
/// record, read ahead one record in blocking mode if the queue is empty, then
/// consume the head record (or its deferred error). Maps onto the C two-out
/// shape via [`XLogReadResult`]: `record` is whether a record header was
/// returned, `err` the deferred `*errormsg`.
pub fn XLogReadRecord(handle: XLogReaderHandle) -> XLogReadResult {
    with_reader(handle, |state| {
        // Release last returned record, if there is one.
        XLogReleasePreviousRecord(state);

        // Call XLogReadAhead() in blocking mode to make sure there is something
        // in the queue, though we don't use the result. A page-read ereport
        // surfaces here; logical.c's XLogReadResult has no Err arm, so an error
        // from the callback is reported as a deferred message (the reader's
        // errormsg_buf), matching the C which leaves *errormsg set on the NULL
        // return.
        if !XLogReaderHasQueuedRecordOrError(state) {
            if let Err(e) = XLogReadAhead(state, false /* nonblocking */) {
                return XLogReadResult {
                    record: false,
                    err: Some(e.message().to_string()),
                };
            }
        }

        // Consume the head record or error.
        match XLogNextRecord(state) {
            Some(_lsn) => XLogReadResult {
                record: true,
                err: None,
            },
            None => XLogReadResult {
                record: false,
                err: xlog_reader_deferred_errmsg(state),
            },
        }
    })
}

/// `reader->EndRecPtr` (xlogreader.h) of the handle reader.
pub fn reader_EndRecPtr(handle: XLogReaderHandle) -> XLogRecPtr {
    with_reader(handle, |state| state.EndRecPtr)
}

/// `XLogRecGetFullXid(reader)` (xlogreader.c:2187) for the handle reader — the
/// `FullTransactionId` of the reader's current record. Delegates to the
/// value-typed [`crate::XLogRecGetFullXid`]; only safe during replay.
pub fn XLogRecGetFullXid(handle: XLogReaderHandle) -> types_core::FullTransactionId {
    with_reader(handle, |state| crate::XLogRecGetFullXid(state))
}

/// `reader->ReadRecPtr` (xlogreader.h) of the handle reader.
pub fn reader_ReadRecPtr(handle: XLogReaderHandle) -> XLogRecPtr {
    with_reader(handle, |state| state.ReadRecPtr)
}

// ---------------------------------------------------------------------------
// Handle-based decoded-record accessors consumed by logical decoding
// (`decode.c`). Each reads the reader's *current* decoded record
// (`reader->record`, the record `XLogReadRecord`/`XLogNextRecord` just made
// current). decode.c only ever calls these after a successful read, so the
// record is present; a `None` means the C would have dereferenced NULL, which
// cannot happen on the logical-decoding path.
// ---------------------------------------------------------------------------

/// `XLogRecGetInfo(reader->record)`.
pub fn xlog_rec_get_info(handle: XLogReaderHandle) -> u8 {
    with_reader(handle, |state| {
        state
            .record
            .as_ref()
            .expect("xlog_rec_get_info called without a decoded record")
            .info()
    })
}

/// `XLogRecGetRmid(reader->record)`.
pub fn xlog_rec_get_rmid(handle: XLogReaderHandle) -> u8 {
    with_reader(handle, |state| {
        state
            .record
            .as_ref()
            .expect("xlog_rec_get_rmid called without a decoded record")
            .header()
            .rmid()
    })
}

/// `XLogRecGetXid(reader->record)`.
pub fn xlog_rec_get_xid(handle: XLogReaderHandle) -> types_core::primitive::TransactionId {
    with_reader(handle, |state| {
        state
            .record
            .as_ref()
            .expect("xlog_rec_get_xid called without a decoded record")
            .xid()
    })
}

/// `XLogRecGetTopXid(reader->record)`.
pub fn xlog_rec_get_top_xid(handle: XLogReaderHandle) -> types_core::primitive::TransactionId {
    with_reader(handle, |state| {
        state
            .record
            .as_ref()
            .expect("xlog_rec_get_top_xid called without a decoded record")
            .toplevel_xid()
    })
}

/// `XLogRecGetOrigin(reader->record)`.
pub fn xlog_rec_get_origin(handle: XLogReaderHandle) -> types_core::primitive::RepOriginId {
    with_reader(handle, |state| {
        state
            .record
            .as_ref()
            .expect("xlog_rec_get_origin called without a decoded record")
            .record_origin()
    })
}

/// `XLogRecGetData(reader->record)` — the main data area, copied out.
pub fn xlog_rec_get_main_data(handle: XLogReaderHandle) -> alloc::vec::Vec<u8> {
    with_reader(handle, |state| {
        state
            .record
            .as_ref()
            .expect("xlog_rec_get_main_data called without a decoded record")
            .data()
            .to_vec()
    })
}

/// `XLogRecGetDataLen(reader->record)`.
pub fn xlog_rec_get_main_data_len(handle: XLogReaderHandle) -> u32 {
    with_reader(handle, |state| {
        state
            .record
            .as_ref()
            .expect("xlog_rec_get_main_data_len called without a decoded record")
            .main_data_len()
    })
}

/// `XLogRecGetBlockTagExtended(reader->record, block_id, &rlocator, ...)` —
/// the relation locator of backup block `block_id`, `None` when not in use.
pub fn xlog_rec_get_block_tag(
    handle: XLogReaderHandle,
    block_id: u8,
) -> Option<types_storage::RelFileLocator> {
    with_reader(handle, |state| {
        crate::xlog_rec_get_block_tag_extended(state, block_id).map(|tag| tag.rlocator)
    })
}

/// `XLogRecGetBlockData(reader->record, block_id, &len)` — the per-block data
/// bytes, copied out (`None` when the block has no data).
pub fn xlog_rec_get_block_data(
    handle: XLogReaderHandle,
    block_id: u8,
) -> Option<alloc::vec::Vec<u8>> {
    with_reader(handle, |state| {
        state
            .record
            .as_ref()
            .and_then(|d| d.block_data(block_id as usize))
            .map(|b| b.to_vec())
    })
}

/// Install the handle seams this unit owns.
pub fn init_seams() {
    seam::XLogReaderAllocate::set(XLogReaderAllocate);
    seam::XLogReaderFree::set(XLogReaderFree);
    seam::XLogBeginRead::set(XLogBeginRead);
    seam::XLogReadRecord::set(XLogReadRecord);
    seam::reader_EndRecPtr::set(reader_EndRecPtr);
    seam::reader_ReadRecPtr::set(reader_ReadRecPtr);
    seam::xlog_rec_get_info::set(xlog_rec_get_info);
    seam::xlog_rec_get_rmid::set(xlog_rec_get_rmid);
    seam::xlog_rec_get_xid::set(xlog_rec_get_xid);
    seam::xlog_rec_get_top_xid::set(xlog_rec_get_top_xid);
    seam::xlog_rec_get_origin::set(xlog_rec_get_origin);
    seam::xlog_rec_get_main_data::set(xlog_rec_get_main_data);
    seam::xlog_rec_get_main_data_len::set(xlog_rec_get_main_data_len);
    seam::xlog_rec_get_block_tag::set(xlog_rec_get_block_tag);
    seam::xlog_rec_get_block_data::set(xlog_rec_get_block_data);
}
