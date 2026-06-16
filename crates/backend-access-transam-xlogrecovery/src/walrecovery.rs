//! The recovery process's live WAL reading machinery: the part of
//! `InitWalRecovery` (xlogrecovery.c:519) that allocates the recovery
//! `XLogReaderState` (with `routine.page_read = XLogPageRead`) and the
//! `XLogPrefetcher` wrapped around it, plus the `XLogRecGetRmid`/`XLogRecGetInfo`/
//! `XLogRecGetTotalLen` accessors and the prefetcher read-record entry points the
//! `ReadRecord` retry loop drives.
//!
//! # The reader/prefetcher holder (C file-statics `xlogreader`/`xlogprefetcher`)
//!
//! In C these are two file-scope `static` pointers (xlogrecovery.c:190-193)
//! allocated once for the startup process's whole recovery and dereferenced from
//! `XLogPageRead`, `ReadRecord`, etc. The prefetcher embeds an
//! `XLogReaderState *reader` it dereferences across calls — a self-borrow if the
//! reader and the prefetcher both lived in one thread-local. We mirror the C
//! file-static lifetime exactly with the crate's audited raw-pointer escape
//! (`shmem.rs::ctl_ptr`): the reader lives in a heap-leaked process-lifetime
//! `MemoryContext`, behind a `*mut XLogReaderState<'static>` thread-local; the
//! prefetcher is built once over a `&'static mut` derived from that pointer and
//! stored behind its own `*mut` thread-local. The startup process is
//! single-threaded over recovery, so the aliasing the raw pointers permit is the
//! same single-writer discipline C's file-statics rely on.
//!
//! The `RecordRef` opaque handle the `xlog_rec_*` accessor seams take is now
//! resolved against this held reader's `record` field directly — `RecordRef(0)`
//! is the C NULL (no record), any non-zero value names "the held reader's
//! current decoded record" (the prefetcher only ever exposes one current record
//! at a time, exactly as the C `XLogRecGetXXX(xlogreader)` macros read
//! `xlogreader->record`). No side registry is kept.

extern crate alloc;
extern crate std;

use core::cell::Cell;

use alloc::boxed::Box;

use mcx::MemoryContext;
use types_core::XLogRecPtr;
use types_error::PgResult;
use types_wal::rmgr::XLogReaderState;
use types_wal::xlog_consts::XLOG_BLCKSZ;
use types_wal::xlogrecovery_carriers::{ReadRecordResult, RecordRef};

use backend_access_transam_xlogprefetcher::XLogPrefetcher;

use crate::pageread::{self, XLogPageReadPrivate};

/// `DEFAULT_DECODE_BUFFER_SIZE` (xlogreader.c:65) — the fallback the reader uses
/// when `wal_decode_buffer_size` has not been set; the recovery driver always
/// sets it from the GUC, but mirror the reader default for the holder.
const DEFAULT_DECODE_BUFFER_SIZE: usize = 64 * 1024;
/// `MAX_ERRORMSG_LEN` (xlogreader.c:59) — the reader's error buffer capacity.
const MAX_ERRORMSG_LEN: usize = 1000;

std::thread_local! {
    /// `static XLogReaderState *xlogreader = NULL;` (xlogrecovery.c:190). The
    /// recovery process's WAL reader, allocated in a process-lifetime leaked
    /// context. Null before `init_wal_recovery_reader`.
    static XLOGREADER: Cell<*mut XLogReaderState<'static>> =
        const { Cell::new(core::ptr::null_mut()) };
    /// `static XLogPrefetcher *xlogprefetcher = NULL;` (xlogrecovery.c:193). The
    /// recovery process's prefetcher, wrapping the reader above. Null before
    /// `init_wal_recovery_reader`.
    static XLOGPREFETCHER: Cell<*mut XLogPrefetcher<'static, 'static, 'static>> =
        const { Cell::new(core::ptr::null_mut()) };
}

/// Borrow the held recovery reader. Panics (the C NULL deref) if the startup
/// process has not run `init_wal_recovery_reader`.
///
/// SAFETY: the startup process owns the single `XLogReaderState` for the whole
/// of recovery and is single-threaded; `p` points at the live reader in its
/// process-lifetime leaked context.
#[inline]
#[allow(clippy::mut_from_ref)]
fn reader() -> &'static mut XLogReaderState<'static> {
    let p = XLOGREADER.with(Cell::get);
    debug_assert!(!p.is_null(), "recovery reader accessed before InitWalRecovery");
    unsafe { &mut *p }
}

/// Borrow the held recovery prefetcher. Panics (the C NULL deref) if the startup
/// process has not run `init_wal_recovery_reader`.
///
/// SAFETY: see [`reader`]. The prefetcher holds a raw-derived `&'static mut` to
/// the same reader; the single-threaded startup process never aliases it
/// concurrently.
#[inline]
#[allow(clippy::mut_from_ref)]
fn prefetcher() -> &'static mut XLogPrefetcher<'static, 'static, 'static> {
    let p = XLOGPREFETCHER.with(Cell::get);
    debug_assert!(
        !p.is_null(),
        "recovery prefetcher accessed before InitWalRecovery"
    );
    unsafe { &mut *p }
}

/// `InitWalRecovery` (xlogrecovery.c:561-582) reader/prefetcher allocation leg:
/// `private = palloc0(...); xlogreader = XLogReaderAllocate(wal_segment_size,
/// NULL, XL_ROUTINE(.page_read = &XLogPageRead, .segment_open = NULL,
/// .segment_close = wal_segment_close), private); ...
/// XLogReaderSetDecodeBuffer(xlogreader, NULL, wal_decode_buffer_size);
/// xlogprefetcher = XLogPrefetcherAllocate(xlogreader);`
///
/// Allocates the recovery process's live reader and prefetcher for the duration
/// of recovery and installs them behind the process-lifetime holder. The reader
/// dispatches page reads through [`pageread::x_log_page_read`] (the C
/// `&XLogPageRead`); `segment_open` is NULL (the recovery driver opens segments
/// itself in `XLogFileRead`). `private_data` holds the [`XLogPageReadPrivate`]
/// scratch the page-read driver reads, exactly as C `palloc0`s it.
///
/// `Err` carries the C `ereport(ERROR, ERRCODE_OUT_OF_MEMORY)` for a failed
/// reader allocation.
pub fn init_wal_recovery_reader(
    wal_segment_size: i32,
    system_identifier: u64,
    wal_decode_buffer_size: usize,
) -> PgResult<()> {
    // The reader's own MemoryContext: heap-allocated and leaked to 'static for
    // the recovery process's lifetime so the reader's 'mcx decode payload can
    // borrow it (the C reader `MemoryContext`).
    let ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("xlogreader")));
    let arena = ctx.mcx();

    let mut state: XLogReaderState<'static> = XLogReaderState {
        decode_arena: Some(arena),
        ..Default::default()
    };

    // XL_ROUTINE(.page_read = &XLogPageRead, .segment_open = NULL,
    //            .segment_close = wal_segment_close).
    // The recovery driver opens/closes segment files itself (XLogFileRead /
    // close(readFile)); segment_close is therefore unused by this reader's
    // page_read path, mirrored as None (the substrate wal_segment_close lives
    // in xlogutils and is only reached by the built-in read helpers, which the
    // recovery page_read replaces).
    state.routine.page_read = Some(pageread::x_log_page_read);

    state.system_identifier = system_identifier;

    // Permanently allocate readBuf (XLOG_BLCKSZ); the page-read driver reads
    // pages into it.
    state.readBuf = {
        let mut v = mcx::vec_with_capacity_in(arena, XLOG_BLCKSZ).map_err(|_| arena.oom(XLOG_BLCKSZ))?;
        v.resize(XLOG_BLCKSZ, 0);
        Some(v)
    };

    // WALOpenSegmentInit: ws_file defaults to -1; set the segment size.
    state.segcxt.ws_segsize = wal_segment_size;

    // errormsg_buf = palloc(MAX_ERRORMSG_LEN + 1); start empty.
    let _ = mcx::vec_with_capacity_in::<u8>(arena, MAX_ERRORMSG_LEN + 1)
        .map_err(|_| arena.oom(MAX_ERRORMSG_LEN + 1))?;
    state.errormsg_buf = Some(mcx::PgString::new_in(arena));
    state.errormsg_deferred = false;

    // private = palloc0(sizeof(XLogPageReadPrivate)) — the page-read scratch,
    // type-erased into the reader's `private_data: Option<PgBox<dyn Any>>`.
    {
        let boxed = mcx::alloc_in(arena, XLogPageReadPrivate::default())?;
        let (ptr, alloc) = mcx::PgBox::into_raw_with_allocator(boxed);
        // SAFETY: `ptr` came from `into_raw_with_allocator` with `alloc`; the
        // cast only attaches the `dyn Any` vtable (no `CoerceUnsized` on stable).
        let erased: mcx::PgBox<'static, dyn core::any::Any> =
            unsafe { mcx::PgBox::from_raw_in(ptr as *mut dyn core::any::Any, alloc) };
        state.private_data = Some(erased);
    }

    // XLogReaderSetDecodeBuffer(xlogreader, NULL, wal_decode_buffer_size).
    let decode_buffer_size = if wal_decode_buffer_size != 0 {
        wal_decode_buffer_size
    } else {
        DEFAULT_DECODE_BUFFER_SIZE
    };
    backend_access_transam_xlogreader::XLogReaderSetDecodeBuffer(&mut state, decode_buffer_size);

    // Allocate an initial readRecordBuf of minimal size (allocate_recordbuf in
    // XLogReaderAllocate).
    backend_access_transam_xlogreader::allocate_recordbuf(&mut state, 0)?;

    // Install the reader behind the process-lifetime holder, then build the
    // prefetcher over a raw-derived &'static mut to it (the C
    // `XLogPrefetcherAllocate(xlogreader)` embedding the reader pointer).
    let reader_ptr: *mut XLogReaderState<'static> = Box::into_raw(Box::new(state));
    XLOGREADER.with(|c| c.set(reader_ptr));

    // The prefetcher's own context (the C CurrentMemoryContext at
    // XLogPrefetcherAllocate; here a second process-lifetime leaked context so
    // its filter hash/queue outlive the call).
    let pctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("xlogprefetcher")));
    // SAFETY: `reader_ptr` was just produced by `Box::into_raw` and is the sole
    // live reference to the reader for the single-threaded startup process.
    let reader_mut: &'static mut XLogReaderState<'static> = unsafe { &mut *reader_ptr };
    let prefetcher = XLogPrefetcher::XLogPrefetcherAllocate(pctx.mcx(), reader_mut)?;
    let prefetcher_ptr = Box::into_raw(Box::new(prefetcher));
    XLOGPREFETCHER.with(|c| c.set(prefetcher_ptr));

    Ok(())
}

// ===========================================================================
// Held-reader accessors for the replay driver.
//
// The replay family (`replay.rs`) is handed an opaque `RecordRef` naming "the
// held reader's current decoded record"; to drive `GetRmgr().rm_redo`, read the
// record header (`xl_rmid`/`xl_info`/`xl_xid`), the main data area
// (`XLogRecGetData`), and the reader cursor (`ReadRecPtr`/`EndRecPtr`), it needs
// the live reader. These mirror the C `xlogreader` file-static dereference.
// ===========================================================================

/// `xlogreader` (the held recovery reader) as a shared borrow. Mirrors the C
/// `XLogRecGetXXX(xlogreader)` macro dereference.
#[inline]
pub(crate) fn reader_state() -> &'static XLogReaderState<'static> {
    let p = XLOGREADER.with(Cell::get);
    debug_assert!(!p.is_null(), "recovery reader accessed before InitWalRecovery");
    unsafe { &*p }
}

/// `xlogreader` (the held recovery reader) as a mutable borrow — used to invoke
/// `rm_redo(xlogreader)` (the `RmRedo` callback takes `&mut XLogReaderState`).
#[inline]
#[allow(clippy::mut_from_ref)]
pub(crate) fn reader_state_mut() -> &'static mut XLogReaderState<'static> {
    reader()
}

// ===========================================================================
// XLogRecGetRmid / XLogRecGetInfo / XLogRecGetTotalLen seam installs.
//
// The recovery driver's ReadCheckpointRecord reads the current record's header
// fields through these `RecordRef`-keyed seams. The RecordRef names the held
// reader's current record (any non-zero value); we resolve it against the held
// reader directly and delegate to the xlogreader accessors over reader->record.
// ===========================================================================

/// `XLogRecGetRmid(record)` over the held recovery reader.
fn xlog_rec_rmid(_record: RecordRef) -> u8 {
    backend_access_transam_xlogreader::XLogRecGetRmid(reader())
}

/// `XLogRecGetInfo(record)` over the held recovery reader.
fn xlog_rec_info(_record: RecordRef) -> u8 {
    backend_access_transam_xlogreader::XLogRecGetInfo(reader())
}

/// `XLogRecGetTotalLen(record)` over the held recovery reader.
fn xlog_rec_total_len(_record: RecordRef) -> u32 {
    backend_access_transam_xlogreader::XLogRecGetTotalLen(reader())
}

// ===========================================================================
// XLogPrefetcherBeginRead / XLogPrefetcherReadRecord seam installs.
// ===========================================================================

/// `XLogPrefetcherBeginRead(xlogprefetcher, RecPtr)` over the held prefetcher.
fn prefetcher_begin_read(rec_ptr: XLogRecPtr) {
    prefetcher().XLogPrefetcherBeginRead(rec_ptr);
}

/// `XLogPrefetcherComputeStats(xlogprefetcher)` over the held prefetcher
/// (xlogprefetcher.c:409) — publish the prefetcher's distance/depth gauges to
/// shared memory before the recovery driver sleeps waiting for streamed WAL.
fn prefetcher_compute_stats() {
    prefetcher().XLogPrefetcherComputeStats();
}

/// `record = XLogPrefetcherReadRecord(xlogprefetcher, &errmsg)` over the held
/// prefetcher (xlogprefetcher.c:980), bundled with the reader-state fields the
/// `ReadRecord` retry loop inspects (xlogrecovery.c:3171-3231) into the
/// [`ReadRecordResult`] carrier. `record == RecordRef(0)` is the C NULL return
/// (end-of-WAL / no record decoded).
///
/// `maintenance_io_concurrency` is bufmgr.c's GUC and `io_direct_flags` fd.c's
/// global; C `XLogPrefetcherReadRecord` reads them directly. We read them off
/// their owners (the bufmgr seam — its GUC accessor is panic-until-owner, the
/// established repo state — and the merged fd unit's real `io_direct_flags`).
fn prefetcher_read_record() -> ReadRecordResult {
    let maintenance_io_concurrency =
        backend_storage_buffer_bufmgr_seams::maintenance_io_concurrency::call();
    let io_direct_flags = backend_storage_file_fd::vfd_core::io_direct_flags();

    let p = prefetcher();
    let outcome = p.XLogPrefetcherReadRecord(maintenance_io_concurrency, io_direct_flags);

    // The reader holds every state field the retry loop reads; it is the same
    // reader the prefetcher just drove.
    let r = reader();

    use types_wal::wal::XLogNextRecordResult;
    let (record, errormsg) = match outcome {
        Ok(XLogNextRecordResult::Record { .. }) => {
            // Got a record: name it with a non-zero RecordRef (the held
            // reader's current record). The exact value is irrelevant — the
            // accessor seams resolve against the held reader, not a registry.
            (RecordRef(1), None)
        }
        Ok(XLogNextRecordResult::NoRecord { errmsg }) => {
            // C NULL return: *errmsg points into the reader's errormsg_buf.
            (RecordRef(0), errmsg.map(alloc::string::String::from))
        }
        Err(e) => {
            // An ereport(ERROR) inside the page-read callback. The C code lets
            // it propagate out of XLogPrefetcherReadRecord; the retry loop's
            // seam contract is infallible (it returns a bundled result), so the
            // error surfaces here as a NULL record carrying its message — the
            // recovery driver re-reports it at emode_for_corrupt_record. This
            // mirrors the C behaviour where an elog(ERROR) from page_read
            // unwinds; here we surface the text so the same emode path runs.
            (RecordRef(0), Some(alloc::string::String::from(e.message())))
        }
    };

    ReadRecordResult {
        record,
        read_rec_ptr: r.ReadRecPtr,
        end_rec_ptr: r.EndRecPtr,
        errormsg,
        aborted_rec_ptr: r.abortedRecPtr,
        missing_contrec_ptr: r.missingContrecPtr,
        latest_page_tli: r.latestPageTLI,
        latest_page_ptr: r.latestPagePtr,
        seg_tli: r.seg.ws_tli,
        // readSource is the page-read driver's file-static, the source the read
        // actually came from.
        read_source: pageread::read_source(),
    }
}

/// Install the 5 reader/prefetcher record seams the recovery `ReadRecord` loop
/// drives, now resolvable against the held reader/prefetcher. These declarations
/// live in `xlogreader-seams` / `xlogprefetcher-seams` (the C owners of the
/// record/prefetcher), but only this holder can resolve a `RecordRef` against
/// the live recovery reader, so it is the installer (a sanctioned cross-crate
/// install).
pub fn init_holder_seams() {
    backend_access_transam_xlogreader_seams::xlog_rec_rmid::set(xlog_rec_rmid);
    backend_access_transam_xlogreader_seams::xlog_rec_info::set(xlog_rec_info);
    backend_access_transam_xlogreader_seams::xlog_rec_total_len::set(xlog_rec_total_len);
    backend_access_transam_xlogprefetcher_seams::prefetcher_begin_read::set(prefetcher_begin_read);
    backend_access_transam_xlogprefetcher_seams::prefetcher_read_record::set(prefetcher_read_record);
    backend_access_transam_xlogprefetcher_seams::prefetcher_compute_stats::set(
        prefetcher_compute_stats,
    );
}
