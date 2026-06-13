#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::manual_is_multiple_of)]

//! `backend-access-transam-xlogutils` — an owned-tree Rust port of
//! `src/backend/access/transam/xlogutils.c` (PostgreSQL 18.3).
//!
//! xlogutils.c is the WAL-replay support layer. None of this code runs during
//! normal operation — it is used exclusively by XLOG replay (redo) functions
//! and by logical decoding's read-WAL-as-a-relation callbacks. It provides the
//! invalid-page table, the redo buffer fetchers, the drop/truncate hooks that
//! purge invalid-page records, the fake relcache helpers,
//! `XLogReadDetermineTimeline`, and the read-WAL-as-a-relation callbacks.
//!
//! The invalid-page bookkeeping, all the redo branch logic, the timeline math,
//! the path/filename formatting, and the read-local-xlog-page control flow are
//! ported in-crate. The genuinely-external operations cross per-owner seams:
//! the bufmgr/smgr, the decoded-record / `XLogReaderState` accessors owned by
//! xlogreader, the timeline subsystem, the recovery / flush-position globals +
//! `WALRead`, the fd layer, interrupts + sleep, relpath, and fake-relcache
//! allocation.
//!
//! C's file-static per-backend globals owned by this file (`InRecovery`,
//! `standbyState`, `ignore_invalid_pages`, `invalid_page_tab`) are
//! `thread_local`s; neighbors read/write the first three through this crate's
//! inward seams (see `init_seams`). `reachedConsistency` is read through its
//! xlogrecovery owner's seam.

extern crate alloc;

use alloc::collections::BTreeMap;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::cell::RefCell;

use backend_utils_error::{elog, ereport, message_level_is_interesting};
use types_error::error::{
    DEBUG1, DEBUG2, DEBUG3, ERRCODE_DATA_CORRUPTED, ERRCODE_INTERNAL_ERROR, ERROR, PANIC, WARNING,
};
use types_error::ErrorLevel;
use types_error::PgResult;

use types_core::primitive::{
    BlockNumber, Buffer, ForkNumber, Oid, TimeLineID, XLogRecPtr, XLogSegNo, BLCKSZ,
    INIT_FORKNUM, INVALID_PROC_NUMBER, InvalidBlockNumber,
};
use types_core::xact::InvalidXLogRecPtr;
use types_storage::{ReadBufferMode, RelFileLocator};
use types_wal::rmgr::XLogReaderState;
use types_wal::{HotStandbyState, XLogRedoAction, BKPBLOCK_WILL_INIT, STANDBY_DISABLED, XLOG_BLCKSZ};

use backend_access_transam_timeline_seams as timeline_seam;
use backend_access_transam_xlog_seams as xlog_seam;
use backend_access_transam_xlog_seams::{WalReadErrorInfo, WalReadOutcome};
use backend_access_transam_xlogreader_seams as reader_seam;
use backend_access_transam_xlogreader_seams::XLogBlockTag;
use backend_access_transam_xlogrecovery_seams as recovery_seam;
use backend_storage_buffer_bufmgr_seams as buf_seam;
use backend_storage_file_fd_seams as fd_seam;
use backend_storage_smgr_seams as smgr_seam;
use common_relpath_seams as relpath_seam;

/// `InvalidBuffer` (`storage/buf.h`) — zero.
const InvalidBuffer: Buffer = 0;

/// The hash key of the invalid-page table — `xl_invalid_page_key`.
/// "we currently assume xl_invalid_page_key contains no padding".
///
/// Modelled as a [`BTreeMap`] key; the C uses a `dynahash` `HTAB` but only ever
/// does keyed lookup, filtered removal, and an unordered WARNING dump, so
/// iteration order is not observable behavior.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct XlInvalidPageKey {
    /// `locator` — the relation.
    locator: RelFileLocator,
    /// `forkno` — the fork number.
    forkno: ForkNumber,
    /// `blkno` — the page.
    blkno: BlockNumber,
}

thread_local! {
    /// `static HTAB *invalid_page_tab` — `None` until first needed (matching
    /// the C `invalid_page_tab == NULL` create-on-demand pattern). The `bool`
    /// value is `xl_invalid_page.present`.
    static INVALID_PAGE_TAB: RefCell<Option<BTreeMap<XlInvalidPageKey, bool>>> =
        const { RefCell::new(None) };

    /// `bool InRecovery` (xlogutils.c global) — "this process is replaying WAL
    /// records".
    static IN_RECOVERY: RefCell<bool> = const { RefCell::new(false) };

    /// `HotStandbyState standbyState` (xlogutils.c global).
    static STANDBY_STATE: RefCell<HotStandbyState> = const { RefCell::new(STANDBY_DISABLED) };

    /// `bool ignore_invalid_pages` (xlogutils.c GUC, default false).
    static IGNORE_INVALID_PAGES: RefCell<bool> = const { RefCell::new(false) };
}

// ---------------------------------------------------------------------------
// Per-backend global accessors (read/written by neighbors through this crate's
// inward seams; see init_seams).
// ---------------------------------------------------------------------------

/// `InRecovery` (xlogutils.c global).
pub fn in_recovery() -> bool {
    IN_RECOVERY.with(|f| *f.borrow())
}

/// Set `InRecovery`.
pub fn set_in_recovery(value: bool) {
    IN_RECOVERY.with(|f| *f.borrow_mut() = value);
}

/// `standbyState` (xlogutils.c global).
pub fn standby_state() -> HotStandbyState {
    STANDBY_STATE.with(|f| *f.borrow())
}

/// Set `standbyState`.
pub fn set_standby_state(state: HotStandbyState) {
    STANDBY_STATE.with(|f| *f.borrow_mut() = state);
}

/// `ignore_invalid_pages` (xlogutils.c GUC).
pub fn ignore_invalid_pages() -> bool {
    IGNORE_INVALID_PAGES.with(|f| *f.borrow())
}

/// Set `ignore_invalid_pages` (driven by the GUC machinery).
pub fn set_ignore_invalid_pages(value: bool) {
    IGNORE_INVALID_PAGES.with(|f| *f.borrow_mut() = value);
}

// ---------------------------------------------------------------------------
// XLOG file-name / path formatting (xlog_internal.h inlines used in-file).
// ---------------------------------------------------------------------------

/// `XLogSegmentsPerXLogId(wal_segsz_bytes)`.
#[inline]
fn XLogSegmentsPerXLogId(wal_segsz_bytes: i32) -> u64 {
    0x1_0000_0000_u64 / wal_segsz_bytes as u64
}

/// `XLogFileName(fname, tli, logSegNo, wal_segsz_bytes)` -> the bare segment
/// filename `"%08X%08X%08X"`.
fn XLogFileName(tli: TimeLineID, logSegNo: XLogSegNo, wal_segsz_bytes: i32) -> String {
    let per = XLogSegmentsPerXLogId(wal_segsz_bytes);
    format!(
        "{:08X}{:08X}{:08X}",
        tli,
        (logSegNo / per) as u32,
        (logSegNo % per) as u32
    )
}

/// `XLogFilePath(path, tli, logSegNo, wal_segsz_bytes)` -> `XLOGDIR
/// "/%08X%08X%08X"`.
fn XLogFilePath(tli: TimeLineID, logSegNo: XLogSegNo, wal_segsz_bytes: i32) -> String {
    let per = XLogSegmentsPerXLogId(wal_segsz_bytes);
    format!(
        "pg_wal/{:08X}{:08X}{:08X}",
        tli,
        (logSegNo / per) as u32,
        (logSegNo % per) as u32
    )
}

/// `relpathperm(locator, forkno).str` — the on-disk path of a permanent
/// relation fork. `relpathperm` is `relpathbackend(locator, INVALID_PROC_NUMBER,
/// forkno)`.
fn relpathperm(locator: RelFileLocator, forkno: ForkNumber) -> String {
    relpath_seam::relpathbackend::call(locator, INVALID_PROC_NUMBER, forkno)
}

// ---------------------------------------------------------------------------
// Invalid-page bookkeeping (xlogutils.c lines 84-262).
// ---------------------------------------------------------------------------

/// Report a reference to an invalid page (`report_invalid_page`).
fn report_invalid_page(
    elevel: ErrorLevel,
    locator: RelFileLocator,
    forkno: ForkNumber,
    blkno: BlockNumber,
    present: bool,
) -> PgResult<()> {
    let path = relpathperm(locator, forkno);

    if present {
        elog(
            elevel,
            format!("page {blkno} of relation {path} is uninitialized"),
        )
    } else {
        elog(
            elevel,
            format!("page {blkno} of relation {path} does not exist"),
        )
    }
}

/// Log a reference to an invalid page (`log_invalid_page`).
fn log_invalid_page(
    locator: RelFileLocator,
    forkno: ForkNumber,
    blkno: BlockNumber,
    present: bool,
) -> PgResult<()> {
    // Once recovery has reached a consistent state, the invalid-page table
    // should be empty and remain so. If a reference to an invalid page is found
    // after consistency is reached, PANIC immediately.
    if recovery_seam::reached_consistency::call() {
        report_invalid_page(WARNING, locator, forkno, blkno, present)?;
        elog(
            if ignore_invalid_pages() { WARNING } else { PANIC },
            "WAL contains references to invalid pages",
        )?;
    }

    // Log references to invalid pages at DEBUG1 level.
    if message_level_is_interesting(DEBUG1) {
        report_invalid_page(DEBUG1, locator, forkno, blkno, present)?;
    }

    // we currently assume xl_invalid_page_key contains no padding
    let key = XlInvalidPageKey {
        locator,
        forkno,
        blkno,
    };

    // create hash table when first needed; HASH_ENTER: if not found, fill in
    // "present"; if found, leave it as it was.
    INVALID_PAGE_TAB.with(|tab| {
        tab.borrow_mut()
            .get_or_insert_with(BTreeMap::new)
            .entry(key)
            .or_insert(present);
    });

    Ok(())
}

/// Forget any invalid pages >= minblkno, because they've been dropped
/// (`forget_invalid_pages`).
fn forget_invalid_pages(
    locator: RelFileLocator,
    forkno: ForkNumber,
    minblkno: BlockNumber,
) -> PgResult<()> {
    let to_remove: Vec<XlInvalidPageKey> = INVALID_PAGE_TAB.with(|tab| {
        let tab = tab.borrow();
        match tab.as_ref() {
            None => Vec::new(),
            Some(tab) => tab
                .keys()
                .filter(|key| {
                    key.locator == locator && key.forkno == forkno && key.blkno >= minblkno
                })
                .copied()
                .collect(),
        }
    });

    for key in to_remove {
        let path = relpathperm(key.locator, forkno);
        elog(
            DEBUG2,
            format!("page {} of relation {} has been dropped", key.blkno, path),
        )?;

        let removed = INVALID_PAGE_TAB.with(|tab| {
            tab.borrow_mut()
                .as_mut()
                .and_then(|tab| tab.remove(&key))
                .is_some()
        });
        if !removed {
            elog(ERROR, "hash table corrupted")?;
        }
    }

    Ok(())
}

/// Forget any invalid pages in a whole database (`forget_invalid_pages_db`).
fn forget_invalid_pages_db(dbid: Oid) -> PgResult<()> {
    let to_remove: Vec<XlInvalidPageKey> = INVALID_PAGE_TAB.with(|tab| {
        let tab = tab.borrow();
        match tab.as_ref() {
            None => Vec::new(),
            Some(tab) => tab
                .keys()
                .filter(|key| key.locator.dbOid == dbid)
                .copied()
                .collect(),
        }
    });

    for key in to_remove {
        let path = relpathperm(key.locator, key.forkno);
        elog(
            DEBUG2,
            format!("page {} of relation {} has been dropped", key.blkno, path),
        )?;

        let removed = INVALID_PAGE_TAB.with(|tab| {
            tab.borrow_mut()
                .as_mut()
                .and_then(|tab| tab.remove(&key))
                .is_some()
        });
        if !removed {
            elog(ERROR, "hash table corrupted")?;
        }
    }

    Ok(())
}

/// Are there any unresolved references to invalid pages?
/// (`XLogHaveInvalidPages`).
pub fn XLogHaveInvalidPages() -> bool {
    INVALID_PAGE_TAB.with(|tab| tab.borrow().as_ref().is_some_and(|tab| !tab.is_empty()))
}

/// Complain about any remaining invalid-page entries (`XLogCheckInvalidPages`).
pub fn XLogCheckInvalidPages() -> PgResult<()> {
    let tab = match INVALID_PAGE_TAB.with(|tab| tab.borrow_mut().take()) {
        None => return Ok(()), // nothing to do
        Some(tab) => tab,
    };

    let mut foundone = false;

    // Our strategy is to emit WARNING messages for all remaining entries and
    // only PANIC after we've dumped all the available info.
    for (key, present) in &tab {
        report_invalid_page(WARNING, key.locator, key.forkno, key.blkno, *present)?;
        foundone = true;
    }

    if foundone {
        elog(
            if ignore_invalid_pages() { WARNING } else { PANIC },
            "WAL contains references to invalid pages",
        )?;
    }

    // hash_destroy(invalid_page_tab); invalid_page_tab = NULL; — the `take`
    // above already cleared the global; dropping `tab` frees the table.
    drop(tab);

    Ok(())
}

// ---------------------------------------------------------------------------
// Redo buffer fetchers (xlogutils.c lines 302-542).
// ---------------------------------------------------------------------------

/// `XLogReadBufferForRedo` (lines 302-308). Read a page during XLOG replay.
/// Returns the redo action and the buffer (the C `Buffer *buf` out-param).
pub fn XLogReadBufferForRedo(
    record: &XLogReaderState<'_>,
    block_id: u8,
) -> PgResult<(XLogRedoAction, Buffer)> {
    XLogReadBufferForRedoExtended(record, block_id, ReadBufferMode::Normal, false)
}

/// Pin and lock a buffer referenced by a WAL record, for re-initializing it
/// (`XLogInitBufferForRedo`, lines 314-322).
pub fn XLogInitBufferForRedo(record: &XLogReaderState<'_>, block_id: u8) -> PgResult<Buffer> {
    let (_action, buf) =
        XLogReadBufferForRedoExtended(record, block_id, ReadBufferMode::ZeroAndLock, false)?;
    Ok(buf)
}

/// `XLogReadBufferForRedoExtended` (lines 339-428). Returns the redo action and
/// the buffer (the C `Buffer *buf` out-param; `InvalidBuffer` for `BLK_NOTFOUND`).
pub fn XLogReadBufferForRedoExtended(
    record: &XLogReaderState<'_>,
    block_id: u8,
    mode: ReadBufferMode,
    get_cleanup_lock: bool,
) -> PgResult<(XLogRedoAction, Buffer)> {
    let lsn: XLogRecPtr = record.EndRecPtr;

    let tag = match reader_seam::xlog_rec_get_block_tag_extended::call(record, block_id)? {
        None => {
            // Caller specified a bogus block_id
            elog(
                PANIC,
                format!("failed to locate backup block with ID {block_id} in WAL record"),
            )?;
            unreachable!("elog(PANIC) returned Ok");
        }
        Some(tag) => tag,
    };
    let XLogBlockTag {
        rlocator,
        forknum,
        blkno,
        prefetch_buffer,
    } = tag;

    // Make sure that if the block is marked with WILL_INIT, the caller is going
    // to initialize it. And vice versa.
    let zeromode =
        mode == ReadBufferMode::ZeroAndLock || mode == ReadBufferMode::ZeroAndCleanupLock;
    let willinit =
        (reader_seam::xlog_rec_get_block_flags::call(record, block_id)? & BKPBLOCK_WILL_INIT) != 0;
    if willinit && !zeromode {
        elog(
            PANIC,
            "block with WILL_INIT flag in WAL record must be zeroed by redo routine",
        )?;
    }
    if !willinit && zeromode {
        elog(
            PANIC,
            "block to be initialized in redo routine must be marked with WILL_INIT flag in the WAL record",
        )?;
    }

    // If it has a full-page image and it should be restored, do it.
    if reader_seam::xlog_rec_block_image_apply::call(record, block_id)? {
        debug_assert!(reader_seam::xlog_rec_has_block_image::call(record, block_id)?);
        let buf = XLogReadBufferExtended(
            rlocator,
            forknum,
            blkno,
            if get_cleanup_lock {
                ReadBufferMode::ZeroAndCleanupLock
            } else {
                ReadBufferMode::ZeroAndLock
            },
            prefetch_buffer,
        )?;
        // BufferGetPage(*buf) / RestoreBlockImage onto that page (a relation
        // Page is BLCKSZ bytes); the bufmgr side owns the page, so we hand it
        // the buffer id rather than a `Page` pointer. A debug assert keeps the
        // page-size invariant the C codes against in sight.
        debug_assert_eq!(BLCKSZ, XLOG_BLCKSZ);
        if !reader_seam::restore_block_image::call(record, block_id, buf)? {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INTERNAL_ERROR)
                .errmsg_internal(reader_seam::reader_errormsg_buf::call(record))
                .into_error());
        }

        // The page may be uninitialized. If so, we can't set the LSN because
        // that would corrupt the page.
        if !buf_seam::page_is_new::call(buf)? {
            buf_seam::page_set_lsn::call(buf, lsn)?;
        }

        buf_seam::mark_buffer_dirty::call(buf);

        // At the end of crash recovery the init forks of unlogged relations are
        // copied, without going through shared buffers. So we need to force the
        // on-disk state of init forks to always be in sync with the state in
        // shared buffers.
        if forknum == INIT_FORKNUM {
            buf_seam::flush_one_buffer::call(buf)?;
        }

        Ok((XLogRedoAction::BlkRestored, buf))
    } else {
        let buf = XLogReadBufferExtended(rlocator, forknum, blkno, mode, prefetch_buffer)?;
        if BufferIsValid(buf) {
            if mode != ReadBufferMode::ZeroAndLock && mode != ReadBufferMode::ZeroAndCleanupLock {
                if get_cleanup_lock {
                    buf_seam::lock_buffer_for_cleanup::call(buf)?;
                } else {
                    buf_seam::lock_buffer_exclusive::call(buf)?;
                }
            }
            if lsn <= buf_seam::page_get_lsn::call(buf)? {
                Ok((XLogRedoAction::BlkDone, buf))
            } else {
                Ok((XLogRedoAction::BlkNeedsRedo, buf))
            }
        } else {
            Ok((XLogRedoAction::BlkNotFound, buf))
        }
    }
}

/// `XLogReadBufferExtended` (lines 459-542). Read a page during XLOG replay.
///
/// The buffer-manager + smgr core (recent-buffer fast path, smgr open/create/
/// nblocks, read-or-extend) lives behind the seam; the C function's
/// `log_invalid_page` calls for the (in-crate) invalid-page table are kept here
/// by checking the returned buffer and the page's all-zeroes state via the
/// seam. The seam's `xlog_read_buffer_extended` returns `InvalidBuffer` exactly
/// when the C `XLogReadBufferExtended` would have, *before* the all-zeroes
/// RBM_NORMAL check; that check, and its `log_invalid_page`, are re-applied
/// here so the invalid-page table stays in-crate.
pub fn XLogReadBufferExtended(
    rlocator: RelFileLocator,
    forknum: ForkNumber,
    blkno: BlockNumber,
    mode: ReadBufferMode,
    recent_buffer: Buffer,
) -> PgResult<Buffer> {
    // Assert(blkno != P_NEW); — P_NEW == InvalidBlockNumber == 0xFFFFFFFF.
    debug_assert!(blkno != P_NEW);

    // The whole smgr-open / smgrcreate / smgrnblocks / read-or-extend body —
    // including the RBM_NORMAL "page doesn't exist in file" branch that, in C,
    // calls `log_invalid_page(rlocator, forknum, blkno, false)` — happens in
    // the seam. The seam returns `InvalidBuffer` for that case; we then record
    // the invalid page here so the table stays in-crate.
    let buffer =
        buf_seam::xlog_read_buffer_extended::call(rlocator, forknum, blkno, mode, recent_buffer)?;

    if !BufferIsValid(buffer) {
        // RBM_NORMAL missing-page: log it (RBM_NORMAL_NO_LOG does not).
        if mode == ReadBufferMode::Normal {
            log_invalid_page(rlocator, forknum, blkno, false)?;
        }
        return Ok(InvalidBuffer);
    }

    if mode == ReadBufferMode::Normal {
        // check that page has been initialized
        //
        // We assume that PageIsNew is safe without a lock. During recovery,
        // there should be no other backends that could modify the buffer at the
        // same time.
        if buf_seam::page_is_new::call(buffer)? {
            buf_seam::release_buffer::call(buffer);
            log_invalid_page(rlocator, forknum, blkno, true)?;
            return Ok(InvalidBuffer);
        }
    }

    Ok(buffer)
}

// ---------------------------------------------------------------------------
// Fake relcache entries (xlogutils.c lines 570-621).
// ---------------------------------------------------------------------------

/// Create a fake relation cache entry for a physical relation
/// (`CreateFakeRelcacheEntry`, lines 570-612). The struct allocation, field
/// setup, and non-pinned `SMgrRelation` happen in the relcache subsystem behind
/// the seam.
pub fn CreateFakeRelcacheEntry(
    mcx: mcx::Mcx<'_>,
    rlocator: RelFileLocator,
) -> PgResult<types_rel::RelationData<'_>> {
    backend_utils_cache_relcache_seams::create_fake_relcache_entry::call(mcx, rlocator)
}

/// Free a fake relation cache entry (`FreeFakeRelcacheEntry`, lines 617-621).
pub fn FreeFakeRelcacheEntry(fakerel: types_rel::RelationData<'_>) {
    backend_utils_cache_relcache_seams::free_fake_relcache_entry::call(fakerel)
}

// ---------------------------------------------------------------------------
// Drop/truncate hooks (xlogutils.c lines 629-664).
// ---------------------------------------------------------------------------

/// Drop a relation during XLOG replay (`XLogDropRelation`, lines 629-633).
pub fn XLogDropRelation(rlocator: RelFileLocator, forknum: ForkNumber) -> PgResult<()> {
    forget_invalid_pages(rlocator, forknum, 0)
}

/// Drop a whole database during XLOG replay (`XLogDropDatabase`, lines
/// 640-652).
pub fn XLogDropDatabase(dbid: Oid) -> PgResult<()> {
    // This is unnecessarily heavy-handed, as it will close SMgrRelation objects
    // for other databases as well. DROP DATABASE occurs seldom enough that it's
    // not worth introducing a variant of smgrdestroy for just this purpose.
    smgr_seam::smgrdestroyall::call()?;

    forget_invalid_pages_db(dbid)
}

/// Truncate a relation during XLOG replay (`XLogTruncateRelation`, lines
/// 659-664).
pub fn XLogTruncateRelation(
    rlocator: RelFileLocator,
    forkNum: ForkNumber,
    nblocks: BlockNumber,
) -> PgResult<()> {
    forget_invalid_pages(rlocator, forkNum, nblocks)
}

// ---------------------------------------------------------------------------
// Timeline determination (xlogutils.c lines 706-802).
// ---------------------------------------------------------------------------

/// `XLogReadDetermineTimeline` (lines 706-802). Determine which timeline to
/// read an xlog page from and set the `XLogReaderState`'s `currTLI` to it.
pub fn XLogReadDetermineTimeline(
    state: &mut XLogReaderState<'_>,
    wantPage: XLogRecPtr,
    wantLength: u32,
    currTLI: TimeLineID,
) -> PgResult<()> {
    // `ws_segsize` is a C `int`; promote to u64 for the XLogRecPtr arithmetic,
    // matching `state->seg.ws_segno * state->segcxt.ws_segsize + state->segoff`.
    let ws_segsize: u64 = reader_seam::reader_seg_size::call(state) as u64;
    let read_len = reader_seam::reader_read_len::call(state);
    let lastReadPage: XLogRecPtr = reader_seam::reader_seg_segno::call(state) * ws_segsize
        + reader_seam::reader_segoff::call(state) as u64;

    debug_assert!(wantPage != InvalidXLogRecPtr && wantPage % XLOG_BLCKSZ as u64 == 0);
    debug_assert!(wantLength as usize <= XLOG_BLCKSZ);
    debug_assert!(read_len == 0 || read_len as usize <= XLOG_BLCKSZ);
    debug_assert!(currTLI != 0);

    // If the desired page is currently read in and valid, we have nothing to
    // do.
    if lastReadPage == wantPage
        && read_len != 0
        && lastReadPage + read_len as u64
            >= wantPage + min_u64(wantLength as u64, (XLOG_BLCKSZ - 1) as u64)
    {
        return Ok(());
    }

    // If we're reading from the current timeline, it hasn't become historical
    // and the page we're reading is after the last page read, we can again just
    // carry on.
    if reader_seam::reader_curr_tli::call(state) == currTLI && wantPage >= lastReadPage {
        debug_assert!(reader_seam::reader_curr_tli_valid_until::call(state) == InvalidXLogRecPtr);
        return Ok(());
    }

    // If we're just reading pages from a previously validated historical
    // timeline and the timeline we're reading from is valid until the end of
    // the current segment we can just keep reading.
    if reader_seam::reader_curr_tli_valid_until::call(state) != InvalidXLogRecPtr
        && reader_seam::reader_curr_tli::call(state) != currTLI
        && reader_seam::reader_curr_tli::call(state) != 0
        && ((wantPage + wantLength as u64) / ws_segsize)
            < (reader_seam::reader_curr_tli_valid_until::call(state) / ws_segsize)
    {
        return Ok(());
    }

    // If we reach this point we're either looking up a page for random access,
    // the current timeline just became historical, or we're reading from a new
    // segment containing a timeline switch. In all cases we need to determine
    // the newest timeline on the segment.
    {
        // We need to re-read the timeline history in case it's been changed by
        // a promotion or replay from a cascaded replica. (readTimeLineHistory +
        // tliOfPointInHistory + tliSwitchPoint live in the timeline subsystem.)
        let endOfSegment: XLogRecPtr = ((wantPage / ws_segsize) + 1) * ws_segsize - 1;
        debug_assert!(wantPage / ws_segsize == endOfSegment / ws_segsize);

        // We need to re-read the timeline history in case it's been changed
        // by a promotion or replay from a cascaded replica. The C reads
        // `readTimeLineHistory(currTLI)` into the current memory context and
        // frees it at the end of the block; a scoped context dropped at block
        // end mirrors that free.
        let history_ctx = mcx::MemoryContext::new("xlogutils timeline history");
        let history = timeline_seam::read_timeline_history::call(history_ctx.mcx(), currTLI)?;

        // Find the timeline of the last LSN on the segment containing wantPage.
        let new_curr_tli =
            timeline_seam::tli_of_point_in_history::call(endOfSegment, &history)?;
        reader_seam::reader_set_curr_tli::call(state, new_curr_tli);
        let (valid_until, next_tli) =
            timeline_seam::tli_switch_point::call(new_curr_tli, &history)?;
        reader_seam::reader_set_curr_tli_valid_until::call(state, valid_until);
        reader_seam::reader_set_next_tli::call(state, next_tli);

        debug_assert!(
            valid_until == InvalidXLogRecPtr
                || wantPage + (wantLength as u64) < valid_until
        );

        elog(
            DEBUG3,
            format!(
                "switched to timeline {} valid until {}",
                new_curr_tli,
                lsn_format_args(valid_until)
            ),
        )?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Local-WAL segment open/close + page read (xlogutils.c lines 805-1034).
// ---------------------------------------------------------------------------

/// `XLogReaderRoutine->segment_open` callback for local pg_wal files
/// (`wal_segment_open`, lines 805-827).
pub fn wal_segment_open(
    state: &mut XLogReaderState<'_>,
    nextSegNo: XLogSegNo,
    tli_p: TimeLineID,
) -> PgResult<()> {
    let tli = tli_p;
    let path = XLogFilePath(tli, nextSegNo, reader_seam::reader_seg_size::call(state));

    match fd_seam::basic_open_file::call(&path) {
        Ok(fd) => {
            // state->seg.ws_file = fd;
            reader_seam::reader_set_ws_file::call(state, fd);
            Ok(())
        }
        Err(err_errno) => {
            if err_errno == ENOENT {
                Err(ereport(ERROR)
                    .with_saved_errno(err_errno)
                    .errcode_for_file_access()
                    .errmsg(format!(
                        "requested WAL segment {path} has already been removed"
                    ))
                    .into_error())
            } else {
                Err(ereport(ERROR)
                    .with_saved_errno(err_errno)
                    .errcode_for_file_access()
                    .errmsg(format!("could not open file \"{path}\": %m"))
                    .into_error())
            }
        }
    }
}

/// Stock `XLogReaderRoutine->segment_close` callback (`wal_segment_close`,
/// lines 830-836).
pub fn wal_segment_close(state: &mut XLogReaderState<'_>) {
    // close(state->seg.ws_file); need to check errno? — and set ws_file = -1.
    reader_seam::reader_close_ws_file::call(state);
}

/// `XLogReaderRoutine->page_read` callback for reading local xlog files
/// (`read_local_xlog_page`, lines 844-850).
pub fn read_local_xlog_page(
    state: &mut XLogReaderState<'_>,
    targetPagePtr: XLogRecPtr,
    reqLen: i32,
    targetRecPtr: XLogRecPtr,
    cur_page: &mut [u8],
) -> PgResult<i32> {
    read_local_xlog_page_guts(state, targetPagePtr, reqLen, targetRecPtr, cur_page, true)
}

/// Same as [`read_local_xlog_page`] except it doesn't wait for future WAL to be
/// available (`read_local_xlog_page_no_wait`, lines 856-863).
pub fn read_local_xlog_page_no_wait(
    state: &mut XLogReaderState<'_>,
    targetPagePtr: XLogRecPtr,
    reqLen: i32,
    targetRecPtr: XLogRecPtr,
    cur_page: &mut [u8],
) -> PgResult<i32> {
    read_local_xlog_page_guts(state, targetPagePtr, reqLen, targetRecPtr, cur_page, false)
}

/// Implementation of [`read_local_xlog_page`] and its no-wait version
/// (`read_local_xlog_page_guts`, lines 868-1004).
fn read_local_xlog_page_guts(
    state: &mut XLogReaderState<'_>,
    targetPagePtr: XLogRecPtr,
    reqLen: i32,
    _targetRecPtr: XLogRecPtr,
    cur_page: &mut [u8],
    wait_for_wal: bool,
) -> PgResult<i32> {
    let mut read_upto: XLogRecPtr;
    let mut tli: TimeLineID;
    let count: i32;

    let loc: XLogRecPtr = targetPagePtr + reqLen as u64;

    // Loop waiting for xlog to be available if necessary.
    loop {
        // Determine the limit of xlog we can currently read to, and what the
        // most recent timeline is.
        let currTLI: TimeLineID;
        if !xlog_seam::recovery_in_progress::call() {
            let (ru, t) = xlog_seam::get_flush_rec_ptr::call();
            read_upto = ru;
            currTLI = t;
        } else {
            let (ru, t) = recovery_seam::get_xlog_replay_rec_ptr_tli::call();
            read_upto = ru;
            currTLI = t;
        }
        tli = currTLI;

        // Check which timeline to get the record from. We have to do it each
        // time through the loop because if we're in recovery as a cascading
        // standby, the current timeline might've become historical.
        XLogReadDetermineTimeline(state, targetPagePtr, reqLen as u32, tli)?;

        if reader_seam::reader_curr_tli::call(state) == currTLI {
            if loc <= read_upto {
                break;
            }

            // If asked, let's not wait for future WAL.
            if !wait_for_wal {
                // Inform the caller of read_local_xlog_page_no_wait that the end
                // of WAL has been reached.
                reader_seam::reader_set_private_end_of_wal::call(state);
                break;
            }

            backend_tcop_postgres_seams::check_for_interrupts::call()?;
            port_pgsleep_seams::pg_usleep::call(1000);
        } else {
            // We're on a historical timeline, so limit reading to the switch
            // point where we moved to the next timeline.
            read_upto = reader_seam::reader_curr_tli_valid_until::call(state);

            // Setting tli to our wanted record's TLI is slightly wrong; the page
            // might begin on an older timeline if it contains a timeline switch.
            tli = reader_seam::reader_curr_tli::call(state);

            // No need to wait on a historical timeline.
            break;
        }
    }

    if targetPagePtr + XLOG_BLCKSZ as u64 <= read_upto {
        // more than one block available; read only that block, have caller come
        // back if they need more.
        count = XLOG_BLCKSZ as i32;
    } else if targetPagePtr + reqLen as u64 > read_upto {
        // not enough data there
        return Ok(-1);
    } else {
        // enough bytes available to satisfy the request
        count = (read_upto - targetPagePtr) as i32;
    }

    // WALRead(state, cur_page, targetPagePtr, count, tli, &errinfo): the seam
    // reads `count` bytes and returns them owned (the C writes them through the
    // `cur_page` pointer); we copy them in here.
    match xlog_seam::wal_read::call(targetPagePtr, count, tli) {
        WalReadOutcome::Ok(bytes) => {
            let n = (count as usize).min(bytes.len()).min(cur_page.len());
            cur_page[..n].copy_from_slice(&bytes[..n]);
        }
        WalReadOutcome::Error(errinfo) => {
            WALReadRaiseError(&errinfo)?;
        }
    }

    // number of valid bytes in the buffer
    Ok(count)
}

/// Backend-specific convenience code to handle read errors encountered by
/// `WALRead()` (`WALReadRaiseError`, lines 1010-1034).
pub fn WALReadRaiseError(errinfo: &WalReadErrorInfo) -> PgResult<()> {
    let WalReadErrorInfo {
        wre_errno,
        wre_off,
        wre_req,
        wre_read,
        wre_seg_segno,
        wre_seg_tli,
    } = *errinfo;

    let fname = XLogFileName(wre_seg_tli, wre_seg_segno, xlog_seam::wal_segment_size::call());

    if wre_read < 0 {
        // errno = errinfo->wre_errno;
        Err(ereport(ERROR)
            .with_saved_errno(wre_errno)
            .errcode_for_file_access()
            .errmsg(format!(
                "could not read from WAL segment {fname}, offset {wre_off}: %m"
            ))
            .into_error())
    } else if wre_read == 0 {
        Err(ereport(ERROR)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg(format!(
                "could not read from WAL segment {fname}, offset {wre_off}: read {wre_read} of {wre_req}"
            ))
            .into_error())
    } else {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Small helpers (C macros / inlines used by this file).
// ---------------------------------------------------------------------------

/// `Min(a, b)` for `uint64`.
#[inline]
fn min_u64(a: u64, b: u64) -> u64 {
    if a < b {
        a
    } else {
        b
    }
}

/// `P_NEW` (storage/bufmgr.h): `InvalidBlockNumber`, used as the "grow the file"
/// sentinel that `XLogReadBufferExtended` asserts against.
const P_NEW: BlockNumber = InvalidBlockNumber;

/// `ENOENT` — "No such file or directory" (matched by `wal_segment_open`).
const ENOENT: i32 = 2;

/// `BufferIsValid(buffer)` (storage/bufmgr.h): a buffer reference is valid if it
/// is not `InvalidBuffer`. (Recovery never deals with local buffers here.)
#[inline]
fn BufferIsValid(buffer: Buffer) -> bool {
    buffer != InvalidBuffer
}

/// `LSN_FORMAT_ARGS(lsn)` rendered as the `"%X/%X"` text used in the DEBUG3
/// message.
#[inline]
fn lsn_format_args(lsn: XLogRecPtr) -> String {
    format!("{:X}/{:X}", (lsn >> 32) as u32, lsn as u32)
}

// ---------------------------------------------------------------------------
// Seam installation.
// ---------------------------------------------------------------------------

/// Install this crate's inward seams (the per-backend globals + redo fetcher it
/// owns).
pub fn init_seams() {
    backend_access_transam_xlogutils_seams::standby_state::set(standby_state);
    backend_access_transam_xlogutils_seams::set_standby_state::set(set_standby_state);
    backend_access_transam_xlogutils_seams::in_recovery::set(in_recovery);
    backend_access_transam_xlogutils_seams::set_in_recovery::set(set_in_recovery);
    backend_access_transam_xlogutils_seams::ignore_invalid_pages::set(ignore_invalid_pages);
    backend_access_transam_xlogutils_seams::set_ignore_invalid_pages::set(set_ignore_invalid_pages);
    backend_access_transam_xlogutils_seams::xlog_read_buffer_for_redo::set(
        |record, block_id| XLogReadBufferForRedo(record, block_id),
    );
}

#[cfg(test)]
mod tests;
