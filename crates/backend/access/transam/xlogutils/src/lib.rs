//! xlogutils.c — WAL-replay support; recovery-only. The `InRecovery` /
//! `standbyState` / `ignore_invalid_pages` stores live here (C's home for
//! them is xlogutils.c); xlog/xlogrecovery write via the setters (direct
//! dep), slru reads via xlogutils_seams::in_recovery.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;

use ::elog::{elog, ereport, message_level_is_interesting};
use types_core::{
    BlockNumber, Buffer, ForkNumber, InvalidBlockNumber, InvalidBuffer, Oid, TimeLineID,
    XLogRecPtr, XLogSegNo, INVALID_PROC_NUMBER,
};
use types_error::{
    ErrorLevel, ErrorLocation, PgResult, DEBUG1, DEBUG2, DEBUG3, ERRCODE_DATA_CORRUPTED,
    ERRCODE_INTERNAL_ERROR, ERROR, PANIC, WARNING,
};
use types_storage::{ReadBufferMode, RelFileLocator, RelFileLocatorBackend};
use xlogreader_seams::{
    WALReadError, XLogReaderState, BKPBLOCK_WILL_INIT, XLOG_BLCKSZ,
};

pub const InvalidXLogRecPtr: XLogRecPtr = 0;
const P_NEW: BlockNumber = InvalidBlockNumber;

#[repr(i32)]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum HotStandbyState {
    STANDBY_DISABLED = 0,
    STANDBY_INITIALIZED,
    STANDBY_SNAPSHOT_PENDING,
    STANDBY_SNAPSHOT_READY,
}
pub use HotStandbyState::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum XLogRedoAction {
    BLK_NEEDS_REDO,
    BLK_DONE,
    BLK_RESTORED,
    BLK_NOTFOUND,
}
pub use XLogRedoAction::*;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct XlInvalidPageKey {
    locator: RelFileLocator,
    forkno: ForkNumber,
    blkno: BlockNumber,
}

thread_local! {
    static IN_RECOVERY: Cell<bool> = const { Cell::new(false) };
    static STANDBY_STATE: Cell<HotStandbyState> = const { Cell::new(STANDBY_DISABLED) };
    static IGNORE_INVALID_PAGES: Cell<bool> = const { Cell::new(false) };
    // C's dynahash invalid_page_tab; cold, recovery-only, normally empty —
    // BTreeMap avoids SipHash and gives a deterministic WARNING dump.
    static INVALID_PAGE_TAB: RefCell<Option<BTreeMap<XlInvalidPageKey, bool>>> =
        const { RefCell::new(None) };
}

pub fn in_recovery() -> bool {
    IN_RECOVERY.with(|f| f.get())
}

pub fn set_in_recovery(value: bool) {
    IN_RECOVERY.with(|f| f.set(value));
}

pub fn standby_state() -> HotStandbyState {
    STANDBY_STATE.with(|f| f.get())
}

pub fn set_standby_state(state: HotStandbyState) {
    STANDBY_STATE.with(|f| f.set(state));
}

pub fn ignore_invalid_pages() -> bool {
    IGNORE_INVALID_PAGES.with(|f| f.get())
}

pub fn set_ignore_invalid_pages(value: bool) {
    IGNORE_INVALID_PAGES.with(|f| f.set(value));
}

// InHotStandby (xlogutils.h).
pub fn InHotStandby() -> bool {
    standby_state() >= STANDBY_SNAPSHOT_PENDING
}

#[cold]
#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

fn relpathperm(locator: RelFileLocator, forkno: ForkNumber) -> String {
    relpath_seams::relpathperm::call(locator, forkno)
}

#[cold]
fn report_invalid_page(
    elevel: ErrorLevel,
    locator: RelFileLocator,
    forkno: ForkNumber,
    blkno: BlockNumber,
    present: bool,
) -> PgResult<()> {
    let path = relpathperm(locator, forkno);
    if present {
        elog(elevel, format!("page {blkno} of relation {path} is uninitialized"))
    } else {
        elog(elevel, format!("page {blkno} of relation {path} does not exist"))
    }
}

fn log_invalid_page(
    locator: RelFileLocator,
    forkno: ForkNumber,
    blkno: BlockNumber,
    present: bool,
) -> PgResult<()> {
    // After consistency the table must stay empty: PANIC immediately.
    if xlogrecovery_seams::reached_consistency::call() {
        report_invalid_page(WARNING, locator, forkno, blkno, present)?;
        elog(
            if ignore_invalid_pages() { WARNING } else { PANIC },
            "WAL contains references to invalid pages",
        )?;
    }

    if message_level_is_interesting(DEBUG1) {
        report_invalid_page(DEBUG1, locator, forkno, blkno, present)?;
    }

    let key = XlInvalidPageKey {
        locator,
        forkno,
        blkno,
    };
    // HASH_ENTER semantics: a repeat reference leaves `present` as it was.
    INVALID_PAGE_TAB.with(|tab| {
        tab.borrow_mut()
            .get_or_insert_with(BTreeMap::new)
            .entry(key)
            .or_insert(present);
    });
    Ok(())
}

fn forget_invalid_pages(
    locator: RelFileLocator,
    forkno: ForkNumber,
    minblkno: BlockNumber,
) -> PgResult<()> {
    let removed: Vec<XlInvalidPageKey> = INVALID_PAGE_TAB.with(|tab| {
        let mut tab = tab.borrow_mut();
        let Some(tab) = tab.as_mut() else {
            return Vec::new();
        };
        let victims: Vec<XlInvalidPageKey> = tab
            .keys()
            .filter(|key| key.locator == locator && key.forkno == forkno && key.blkno >= minblkno)
            .copied()
            .collect();
        for key in &victims {
            tab.remove(key);
        }
        victims
    });

    for key in removed {
        elog(
            DEBUG2,
            format!(
                "page {} of relation {} has been dropped",
                key.blkno,
                relpathperm(key.locator, forkno)
            ),
        )?;
    }
    Ok(())
}

fn forget_invalid_pages_db(dbid: Oid) -> PgResult<()> {
    let removed: Vec<XlInvalidPageKey> = INVALID_PAGE_TAB.with(|tab| {
        let mut tab = tab.borrow_mut();
        let Some(tab) = tab.as_mut() else {
            return Vec::new();
        };
        let victims: Vec<XlInvalidPageKey> = tab
            .keys()
            .filter(|key| key.locator.dbOid == dbid)
            .copied()
            .collect();
        for key in &victims {
            tab.remove(key);
        }
        victims
    });

    for key in removed {
        elog(
            DEBUG2,
            format!(
                "page {} of relation {} has been dropped",
                key.blkno,
                relpathperm(key.locator, key.forkno)
            ),
        )?;
    }
    Ok(())
}

pub fn XLogHaveInvalidPages() -> bool {
    INVALID_PAGE_TAB.with(|tab| tab.borrow().as_ref().is_some_and(|t| !t.is_empty()))
}

pub fn XLogCheckInvalidPages() -> PgResult<()> {
    // take() is C's hash_destroy+NULL; WARN all entries before the PANIC.
    let Some(tab) = INVALID_PAGE_TAB.with(|tab| tab.borrow_mut().take()) else {
        return Ok(());
    };

    let mut foundone = false;
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
    Ok(())
}

pub fn XLogReadBufferForRedo(
    record: &XLogReaderState,
    block_id: u8,
) -> PgResult<(XLogRedoAction, Buffer)> {
    XLogReadBufferForRedoExtended(record, block_id, ReadBufferMode::Normal, false)
}

// XLogFlushBufferForRedoIfInit (upstream 62760571): init forks are copied to
// the main fork directly from disk at end of crash recovery, bypassing shared
// buffers, so redo routines that dirty an init-fork buffer without restoring
// a full-page image must flush it immediately.
pub fn XLogFlushBufferForRedoIfInit(
    record: &XLogReaderState,
    block_id: u8,
    buffer: Buffer,
) -> PgResult<()> {
    debug_assert!(BufferIsValid(buffer));
    let Some((_, forknum, _, _)) = record.block_tag_extended(block_id) else {
        elog(
            PANIC,
            format!("failed to locate backup block with ID {block_id} in WAL record"),
        )?;
        unreachable!("elog(PANIC) returned");
    };
    if forknum == ForkNumber::INIT_FORKNUM {
        bufmgr_seams::flush_one_buffer::call(buffer)?;
    }
    Ok(())
}

pub fn XLogInitBufferForRedo(record: &XLogReaderState, block_id: u8) -> PgResult<Buffer> {
    let (_, buf) =
        XLogReadBufferForRedoExtended(record, block_id, ReadBufferMode::ZeroAndLock, false)?;
    Ok(buf)
}

// C's Buffer *buf out-param is the second tuple element.
pub fn XLogReadBufferForRedoExtended(
    record: &XLogReaderState,
    block_id: u8,
    mode: ReadBufferMode,
    get_cleanup_lock: bool,
) -> PgResult<(XLogRedoAction, Buffer)> {
    let lsn = record.EndRecPtr;

    let Some((rlocator, forknum, blkno, prefetch_buffer)) = record.block_tag_extended(block_id)
    else {
        elog(
            PANIC,
            format!("failed to locate backup block with ID {block_id} in WAL record"),
        )?;
        unreachable!("elog(PANIC) returned");
    };

    let zeromode = matches!(
        mode,
        ReadBufferMode::ZeroAndLock | ReadBufferMode::ZeroAndCleanupLock
    );
    let willinit = record.block(block_id).flags & BKPBLOCK_WILL_INIT != 0;
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

    if record.block_image_apply(block_id) {
        debug_assert!(record.has_block_image(block_id));
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
        if let Err(errormsg) = xlogreader_seams::restore_block_image::call(record, block_id, buf)? {
            ereport(ERROR)
                .errcode(ERRCODE_INTERNAL_ERROR)
                .errmsg_internal(errormsg)
                .finish(loc("XLogReadBufferForRedoExtended"))?;
        }

        if !bufmgr_seams::buffer_page_is_new::call(buf) {
            bufmgr_seams::buffer_page_set_lsn::call(buf, lsn);
        }

        bufmgr_seams::mark_buffer_dirty::call(buf)?;

        // Init forks bypass shared buffers at end of crash recovery.
        if forknum == ForkNumber::INIT_FORKNUM {
            bufmgr_seams::flush_one_buffer::call(buf)?;
        }

        Ok((BLK_RESTORED, buf))
    } else {
        let buf = XLogReadBufferExtended(rlocator, forknum, blkno, mode, prefetch_buffer)?;
        if BufferIsValid(buf) {
            if !zeromode {
                if get_cleanup_lock {
                    bufmgr_seams::lock_buffer_for_cleanup::call(buf)?;
                } else {
                    bufmgr_seams::lock_buffer::call(buf, bufmgr_seams::BUFFER_LOCK_EXCLUSIVE)?;
                }
            }
            if lsn <= bufmgr_seams::buffer_page_get_lsn::call(buf) {
                Ok((BLK_DONE, buf))
            } else {
                Ok((BLK_NEEDS_REDO, buf))
            }
        } else {
            Ok((BLK_NOTFOUND, buf))
        }
    }
}

pub fn XLogReadBufferExtended(
    rlocator: RelFileLocator,
    forknum: ForkNumber,
    blkno: BlockNumber,
    mode: ReadBufferMode,
    recent_buffer: Buffer,
) -> PgResult<Buffer> {
    debug_assert!(blkno != P_NEW);
    let buffer;

    'recent_buffer_fast_path: {
        if BufferIsValid(recent_buffer)
            && mode == ReadBufferMode::Normal
            && bufmgr_seams::read_recent_buffer::call(rlocator, forknum, blkno, recent_buffer)?
        {
            buffer = recent_buffer;
            break 'recent_buffer_fast_path;
        }

        // C smgropen's handle crosses the seams as its locator key.
        let smgr = RelFileLocatorBackend {
            locator: rlocator,
            backend: INVALID_PROC_NUMBER,
        };

        smgr_seams::smgr_create::call(smgr, forknum, true)?;

        let lastblock = smgr_seams::smgr_nblocks::call(smgr, forknum)?;

        if blkno < lastblock {
            buffer = bufmgr_seams::read_buffer_without_relcache::call(
                rlocator, forknum, blkno, mode, None, true,
            )?;
        } else {
            if mode == ReadBufferMode::Normal {
                log_invalid_page(rlocator, forknum, blkno, false)?;
                return Ok(InvalidBuffer);
            }
            if mode == ReadBufferMode::NormalNoLog {
                return Ok(InvalidBuffer);
            }
            debug_assert!(in_recovery());
            buffer = bufmgr_seams::extend_buffered_rel_to::call(
                smgr,
                forknum,
                None,
                bufmgr_seams::EB_PERFORMING_RECOVERY | bufmgr_seams::EB_SKIP_EXTENSION_LOCK,
                blkno + 1,
                mode,
            )?;
        }
    }

    if mode == ReadBufferMode::Normal {
        // PageIsNew without a lock: recovery has no concurrent writers.
        if bufmgr_seams::buffer_page_is_new::call(buffer) {
            bufmgr_seams::release_buffer::call(buffer)?;
            log_invalid_page(rlocator, forknum, blkno, true)?;
            return Ok(InvalidBuffer);
        }
    }

    Ok(buffer)
}

pub struct FakeRelcacheEntry {
    rel: ::types_rel::RelationData<'static>,
}

impl core::ops::Deref for FakeRelcacheEntry {
    type Target = ::types_rel::RelationData<'static>;

    fn deref(&self) -> &Self::Target {
        &self.rel
    }
}

// Only the physical-storage fields carry meaning (C's contract): rd_locator,
// rd_backend, relpersistence, lockRelId; everything else is C's palloc0 zero.
#[cold]
pub fn CreateFakeRelcacheEntry(rlocator: RelFileLocator) -> FakeRelcacheEntry {
    use std::rc::Rc;
    use ::mcx::PgVec;
    use ::types_rel::{FormData_pg_class, LockInfoData, LockRelId, RelationData};
    use ::types_tuple::{NameData, TupleDescData};

    thread_local! {
        // Backs the entry's empty PgVecs only; nothing is ever allocated in it.
        static FAKE_REL_CX: &'static ::mcx::MemoryContext =
            ::mcx::session_root("fake relcache");
    }
    let mcx = FAKE_REL_CX.with(|cx| cx.mcx());

    let mut relname = NameData::default();
    relname.namestrcpy(&rlocator.relNumber.to_string());
    FakeRelcacheEntry {
        rel: RelationData {
            rd_locator: Cell::new(rlocator),
            rd_smgr: Cell::new(None),
            rd_id: 0,
            rd_backend: INVALID_PROC_NUMBER,
            rd_islocaltemp: false,
            rd_isvalid: Cell::new(false),
            rd_createSubid: Cell::new(0),
            rd_newRelfilelocatorSubid: Cell::new(0),
            rd_firstRelfilelocatorSubid: Cell::new(0),
            rd_droppedSubid: Cell::new(0),
            rd_lockInfo: LockInfoData {
                lockRelId: LockRelId { relId: rlocator.relNumber, dbId: rlocator.dbOid },
            },
            rd_rel: FormData_pg_class {
                relname,
                relnamespace: 0,
                reltype: 0,
                relowner: 0,
                relam: 0,
                relfilenode: rlocator.relNumber,
                reltablespace: 0,
                relpages: 0,
                reltuples: 0.0,
                relallvisible: 0,
                reltoastrelid: 0,
                relhasindex: false,
                relisshared: false,
                relpersistence: types_core::RELPERSISTENCE_PERMANENT,
                relkind: 0,
                relhassubclass: false,
                relrowsecurity: false,
                relispopulated: false,
                relreplident: 0,
                relispartition: false,
                relfrozenxid: 0,
                relminmxid: 0,
            },
            rd_att: Rc::new(TupleDescData {
                natts: 0,
                tdtypeid: 0,
                tdtypmod: -1,
                tdrefcount: -1,
                constr: None,
                compact_attrs: PgVec::new_in(mcx),
                attrs: PgVec::new_in(mcx),
            }),
            rd_index: None,
            rd_opcintype: PgVec::new_in(mcx),
            rd_opfamily: PgVec::new_in(mcx),
            rd_indoption: PgVec::new_in(mcx),
            rd_indcollation: PgVec::new_in(mcx),
            rd_options: None,
            pgstat_enabled: Cell::new(false),
            pgstat_link: core::cell::Cell::new((0, core::ptr::null_mut())),
            rd_amcache: Default::default(),
            rd_amcache_hash: Default::default(), rd_amcache_gin: Default::default(), rd_amcache_spgist: Default::default(),
            rd_support: PgVec::new_in(mcx),
            rd_supportinfo: Default::default(),
            rd_opcoptions: Default::default(),
            rd_indexlist: Default::default(),
            rd_trigdesc: Default::default(),
            rd_hastriggers: false, rd_hasrules: false,
        },
    }
}

pub fn FreeFakeRelcacheEntry(fakerel: FakeRelcacheEntry) {
    // An rd_smgr pin would leak past the entry; no fake-entry path takes one.
    debug_assert!(fakerel.rel.rd_smgr.get().is_none());
}

pub fn XLogDropRelation(rlocator: RelFileLocator, forknum: ForkNumber) -> PgResult<()> {
    forget_invalid_pages(rlocator, forknum, 0)
}

pub fn XLogDropDatabase(dbid: Oid) -> PgResult<()> {
    // Heavy-handed (closes SMgrRelations of other databases too), matching C.
    smgr_seams::smgr_destroy_all::call()?;
    forget_invalid_pages_db(dbid)
}

pub fn XLogTruncateRelation(
    rlocator: RelFileLocator,
    forkNum: ForkNumber,
    nblocks: BlockNumber,
) -> PgResult<()> {
    forget_invalid_pages(rlocator, forkNum, nblocks)
}

pub fn XLogReadDetermineTimeline(
    state: &mut XLogReaderState,
    wantPage: XLogRecPtr,
    wantLength: u32,
    currTLI: TimeLineID,
) -> PgResult<()> {
    let ws_segsize = state.segcxt.ws_segsize as u64;
    let lastReadPage: XLogRecPtr = state.seg.ws_segno * ws_segsize + state.segoff as u64;

    debug_assert!(wantPage != InvalidXLogRecPtr && wantPage % XLOG_BLCKSZ as u64 == 0);
    debug_assert!(wantLength as usize <= XLOG_BLCKSZ);
    debug_assert!(state.readLen == 0 || state.readLen as usize <= XLOG_BLCKSZ);
    debug_assert!(currTLI != 0);

    if lastReadPage == wantPage
        && state.readLen != 0
        && lastReadPage + state.readLen as u64
            >= wantPage + (wantLength as u64).min((XLOG_BLCKSZ - 1) as u64)
    {
        return Ok(());
    }

    if state.currTLI == currTLI && wantPage >= lastReadPage {
        debug_assert!(state.currTLIValidUntil == InvalidXLogRecPtr);
        return Ok(());
    }

    if state.currTLIValidUntil != InvalidXLogRecPtr
        && state.currTLI != currTLI
        && state.currTLI != 0
        && ((wantPage + wantLength as u64) / ws_segsize)
            < (state.currTLIValidUntil / ws_segsize)
    {
        return Ok(());
    }

    {
        // The scoped context is C's palloc'd history list + list_free_deep;
        // cold — runs only on timeline switch or random access.
        let endOfSegment: XLogRecPtr = ((wantPage / ws_segsize) + 1) * ws_segsize - 1;
        debug_assert!(wantPage / ws_segsize == endOfSegment / ws_segsize);

        let history_cx = mcx::MemoryContext::new("xlogutils timeline history");
        let timelineHistory =
            timeline_seams::read_timeline_history::call(history_cx.mcx(), currTLI)?;

        state.currTLI =
            timeline_seams::tli_of_point_in_history::call(endOfSegment, &timelineHistory)?;
        let (valid_until, next_tli) =
            timeline_seams::tli_switch_point::call(state.currTLI, &timelineHistory)?;
        state.currTLIValidUntil = valid_until;
        state.nextTLI = next_tli;

        debug_assert!(
            state.currTLIValidUntil == InvalidXLogRecPtr
                || wantPage + (wantLength as u64) < state.currTLIValidUntil
        );

        elog(
            DEBUG3,
            format!(
                "switched to timeline {} valid until {:X}/{:X}",
                state.currTLI,
                (state.currTLIValidUntil >> 32) as u32,
                state.currTLIValidUntil as u32
            ),
        )?;
    }

    Ok(())
}

fn XLogSegmentsPerXLogId(wal_segsz_bytes: i32) -> u64 {
    0x1_0000_0000_u64 / wal_segsz_bytes as u64
}

fn XLogFileName(tli: TimeLineID, logSegNo: XLogSegNo, wal_segsz_bytes: i32) -> String {
    let per = XLogSegmentsPerXLogId(wal_segsz_bytes);
    format!("{:08X}{:08X}{:08X}", tli, (logSegNo / per) as u32, (logSegNo % per) as u32)
}

fn XLogFilePath(tli: TimeLineID, logSegNo: XLogSegNo, wal_segsz_bytes: i32) -> String {
    format!("pg_wal/{}", XLogFileName(tli, logSegNo, wal_segsz_bytes))
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

pub fn wal_segment_open(
    state: &mut XLogReaderState,
    nextSegNo: XLogSegNo,
    tli_p: &mut TimeLineID,
) -> PgResult<()> {
    let tli = *tli_p;
    let path = XLogFilePath(tli, nextSegNo, state.segcxt.ws_segsize);
    let fd = file_seams::basic_open_file::call(&path, libc::O_RDONLY);
    if fd >= 0 {
        state.seg.ws_file = fd;
        return Ok(());
    }

    let en = current_errno();
    if en == libc::ENOENT {
        ereport(ERROR)
            .with_saved_errno(en)
            .errcode_for_file_access()
            .errmsg(format!("requested WAL segment {path} has already been removed"))
            .finish(loc("wal_segment_open"))?;
    } else {
        ereport(ERROR)
            .with_saved_errno(en)
            .errcode_for_file_access()
            .errmsg(format!("could not open file \"{path}\": %m"))
            .finish(loc("wal_segment_open"))?;
    }
    unreachable!("wal_segment_open reported below ERROR");
}

pub fn wal_segment_close(state: &mut XLogReaderState) {
    // SAFETY: closing the fd this reader opened; C ignores errno here too.
    unsafe { libc::close(state.seg.ws_file) };
    state.seg.ws_file = -1;
}

pub fn read_local_xlog_page(
    state: &mut XLogReaderState,
    targetPagePtr: XLogRecPtr,
    reqLen: i32,
    targetRecPtr: XLogRecPtr,
    cur_page: &mut [u8],
) -> PgResult<i32> {
    read_local_xlog_page_guts(state, targetPagePtr, reqLen, targetRecPtr, cur_page, true)
}

pub fn read_local_xlog_page_no_wait(
    state: &mut XLogReaderState,
    targetPagePtr: XLogRecPtr,
    reqLen: i32,
    targetRecPtr: XLogRecPtr,
    cur_page: &mut [u8],
) -> PgResult<i32> {
    read_local_xlog_page_guts(state, targetPagePtr, reqLen, targetRecPtr, cur_page, false)
}

// CHECK_FOR_INTERRUPTS(): ProcessInterrupts (tcop/postgres.c) is unported.
fn check_for_interrupts() {}

fn read_local_xlog_page_guts(
    state: &mut XLogReaderState,
    targetPagePtr: XLogRecPtr,
    reqLen: i32,
    _targetRecPtr: XLogRecPtr,
    cur_page: &mut [u8],
    wait_for_wal: bool,
) -> PgResult<i32> {
    let loc_ = targetPagePtr + reqLen as u64;
    let mut read_upto: XLogRecPtr;
    let mut tli: TimeLineID;

    loop {
        let (ru, currTLI) = if !transam_xlog_seams::recovery_in_progress::call() {
            transam_xlog_seams::get_flush_rec_ptr::call()
        } else {
            xlogrecovery_seams::get_xlog_replay_rec_ptr::call()
        };
        read_upto = ru;
        tli = currTLI;

        // Re-checked per iteration: a cascading standby's timeline can
        // become historical while this process stays in recovery.
        XLogReadDetermineTimeline(state, targetPagePtr, reqLen as u32, tli)?;

        if state.currTLI == currTLI {
            if loc_ <= read_upto {
                break;
            }

            if !wait_for_wal {
                state.private_end_of_wal = true;
                break;
            }

            check_for_interrupts();
            unsafe { libc::usleep(1000) };
        } else {
            // Historical timeline: read only to the switch point.
            read_upto = state.currTLIValidUntil;
            tli = state.currTLI;
            break;
        }
    }

    let count: i32 = if targetPagePtr + XLOG_BLCKSZ as u64 <= read_upto {
        XLOG_BLCKSZ as i32
    } else if targetPagePtr + reqLen as u64 > read_upto {
        return Ok(-1);
    } else {
        (read_upto - targetPagePtr) as i32
    };

    if let Err(errinfo) = xlogreader_seams::wal_read::call(
        state,
        &mut cur_page[..count as usize],
        targetPagePtr,
        count as usize,
        tli,
    )? {
        WALReadRaiseError(&errinfo)?;
    }

    Ok(count)
}

pub fn WALReadRaiseError(errinfo: &WALReadError) -> PgResult<()> {
    let seg = &errinfo.wre_seg;
    let fname = XLogFileName(seg.ws_tli, seg.ws_segno, transam_xlog_seams::wal_segment_size::call());

    if errinfo.wre_read < 0 {
        ereport(ERROR)
            .with_saved_errno(errinfo.wre_errno)
            .errcode_for_file_access()
            .errmsg(format!(
                "could not read from WAL segment {fname}, offset {}: %m",
                errinfo.wre_off
            ))
            .finish(loc("WALReadRaiseError"))?;
    } else if errinfo.wre_read == 0 {
        ereport(ERROR)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg(format!(
                "could not read from WAL segment {fname}, offset {}: read {} of {}",
                errinfo.wre_off, errinfo.wre_read, errinfo.wre_req
            ))
            .finish(loc("WALReadRaiseError"))?;
    }
    Ok(())
}

#[inline]
fn BufferIsValid(buffer: Buffer) -> bool {
    buffer != InvalidBuffer
}

pub fn init_seams() {
    xlogutils_seams::in_recovery::set(in_recovery);
    xlogutils_seams::in_hot_standby::set(InHotStandby);
    guc_tables::vars::ignore_invalid_pages.install(guc_tables::GucVarAccessors {
        get: ignore_invalid_pages,
        set: set_ignore_invalid_pages,
    });
}
