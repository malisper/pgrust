#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU16, AtomicU32};
use std::sync::OnceLock;

use elog::ereport;
use init_small::globals;
use lwlock::{LWLockAcquire, LWLockRelease, LWLock, LW_EXCLUSIVE, LW_SHARED};
use slru::{
    check_slru_buffers, LwGuard, SimpleLruAutotuneBuffers, SimpleLruDoesPhysicalPageExist,
    SimpleLruGetBankLock, SimpleLruInit, SimpleLruReadPage, SimpleLruReadPage_ReadOnly,
    SimpleLruShmemSize, SimpleLruTruncate, SimpleLruWriteAll, SimpleLruWritePage,
    SimpleLruZeroPage, SlruCtlData, SlruPagePrecedesUnitTests, SlruPath, SlruScanDirCbDeleteAll,
    SlruScanDirCbReportPresence, SlruScanDirectory, SlruSyncFileTag, SLRU_MAX_ALLOWED_BUFFERS,
};
use types_core::{
    FirstNormalTransactionId, FullTransactionId, InvalidRepOriginId, InvalidTransactionId,
    RepOriginId, Size, TimestampTz, TransactionId, TransactionIdEquals, TransactionIdIsNormal,
    TransactionIdIsValid, TransactionIdPrecedes, BLCKSZ,
};
use types_error::{
    ErrorLocation, PgResult, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR, PANIC,
};
use types_guc::{GucContext::PGC_POSTMASTER, GucSource};
use types_storage::storage::{COMMIT_TS_LOCK, LWTRANCHE_COMMITTS_BUFFER, LWTRANCHE_COMMITTS_SLRU};
use types_storage::sync::{FileTag, SyncRequestHandler};
use varsup::TransamVariables;
use xlogreader_seams::XLogReaderState;

pub const RM_COMMIT_TS_ID: u8 = 18;
pub const COMMIT_TS_ZEROPAGE: u8 = 0x00;
pub const COMMIT_TS_TRUNCATE: u8 = 0x10;
const XLR_INFO_MASK: u8 = 0x0F;

pub const SizeOfCommitTimestampEntry: usize = 10;
pub const COMMIT_TS_XACTS_PER_PAGE: u32 = (BLCKSZ / SizeOfCommitTimestampEntry) as u32;

// DT_NOBEGIN (TIMESTAMP_NOBEGIN, datatype/timestamp.h).
const DT_NOBEGIN: TimestampTz = TimestampTz::MIN;

static COMMIT_TS_CTL: OnceLock<SlruCtlData> = OnceLock::new();

// CommitTimestampShared; word atomics mirror C's aligned-word access, fields
// serialized by CommitTsLock except where C documents an unlocked read.
struct CommitTsSharedData {
    xidLastCommit: AtomicU32,
    timeLastCommit: AtomicI64,
    nodeidLastCommit: AtomicU16,
    commitTsActive: AtomicBool,
}

static COMMIT_TS_SHARED: OnceLock<CommitTsSharedData> = OnceLock::new();

fn CommitTsCtl() -> &'static SlruCtlData {
    COMMIT_TS_CTL
        .get()
        .unwrap_or_else(|| panic!("commit_ts accessed before CommitTsShmemInit"))
}

fn shared() -> &'static CommitTsSharedData {
    COMMIT_TS_SHARED
        .get()
        .unwrap_or_else(|| panic!("commitTsShared accessed before CommitTsShmemInit"))
}

fn CommitTsLock() -> &'static LWLock {
    lwlock::main_lock(COMMIT_TS_LOCK)
}

#[inline]
fn TransactionIdToCTsPage(xid: TransactionId) -> i64 {
    xid as i64 / COMMIT_TS_XACTS_PER_PAGE as i64
}

#[inline]
fn TransactionIdToCTsEntry(xid: TransactionId) -> u32 {
    xid % COMMIT_TS_XACTS_PER_PAGE
}

fn write_entry(page: &mut [u8], entryno: u32, time: TimestampTz, nodeid: RepOriginId) {
    let base = SizeOfCommitTimestampEntry * entryno as usize;
    page[base..base + 8].copy_from_slice(&time.to_ne_bytes());
    page[base + 8..base + 10].copy_from_slice(&nodeid.to_ne_bytes());
}

fn read_entry(page: &[u8], entryno: u32) -> (TimestampTz, RepOriginId) {
    let base = SizeOfCommitTimestampEntry * entryno as usize;
    let time = TimestampTz::from_ne_bytes(page[base..base + 8].try_into().unwrap());
    let nodeid = RepOriginId::from_ne_bytes(page[base + 8..base + 10].try_into().unwrap());
    (time, nodeid)
}

#[track_caller]
fn loc(routine: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, routine)
}

pub fn TransactionTreeSetCommitTsData(
    xid: TransactionId,
    subxids: &[TransactionId],
    timestamp: TimestampTz,
    nodeid: RepOriginId,
) -> PgResult<()> {
    // Unlocked read: on a standby only the recovery process (this caller)
    // flips the flag.
    if !shared().commitTsActive.load(Relaxed) {
        return Ok(());
    }

    let nsubxids = subxids.len();
    let newestXact = if nsubxids > 0 { subxids[nsubxids - 1] } else { xid };

    let mut headxid = xid;
    let mut i = 0usize;
    loop {
        let pageno = TransactionIdToCTsPage(headxid);
        let mut j = i;
        while j < nsubxids {
            if TransactionIdToCTsPage(subxids[j]) != pageno {
                break;
            }
            j += 1;
        }

        SetXidCommitTsInPage(headxid, &subxids[i..j], timestamp, nodeid, pageno)?;

        if j >= nsubxids {
            break;
        }
        headxid = subxids[j];
        i = j + 1;
    }

    LWLockAcquire(CommitTsLock(), LW_EXCLUSIVE, globals::MyProcNumber())?;
    let s = shared();
    s.xidLastCommit.store(xid, Relaxed);
    s.timeLastCommit.store(timestamp, Relaxed);
    s.nodeidLastCommit.store(nodeid, Relaxed);

    let tv = TransamVariables();
    if TransactionIdPrecedes(tv.newestCommitTsXid.load(Relaxed), newestXact) {
        tv.newestCommitTsXid.store(newestXact, Relaxed);
    }
    LWLockRelease(CommitTsLock())
}

fn SetXidCommitTsInPage(
    xid: TransactionId,
    subxids: &[TransactionId],
    ts: TimestampTz,
    nodeid: RepOriginId,
    pageno: i64,
) -> PgResult<()> {
    let ctl = CommitTsCtl();
    let mut bank = LwGuard::acquire(SimpleLruGetBankLock(ctl, pageno), LW_EXCLUSIVE)?;

    let slotno = SimpleLruReadPage(ctl, pageno, true, xid, &mut bank)?;

    TransactionIdSetCommitTs(xid, ts, nodeid, slotno, &mut bank);
    for &subxid in subxids {
        TransactionIdSetCommitTs(subxid, ts, nodeid, slotno, &mut bank);
    }

    ctl.mark_page_dirty(slotno, &bank);
    bank.release()
}

fn TransactionIdSetCommitTs(
    xid: TransactionId,
    ts: TimestampTz,
    nodeid: RepOriginId,
    slotno: usize,
    bank: &mut LwGuard,
) {
    let entryno = TransactionIdToCTsEntry(xid);
    debug_assert!(TransactionIdIsNormal(xid));
    write_entry(CommitTsCtl().page_buffer_mut(slotno, bank), entryno, ts, nodeid);
}

pub fn TransactionIdGetCommitTsData(
    xid: TransactionId,
) -> PgResult<Option<(TimestampTz, RepOriginId)>> {
    let pageno = TransactionIdToCTsPage(xid);
    let entryno = TransactionIdToCTsEntry(xid);

    if !TransactionIdIsValid(xid) {
        ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "cannot retrieve commit timestamp for transaction {xid}"
            ))
            .finish(loc("TransactionIdGetCommitTsData"))?;
    } else if !TransactionIdIsNormal(xid) {
        // Frozen and bootstrap xids are always committed far in the past.
        return Ok(None);
    }

    LWLockAcquire(CommitTsLock(), LW_SHARED, globals::MyProcNumber())?;

    let s = shared();
    if !s.commitTsActive.load(Relaxed) {
        LWLockRelease(CommitTsLock())?;
        return error_commit_ts_disabled("TransactionIdGetCommitTsData");
    }

    if s.xidLastCommit.load(Relaxed) == xid {
        let ts = s.timeLastCommit.load(Relaxed);
        let nodeid = s.nodeidLastCommit.load(Relaxed);
        LWLockRelease(CommitTsLock())?;
        return Ok((ts != 0).then_some((ts, nodeid)));
    }

    let tv = TransamVariables();
    let oldestCommitTsXid = tv.oldestCommitTsXid.load(Relaxed);
    let newestCommitTsXid = tv.newestCommitTsXid.load(Relaxed);
    debug_assert_eq!(
        TransactionIdIsValid(oldestCommitTsXid),
        TransactionIdIsValid(newestCommitTsXid)
    );
    LWLockRelease(CommitTsLock())?;

    if !TransactionIdIsValid(oldestCommitTsXid)
        || TransactionIdPrecedes(xid, oldestCommitTsXid)
        || TransactionIdPrecedes(newestCommitTsXid, xid)
    {
        return Ok(None);
    }

    let ctl = CommitTsCtl();
    let (slotno, bank) = SimpleLruReadPage_ReadOnly(ctl, pageno, xid)?;
    let (ts, nodeid) = read_entry(ctl.page_buffer(slotno, &bank), entryno);
    bank.release()?;

    Ok((ts != 0).then_some((ts, nodeid)))
}

pub fn GetLatestCommitTsData() -> PgResult<(TransactionId, TimestampTz, RepOriginId)> {
    LWLockAcquire(CommitTsLock(), LW_SHARED, globals::MyProcNumber())?;

    let s = shared();
    if !s.commitTsActive.load(Relaxed) {
        LWLockRelease(CommitTsLock())?;
        return error_commit_ts_disabled("GetLatestCommitTsData");
    }

    let xid = s.xidLastCommit.load(Relaxed);
    let ts = s.timeLastCommit.load(Relaxed);
    let nodeid = s.nodeidLastCommit.load(Relaxed);
    LWLockRelease(CommitTsLock())?;

    Ok((xid, ts, nodeid))
}

#[cold]
fn error_commit_ts_disabled<T>(routine: &'static str) -> PgResult<T> {
    let hint = if transam_xlog_seams::recovery_in_progress::call() {
        "Make sure the configuration parameter \"track_commit_timestamp\" is set on the primary server."
    } else {
        "Make sure the configuration parameter \"track_commit_timestamp\" is set."
    };
    ereport(ERROR)
        .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        .errmsg("could not get commit timestamp data")
        .errhint(hint)
        .finish(loc(routine))?;
    unreachable!("error_commit_ts_disabled returned");
}

fn CommitTsShmemBuffers() -> i32 {
    if globals::commit_timestamp_buffers() == 0 {
        return SimpleLruAutotuneBuffers(512, 1024);
    }
    globals::commit_timestamp_buffers()
        .max(16)
        .min(SLRU_MAX_ALLOWED_BUFFERS)
}

pub fn CommitTsShmemSize() -> Size {
    SimpleLruShmemSize(CommitTsShmemBuffers(), 0) + core::mem::size_of::<CommitTsSharedData>()
}

pub fn CommitTsShmemInit() -> PgResult<()> {
    if globals::commit_timestamp_buffers() == 0 {
        let buf = CommitTsShmemBuffers().to_string();
        guc::SetConfigOption(
            "commit_timestamp_buffers",
            Some(&buf),
            PGC_POSTMASTER,
            GucSource::PGC_S_DYNAMIC_DEFAULT,
        )?;

        // An explicit commit_timestamp_buffers=0 in the config file outranks
        // PGC_S_DYNAMIC_DEFAULT; force the matter with PGC_S_OVERRIDE.
        if globals::commit_timestamp_buffers() == 0 {
            guc::SetConfigOption(
                "commit_timestamp_buffers",
                Some(&buf),
                PGC_POSTMASTER,
                GucSource::PGC_S_OVERRIDE,
            )?;
        }
    }
    debug_assert!(globals::commit_timestamp_buffers() != 0);

    let mut ctl = SimpleLruInit(
        "commit_timestamp",
        CommitTsShmemBuffers(),
        0,
        "pg_commit_ts",
        LWTRANCHE_COMMITTS_BUFFER,
        LWTRANCHE_COMMITTS_SLRU,
        SyncRequestHandler::SYNC_HANDLER_COMMIT_TS,
        false,
    )?;
    ctl.PagePrecedes = Some(CommitTsPagePrecedes);
    SlruPagePrecedesUnitTests(&ctl, COMMIT_TS_XACTS_PER_PAGE as i32);

    if COMMIT_TS_CTL.set(ctl).is_err() {
        panic!("CommitTsShmemInit called twice");
    }

    COMMIT_TS_SHARED
        .set(CommitTsSharedData {
            xidLastCommit: AtomicU32::new(InvalidTransactionId),
            timeLastCommit: AtomicI64::new(DT_NOBEGIN),
            nodeidLastCommit: AtomicU16::new(InvalidRepOriginId),
            commitTsActive: AtomicBool::new(false),
        })
        .unwrap_or_else(|_| panic!("CommitTsShmemInit called twice"));
    Ok(())
}

/// Crash-cycle reset in place (notes/crash-restart-design.md); StartupCommitTs
/// / CompleteCommitTsInitialization re-activate as after C's shmem re-create.
pub fn CommitTsShmemResetAfterCrash() {
    slru::SimpleLruResetAfterCrash(CommitTsCtl());
    let s = shared();
    s.xidLastCommit.store(InvalidTransactionId, Relaxed);
    s.timeLastCommit.store(DT_NOBEGIN, Relaxed);
    s.nodeidLastCommit.store(InvalidRepOriginId, Relaxed);
    s.commitTsActive.store(false, Relaxed);
}

pub fn check_commit_ts_buffers(newval: i32) -> (bool, Option<String>) {
    check_slru_buffers("commit_timestamp_buffers", newval)
}

// C's BootStrapCommitTs is empty: segments are created by ActivateCommitTs.
pub fn BootStrapCommitTs() {}

fn ZeroCommitTsPage(pageno: i64, writeXlog: bool, bank: &mut LwGuard) -> PgResult<usize> {
    let slotno = SimpleLruZeroPage(CommitTsCtl(), pageno, bank)?;

    if writeXlog {
        WriteZeroPageXlogRec(pageno)?;
    }

    Ok(slotno)
}

pub fn StartupCommitTs() -> PgResult<()> {
    ActivateCommitTs()
}

pub fn CompleteCommitTsInitialization() -> PgResult<()> {
    if !guc_tables::vars::track_commit_timestamp.read() {
        DeactivateCommitTs()
    } else {
        ActivateCommitTs()
    }
}

pub fn CommitTsParameterChange(newvalue: bool, _oldvalue: bool) -> PgResult<()> {
    // Recovery process only, so the unlocked read is fine.
    if newvalue {
        if !shared().commitTsActive.load(Relaxed) {
            ActivateCommitTs()?;
        }
    } else if shared().commitTsActive.load(Relaxed) {
        DeactivateCommitTs()?;
    }
    Ok(())
}

fn ActivateCommitTs() -> PgResult<()> {
    if miscinit_seams::is_bootstrap_processing_mode::call() {
        return Ok(());
    }

    LWLockAcquire(CommitTsLock(), LW_EXCLUSIVE, globals::MyProcNumber())?;
    if shared().commitTsActive.load(Relaxed) {
        return LWLockRelease(CommitTsLock());
    }
    LWLockRelease(CommitTsLock())?;

    let ctl = CommitTsCtl();
    let xid = FullTransactionId::from_u64(TransamVariables().nextXid.load(Relaxed)).xid();
    let pageno = TransactionIdToCTsPage(xid);

    ctl.set_latest_page_number(pageno);

    // Enabled now but not in the previous run: seed oldest/newest to nextXid
    // so we never read data that was not written.
    LWLockAcquire(CommitTsLock(), LW_EXCLUSIVE, globals::MyProcNumber())?;
    let tv = TransamVariables();
    if tv.oldestCommitTsXid.load(Relaxed) == InvalidTransactionId {
        let next = varsup::ReadNextTransactionId()?;
        tv.oldestCommitTsXid.store(next, Relaxed);
        tv.newestCommitTsXid.store(next, Relaxed);
    }
    LWLockRelease(CommitTsLock())?;

    if !SimpleLruDoesPhysicalPageExist(ctl, pageno)? {
        let mut bank = LwGuard::acquire(SimpleLruGetBankLock(ctl, pageno), LW_EXCLUSIVE)?;
        let slotno = ZeroCommitTsPage(pageno, false, &mut bank)?;
        SimpleLruWritePage(ctl, slotno, &mut bank)?;
        debug_assert!(!ctl.page_dirty(slotno, &bank));
        bank.release()?;
    }

    LWLockAcquire(CommitTsLock(), LW_EXCLUSIVE, globals::MyProcNumber())?;
    shared().commitTsActive.store(true, Relaxed);
    LWLockRelease(CommitTsLock())
}

fn DeactivateCommitTs() -> PgResult<()> {
    LWLockAcquire(CommitTsLock(), LW_EXCLUSIVE, globals::MyProcNumber())?;

    let s = shared();
    s.commitTsActive.store(false, Relaxed);
    s.xidLastCommit.store(InvalidTransactionId, Relaxed);
    s.timeLastCommit.store(DT_NOBEGIN, Relaxed);
    s.nodeidLastCommit.store(InvalidRepOriginId, Relaxed);

    let tv = TransamVariables();
    tv.oldestCommitTsXid.store(InvalidTransactionId, Relaxed);
    tv.newestCommitTsXid.store(InvalidTransactionId, Relaxed);

    // Remove *all* files so a later re-enable leaves no gap in the file
    // sequence; done under CommitTsLock (rare, replica-only path in C).
    SlruScanDirectory(CommitTsCtl(), SlruScanDirCbDeleteAll)?;

    LWLockRelease(CommitTsLock())
}

pub fn CheckPointCommitTs() -> PgResult<()> {
    SimpleLruWriteAll(CommitTsCtl(), true)
}

pub fn ExtendCommitTs(newestXact: TransactionId) -> PgResult<()> {
    // Unlocked read: only called from GetNewTransactionId, never in a standby.
    debug_assert!(!xlogutils_seams::in_recovery::call());
    if !shared().commitTsActive.load(Relaxed) {
        return Ok(());
    }

    // Work only at the first XID of a page; just after wraparound the first
    // XID of page zero is FirstNormalTransactionId.
    if TransactionIdToCTsEntry(newestXact) != 0
        && !TransactionIdEquals(newestXact, FirstNormalTransactionId)
    {
        return Ok(());
    }

    let ctl = CommitTsCtl();
    let pageno = TransactionIdToCTsPage(newestXact);
    let mut bank = LwGuard::acquire(SimpleLruGetBankLock(ctl, pageno), LW_EXCLUSIVE)?;

    ZeroCommitTsPage(pageno, true, &mut bank)?;

    bank.release()
}

pub fn TruncateCommitTs(oldestXact: TransactionId) -> PgResult<()> {
    let ctl = CommitTsCtl();
    let cutoffPage = TransactionIdToCTsPage(oldestXact);

    if !SlruScanDirectory(ctl, |ctl, filename, segpage| {
        SlruScanDirCbReportPresence(ctl, filename, segpage, cutoffPage)
    })? {
        return Ok(());
    }

    WriteTruncateXlogRec(cutoffPage, oldestXact)?;

    SimpleLruTruncate(ctl, cutoffPage)
}

pub fn SetCommitTsLimit(oldestXact: TransactionId, newestXact: TransactionId) -> PgResult<()> {
    LWLockAcquire(CommitTsLock(), LW_EXCLUSIVE, globals::MyProcNumber())?;
    let tv = TransamVariables();
    if tv.oldestCommitTsXid.load(Relaxed) != InvalidTransactionId {
        if TransactionIdPrecedes(tv.oldestCommitTsXid.load(Relaxed), oldestXact) {
            tv.oldestCommitTsXid.store(oldestXact, Relaxed);
        }
        if TransactionIdPrecedes(newestXact, tv.newestCommitTsXid.load(Relaxed)) {
            tv.newestCommitTsXid.store(newestXact, Relaxed);
        }
    } else {
        debug_assert_eq!(tv.newestCommitTsXid.load(Relaxed), InvalidTransactionId);
        tv.oldestCommitTsXid.store(oldestXact, Relaxed);
        tv.newestCommitTsXid.store(newestXact, Relaxed);
    }
    LWLockRelease(CommitTsLock())
}

pub fn AdvanceOldestCommitTsXid(oldestXact: TransactionId) -> PgResult<()> {
    LWLockAcquire(CommitTsLock(), LW_EXCLUSIVE, globals::MyProcNumber())?;
    let tv = TransamVariables();
    if tv.oldestCommitTsXid.load(Relaxed) != InvalidTransactionId
        && TransactionIdPrecedes(tv.oldestCommitTsXid.load(Relaxed), oldestXact)
    {
        tv.oldestCommitTsXid.store(oldestXact, Relaxed);
    }
    LWLockRelease(CommitTsLock())
}

// TransactionIdPrecedes offset by FirstNormal+1; unlike clog,
// (1 << 31) % COMMIT_TS_XACTS_PER_PAGE != 0 (commit_ts.c CommitTsPagePrecedes).
fn CommitTsPagePrecedes(page1: i64, page2: i64) -> bool {
    let mut xid1 = (page1 as TransactionId).wrapping_mul(COMMIT_TS_XACTS_PER_PAGE);
    xid1 = xid1.wrapping_add(FirstNormalTransactionId + 1);
    let mut xid2 = (page2 as TransactionId).wrapping_mul(COMMIT_TS_XACTS_PER_PAGE);
    xid2 = xid2.wrapping_add(FirstNormalTransactionId + 1);

    TransactionIdPrecedes(xid1, xid2)
        && TransactionIdPrecedes(xid1, xid2.wrapping_add(COMMIT_TS_XACTS_PER_PAGE - 1))
}

fn WriteZeroPageXlogRec(pageno: i64) -> PgResult<()> {
    xloginsert_seams::xlog_insert::call(
        RM_COMMIT_TS_ID,
        COMMIT_TS_ZEROPAGE,
        &[&pageno.to_ne_bytes()],
    )?;
    Ok(())
}

// xl_commit_ts_truncate: pageno(8) + oldestXid(4), SizeOfCommitTsTruncate = 12.
fn WriteTruncateXlogRec(pageno: i64, oldestXid: TransactionId) -> PgResult<()> {
    let mut xlrec = [0u8; 12];
    xlrec[0..8].copy_from_slice(&pageno.to_ne_bytes());
    xlrec[8..12].copy_from_slice(&oldestXid.to_ne_bytes());

    xloginsert_seams::xlog_insert::call(RM_COMMIT_TS_ID, COMMIT_TS_TRUNCATE, &[&xlrec])?;
    Ok(())
}

pub fn commit_ts_redo(record: &mut XLogReaderState) -> PgResult<()> {
    let ctl = CommitTsCtl();
    let decoded = record
        .record
        .as_ref()
        .expect("commit_ts_redo dispatched on a reader with no decoded record");
    let info = decoded.xl_info & !XLR_INFO_MASK;

    debug_assert!(decoded.max_block_id < 0);
    // SAFETY: the reader's current record stays `decoded` for this whole call.
    let data = unsafe { decoded.main_data_bytes() };

    if info == COMMIT_TS_ZEROPAGE {
        let pageno =
            i64::from_ne_bytes(data[..8].try_into().expect("short COMMIT_TS_ZEROPAGE record"));

        let mut bank = LwGuard::acquire(SimpleLruGetBankLock(ctl, pageno), LW_EXCLUSIVE)?;

        let slotno = ZeroCommitTsPage(pageno, false, &mut bank)?;
        SimpleLruWritePage(ctl, slotno, &mut bank)?;
        debug_assert!(!ctl.page_dirty(slotno, &bank));

        bank.release()
    } else if info == COMMIT_TS_TRUNCATE {
        let pageno =
            i64::from_ne_bytes(data[..8].try_into().expect("short COMMIT_TS_TRUNCATE record"));
        let oldest_xid = TransactionId::from_ne_bytes(
            data[8..12].try_into().expect("short COMMIT_TS_TRUNCATE record"),
        );

        AdvanceOldestCommitTsXid(oldest_xid)?;

        // During replay latest_page_number is not set up; seed it to bypass
        // the sanity test in SimpleLruTruncate.
        ctl.set_latest_page_number(pageno);

        SimpleLruTruncate(ctl, pageno)
    } else {
        ereport(PANIC)
            .errmsg(format!("commit_ts_redo: unknown op code {info}"))
            .finish(loc("commit_ts_redo"))?;
        unreachable!("commit_ts_redo PANIC returned");
    }
}

pub fn committssyncfiletag(ftag: &FileTag) -> PgResult<(i32, SlruPath)> {
    SlruSyncFileTag(CommitTsCtl(), ftag)
}

// track_commit_timestamp's conf->variable backing (commit_ts.c GUC home).
thread_local! {
    static TRACK_COMMIT_TIMESTAMP: core::cell::Cell<bool> = const { core::cell::Cell::new(false) };
}

pub fn init_seams() {
    fn check_hook(
        newval: &mut i32,
        _extra: &mut Option<guc_tables::GucHookExtra>,
        _source: GucSource,
    ) -> PgResult<bool> {
        let (ok, detail) = check_commit_ts_buffers(*newval);
        if !ok {
            if let Some(d) = detail {
                guc_seams::guc_check_errdetail::call(d);
            }
        }
        Ok(ok)
    }
    guc_tables::hooks::check_commit_ts_buffers.install(check_hook);

    guc_tables::vars::track_commit_timestamp.install(guc_tables::GucVarAccessors {
        get: || TRACK_COMMIT_TIMESTAMP.with(core::cell::Cell::get),
        set: |v| TRACK_COMMIT_TIMESTAMP.with(|c| c.set(v)),
    });

    commit_ts_seams::transaction_tree_set_commit_ts_data::set(TransactionTreeSetCommitTsData);
    commit_ts_seams::startup_commit_ts::set(StartupCommitTs);
    commit_ts_seams::complete_commit_ts_initialization::set(CompleteCommitTsInitialization);
    commit_ts_seams::check_point_commit_ts::set(CheckPointCommitTs);
    commit_ts_seams::set_commit_ts_limit::set(SetCommitTsLimit);
    commit_ts_seams::extend_commit_ts::set(ExtendCommitTs);
    commit_ts_seams::commit_ts_parameter_change::set(CommitTsParameterChange);

    fmgr_builtins::register_builtins();
}

pub mod fmgr_builtins;

#[cfg(test)]
mod tests;
