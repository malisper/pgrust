//! Port of `src/backend/postmaster/walsummarizer.c` (PostgreSQL 18.3): the WAL
//! summarizer background process.
//!
//! It continuously scans the WAL and periodically emits a summary file
//! describing which relation-fork blocks were modified by the records in an
//! LSN range (see `backup/walsummary.c` and `common/blkreftable.c`), and
//! removes old summary files once the WAL they cover has been recycled.
//!
//! The shared-memory control block `WalSummarizerData` is faithful shared
//! memory (`#[repr(C)]`, placed by `ShmemInitStruct`, guarded by the built-in
//! `WALSummarizerLock`). The reader's `SummarizerReadLocalXLogPrivate`
//! private-data (tli / historic / read_upto / end_of_wal) is summarizer-owned
//! per-backend state kept here, keyed by the reader's registry handle.
//!
//! The file-scope `static long sleep_quanta` / `pages_read_since_last_sleep`
//! and the `redo_pointer_at_last_summary_removal` global, plus the two GUCs
//! (`summarize_wal`, `wal_summary_keep_time`), are per-backend process state,
//! so they are `thread_local!` here (AGENTS.md "Backend-global state"), never
//! shared statics.

#![allow(non_snake_case)]

use std::cell::Cell;
use std::collections::HashMap;

use ::mcx::Mcx;

use ::types_core::{
    ForkNumber, Oid, TimeLineID, TimestampTz, XLogRecPtr, XLogSegNo, FSM_FORKNUM, INVALID_PROC_NUMBER,
    MAIN_FORKNUM, MAX_FORKNUM, MAXPGPATH, VISIBILITYMAP_FORKNUM,
};
use ::types_error::{
    DEBUG1, ERRCODE_INTERNAL_ERROR, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR, PgError,
    PgResult, WARNING,
};
use ::types_error::ErrorLocation;
use ::utils_error::ereport;
use ::types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};
use ::types_storage::{LWLockMode, RelFileLocator};
use ::types_pgstat::wait_event::{WAIT_EVENT_WAL_SUMMARIZER_ERROR, WAIT_EVENT_WAL_SUMMARIZER_WAL};
use ::types_startup::StartupData;
use ::wal::RM_XACT_ID;
use ::types_walsummarizer::{BlockTag, ReadRecordResult, WalSummarizerData, WalSummaryFile, XLogReaderHandle};

use ::types_blkreftable::BlockRefTable;

use ipc_shmem_seams as shmem;
use dsm_core_seams as ipc;
use latch_seams as latch;
use procsignal_seams as procsignal;
use procarray_seams as procarray;
use lwlock_seams as lwlock;
use condition_variable_seams as cv;
use aio_seams as aio;
use fd_seams as fd;
use auxprocess_seams as auxprocess;
use init_small_seams as initsmall;
use miscinit_seams as miscinit;
use guc_file_seams as gucfile;
use mcxt_seams as mcxt;
use resowner_seams_2 as resowner;
use dynahash_seams as dynahash;
use waitevent_seams as waitevent;
use walstats_seams as walstats;
use timestamp_seams as timestamp;
use transam_xlog_seams as xlog;
use xlogrecovery_seams as xlogrecovery;
use xlogreader_seams as xlogreader;
use timeline_seams as timeline;
use xactdesc_seams as xactdesc;
use walreceiver_seams as walreceiver;
use walsummary_seams as walsummary;
use postgres_seams as postgres;
use common_blkreftable as blkreftable;

use interrupt as interrupt;

#[cfg(test)]
mod tests;

// ---------------------------------------------------------------------------
// Constants (matched to walsummarizer.c / the C headers).
// ---------------------------------------------------------------------------

/// `MAX_SLEEP_QUANTA` (walsummarizer.c). The sleep time is a multiple of 200ms
/// and will not exceed thirty seconds (150 * 200 = 30 * 1000).
const MAX_SLEEP_QUANTA: i64 = 150;
/// `MS_PER_SLEEP_QUANTUM` (walsummarizer.c).
const MS_PER_SLEEP_QUANTUM: i64 = 200;

/// `wal_summary_keep_time` boot default: `10 * HOURS_PER_DAY * MINS_PER_HOUR`
/// (timestamp.h: HOURS_PER_DAY=24, MINS_PER_HOUR=60).
const WAL_SUMMARY_KEEP_TIME_DEFAULT: i32 = 10 * 24 * 60;
/// `SECS_PER_MINUTE` (timestamp.h).
const SECS_PER_MINUTE: i64 = 60;

/// `WALSummarizerLock` — built-in individual LWLock #49 (`lwlocklist.h`).
const WAL_SUMMARIZER_LOCK: usize = 49;

/// `XLOGDIR` (`access/xlog_internal.h`).
const XLOGDIR: &str = "pg_wal";
/// `XLOG_BLCKSZ` (`pg_config.h`).
const XLOG_BLCKSZ: i32 = 8192;

/// `WAL_LEVEL_MINIMAL` (`access/xlog.h`).
const WAL_LEVEL_MINIMAL: i32 = 0;

// Resource manager ids (access/rmgrlist.h). RM_XLOG_ID=0, RM_XACT_ID=1 (from
// wal), RM_SMGR_ID=2, RM_DBASE_ID=4.
const RM_XLOG_ID: u8 = 0;
const RM_SMGR_ID: u8 = 2;
const RM_DBASE_ID: u8 = 4;

// XLOG (RM_XLOG_ID) info bytes (catalog/pg_control.h).
const XLOG_CHECKPOINT_SHUTDOWN: u8 = 0x00;
const XLOG_PARAMETER_CHANGE: u8 = 0x60;
const XLOG_END_OF_RECOVERY: u8 = 0x90;
const XLOG_CHECKPOINT_REDO: u8 = 0xE0;

// SMGR (RM_SMGR_ID) info bytes (catalog/storage_xlog.h).
const XLOG_SMGR_CREATE: u8 = 0x10;
const XLOG_SMGR_TRUNCATE: u8 = 0x20;
// xl_smgr_truncate flags (catalog/storage_xlog.h).
const SMGR_TRUNCATE_HEAP: i32 = 0x0001;
const SMGR_TRUNCATE_VM: i32 = 0x0002;

// DBASE (RM_DBASE_ID) info bytes (commands/dbcommands_xlog.h).
const XLOG_DBASE_CREATE_FILE_COPY: u8 = 0x00;
const XLOG_DBASE_CREATE_WAL_LOG: u8 = 0x10;
const XLOG_DBASE_DROP: u8 = 0x20;

// XACT (RM_XACT_ID) info bits (access/xact.h).
const XLOG_XACT_COMMIT: u8 = 0x00;
const XLOG_XACT_ABORT: u8 = 0x20;
const XLOG_XACT_COMMIT_PREPARED: u8 = 0x30;
const XLOG_XACT_ABORT_PREPARED: u8 = 0x40;
const XLOG_XACT_OPMASK: u8 = 0x70;

/// `XLR_INFO_MASK` (access/xlogrecord.h).
const XLR_INFO_MASK: u8 = 0x0F;

/// `InvalidXLogRecPtr`.
const INVALID_XLOG_REC_PTR: XLogRecPtr = 0;

#[inline]
fn XLogRecPtrIsInvalid(ptr: XLogRecPtr) -> bool {
    ptr == INVALID_XLOG_REC_PTR
}

// ---------------------------------------------------------------------------
// Per-backend process state (file-scope statics in walsummarizer.c).
// ---------------------------------------------------------------------------

thread_local! {
    /// `static WalSummarizerData *WalSummarizerCtl;` — the shmem control
    /// block, set by `WalSummarizerShmemInit`. A genuinely-shared pointer;
    /// 0 = NULL (not yet attached).
    static WAL_SUMMARIZER_CTL: Cell<*mut WalSummarizerData> = const { Cell::new(core::ptr::null_mut()) };

    /// `static long sleep_quanta = 1;`
    static SLEEP_QUANTA: Cell<i64> = const { Cell::new(1) };
    /// `static long pages_read_since_last_sleep = 0;`
    static PAGES_READ_SINCE_LAST_SLEEP: Cell<i64> = const { Cell::new(0) };
    /// `static XLogRecPtr redo_pointer_at_last_summary_removal = InvalidXLogRecPtr;`
    static REDO_POINTER_AT_LAST_SUMMARY_REMOVAL: Cell<XLogRecPtr> = const { Cell::new(INVALID_XLOG_REC_PTR) };

    /// `bool summarize_wal = false;` (GUC).
    static SUMMARIZE_WAL: Cell<bool> = const { Cell::new(false) };
    /// `int wal_summary_keep_time = 10 * HOURS_PER_DAY * MINS_PER_HOUR;` (GUC).
    static WAL_SUMMARY_KEEP_TIME: Cell<i32> = const { Cell::new(WAL_SUMMARY_KEEP_TIME_DEFAULT) };

    /// `SummarizerReadLocalXLogPrivate` private-data, keyed by the reader's
    /// registry handle. C attaches it as the reader's `private_data`; it is
    /// summarizer-owned state, so it lives here.
    static WS_PRIVATE: std::cell::RefCell<HashMap<XLogReaderHandle, SummarizerPrivate>> =
        std::cell::RefCell::new(HashMap::new());
}

/// `SummarizerReadLocalXLogPrivate` (walsummarizer.c) — the reader's page-read
/// callback private data.
#[derive(Clone, Copy, Debug)]
struct SummarizerPrivate {
    tli: TimeLineID,
    historic: bool,
    read_upto: XLogRecPtr,
    end_of_wal: bool,
}

fn ws_private_get<R>(handle: XLogReaderHandle, who: &str, f: impl FnOnce(&mut SummarizerPrivate) -> R) -> R {
    WS_PRIVATE.with(|m| {
        let mut m = m.borrow_mut();
        let p = m
            .get_mut(&handle)
            .unwrap_or_else(|| panic!("walsummarizer {who}: unknown reader handle {handle:?}"));
        f(p)
    })
}

/// Read the `summarize_wal` GUC.
#[inline]
pub fn summarize_wal_enabled() -> bool {
    SUMMARIZE_WAL.with(Cell::get)
}
/// Set the `summarize_wal` GUC (assign hook).
#[inline]
pub fn set_summarize_wal(value: bool) {
    SUMMARIZE_WAL.with(|c| c.set(value));
}
/// Read the `wal_summary_keep_time` GUC.
#[inline]
pub fn wal_summary_keep_time() -> i32 {
    WAL_SUMMARY_KEEP_TIME.with(Cell::get)
}
/// Set the `wal_summary_keep_time` GUC (assign hook).
#[inline]
pub fn set_wal_summary_keep_time(value: i32) {
    WAL_SUMMARY_KEEP_TIME.with(|c| c.set(value));
}

// ---------------------------------------------------------------------------
// Small helpers.
// ---------------------------------------------------------------------------

/// Borrow the shmem-resident `WalSummarizerData` (`WalSummarizerCtl`). Callers
/// hold `WALSummarizerLock` when touching the lock-protected fields, as in C.
/// Panics if accessed before `WalSummarizerShmemInit` attaches the segment
/// (the C code dereferences `WalSummarizerCtl` unconditionally on these paths).
#[inline]
fn ctl<'a>() -> &'a mut WalSummarizerData {
    let p = WAL_SUMMARIZER_CTL.with(Cell::get);
    // SAFETY: the pointer addresses the process's shared-memory control block,
    // placed once by WalSummarizerShmemInit and never moved; the summarizer is
    // the single mutator of the lock-protected fields while holding the lock.
    unsafe { p.as_mut().expect("WalSummarizerCtl accessed before ShmemInit") }
}

/// `LSN_FORMAT_ARGS(lsn)` rendered as `%X/%X`.
fn lsn_fmt(lsn: XLogRecPtr) -> String {
    format!("{:X}/{:X}", (lsn >> 32) as u32, lsn as u32)
}

fn loc(func: &str) -> ErrorLocation {
    ErrorLocation {
        filename: Some("walsummarizer.c".to_string()),
        lineno: 0,
        funcname: Some(func.to_string()),
    }
}

/// `LWLockAcquire(WALSummarizerLock, mode)`. The summarizer's lock sections do
/// not take a `?` between acquire and the matching [`lock_release`], so no
/// guard is required; the error-recovery path releases via
/// [`lwlock::lwlock_release_all`] (C's `LWLockReleaseAll`).
#[inline]
fn lock_acquire(exclusive: bool) -> PgResult<()> {
    let mode = if exclusive { LWLockMode::LW_EXCLUSIVE } else { LWLockMode::LW_SHARED };
    lwlock::lwlock_acquire_main::call(WAL_SUMMARIZER_LOCK, mode).map(|_| ())
}

#[inline]
fn lock_release() -> PgResult<()> {
    lwlock::lwlock_release_main::call(WAL_SUMMARIZER_LOCK)
}

// ===========================================================================
// 1. WalSummarizerShmemSize
// ===========================================================================

/// `WalSummarizerShmemSize` -- amount of shared memory required.
pub fn WalSummarizerShmemSize() -> usize {
    core::mem::size_of::<WalSummarizerData>()
}

// ===========================================================================
// 2. WalSummarizerShmemInit
// ===========================================================================

/// `WalSummarizerShmemInit` -- create or attach to the shared memory segment.
pub fn WalSummarizerShmemInit() -> PgResult<()> {
    let (addr, found) = shmem::shmem_init_struct::call("WalSummarizerCtl", WalSummarizerShmemSize())?;
    let p = addr as *mut WalSummarizerData;
    WAL_SUMMARIZER_CTL.with(|c| c.set(p));

    if !found {
        // First time through, so initialize. We're just filling in dummy
        // values here -- the real initialization happens when
        // GetOldestUnsummarizedLSN() is first called. ConditionVariableInit
        // is the in-place SpinLockInit + proclist_init for summary_file_cv.
        let c = ctl();
        c.initialized = false;
        c.summarized_tli = 0;
        c.summarized_lsn = INVALID_XLOG_REC_PTR;
        c.lsn_is_exact = false;
        c.summarizer_pgprocno = INVALID_PROC_NUMBER;
        c.pending_lsn = INVALID_XLOG_REC_PTR;
        c.summary_file_cv = condvar::ConditionVariable::new();
    }
    Ok(())
}

// ===========================================================================
// 3. WalSummarizerMain
// ===========================================================================

/// `WalSummarizerMain` -- entry point for the walsummarizer process. The C
/// `Assert(startup_data_len == 0)` is the `StartupData::None` match.
pub fn WalSummarizerMain(startup_data: &StartupData) -> PgResult<()> {
    let mut current_lsn: XLogRecPtr;
    let mut current_tli: TimeLineID;
    let mut exact: bool;
    let mut switch_lsn: XLogRecPtr = INVALID_XLOG_REC_PTR;
    let mut switch_tli: TimeLineID = 0;

    debug_assert!(matches!(startup_data, StartupData::None));

    // MyBackendType = B_WAL_SUMMARIZER; AuxiliaryProcessMainCommon(); the
    // pqsignal() block + the SIGCHLD reset + sigprocmask(SIG_SETMASK) are
    // performed by the host's auxiliary-process bootstrap.
    miscinit::set_my_backend_type_wal_summarizer::call();
    auxprocess::auxiliary_process_main_common::call()?;

    log_debug1("WAL summarizer started")?;

    // Advertise ourselves.
    ipc::on_shmem_exit::call(wal_summarizer_shutdown_callback, types_datum_unit())?;
    lock_acquire(true)?;
    ctl().summarizer_pgprocno = initsmall::my_proc_number::call();
    lock_release()?;

    // The sigsetjmp(local_sigjmp_buf) error handler is realized by catching
    // any PgError out of the per-iteration work: we run the cleanup block and
    // then WaitLatch(NULL, WL_TIMEOUT|WL_EXIT_ON_PM_DEATH, 10000, ERROR) before
    // resuming the loop. The one-time pre-loop setup above is not retried,
    // matching C (setjmp is armed only after that setup).

    // Fetch information about previous progress from shared memory, and ask
    // GetOldestUnsummarizedLSN to reset pending_lsn to summarized_lsn.
    let (clsn, ctli, cexact) = GetOldestUnsummarizedLSN(true, true)?;
    current_lsn = clsn;
    current_tli = ctli.unwrap_or(0);
    exact = cexact.unwrap_or(false);
    if XLogRecPtrIsInvalid(current_lsn) {
        ipc::proc_exit::call(0, initsmall::my_proc_pid::call());
    }

    // Loop forever.
    loop {
        match wal_summarizer_main_iteration(
            &mut current_lsn,
            &mut current_tli,
            &mut exact,
            &mut switch_lsn,
            &mut switch_tli,
        ) {
            Ok(()) => {}
            Err(err) => {
                // --- sigsetjmp error handler (walsummarizer.c:278-325) ---
                error_recovery_cleanup(&err)?;
                // Sleep 10 seconds before resuming to avoid excessive logging.
                latch::wait_latch_no_latch::call(
                    WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                    10000,
                    WAIT_EVENT_WAL_SUMMARIZER_ERROR,
                )?;
            }
        }
    }
}

/// The `sigsetjmp(local_sigjmp_buf)` cleanup block (walsummarizer.c:279-309):
/// reset the error stack by hand, hold interrupts, emit the error report,
/// release resources, flush error state, reset the work context, resume
/// interrupts.
fn error_recovery_cleanup(err: &PgError) -> PgResult<()> {
    // HOLD_INTERRUPTS(): prevent interrupts while cleaning up.
    miscinit::hold_interrupts::call();

    // EmitErrorReport(): report the error to the server log.
    ::utils_error::emit_error_report_for(err);

    // Release resources we might have acquired.
    let _ = lwlock::lwlock_release_all::call();
    let _ = cv::condition_variable_cancel_sleep::call();
    waitevent::pgstat_report_wait_end::call();
    aio::pgaio_error_cleanup::call();
    resowner::release_aux_process_resources::call(false)?;
    fd::at_eoxact_files::call(false);
    dynahash::at_eoxact_hash_tables::call(false);

    // FlushErrorState() + MemoryContextReset of the work context are handled by
    // the host (the work context lifetime is owned at the entry point); here
    // we have nothing further to reset.
    ::utils_error::FlushErrorState();

    // RESUME_INTERRUPTS().
    miscinit::resume_interrupts::call();
    Ok(())
}

/// One pass of `WalSummarizerMain`'s `for (;;)` loop body (walsummarizer.c:351-443).
fn wal_summarizer_main_iteration(
    current_lsn: &mut XLogRecPtr,
    current_tli: &mut TimeLineID,
    exact: &mut bool,
    switch_lsn: &mut XLogRecPtr,
    switch_tli: &mut TimeLineID,
) -> PgResult<()> {
    // MemoryContextReset of the work context is owned by the host.

    // Process any signals received recently.
    ProcessWalSummarizerInterrupts()?;

    // If it's time to remove any old WAL summaries, do that now.
    MaybeRemoveOldWalSummaries()?;

    // Find the LSN and TLI up to which we can safely summarize.
    let (latest_lsn, latest_tli) = GetLatestLSN();

    // If we're summarizing a historic timeline and haven't yet computed the
    // point at which to switch to the next timeline, do that now.
    if *current_tli != latest_tli && XLogRecPtrIsInvalid(*switch_lsn) {
        with_top_mcx(|mcx| {
            let tles = timeline::read_timeline_history::call(mcx, latest_tli)?;
            let (sl, st) = timeline::tli_switch_point::call(*current_tli, &tles)?;
            *switch_lsn = sl;
            *switch_tli = st;
            Ok(())
        })?;
        log_debug1(&format!(
            "switch point from TLI {} to TLI {} is at {}",
            *current_tli,
            *switch_tli,
            lsn_fmt(*switch_lsn)
        ))?;
    }

    // If we've reached the switch LSN, we can't summarize anything else on this
    // timeline. Switch to the next timeline and go around again.
    if !XLogRecPtrIsInvalid(*switch_lsn) && *current_lsn >= *switch_lsn {
        *current_tli = *switch_tli;
        *current_lsn = *switch_lsn;
        *switch_lsn = INVALID_XLOG_REC_PTR;
        *switch_tli = 0;

        lock_acquire(true)?;
        {
            let c = ctl();
            c.summarized_lsn = *current_lsn;
            c.summarized_tli = *current_tli;
            c.lsn_is_exact = true;
            c.pending_lsn = *current_lsn;
        }
        lock_release()?;
        return Ok(());
    }

    // Summarize WAL.
    let end_of_summary_lsn =
        SummarizeWAL(*current_tli, *current_lsn, *exact, *switch_lsn, latest_lsn)?;
    debug_assert!(!XLogRecPtrIsInvalid(end_of_summary_lsn));
    debug_assert!(end_of_summary_lsn >= *current_lsn);

    // Update state for next loop iteration.
    *current_lsn = end_of_summary_lsn;
    *exact = true;

    lock_acquire(true)?;
    {
        let c = ctl();
        c.summarized_lsn = end_of_summary_lsn;
        c.summarized_tli = *current_tli;
        c.lsn_is_exact = true;
        c.pending_lsn = end_of_summary_lsn;
    }
    lock_release()?;

    // Wake up anyone waiting for more summary files to be written.
    cv::condition_variable_broadcast::call(&ctl().summary_file_cv);
    Ok(())
}

/// `on_shmem_exit` adapter for [`WalSummarizerShutdown`].
fn wal_summarizer_shutdown_callback(_code: i32, _arg: types_tuple::Datum<'static>) -> PgResult<()> {
    WalSummarizerShutdown()
}

#[inline]
fn types_datum_unit() -> types_tuple::Datum<'static> {
    // C: `(Datum) 0`.
    types_tuple::Datum::null()
}

// ===========================================================================
// 4. GetWalSummarizerState
// ===========================================================================

/// Information returned by [`GetWalSummarizerState`].
pub struct WalSummarizerState {
    pub summarized_tli: TimeLineID,
    pub summarized_lsn: XLogRecPtr,
    pub pending_lsn: XLogRecPtr,
    pub summarizer_pid: i32,
}

/// `GetWalSummarizerState` -- get information about the state of the WAL summarizer.
pub fn GetWalSummarizerState() -> PgResult<WalSummarizerState> {
    lock_acquire(false)?;

    let result;
    if !ctl().initialized {
        // If initialized is false, the rest of the structure is undefined.
        result = WalSummarizerState {
            summarized_tli: 0,
            summarized_lsn: INVALID_XLOG_REC_PTR,
            pending_lsn: INVALID_XLOG_REC_PTR,
            summarizer_pid: -1,
        };
    } else {
        let summarizer_pgprocno = ctl().summarizer_pgprocno;
        let summarized_tli = ctl().summarized_tli;
        let summarized_lsn = ctl().summarized_lsn;

        if summarizer_pgprocno == INVALID_PROC_NUMBER {
            // The summarizer has exited; processing beyond summarized_lsn is
            // irrelevant now.
            result = WalSummarizerState {
                summarized_tli,
                summarized_lsn,
                pending_lsn: summarized_lsn,
                summarizer_pid: -1,
            };
        } else {
            let pending_lsn = ctl().pending_lsn;
            // We don't fuss over inexact answers; normalize invalid PIDs to -1.
            let mut summarizer_pid = procarray::proc_number_get_proc_pid::call(summarizer_pgprocno);
            if summarizer_pid <= 0 {
                summarizer_pid = -1;
            }
            result = WalSummarizerState {
                summarized_tli,
                summarized_lsn,
                pending_lsn,
                summarizer_pid,
            };
        }
    }
    lock_release()?;
    Ok(result)
}

// ===========================================================================
// 5. GetOldestUnsummarizedLSN
// ===========================================================================

/// `GetOldestUnsummarizedLSN` -- oldest LSN not yet summarized; updates shmem.
///
/// `want_tli`/`want_exact` mirror the non-NULL `tli`/`lsn_is_exact` out
/// pointers in C. Returns `(lsn, tli_opt, lsn_is_exact_opt)`.
pub fn GetOldestUnsummarizedLSN(
    want_tli: bool,
    want_exact: bool,
) -> PgResult<(XLogRecPtr, Option<TimeLineID>, Option<bool>)> {
    let mut unsummarized_lsn: XLogRecPtr = INVALID_XLOG_REC_PTR;
    let mut unsummarized_tli: TimeLineID = 0;
    let mut should_make_exact = false;
    let am_wal_summarizer = miscinit::am_wal_summarizer_process::call();

    let mut out_tli: Option<TimeLineID> = None;
    let mut out_exact: Option<bool> = None;

    // If not summarizing WAL, do nothing.
    if !summarize_wal_enabled() {
        return Ok((INVALID_XLOG_REC_PTR, None, None));
    }

    // If we are not the WAL summarizer, normally just read shmem. As an
    // exception, if shmem isn't initialized yet, initialize it so we read legal
    // values and don't remove any WAL too early.
    if !am_wal_summarizer {
        lock_acquire(false)?;
        if ctl().initialized {
            unsummarized_lsn = ctl().summarized_lsn;
            if want_tli {
                out_tli = Some(ctl().summarized_tli);
            }
            if want_exact {
                out_exact = Some(ctl().lsn_is_exact);
            }
            lock_release()?;
            return Ok((unsummarized_lsn, out_tli, out_exact));
        }
        lock_release()?;
    }

    // Find the oldest timeline on which WAL still exists, and the earliest
    // segment for which it exists.
    let (_lsn, latest_tli) = GetLatestLSN();
    let wal_segment_size = xlog::wal_segment_size::call();
    with_top_mcx(|mcx| {
        let tles = timeline::read_timeline_history::call(mcx, latest_tli)?;
        let mut n = tles.len() as isize - 1;
        while n >= 0 {
            let tle = tles[n as usize];
            let oldest_segno: XLogSegNo = xlog::xlog_get_oldest_segno::call(tle.tli);
            if oldest_segno != 0 {
                unsummarized_lsn = XLogSegNoOffsetToRecPtr(oldest_segno, 0, wal_segment_size);
                unsummarized_tli = tle.tli;
                break;
            }
            n -= 1;
        }
        Ok(())
    })?;

    // Don't summarize anything older than the end LSN of the newest summary
    // file that exists for this timeline.
    with_top_mcx(|mcx| {
        let existing_summaries = walsummary::get_wal_summaries::call(
            mcx,
            unsummarized_tli,
            INVALID_XLOG_REC_PTR,
            INVALID_XLOG_REC_PTR,
        )?;
        for ws in existing_summaries.iter() {
            if ws.end_lsn > unsummarized_lsn {
                unsummarized_lsn = ws.end_lsn;
                should_make_exact = true;
            }
        }
        Ok(())
    })?;

    // It really should not be possible to find no WAL.
    if unsummarized_tli == 0 {
        return ereport(ERROR)
            .errcode(ERRCODE_INTERNAL_ERROR)
            .errmsg_internal(format!("no WAL found on timeline {latest_tli}"))
            .finish(loc("GetOldestUnsummarizedLSN"))
            .map(|_| (INVALID_XLOG_REC_PTR, None, None));
    }

    // If we're the WAL summarizer, store the computed values into shmem (they
    // are authoritative). Otherwise only store if shmem is uninitialized.
    lock_acquire(true)?;
    if am_wal_summarizer || !ctl().initialized {
        let c = ctl();
        c.initialized = true;
        c.summarized_lsn = unsummarized_lsn;
        c.summarized_tli = unsummarized_tli;
        c.lsn_is_exact = should_make_exact;
        c.pending_lsn = unsummarized_lsn;
    } else {
        unsummarized_lsn = ctl().summarized_lsn;
    }

    if want_tli {
        out_tli = Some(ctl().summarized_tli);
    }
    if want_exact {
        out_exact = Some(ctl().lsn_is_exact);
    }
    lock_release()?;

    Ok((unsummarized_lsn, out_tli, out_exact))
}

/// `XLogSegNoOffsetToRecPtr(segno, offset, wal_segsz_bytes, dest)`
/// (`access/xlog_internal.h`): `dest = segno * wal_segsz + offset`.
#[inline]
fn XLogSegNoOffsetToRecPtr(segno: XLogSegNo, offset: u32, wal_segsz_bytes: i32) -> XLogRecPtr {
    segno
        .wrapping_mul(wal_segsz_bytes as u64)
        .wrapping_add(offset as u64)
}

// ===========================================================================
// 6. WakeupWalSummarizer
// ===========================================================================

/// `WakeupWalSummarizer` -- wake up the WAL summarizer process.
pub fn WakeupWalSummarizer() -> PgResult<()> {
    if WAL_SUMMARIZER_CTL.with(Cell::get).is_null() {
        return Ok(());
    }

    lock_acquire(false)?;
    let pgprocno = ctl().summarizer_pgprocno;
    lock_release()?;

    if pgprocno != INVALID_PROC_NUMBER {
        latch::set_latch_by_proc_number::call(pgprocno);
    }
    Ok(())
}

// ===========================================================================
// 7. WaitForWalSummarization
// ===========================================================================

/// `WaitForWalSummarization` -- wait until summarization reaches `lsn`, or time
/// out with an error if the summarizer seems stuck.
pub fn WaitForWalSummarization(lsn: XLogRecPtr) -> PgResult<()> {
    let mut prior_pending_lsn: XLogRecPtr = INVALID_XLOG_REC_PTR;
    let mut deadcycles: i32 = 0;

    let initial_time: TimestampTz = timestamp::get_current_timestamp::call();
    let mut cycle_time: TimestampTz = initial_time;

    loop {
        let mut timeout_in_ms: i64 = 10000;

        // CHECK_FOR_INTERRUPTS(): a cancel/terminate surfaces as Err.
        postgres::check_for_interrupts::call()?;

        // If WAL summarization is disabled while we wait, give up.
        if !summarize_wal_enabled() {
            return Ok(());
        }

        // If the LSN summarized on disk has reached the target value, stop.
        lock_acquire(false)?;
        let summarized_lsn = ctl().summarized_lsn;
        let pending_lsn = ctl().pending_lsn;
        lock_release()?;

        if summarized_lsn >= lsn {
            break;
        }

        let current_time: TimestampTz = timestamp::get_current_timestamp::call();

        // Have we finished the current cycle of waiting?
        if TimestampDifferenceMilliseconds(cycle_time, current_time) >= timeout_in_ms {
            cycle_time = TimestampTzPlusMilliseconds(cycle_time, timeout_in_ms);

            if pending_lsn > prior_pending_lsn {
                prior_pending_lsn = pending_lsn;
                deadcycles = 0;
            } else {
                deadcycles += 1;
            }

            // A full minute without absorbing a single WAL record => error.
            if deadcycles >= 6 {
                return ereport(ERROR)
                    .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    .errmsg("WAL summarization is not progressing")
                    .errdetail(format!(
                        "Summarization is needed through {}, but is stuck at {} on disk and {} in memory.",
                        lsn_fmt(lsn),
                        lsn_fmt(summarized_lsn),
                        lsn_fmt(pending_lsn)
                    ))
                    .finish(loc("WaitForWalSummarization"));
            }

            // Otherwise, let the user know what's happening.
            let elapsed_seconds =
                TimestampDifferenceMilliseconds(initial_time, current_time) / 1000;
            ereport(WARNING)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg_plural(
                    format!(
                        "still waiting for WAL summarization through {} after {} second",
                        lsn_fmt(lsn),
                        elapsed_seconds
                    ),
                    format!(
                        "still waiting for WAL summarization through {} after {} seconds",
                        lsn_fmt(lsn),
                        elapsed_seconds
                    ),
                    elapsed_seconds as u64,
                )
                .errdetail(format!(
                    "Summarization has reached {} on disk and {} in memory.",
                    lsn_fmt(summarized_lsn),
                    lsn_fmt(pending_lsn)
                ))
                .finish(loc("WaitForWalSummarization"))?;
        }

        // Align the wait time to prevent drift.
        timeout_in_ms -= TimestampDifferenceMilliseconds(cycle_time, current_time);

        // Wait and see.
        cv::condition_variable_timed_sleep::call(
            &ctl().summary_file_cv,
            timeout_in_ms,
            WAIT_EVENT_WAL_SUMMARIZER_WAL,
        )?;
    }

    cv::condition_variable_cancel_sleep::call();
    Ok(())
}

/// `TimestampDifferenceMilliseconds(start_time, stop_time)` (timestamp.c):
/// difference in whole milliseconds, clamped to a C `long`, never negative.
fn TimestampDifferenceMilliseconds(start_time: TimestampTz, stop_time: TimestampTz) -> i64 {
    let diff = stop_time.wrapping_sub(start_time);
    if diff <= 0 {
        0
    } else {
        ((diff as i128 + 999) / 1000).min(i64::MAX as i128) as i64
    }
}

/// `TimestampTzPlusMilliseconds(tz, ms)` (utils/timestamp.h).
#[inline]
fn TimestampTzPlusMilliseconds(tz: TimestampTz, ms: i64) -> TimestampTz {
    tz.wrapping_add(ms.wrapping_mul(1000))
}

// ===========================================================================
// 8. WalSummarizerShutdown
// ===========================================================================

/// `WalSummarizerShutdown` -- on exit, mark that we're no longer running
/// (`on_shmem_exit` callback).
pub fn WalSummarizerShutdown() -> PgResult<()> {
    lock_acquire(true)?;
    ctl().summarizer_pgprocno = INVALID_PROC_NUMBER;
    lock_release()?;
    Ok(())
}

// ===========================================================================
// 9. GetLatestLSN
// ===========================================================================

/// `GetLatestLSN` -- latest LSN eligible to be summarized, with `*tli` set to
/// the corresponding timeline. Returns `(lsn, tli)`.
fn GetLatestLSN() -> (XLogRecPtr, TimeLineID) {
    if !xlog::recovery_in_progress::call() {
        // Don't summarize WAL before it's flushed.
        return xlog::get_flush_rec_ptr::call();
    }

    // After the insert TLI is set and before the control file shows the DB in
    // production, RecoveryInProgress() returns true: summarize up to where
    // replay stopped, then prepare to resume at the start of the insert TLI.
    let insert_tli = xlog::get_wal_insertion_timeline_if_set::call();
    if insert_tli != 0 {
        let (replay_lsn, _tli) = xlogrecovery::get_xlog_replay_rec_ptr::call();
        return (replay_lsn, insert_tli);
    }

    // Use the WAL receiver's flush position or the replay position, whichever
    // is further ahead.
    let (flush_lsn, flush_tli) = walreceiver::get_wal_rcv_flush_rec_ptr::call();
    let (replay_lsn, replay_tli) = xlogrecovery::get_xlog_replay_rec_ptr::call();
    if flush_lsn > replay_lsn {
        (flush_lsn, flush_tli)
    } else {
        (replay_lsn, replay_tli)
    }
}

// ===========================================================================
// 10. ProcessWalSummarizerInterrupts
// ===========================================================================

/// `ProcessWalSummarizerInterrupts` -- interrupt handler for the main loop.
fn ProcessWalSummarizerInterrupts() -> PgResult<()> {
    if procsignal::proc_signal_barrier_pending::call() {
        procsignal::process_proc_signal_barrier::call()?;
    }

    if interrupt::ConfigReloadPending() {
        interrupt::SetConfigReloadPending(false);
        gucfile::process_config_file::call(types_guc::GucContext::PGC_SIGHUP)?;
    }

    if interrupt::ShutdownRequestPending() || !summarize_wal_enabled() {
        log_debug1("WAL summarizer shutting down")?;
        ipc::proc_exit::call(0, initsmall::my_proc_pid::call());
    }

    // Perform logging of memory contexts of this process.
    if mcxt::log_memory_context_pending::call() {
        mcxt::process_log_memory_context_interrupt::call()?;
    }
    Ok(())
}

// ===========================================================================
// 11. SummarizeWAL
// ===========================================================================

/// `SummarizeWAL` -- summarize a range of WAL records on a single timeline.
/// Returns the LSN at which the WAL summary actually ends.
fn SummarizeWAL(
    tli: TimeLineID,
    start_lsn: XLogRecPtr,
    exact: bool,
    switch_lsn: XLogRecPtr,
    maximum_lsn: XLogRecPtr,
) -> PgResult<XLogRecPtr> {
    with_top_mcx(|mcx| summarize_wal_in(mcx, tli, start_lsn, exact, switch_lsn, maximum_lsn))
}

fn summarize_wal_in(
    mcx: Mcx<'_>,
    tli: TimeLineID,
    start_lsn: XLogRecPtr,
    exact: bool,
    mut switch_lsn: XLogRecPtr,
    maximum_lsn: XLogRecPtr,
) -> PgResult<XLogRecPtr> {
    let summary_start_lsn: XLogRecPtr;
    let mut summary_end_lsn: XLogRecPtr = switch_lsn;
    let mut fast_forward = true;

    let mut brtab: BlockRefTable = blkreftable::create_empty_block_ref_table(mcx)?;

    // Initialize private data for xlogreader + create xlogreader.
    let historic = !XLogRecPtrIsInvalid(switch_lsn);
    let wal_segment_size = xlog::wal_segment_size::call();
    let xlr: XLogReaderHandle =
        xlogreader::summarizer_xlogreader_allocate::call(wal_segment_size, summarizer_read_local_xlog_page)?;
    WS_PRIVATE.with(|m| {
        m.borrow_mut().insert(
            xlr,
            SummarizerPrivate { tli, historic, read_upto: maximum_lsn, end_of_wal: false },
        );
    });

    // Wrap the body so we always free the reader + private data, mirroring the
    // C unconditional `pfree(private_data); XLogReaderFree(xlogreader)` after
    // the loop (and the explicit free on the early-error path).
    let body = summarize_wal_body(
        mcx,
        xlr,
        tli,
        start_lsn,
        exact,
        &mut switch_lsn,
        &mut summary_end_lsn,
        &mut fast_forward,
        &mut brtab,
    );

    xlogreader::summarizer_xlogreader_free::call(xlr);
    WS_PRIVATE.with(|m| {
        m.borrow_mut().remove(&xlr);
    });

    summary_start_lsn = body?;

    // If a timeline switch occurs, we may make no progress before exiting the
    // loop; then we don't write a summary file. We also skip writing in
    // fast-forward mode.
    if summary_end_lsn > summary_start_lsn && !fast_forward {
        let temp_path = truncate_path(format!("{XLOGDIR}/summaries/temp.summary"));
        let final_path = truncate_path(format!(
            "{}/summaries/{:08X}{:08X}{:08X}{:08X}{:08X}.summary",
            XLOGDIR,
            tli,
            (summary_start_lsn >> 32) as u32,
            summary_start_lsn as u32,
            (summary_end_lsn >> 32) as u32,
            summary_end_lsn as u32
        ));

        // The idiomatic blkreftable port returns the serialized bytes from
        // WriteBlockRefTable rather than streaming via WriteWalSummary.
        let bytes = blkreftable::write_block_ref_table(mcx, &brtab)?;
        walsummary::write_wal_summary_file::call(&temp_path, &final_path, &bytes)?;

        log_debug1(&format!(
            "summarized WAL on TLI {} from {} to {}",
            tli,
            lsn_fmt(summary_start_lsn),
            lsn_fmt(summary_end_lsn)
        ))?;
    }

    // If we skipped a non-zero amount of WAL, log a debug message.
    if summary_end_lsn > summary_start_lsn && fast_forward {
        log_debug1(&format!(
            "skipped summarizing WAL on TLI {} from {} to {}",
            tli,
            lsn_fmt(summary_start_lsn),
            lsn_fmt(summary_end_lsn)
        ))?;
    }

    Ok(summary_end_lsn)
}

/// The setup-and-loop body of `SummarizeWAL` (walsummarizer.c:935-1186), up to
/// just before the reader is freed. Returns `summary_start_lsn`.
#[allow(clippy::too_many_arguments)]
fn summarize_wal_body(
    mcx: Mcx<'_>,
    xlr: XLogReaderHandle,
    tli: TimeLineID,
    start_lsn: XLogRecPtr,
    exact: bool,
    switch_lsn: &mut XLogRecPtr,
    summary_end_lsn: &mut XLogRecPtr,
    fast_forward: &mut bool,
    brtab: &mut BlockRefTable,
) -> PgResult<XLogRecPtr> {
    let summary_start_lsn: XLogRecPtr;

    // When exact = false, search forward for the start of the next record.
    if exact {
        // Even if start_lsn is the beginning of a page rather than the first
        // record on it, use it as the start LSN for the summary file.
        xlogreader::summarizer_xlog_begin_read::call(xlr, start_lsn);
        summary_start_lsn = start_lsn;
    } else {
        let found = xlogreader::summarizer_xlog_find_next_record::call(xlr, start_lsn)?;
        if XLogRecPtrIsInvalid(found) {
            // Hit end-of-WAL while searching: a historic timeline with no valid
            // records after start_lsn and before end of WAL.
            if ws_private_get(xlr, "summarize_wal", |p| p.end_of_wal) {
                log_debug1(&format!(
                    "could not read WAL from timeline {} at {}: end of WAL at {}",
                    tli,
                    lsn_fmt(start_lsn),
                    lsn_fmt(ws_private_get(xlr, "summarize_wal", |p| p.read_upto))
                ))?;
                summary_start_lsn = start_lsn;
                *summary_end_lsn = ws_private_get(xlr, "summarize_wal", |p| p.read_upto);
                *switch_lsn = xlogreader::summarizer_reader_end_rec_ptr::call(xlr);
            } else {
                return ereport(ERROR)
                    .errmsg(format!(
                        "could not find a valid record after {}",
                        lsn_fmt(start_lsn)
                    ))
                    .finish(loc("SummarizeWAL"))
                    .map(|_| INVALID_XLOG_REC_PTR);
            }
        } else {
            summary_start_lsn = found;
        }

        // We shouldn't go backward.
        debug_assert!(summary_start_lsn >= start_lsn);
    }

    // Main loop: read xlog records one by one.
    summarize_wal_loop(mcx, xlr, tli, summary_start_lsn, switch_lsn, summary_end_lsn, fast_forward, brtab)?;

    Ok(summary_start_lsn)
}

/// The `while (1)` body of `SummarizeWAL` (walsummarizer.c:1014-1186).
#[allow(clippy::too_many_arguments)]
fn summarize_wal_loop(
    mcx: Mcx<'_>,
    xlr: XLogReaderHandle,
    tli: TimeLineID,
    summary_start_lsn: XLogRecPtr,
    switch_lsn: &mut XLogRecPtr,
    summary_end_lsn: &mut XLogRecPtr,
    fast_forward: &mut bool,
    brtab: &mut BlockRefTable,
) -> PgResult<()> {
    loop {
        ProcessWalSummarizerInterrupts()?;

        debug_assert!(summary_start_lsn <= xlogreader::summarizer_reader_end_rec_ptr::call(xlr));

        // Read the next record.
        match xlogreader::summarizer_xlog_read_record::call(xlr)? {
            ReadRecordResult::Record => {}
            ReadRecordResult::EndOfWal => {
                // This timeline must be historic and end before a complete
                // record could be read.
                log_debug1(&format!(
                    "could not read WAL from timeline {} at {}: end of WAL at {}",
                    tli,
                    lsn_fmt(xlogreader::summarizer_reader_end_rec_ptr::call(xlr)),
                    lsn_fmt(ws_private_get(xlr, "summarize_wal_loop", |p| p.read_upto))
                ))?;
                *summary_end_lsn = ws_private_get(xlr, "summarize_wal_loop", |p| p.read_upto);
                break;
            }
            ReadRecordResult::Error { errormsg } => {
                return if let Some(msg) = errormsg {
                    ereport(ERROR)
                        .errcode_for_file_access()
                        .errmsg(format!(
                            "could not read WAL from timeline {} at {}: {}",
                            tli,
                            lsn_fmt(xlogreader::summarizer_reader_end_rec_ptr::call(xlr)),
                            msg
                        ))
                        .finish(loc("SummarizeWAL"))
                } else {
                    ereport(ERROR)
                        .errcode_for_file_access()
                        .errmsg(format!(
                            "could not read WAL from timeline {} at {}",
                            tli,
                            lsn_fmt(xlogreader::summarizer_reader_end_rec_ptr::call(xlr))
                        ))
                        .finish(loc("SummarizeWAL"))
                };
            }
        }

        debug_assert!(summary_start_lsn <= xlogreader::summarizer_reader_end_rec_ptr::call(xlr));

        if !XLogRecPtrIsInvalid(*switch_lsn)
            && xlogreader::summarizer_reader_read_rec_ptr::call(xlr) >= *switch_lsn
        {
            // We've read a record that *starts* after the switch LSN; pretend we
            // didn't by bailing out here.
            *summary_end_lsn = *switch_lsn;
            break;
        }

        // Certain record types require special handling.
        let rmid = xlogreader::summarizer_rec_get_rmid::call(xlr);
        if rmid == RM_XLOG_ID {
            // If we've already processed some records when we hit a redo point
            // or shutdown checkpoint, stop summarization before this record.
            if let Some(new_fast_forward) = SummarizeXlogRecord(xlr)? {
                if xlogreader::summarizer_reader_read_rec_ptr::call(xlr) > summary_start_lsn {
                    *summary_end_lsn = xlogreader::summarizer_reader_read_rec_ptr::call(xlr);
                    break;
                } else {
                    *fast_forward = new_fast_forward;
                }
            }
        } else if !*fast_forward {
            // Record types that require extra block-reference-table updates.
            match rmid {
                RM_DBASE_ID => SummarizeDbaseRecord(xlr, &mut *brtab)?,
                RM_SMGR_ID => SummarizeSmgrRecord(xlr, &mut *brtab)?,
                RM_XACT_ID => SummarizeXactRecord(mcx, xlr, &mut *brtab)?,
                _ => {}
            }
        }

        // Feed block references from the record into the block reference table
        // (unless fast-forwarding).
        if !*fast_forward {
            let max_block_id = xlogreader::summarizer_rec_max_block_id::call(xlr);
            let mut block_id: i32 = 0;
            while block_id <= max_block_id {
                if let Some(BlockTag { rlocator, forknum, blocknum }) =
                    xlogreader::summarizer_rec_get_block_tag_extended::call(xlr, block_id)
                {
                    // Ignore the FSM fork, which is not fully WAL-logged.
                    if forknum != FSM_FORKNUM {
                        blkreftable::block_ref_table_mark_block_modified(
                            &mut *brtab, rlocator, forknum, blocknum,
                        )?;
                    }
                }
                block_id += 1;
            }
        }

        // Update our notion of where this summary file ends.
        *summary_end_lsn = xlogreader::summarizer_reader_end_rec_ptr::call(xlr);

        // Also update shared memory.
        lock_acquire(true)?;
        debug_assert!(*summary_end_lsn >= ctl().summarized_lsn);
        ctl().pending_lsn = *summary_end_lsn;
        lock_release()?;

        // If we have a switch LSN and have reached it, stop before reading the
        // next record.
        if !XLogRecPtrIsInvalid(*switch_lsn)
            && xlogreader::summarizer_reader_end_rec_ptr::call(xlr) >= *switch_lsn
        {
            break;
        }
    }
    Ok(())
}

/// `snprintf(buf, MAXPGPATH, ...)` truncation behavior.
fn truncate_path(mut s: String) -> String {
    if s.len() >= MAXPGPATH {
        s.truncate(MAXPGPATH - 1);
    }
    s
}

// ===========================================================================
// 12. SummarizeDbaseRecord
// ===========================================================================

/// `SummarizeDbaseRecord` -- special handling for WAL records with RM_DBASE_ID.
fn SummarizeDbaseRecord(xlr: XLogReaderHandle, brtab: &mut BlockRefTable) -> PgResult<()> {
    let info = xlogreader::summarizer_rec_get_info::call(xlr) & !XLR_INFO_MASK;
    let data = xlogreader::summarizer_rec_get_data::call(xlr);

    // relfilenode zero for a (db OID, tablespace OID) pair means all relations
    // with that pair were recreated.
    if info == XLOG_DBASE_CREATE_FILE_COPY {
        // xl_dbase_create_file_copy_rec { db_id: Oid, tablespace_id: Oid, ... }
        let db_id = read_oid(&data, 0);
        let tablespace_id = read_oid(&data, 4);
        let rlocator = rlocator(tablespace_id, db_id, 0);
        blkreftable::block_ref_table_set_limit_block(&mut *brtab,rlocator, MAIN_FORKNUM, 0)?;
    } else if info == XLOG_DBASE_CREATE_WAL_LOG {
        // xl_dbase_create_wal_log_rec { db_id: Oid, tablespace_id: Oid }
        let db_id = read_oid(&data, 0);
        let tablespace_id = read_oid(&data, 4);
        let rlocator = rlocator(tablespace_id, db_id, 0);
        blkreftable::block_ref_table_set_limit_block(&mut *brtab,rlocator, MAIN_FORKNUM, 0)?;
    } else if info == XLOG_DBASE_DROP {
        // xl_dbase_drop_rec { db_id: Oid, ntablespaces: int, tablespace_ids[] }
        let db_id = read_oid(&data, 0);
        let ntablespaces = read_i32(&data, 4);
        // tablespace_ids[] follows the fixed head at offset 8.
        for i in 0..ntablespaces {
            let spc_oid = read_oid(&data, 8 + (i as usize) * 4);
            let rl = rlocator(spc_oid, db_id, 0);
            blkreftable::block_ref_table_set_limit_block(&mut *brtab,rl, MAIN_FORKNUM, 0)?;
        }
    }
    Ok(())
}

// ===========================================================================
// 13. SummarizeSmgrRecord
// ===========================================================================

/// `SummarizeSmgrRecord` -- special handling for WAL records with RM_SMGR_ID.
fn SummarizeSmgrRecord(xlr: XLogReaderHandle, brtab: &mut BlockRefTable) -> PgResult<()> {
    let info = xlogreader::summarizer_rec_get_info::call(xlr) & !XLR_INFO_MASK;
    let data = xlogreader::summarizer_rec_get_data::call(xlr);

    if info == XLOG_SMGR_CREATE {
        // xl_smgr_create { rlocator: RelFileLocator (12), forkNum: ForkNumber }
        let rl = read_rlocator(&data, 0);
        let fork_num = read_forknum(&data, 12);

        // A new fork on disk: no point tracking which blocks were modified.
        // Ignore the FSM fork.
        if fork_num != FSM_FORKNUM {
            blkreftable::block_ref_table_set_limit_block(&mut *brtab,rl, fork_num, 0)?;
        }
    } else if info == XLOG_SMGR_TRUNCATE {
        // xl_smgr_truncate { blkno: BlockNumber (4), rlocator: RelFileLocator
        // (12, offset 4), flags: int (offset 16) }
        let blkno = read_u32(&data, 0);
        let rl = read_rlocator(&data, 4);
        let flags = read_i32(&data, 16);

        // Truncated fork: no point tracking beyond the truncation point. Ignore
        // SMGR_TRUNCATE_FSM.
        if (flags & SMGR_TRUNCATE_HEAP) != 0 {
            blkreftable::block_ref_table_set_limit_block(&mut *brtab,rl, MAIN_FORKNUM, blkno)?;
        }
        if (flags & SMGR_TRUNCATE_VM) != 0 {
            blkreftable::block_ref_table_set_limit_block(&mut *brtab,rl, VISIBILITYMAP_FORKNUM, blkno)?;
        }
    }
    Ok(())
}

// ===========================================================================
// 14. SummarizeXactRecord
// ===========================================================================

/// `SummarizeXactRecord` -- special handling for WAL records with RM_XACT_ID.
fn SummarizeXactRecord(mcx: Mcx<'_>, xlr: XLogReaderHandle, brtab: &mut BlockRefTable) -> PgResult<()> {
    let info = xlogreader::summarizer_rec_get_info::call(xlr) & !XLR_INFO_MASK;
    let xact_info = info & XLOG_XACT_OPMASK;
    let raw_info = xlogreader::summarizer_rec_get_info::call(xlr);
    let data = xlogreader::summarizer_rec_get_data::call(xlr);

    if xact_info == XLOG_XACT_COMMIT || xact_info == XLOG_XACT_COMMIT_PREPARED {
        // Don't track modified blocks for relations removed on commit.
        let xlocators = xactdesc::parse_commit_record::call(mcx, raw_info, &data)?;
        for xloc in xlocators.iter() {
            let mut forknum: ForkNumber = MAIN_FORKNUM;
            loop {
                if forknum != FSM_FORKNUM {
                    blkreftable::block_ref_table_set_limit_block(&mut *brtab,*xloc, forknum, 0)?;
                }
                if forknum == MAX_FORKNUM {
                    break;
                }
                forknum = next_forknum(forknum);
            }
        }
    } else if xact_info == XLOG_XACT_ABORT || xact_info == XLOG_XACT_ABORT_PREPARED {
        // Don't track modified blocks for relations removed on abort.
        let xlocators = xactdesc::parse_abort_record::call(mcx, raw_info, &data)?;
        for xloc in xlocators.iter() {
            let mut forknum: ForkNumber = MAIN_FORKNUM;
            loop {
                if forknum != FSM_FORKNUM {
                    blkreftable::block_ref_table_set_limit_block(&mut *brtab,*xloc, forknum, 0)?;
                }
                if forknum == MAX_FORKNUM {
                    break;
                }
                forknum = next_forknum(forknum);
            }
        }
    }
    Ok(())
}

/// Step `forknum` to the next `ForkNumber` value (`for (forknum = 0; forknum <=
/// MAX_FORKNUM; ++forknum)`; the C loop walks the enum's integer values).
#[inline]
fn next_forknum(f: ForkNumber) -> ForkNumber {
    match f {
        MAIN_FORKNUM => FSM_FORKNUM,
        FSM_FORKNUM => VISIBILITYMAP_FORKNUM,
        VISIBILITYMAP_FORKNUM => ForkNumber::INIT_FORKNUM,
        // MAX_FORKNUM (INIT_FORKNUM) is the loop terminator; never stepped past.
        other => other,
    }
}

// ===========================================================================
// 15. SummarizeXlogRecord
// ===========================================================================

/// `SummarizeXlogRecord` -- special handling for WAL records with RM_XLOG_ID.
///
/// `None` mirrors the C `return false` (no special handling); `Some(nff)`
/// mirrors `*new_fast_forward = nff; return true;`.
fn SummarizeXlogRecord(xlr: XLogReaderHandle) -> PgResult<Option<bool>> {
    let info = xlogreader::summarizer_rec_get_info::call(xlr) & !XLR_INFO_MASK;
    let data = xlogreader::summarizer_rec_get_data::call(xlr);
    let record_wal_level: i32;

    if info == XLOG_CHECKPOINT_REDO {
        // Payload is wal_level at the time the record was written.
        record_wal_level = read_i32(&data, 0);
    } else if info == XLOG_CHECKPOINT_SHUTDOWN {
        // CheckPoint: wal_level at offset 20 (redo:8, ThisTimeLineID:4,
        // PrevTimeLineID:4, fullPageWrites:bool+pad:4 -> 20).
        record_wal_level = read_i32(&data, 20);
    } else if info == XLOG_PARAMETER_CHANGE {
        // xl_parameter_change: wal_level is the 6th int (offset 20).
        record_wal_level = read_i32(&data, 20);
    } else if info == XLOG_END_OF_RECOVERY {
        // xl_end_of_recovery: wal_level at offset 16 (end_time:8,
        // ThisTimeLineID:4, PrevTimeLineID:4).
        record_wal_level = read_i32(&data, 16);
    } else {
        // No special handling required.
        return Ok(None);
    }

    // Redo can only begin at XLOG_CHECKPOINT_REDO/SHUTDOWN, so we want
    // summarization to begin there; END_OF_RECOVERY / PARAMETER_CHANGE also
    // reach here.
    Ok(Some(record_wal_level == WAL_LEVEL_MINIMAL))
}

// ===========================================================================
// 16. summarizer_read_local_xlog_page
// ===========================================================================

/// `summarizer_read_local_xlog_page` -- xlogreader page-read callback, limited
/// to one timeline. Returns the number of valid bytes (or -1 at end of a
/// historic timeline). Installed as the reader's `.page_read`.
pub fn summarizer_read_local_xlog_page(
    state: XLogReaderHandle,
    target_page_ptr: XLogRecPtr,
    req_len: i32,
    cur_page: &mut [u8],
) -> PgResult<i32> {
    let count: i32;

    ProcessWalSummarizerInterrupts()?;

    loop {
        let read_upto = ws_private_get(state, "page_read", |p| p.read_upto);
        if target_page_ptr + XLOG_BLCKSZ as u64 <= read_upto {
            // More than one block available; read only that block.
            count = XLOG_BLCKSZ;
            break;
        } else if target_page_ptr + req_len as u64 > read_upto {
            // Not enough data.
            if ws_private_get(state, "page_read", |p| p.historic) {
                // Historic timeline: there will never be more data.
                ws_private_get(state, "page_read", |p| p.end_of_wal = true);
                return Ok(-1);
            } else {
                // Current (or recently-current) timeline: more might show up.
                // Delay so we don't tight-loop.
                ProcessWalSummarizerInterrupts()?;
                summarizer_wait_for_wal()?;

                // Recheck end-of-WAL.
                let (latest_lsn, latest_tli) = GetLatestLSN();
                if ws_private_get(state, "page_read", |p| p.tli) == latest_tli {
                    // Still the current timeline; update max LSN.
                    debug_assert!(latest_lsn >= ws_private_get(state, "page_read", |p| p.read_upto));
                    ws_private_get(state, "page_read", |p| p.read_upto = latest_lsn);
                } else {
                    // No longer the latest timeline. Figure out when it ended.
                    let my_tli = ws_private_get(state, "page_read", |p| p.tli);
                    ws_private_get(state, "page_read", |p| p.historic = true);
                    let switchpoint = with_top_mcx(|mcx| {
                        let tles = timeline::read_timeline_history::call(mcx, latest_tli)?;
                        let (sp, _next) = timeline::tli_switch_point::call(my_tli, &tles)?;
                        Ok(sp)
                    })?;
                    // Allow reads up to exactly the switch point.
                    ws_private_get(state, "page_read", |p| p.read_upto = switchpoint);

                    log_debug1(&format!(
                        "timeline {} became historic, can read up to {}",
                        my_tli,
                        lsn_fmt(ws_private_get(state, "page_read", |p| p.read_upto))
                    ))?;
                }
                // Go around and try again.
            }
        } else {
            // Enough bytes available to satisfy the request.
            count = (read_upto - target_page_ptr) as i32;
            break;
        }
    }

    // WALRead(state, cur_page, targetPagePtr, count, tli, &errinfo); on failure
    // WALReadRaiseError(&errinfo).
    let tli = ws_private_get(state, "page_read", |p| p.tli);
    xlogreader::summarizer_wal_read::call(state, cur_page, target_page_ptr, count, tli)?;

    // Track that we read a page, for sleep time calculation.
    PAGES_READ_SINCE_LAST_SLEEP.with(|c| c.set(c.get() + 1));

    Ok(count)
}

// ===========================================================================
// 17. summarizer_wait_for_wal
// ===========================================================================

/// `summarizer_wait_for_wal` -- sleep long enough that more WAL is likely
/// available afterwards.
fn summarizer_wait_for_wal() -> PgResult<()> {
    let pages = PAGES_READ_SINCE_LAST_SLEEP.with(Cell::get);
    let sleep_quanta = SLEEP_QUANTA.with(Cell::get);
    if pages == 0 {
        // No pages read since last sleep: double the sleep time, capped.
        SLEEP_QUANTA.with(|c| c.set(core::cmp::min(sleep_quanta * 2, MAX_SLEEP_QUANTA)));
    } else if pages > 1 {
        // Multiple pages read: reduce by 1 quantum per page beyond the first.
        if pages > sleep_quanta - 1 {
            SLEEP_QUANTA.with(|c| c.set(1));
        } else {
            SLEEP_QUANTA.with(|c| c.set(sleep_quanta - pages));
        }
    }

    // Report pending statistics to the cumulative stats system.
    walstats::pgstat_report_wal::call(false)?;

    // OK, now sleep.
    latch::wait_latch_my_latch::call(
        WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
        SLEEP_QUANTA.with(Cell::get) * MS_PER_SLEEP_QUANTUM,
        WAIT_EVENT_WAL_SUMMARIZER_WAL,
    )?;
    latch::reset_latch_my_latch::call();

    // Reset count of pages read.
    PAGES_READ_SINCE_LAST_SLEEP.with(|c| c.set(0));
    Ok(())
}

// ===========================================================================
// 18. MaybeRemoveOldWalSummaries
// ===========================================================================

/// `MaybeRemoveOldWalSummaries` -- remove WAL summaries whose mtimes are older
/// than `wal_summary_keep_time`.
fn MaybeRemoveOldWalSummaries() -> PgResult<()> {
    let redo_pointer = xlog::get_redo_rec_ptr::call();

    // If WAL summary removal is disabled, do nothing.
    if wal_summary_keep_time() == 0 {
        return Ok(());
    }

    // If the redo pointer has not advanced, do nothing (only try once per
    // checkpoint cycle).
    if redo_pointer == REDO_POINTER_AT_LAST_SUMMARY_REMOVAL.with(Cell::get) {
        return Ok(());
    }
    REDO_POINTER_AT_LAST_SUMMARY_REMOVAL.with(|c| c.set(redo_pointer));

    // Files removable only if last-modification time precedes this cutoff.
    let cutoff_time: i64 =
        timestamp_time_now() - (wal_summary_keep_time() as i64) * SECS_PER_MINUTE;

    let wal_segment_size = xlog::wal_segment_size::call();

    with_top_mcx(|mcx| {
        // All the summaries that currently exist (owned snapshot so we can walk
        // and prune the residual list, matching C's foreach_delete_current).
        let initial = walsummary::get_wal_summaries::call(mcx, 0, INVALID_XLOG_REC_PTR, INVALID_XLOG_REC_PTR)?;
        let mut wslist: Vec<WalSummaryFile> = initial.iter().copied().collect();

        while !wslist.is_empty() {
            ProcessWalSummarizerInterrupts()?;

            // Pick a timeline that still has summary files, and find the oldest
            // LSN that still exists on disk for it.
            let selected_tli = wslist[0].tli;
            let oldest_segno = xlog::xlog_get_oldest_segno::call(selected_tli);
            let mut oldest_lsn: XLogRecPtr = INVALID_XLOG_REC_PTR;
            if oldest_segno != 0 {
                oldest_lsn = XLogSegNoOffsetToRecPtr(oldest_segno, 0, wal_segment_size);
            }

            // Consider each WAL file on the selected timeline; rebuild the
            // residual list of not-yet-considered entries (C: foreach +
            // foreach_delete_current).
            let mut remaining: Vec<WalSummaryFile> = Vec::new();
            for ws in wslist.iter() {
                ProcessWalSummarizerInterrupts()?;

                // Not on this timeline: not time to consider it.
                if selected_tli != ws.tli {
                    remaining.push(*ws);
                    continue;
                }

                // If the WAL is gone, remove it if its mtime is old enough.
                if XLogRecPtrIsInvalid(oldest_lsn) || ws.end_lsn <= oldest_lsn {
                    walsummary::remove_wal_summary_if_older_than::call(*ws, cutoff_time)?;
                }
                // Either way, need not consider it again.
            }
            wslist = remaining;
        }
        Ok(())
    })
}

/// `time(NULL)` (the cutoff base in `MaybeRemoveOldWalSummaries`): current
/// wall-clock seconds. C uses `time(NULL)`; here it is `GetCurrentTimestamp()`
/// (microseconds) converted to seconds.
fn timestamp_time_now() -> i64 {
    // GetCurrentTimestamp() is TimestampTz (microseconds since 2000-01-01);
    // C's time(NULL) is Unix seconds. The file modification times the owner
    // compares against are also Unix seconds, so the seam returns Unix seconds.
    timestamp::get_current_timestamp::call() / 1_000_000 + 946_684_800
}

// ---------------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------------

/// Run `f` with a fresh "Wal Summarizer" work context. C runs each summarizer
/// allocation in the per-process "Wal Summarizer" `AllocSetContext` that
/// `WalSummarizerMain` creates and `MemoryContextReset`s each loop iteration;
/// here each top-level work unit gets its own root context that drops (== the
/// C reset) when `f` returns. `SummarizeWAL` wraps its whole record-walk in one
/// such context so the block-reference table and all loop allocations share it.
fn with_top_mcx<R>(f: impl FnOnce(Mcx<'_>) -> PgResult<R>) -> PgResult<R> {
    let ctx = ::mcx::MemoryContext::new("Wal Summarizer");
    f(ctx.mcx())
}

/// `ereport(DEBUG1, errmsg_internal(...))` — a debug log line (does not
/// propagate; DEBUG1 is below ERROR).
fn log_debug1(msg: &str) -> PgResult<()> {
    ereport(DEBUG1)
        .errmsg_internal(msg.to_string())
        .finish(loc("walsummarizer"))
}

#[inline]
fn rlocator(spc: Oid, db: Oid, rel: Oid) -> RelFileLocator {
    RelFileLocator { spcOid: spc, dbOid: db, relNumber: rel }
}

// ---------------------------------------------------------------------------
// Little-endian byte readers for record payloads (the WAL is laid out in the
// host's native endianness; these mirror the C `memcpy`/struct field reads).
// ---------------------------------------------------------------------------

fn read_u32(data: &[u8], off: usize) -> u32 {
    let mut b = [0u8; 4];
    if off < data.len() {
        let end = (off + 4).min(data.len());
        b[..end - off].copy_from_slice(&data[off..end]);
    }
    u32::from_ne_bytes(b)
}

fn read_i32(data: &[u8], off: usize) -> i32 {
    read_u32(data, off) as i32
}

fn read_oid(data: &[u8], off: usize) -> Oid {
    read_u32(data, off)
}

fn read_forknum(data: &[u8], off: usize) -> ForkNumber {
    match read_i32(data, off) {
        0 => MAIN_FORKNUM,
        1 => FSM_FORKNUM,
        2 => VISIBILITYMAP_FORKNUM,
        3 => ForkNumber::INIT_FORKNUM,
        _ => ForkNumber::InvalidForkNumber,
    }
}

fn read_rlocator(data: &[u8], off: usize) -> RelFileLocator {
    let spc = read_oid(data, off);
    let db = read_oid(data, off + 4);
    let rel = read_oid(data, off + 8);
    rlocator(spc, db, rel)
}

// ---------------------------------------------------------------------------
// Inward seam: the child entry point invoked by postmaster_child_launch.
// ---------------------------------------------------------------------------

/// `wal_summarizer_main` adapter (`-> !`): run [`WalSummarizerMain`]; the
/// summarizer loops forever and only leaves via `proc_exit`, so a returned
/// `Ok` is unreachable and a top-level `Err` is a FATAL escaping with no
/// handler — re-thrown to the process exit.
fn wal_summarizer_main_entry(startup_data: &StartupData) -> ! {
    match WalSummarizerMain(startup_data) {
        Ok(()) => unreachable!("WalSummarizerMain returned Ok; it only exits via proc_exit"),
        Err(err) => {
            // A FATAL/unhandled error before the loop's setjmp is armed.
            ::utils_error::emit_error_report_for(&err);
            ipc::proc_exit::call(1, initsmall::my_proc_pid::call());
        }
    }
}

/// Install every seam this crate owns.
pub fn init_seams() {
    walsummarizer_seams::wal_summarizer_main::set(wal_summarizer_main_entry);
    // Pure-wiring install (assemble/seam-wiring-guard): owner body matches.
    walsummarizer_seams::wal_summarizer_shmem_init::set(WalSummarizerShmemInit);
    // Contract-reconciled install (assemble/seam-contract-reconciles): the seam
    // is now the infallible `-> Size` shape, matching the C `Size` return.
    walsummarizer_seams::wal_summarizer_shmem_size::set(WalSummarizerShmemSize);
    walsummarizer_seams::wait_for_wal_summarization::set(
        WaitForWalSummarization,
    );
    walsummarizer_seams::summarize_wal::set(summarize_wal_enabled);
    // `GetOldestUnsummarizedLSN(NULL, NULL)` as `KeepLogSeg` reads it.
    walsummarizer_seams::get_oldest_unsummarized_lsn::set(|| {
        Ok(GetOldestUnsummarizedLSN(false, false)?.0)
    });
    // `int wal_summary_keep_time` (walsummarizer.c GUC, boot
    // 10 * HOURS_PER_DAY * MINS_PER_HOUR) — install the guc-tables slot over
    // this crate's backing accessors.
    guc_tables::vars::wal_summary_keep_time.install(
        guc_tables::GucVarAccessors {
            get: wal_summary_keep_time,
            set: set_wal_summary_keep_time,
        },
    );
}
