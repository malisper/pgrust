//! walsummarizer.c: the WAL summarizer as a postmaster child thread (pgarch
//! precedent), plus the walsummary.c file helpers it consumes (GetWalSummaries,
//! RemoveWalSummaryIfOlderThan, WriteWalSummary; backend-backup-walsummary
//! stays non-core). GetWalRcvFlushRecPtr is InvalidXLogRecPtr (walreceiver
//! unported): C's max(flush, replay) reduces to the replay LSN.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use std::cell::Cell;
use std::sync::atomic::{
    AtomicBool, AtomicI32, AtomicU32, AtomicU64,
    Ordering::Relaxed,
};
use std::sync::OnceLock;

use condition_variable::{
    ConditionVariable, ConditionVariableBroadcast, ConditionVariableCancelSleep,
    ConditionVariableTimedSleep,
};
use elog::{elog, ereport};
use init_small::globals as g;
use lwlock::{LWLockAcquire, LWLockRelease, LW_EXCLUSIVE, LW_SHARED};
use mcx::{Mcx, MemoryContext};
use mcx::vec_with_capacity_in;
use mcx::PgVec;
use transam_xlog::write::GetFlushRecPtr;
use transam_xlog::{
    wal_segment_size, GetRedoRecPtr, GetWALInsertionTimeLineIfSet,
    RecoveryInProgress, XLogGetOldestSegno, XLogSegNoOffsetToRecPtr, RM_XLOG_ID,
    WAL_LEVEL_MINIMAL, XLOGDIR, XLOG_CHECKPOINT_REDO, XLOG_CHECKPOINT_SHUTDOWN,
    XLOG_END_OF_RECOVERY, XLOG_PARAMETER_CHANGE, XLR_INFO_MASK,
};
use types_core::{
    ForkNumber, RmgrIds, TimeLineID, TimestampTz, XLogRecPtr, XLogSegNo,
    INVALID_PROC_NUMBER,
};
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_INTERNAL_ERROR,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, DEBUG1, DEBUG2, ERROR, FATAL, WARNING,
};
use types_startup::StartupData;
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};
use types_storage::{RelFileLocator, WAL_SUMMARIZER_LOCK};
use xlogreader::{WALRead, XLogReaderRoutine, XLogReaderState, XLogSegmentRoutine};
use xlogreader_seams::XLogReaderState as ReaderView;

#[cfg(test)]
mod tests;

const MAX_SLEEP_QUANTA: i64 = 150;
const MS_PER_SLEEP_QUANTUM: i64 = 200;
const InvalidXLogRecPtr: XLogRecPtr = 0;
const XLOG_BLCKSZ: u64 = 8192;
const MAXFORKNUM: i32 = ForkNumber::INIT_FORKNUM as i32;
const FSM_FORKNUM: ForkNumber = ForkNumber::FSM_FORKNUM;
const MAIN_FORKNUM: ForkNumber = ForkNumber::MAIN_FORKNUM;
const VISIBILITYMAP_FORKNUM: ForkNumber = ForkNumber::VISIBILITYMAP_FORKNUM;

const WAIT_EVENT_WAL_SUMMARIZER_WAL: u32 = 0x0500_0000 + 16;
const WAIT_EVENT_WAL_SUMMARY_READY: u32 = 0x0800_0000 + 55;
const WAIT_EVENT_WAL_SUMMARIZER_ERROR: u32 = 0x0900_0000 + 9;
const WAIT_EVENT_WAL_SUMMARY_WRITE: u32 = 0x0A00_0000 + 77;

const XLOG_DBASE_CREATE_FILE_COPY: u8 = 0x00;
const XLOG_DBASE_CREATE_WAL_LOG: u8 = 0x10;
const XLOG_DBASE_DROP: u8 = 0x20;
const XLOG_SMGR_CREATE: u8 = 0x10;
const XLOG_SMGR_TRUNCATE: u8 = 0x20;
const SMGR_TRUNCATE_HEAP: u32 = 0x0001;
const SMGR_TRUNCATE_VM: u32 = 0x0002;

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

struct WalSummarizerData {
    initialized: AtomicBool,
    summarized_tli: AtomicU32,
    summarized_lsn: AtomicU64,
    lsn_is_exact: AtomicBool,
    summarizer_pgprocno: AtomicI32,
    pending_lsn: AtomicU64,
    summary_file_cv: ConditionVariable,
}

static SHMEM: OnceLock<WalSummarizerData> = OnceLock::new();

fn ctl() -> &'static WalSummarizerData {
    SHMEM.get().expect("WalSummarizer shmem accessed before WalSummarizerShmemInit")
}

fn summarizer_lock() -> &'static lwlock::LWLock {
    lwlock::main_lock(WAL_SUMMARIZER_LOCK)
}

fn lock(mode: lwlock::LWLockMode) -> PgResult<()> {
    LWLockAcquire(summarizer_lock(), mode, g::MyProcNumber())?;
    Ok(())
}

fn unlock() -> PgResult<()> {
    LWLockRelease(summarizer_lock())
}

pub fn WalSummarizerShmemSize() -> usize {
    core::mem::size_of::<WalSummarizerData>()
}

pub fn WalSummarizerShmemInit() {
    SHMEM
        .set(WalSummarizerData {
            initialized: AtomicBool::new(false),
            summarized_tli: AtomicU32::new(0),
            summarized_lsn: AtomicU64::new(InvalidXLogRecPtr),
            lsn_is_exact: AtomicBool::new(false),
            summarizer_pgprocno: AtomicI32::new(INVALID_PROC_NUMBER),
            pending_lsn: AtomicU64::new(InvalidXLogRecPtr),
            summary_file_cv: ConditionVariable::new(),
        })
        .unwrap_or_else(|_| panic!("WalSummarizerShmemInit called twice"));
}

pub fn WalSummarizerShmemResetAfterCrash() {
    let d = ctl();
    d.initialized.store(false, Relaxed);
    d.summarized_tli.store(0, Relaxed);
    d.summarized_lsn.store(InvalidXLogRecPtr, Relaxed);
    d.lsn_is_exact.store(false, Relaxed);
    d.summarizer_pgprocno.store(INVALID_PROC_NUMBER, Relaxed);
    d.pending_lsn.store(InvalidXLogRecPtr, Relaxed);
    condition_variable::cv_reset_after_crash(&d.summary_file_cv);
}

thread_local! {
    static SLEEP_QUANTA: Cell<i64> = const { Cell::new(1) };
    static PAGES_READ_SINCE_LAST_SLEEP: Cell<i64> = const { Cell::new(0) };
    static REDO_POINTER_AT_LAST_SUMMARY_REMOVAL: Cell<XLogRecPtr> =
        const { Cell::new(InvalidXLogRecPtr) };
}

fn summarize_wal() -> bool {
    guc_tables::vars::summarize_wal.read()
}

static WAL_SUMMARY_KEEP_TIME: AtomicI32 = AtomicI32::new(10 * 24 * 60);

fn wal_summary_keep_time() -> i32 {
    guc_tables::vars::wal_summary_keep_time.read()
}

fn lsn_fmt(lsn: XLogRecPtr) -> String {
    format!("{:X}/{:X}", (lsn >> 32) as u32, lsn as u32)
}

fn fatal_exit(e: &PgError) -> ! {
    elog::emit_error_report_for(e);
    ipc::proc_exit(1, g::MyProcPid())
}

pub fn WalSummarizerMain(startup_data: &StartupData) -> ! {
    debug_assert!(matches!(startup_data, StartupData::None));

    miscinit::SetMyBackendType(types_core::BackendType::WalSummarizer);
    if let Err(e) = auxprocess::AuxiliaryProcessMainCommon() {
        fatal_exit(&e);
    }

    let _ = elog(DEBUG1, "WAL summarizer started");

    {
        use procsignal::ThreadSignalHandler::{Ignore, Simple};
        procsignal::pqsignal_thread(procsignal::signums::SIGHUP, Simple(interrupt::SignalHandlerForConfigReload));
        procsignal::pqsignal_thread(
            procsignal::signums::SIGINT,
            Simple(interrupt::SignalHandlerForShutdownRequest),
        );
        procsignal::pqsignal_thread(
            procsignal::signums::SIGTERM,
            Simple(interrupt::SignalHandlerForShutdownRequest),
        );
        procsignal::pqsignal_thread(procsignal::signums::SIGALRM, Ignore);
        procsignal::pqsignal_thread(procsignal::signums::SIGPIPE, Ignore);
        procsignal::pqsignal_thread(procsignal::signums::SIGUSR2, Ignore);
    }

    ipc::on_shmem_exit(wal_summarizer_shutdown, 0);
    if let Err(e) = lock(LW_EXCLUSIVE) {
        fatal_exit(&e);
    }
    ctl().summarizer_pgprocno.store(g::MyProcNumber(), Relaxed);
    let _ = unlock();

    let mut context = MemoryContext::new("Wal Summarizer");

    libpq_pqsignal::unblock_signals();

    loop {
        match run_summarizer(&mut context) {
            Err(e) if e.level < FATAL => {
                error_recovery_cleanup(&e);
                context.reset();
                let _ = latch::WaitLatch(
                    None,
                    WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                    10000,
                    WAIT_EVENT_WAL_SUMMARIZER_ERROR,
                );
            }
            Err(e) => fatal_exit(&e),
            Ok(()) => ipc::proc_exit(0, g::MyProcPid()),
        }
    }
}

fn error_recovery_cleanup(err: &PgError) {
    g::HoldInterrupts();
    elog::emit_error_report_for(err);

    let _ = lwlock::LWLockReleaseAll();
    ConditionVariableCancelSleep();
    waitevent_seams::pgstat_report_wait_end::call();
    if aio_seams::pgaio_error_cleanup::is_installed() {
        aio_seams::pgaio_error_cleanup::call();
    }
    let _ = resowner::ReleaseAuxProcessResources(false);
    let _ = fd::AtEOXact_Files(false);
    dynahash::AtEOXact_HashTables(false);

    elog::FlushErrorState();
    g::ResumeInterrupts();
}

// The body between C's sigsetjmp re-entry point and the bottom of the main
// loop; an ERROR propagates out and WalSummarizerMain re-enters from the top,
// re-fetching progress exactly as C's longjmp path does.
fn run_summarizer(context: &mut MemoryContext) -> PgResult<()> {
    let mut current_tli = 0;
    let mut exact = false;
    let current = GetOldestUnsummarizedLSN(Some(&mut current_tli), Some(&mut exact))?;
    let mut current_lsn = match current {
        None => return Ok(()),
        Some(lsn) => lsn,
    };
    let mut switch_lsn = InvalidXLogRecPtr;
    let mut switch_tli: TimeLineID = 0;

    loop {
        context.reset();

        ProcessWalSummarizerInterrupts()?;
        MaybeRemoveOldWalSummaries(context.mcx())?;

        let (latest_lsn, latest_tli) = GetLatestLSN();

        if current_tli != latest_tli && switch_lsn == InvalidXLogRecPtr {
            let tles = timeline_seams::read_timeline_history::call(context.mcx(), latest_tli)?;
            let (sp, st) = timeline_seams::tli_switch_point::call(current_tli, &tles)?;
            switch_lsn = sp;
            switch_tli = st;
            elog(
                DEBUG1,
                format!(
                    "switch point from TLI {current_tli} to TLI {switch_tli} is at {}",
                    lsn_fmt(switch_lsn)
                ),
            )?;
        }

        if switch_lsn != InvalidXLogRecPtr && current_lsn >= switch_lsn {
            current_tli = switch_tli;
            current_lsn = switch_lsn;
            switch_lsn = InvalidXLogRecPtr;
            switch_tli = 0;

            lock(LW_EXCLUSIVE)?;
            let d = ctl();
            d.summarized_lsn.store(current_lsn, Relaxed);
            d.summarized_tli.store(current_tli, Relaxed);
            d.lsn_is_exact.store(true, Relaxed);
            d.pending_lsn.store(current_lsn, Relaxed);
            unlock()?;
            continue;
        }

        let end_of_summary_lsn =
            SummarizeWAL(current_tli, current_lsn, exact, switch_lsn, latest_lsn)?;
        debug_assert!(end_of_summary_lsn != InvalidXLogRecPtr);
        debug_assert!(end_of_summary_lsn >= current_lsn);

        current_lsn = end_of_summary_lsn;
        exact = true;

        lock(LW_EXCLUSIVE)?;
        let d = ctl();
        d.summarized_lsn.store(end_of_summary_lsn, Relaxed);
        d.summarized_tli.store(current_tli, Relaxed);
        d.lsn_is_exact.store(true, Relaxed);
        d.pending_lsn.store(end_of_summary_lsn, Relaxed);
        unlock()?;

        ConditionVariableBroadcast(&ctl().summary_file_cv);
    }
}

pub struct WalSummarizerState {
    pub summarized_tli: TimeLineID,
    pub summarized_lsn: XLogRecPtr,
    pub pending_lsn: XLogRecPtr,
    pub summarizer_pid: i32,
}

pub fn GetWalSummarizerState() -> PgResult<WalSummarizerState> {
    lock(LW_SHARED)?;
    let d = ctl();
    let state = if !d.initialized.load(Relaxed) {
        WalSummarizerState {
            summarized_tli: 0,
            summarized_lsn: InvalidXLogRecPtr,
            pending_lsn: InvalidXLogRecPtr,
            summarizer_pid: -1,
        }
    } else {
        let pgprocno = d.summarizer_pgprocno.load(Relaxed);
        let summarized_lsn = d.summarized_lsn.load(Relaxed);
        if pgprocno == INVALID_PROC_NUMBER {
            WalSummarizerState {
                summarized_tli: d.summarized_tli.load(Relaxed),
                summarized_lsn,
                pending_lsn: summarized_lsn,
                summarizer_pid: -1,
            }
        } else {
            let mut pid = lmgr_proc::ProcGlobal().allProcs[pgprocno as usize].pid.load(Relaxed);
            if pid <= 0 {
                pid = -1;
            }
            WalSummarizerState {
                summarized_tli: d.summarized_tli.load(Relaxed),
                summarized_lsn,
                pending_lsn: d.pending_lsn.load(Relaxed),
                summarizer_pid: pid,
            }
        }
    };
    unlock()?;
    Ok(state)
}

fn AmWalSummarizerProcess() -> bool {
    miscinit::GetMyBackendType() == types_core::BackendType::WalSummarizer
}

pub fn GetOldestUnsummarizedLSN(
    tli: Option<&mut TimeLineID>,
    lsn_is_exact: Option<&mut bool>,
) -> PgResult<Option<XLogRecPtr>> {
    let am_wal_summarizer = AmWalSummarizerProcess();

    if !summarize_wal() {
        return Ok(None);
    }

    if !am_wal_summarizer {
        lock(LW_SHARED)?;
        let d = ctl();
        if d.initialized.load(Relaxed) {
            let unsummarized_lsn = d.summarized_lsn.load(Relaxed);
            if let Some(t) = tli {
                *t = d.summarized_tli.load(Relaxed);
            }
            if let Some(e) = lsn_is_exact {
                *e = d.lsn_is_exact.load(Relaxed);
            }
            unlock()?;
            return Ok(Some(unsummarized_lsn));
        }
        unlock()?;
        return oldest_unsummarized_slow(tli, lsn_is_exact, false);
    }

    oldest_unsummarized_slow(tli, lsn_is_exact, true)
}

fn oldest_unsummarized_slow(
    tli: Option<&mut TimeLineID>,
    lsn_is_exact: Option<&mut bool>,
    am_wal_summarizer: bool,
) -> PgResult<Option<XLogRecPtr>> {
    let mut unsummarized_lsn = InvalidXLogRecPtr;
    let mut unsummarized_tli: TimeLineID = 0;
    let mut should_make_exact = false;

    let (_, latest_tli) = GetLatestLSN();
    let history_cx = MemoryContext::new("timeline history");
    let tles = timeline_seams::read_timeline_history::call(history_cx.mcx(), latest_tli)?;
    for tle in tles.iter().rev() {
        let oldest_segno = XLogGetOldestSegno(tle.tli)?;
        if oldest_segno != 0 {
            unsummarized_lsn = XLogSegNoOffsetToRecPtr(oldest_segno, 0, wal_segment_size());
            unsummarized_tli = tle.tli;
            break;
        }
    }

    let existing =
        GetWalSummaries(history_cx.mcx(), unsummarized_tli, InvalidXLogRecPtr, InvalidXLogRecPtr)?;
    for ws in &existing {
        if ws.end_lsn > unsummarized_lsn {
            unsummarized_lsn = ws.end_lsn;
            should_make_exact = true;
        }
    }

    if unsummarized_tli == 0 {
        ereport(ERROR)
            .errcode(ERRCODE_INTERNAL_ERROR)
            .errmsg(format!("no WAL found on timeline {latest_tli}"))
            .finish(loc("GetOldestUnsummarizedLSN"))?;
    }

    lock(LW_EXCLUSIVE)?;
    let d = ctl();
    if am_wal_summarizer || !d.initialized.load(Relaxed) {
        d.initialized.store(true, Relaxed);
        d.summarized_lsn.store(unsummarized_lsn, Relaxed);
        d.summarized_tli.store(unsummarized_tli, Relaxed);
        d.lsn_is_exact.store(should_make_exact, Relaxed);
        d.pending_lsn.store(unsummarized_lsn, Relaxed);
    } else {
        unsummarized_lsn = d.summarized_lsn.load(Relaxed);
    }

    if let Some(t) = tli {
        *t = d.summarized_tli.load(Relaxed);
    }
    if let Some(e) = lsn_is_exact {
        *e = d.lsn_is_exact.load(Relaxed);
    }
    unlock()?;

    Ok(Some(unsummarized_lsn))
}

pub fn WakeupWalSummarizer() {
    let Some(d) = SHMEM.get() else { return };
    // As pgarch: unlocked procno read; a stale value pokes the wrong latch at
    // worst and the relaunched summarizer catches up.
    let pgprocno = d.summarizer_pgprocno.load(Relaxed);
    if pgprocno != INVALID_PROC_NUMBER {
        latch::set_latch(&lmgr_proc::ProcGlobal().allProcs[pgprocno as usize].procLatch);
    }
}

pub fn WaitForWalSummarization(lsn: XLogRecPtr) -> PgResult<()> {
    let mut prior_pending_lsn = InvalidXLogRecPtr;
    let mut deadcycles = 0;

    let initial_time = timestamp_seams::get_current_timestamp::call();
    let mut cycle_time = initial_time;

    loop {
        let mut timeout_in_ms: i64 = 10000;

        postgres_seams::check_for_interrupts::call()?;

        if !summarize_wal() {
            return Ok(());
        }

        lock(LW_SHARED)?;
        let summarized_lsn = ctl().summarized_lsn.load(Relaxed);
        let pending_lsn = ctl().pending_lsn.load(Relaxed);
        unlock()?;

        if summarized_lsn >= lsn {
            break;
        }

        let current_time = timestamp_seams::get_current_timestamp::call();

        if diff_ms(cycle_time, current_time) >= timeout_in_ms {
            cycle_time += timeout_in_ms * 1000;

            if pending_lsn > prior_pending_lsn {
                prior_pending_lsn = pending_lsn;
                deadcycles = 0;
            } else {
                deadcycles += 1;
            }

            if deadcycles >= 6 {
                ereport(ERROR)
                    .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    .errmsg("WAL summarization is not progressing")
                    .errdetail(format!(
                        "Summarization is needed through {}, but is stuck at {} on disk and {} in memory.",
                        lsn_fmt(lsn),
                        lsn_fmt(summarized_lsn),
                        lsn_fmt(pending_lsn)
                    ))
                    .finish(loc("WaitForWalSummarization"))?;
            }

            let elapsed_seconds = diff_ms(initial_time, current_time) / 1000;
            let plural = if elapsed_seconds == 1 { "second" } else { "seconds" };
            ereport(WARNING)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!(
                    "still waiting for WAL summarization through {} after {elapsed_seconds} {plural}",
                    lsn_fmt(lsn)
                ))
                .errdetail(format!(
                    "Summarization has reached {} on disk and {} in memory.",
                    lsn_fmt(summarized_lsn),
                    lsn_fmt(pending_lsn)
                ))
                .finish(loc("WaitForWalSummarization"))?;
        }

        timeout_in_ms -= diff_ms(cycle_time, current_time);

        ConditionVariableTimedSleep(
            &ctl().summary_file_cv,
            timeout_in_ms,
            WAIT_EVENT_WAL_SUMMARY_READY,
        )?;
    }

    ConditionVariableCancelSleep();
    Ok(())
}

fn diff_ms(start: TimestampTz, stop: TimestampTz) -> i64 {
    if start >= stop {
        0
    } else {
        (stop - start + 999) / 1000
    }
}

fn wal_summarizer_shutdown(_code: i32, _arg: usize) {
    ctl().summarizer_pgprocno.store(INVALID_PROC_NUMBER, Relaxed);
}

fn GetLatestLSN() -> (XLogRecPtr, TimeLineID) {
    if !RecoveryInProgress() {
        let mut tli: TimeLineID = 0;
        let lsn = GetFlushRecPtr(Some(&mut tli));
        (lsn, tli)
    } else {
        let insert_tli = GetWALInsertionTimeLineIfSet();
        if insert_tli != 0 {
            let (replay_lsn, _) = xlogrecovery::GetXLogReplayRecPtr();
            return (replay_lsn, insert_tli);
        }

        // Walreceiver unported: its flush LSN is invalid, so replay wins.
        let (replay_lsn, replay_tli) = xlogrecovery::GetXLogReplayRecPtr();
        (replay_lsn, replay_tli)
    }
}

fn ProcessWalSummarizerInterrupts() -> PgResult<()> {
    if procsignal_seams::proc_signal_barrier_pending::call() {
        procsignal_seams::process_proc_signal_barrier::call()?;
    }

    if interrupt::ConfigReloadPending() {
        interrupt::SetConfigReloadPending(false);
        guc_file::ProcessConfigFile(types_guc::GucContext::PGC_SIGHUP)?;
    }

    if interrupt::ShutdownRequestPending() || !summarize_wal() {
        elog(DEBUG1, "WAL summarizer shutting down")?;
        ipc::proc_exit(0, g::MyProcPid());
    }

    if mcxt_seams::log_memory_context_pending::is_installed()
        && mcxt_seams::log_memory_context_pending::call()
    {
        mcxt_seams::process_log_memory_context_interrupt::call()?;
    }
    Ok(())
}

struct SummarizerPageRead {
    tli: TimeLineID,
    historic: bool,
    read_upto: XLogRecPtr,
    end_of_wal: bool,
}

impl XLogSegmentRoutine for SummarizerPageRead {
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

impl XLogReaderRoutine for SummarizerPageRead {
    fn page_read(
        &mut self,
        v: &mut ReaderView,
        target_page_ptr: XLogRecPtr,
        req_len: i32,
        _target_rec_ptr: XLogRecPtr,
        cur_page: &mut [u8],
    ) -> PgResult<i32> {
        ProcessWalSummarizerInterrupts()?;

        let count: u64;
        loop {
            if target_page_ptr + XLOG_BLCKSZ <= self.read_upto {
                count = XLOG_BLCKSZ;
                break;
            } else if target_page_ptr + req_len as u64 > self.read_upto {
                if self.historic {
                    self.end_of_wal = true;
                    return Ok(-1);
                }

                ProcessWalSummarizerInterrupts()?;
                summarizer_wait_for_wal()?;

                let (latest_lsn, latest_tli) = GetLatestLSN();
                if self.tli == latest_tli {
                    debug_assert!(latest_lsn >= self.read_upto);
                    self.read_upto = latest_lsn;
                } else {
                    let history_cx = MemoryContext::new("timeline history");
                    let tles =
                        timeline_seams::read_timeline_history::call(history_cx.mcx(), latest_tli)?;
                    let (switchpoint, _) =
                        timeline_seams::tli_switch_point::call(self.tli, &tles)?;

                    self.historic = true;
                    self.read_upto = switchpoint;

                    elog(
                        DEBUG1,
                        format!(
                            "timeline {} became historic, can read up to {}",
                            self.tli,
                            lsn_fmt(self.read_upto)
                        ),
                    )?;
                }
            } else {
                count = self.read_upto - target_page_ptr;
                break;
            }
        }

        let tli = self.tli;
        if let Err(e) = WALRead(v, self, cur_page, target_page_ptr, count as usize, tli)? {
            xlogutils::WALReadRaiseError(&e)?;
            unreachable!();
        }

        PAGES_READ_SINCE_LAST_SLEEP.set(PAGES_READ_SINCE_LAST_SLEEP.get() + 1);
        Ok(count as i32)
    }
}

fn SummarizeWAL(
    tli: TimeLineID,
    start_lsn: XLogRecPtr,
    exact: bool,
    mut switch_lsn: XLogRecPtr,
    maximum_lsn: XLogRecPtr,
) -> PgResult<XLogRecPtr> {
    let cx = MemoryContext::new("SummarizeWAL");
    let mcx = cx.mcx();
    let mut brtab = blkreftable::BlockRefTable::new(mcx);

    let mut routine = SummarizerPageRead {
        tli,
        historic: switch_lsn != InvalidXLogRecPtr,
        read_upto: maximum_lsn,
        end_of_wal: false,
    };
    let mut xlogreader = XLogReaderState::allocate(mcx, wal_segment_size())?;

    let summary_start_lsn;
    let mut summary_end_lsn = switch_lsn;
    let mut fast_forward = true;

    if exact {
        xlogreader.XLogBeginRead(start_lsn);
        summary_start_lsn = start_lsn;
    } else {
        let found = xlogreader.XLogFindNextRecord(&mut routine, start_lsn)?;
        if found == InvalidXLogRecPtr {
            if routine.end_of_wal {
                elog(
                    DEBUG1,
                    format!(
                        "could not read WAL from timeline {tli} at {}: end of WAL at {}",
                        lsn_fmt(start_lsn),
                        lsn_fmt(routine.read_upto)
                    ),
                )?;
                summary_start_lsn = start_lsn;
                summary_end_lsn = routine.read_upto;
                switch_lsn = xlogreader.v.EndRecPtr;
            } else {
                ereport(ERROR)
                .errmsg(format!(
                    "could not find a valid record after {}",
                    lsn_fmt(start_lsn)
                ))
                .finish(loc("SummarizeWAL"))?;
            unreachable!();
            }
        } else {
            summary_start_lsn = found;
        }
        debug_assert!(summary_start_lsn >= start_lsn);
    }

    loop {
        {
            ProcessWalSummarizerInterrupts()?;

            debug_assert!(summary_start_lsn <= xlogreader.v.EndRecPtr);

            let read = xlogreader.XLogReadRecord(&mut routine)?;
            if read.is_none() {
                if routine.end_of_wal {
                    elog(
                        DEBUG1,
                        format!(
                            "could not read WAL from timeline {tli} at {}: end of WAL at {}",
                            lsn_fmt(xlogreader.v.EndRecPtr),
                            lsn_fmt(routine.read_upto)
                        ),
                    )?;
                    summary_end_lsn = routine.read_upto;
                    break;
                }
                let msg = match xlogreader.errormsg() {
                    Some(m) => format!(
                        "could not read WAL from timeline {tli} at {}: {m}",
                        lsn_fmt(xlogreader.v.EndRecPtr)
                    ),
                    None => format!(
                        "could not read WAL from timeline {tli} at {}",
                        lsn_fmt(xlogreader.v.EndRecPtr)
                    ),
                };
                ereport(ERROR)
                    .errcode_for_file_access()
                    .errmsg(msg)
                    .finish(loc("SummarizeWAL"))?;
                unreachable!();
            }

            debug_assert!(summary_start_lsn <= xlogreader.v.EndRecPtr);

            if switch_lsn != InvalidXLogRecPtr && xlogreader.v.ReadRecPtr >= switch_lsn {
                summary_end_lsn = switch_lsn;
                break;
            }

            let rmid = xlogreader.XLogRecGetRmid();
            if rmid == RM_XLOG_ID {
                if let Some(new_fast_forward) = SummarizeXlogRecord(&xlogreader)? {
                    if xlogreader.v.ReadRecPtr > summary_start_lsn {
                        summary_end_lsn = xlogreader.v.ReadRecPtr;
                        break;
                    } else {
                        fast_forward = new_fast_forward;
                    }
                }
            } else if !fast_forward {
                if rmid == RmgrIds::RM_DBASE_ID as u8 {
                    SummarizeDbaseRecord(&xlogreader, &mut brtab)?;
                } else if rmid == RmgrIds::RM_SMGR_ID as u8 {
                    SummarizeSmgrRecord(&xlogreader, &mut brtab)?;
                } else if rmid == xact::RM_XACT_ID {
                    SummarizeXactRecord(&xlogreader, &mut brtab)?;
                }
            }

            if !fast_forward {
                let max_block_id = xlogreader.XLogRecMaxBlockId();
                let mut block_id = 0;
                while block_id <= max_block_id {
                    if let Some((rlocator, forknum, blocknum, _)) =
                        xlogreader.XLogRecGetBlockTagExtended(block_id as u8)
                    {
                        if forknum != FSM_FORKNUM {
                            brtab.mark_block_modified(rlocator, forknum, blocknum)?;
                        }
                    }
                    block_id += 1;
                }
            }

            summary_end_lsn = xlogreader.v.EndRecPtr;

            lock(LW_EXCLUSIVE)?;
            debug_assert!(summary_end_lsn >= ctl().summarized_lsn.load(Relaxed));
            ctl().pending_lsn.store(summary_end_lsn, Relaxed);
            unlock()?;

            if switch_lsn != InvalidXLogRecPtr && xlogreader.v.EndRecPtr >= switch_lsn {
                break;
            }
        }
    }

    drop(xlogreader);

    if summary_end_lsn > summary_start_lsn && !fast_forward {
        let temp_path = format!("{XLOGDIR}/summaries/temp.summary");
        let final_path = format!(
            "{XLOGDIR}/summaries/{:08X}{:08X}{:08X}{:08X}{:08X}.summary",
            tli,
            (summary_start_lsn >> 32) as u32,
            summary_start_lsn as u32,
            (summary_end_lsn >> 32) as u32,
            summary_end_lsn as u32
        );

        let file =
            fd::PathNameOpenFile(&temp_path, libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC)?;
        if file.0 < 0 {
            ereport(ERROR)
                .with_saved_errno(std::io::Error::last_os_error().raw_os_error().unwrap_or(0))
                .errcode_for_file_access()
                .errmsg(format!("could not create file \"{temp_path}\": %m"))
                .finish(loc("SummarizeWAL"))?;
        }

        let mut filepos: i64 = 0;
        let write_result = brtab.write(|data: &[u8]| {
            let nbytes = fd::FileWrite(file, data, filepos, WAIT_EVENT_WAL_SUMMARY_WRITE)?;
            if nbytes != data.len() as isize {
                ereport(ERROR)
                    .with_saved_errno(if nbytes < 0 {
                        std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
                    } else {
                        libc::ENOSPC
                    })
                    .errcode_for_file_access()
                    .errmsg(format!(
                        "could not write file \"{temp_path}\": wrote only {nbytes} of {} bytes",
                        data.len()
                    ))
                    .finish(loc("WriteWalSummary"))?;
            }
            filepos += nbytes as i64;
            Ok(())
        });
        let close_result = fd::FileClose(file);
        write_result?;
        close_result?;

        elog(
            DEBUG1,
            format!(
                "summarized WAL on TLI {tli} from {} to {}",
                lsn_fmt(summary_start_lsn),
                lsn_fmt(summary_end_lsn)
            ),
        )?;

        fd::durable_rename(&temp_path, &final_path, ERROR)?;
    }

    if summary_end_lsn > summary_start_lsn && fast_forward {
        elog(
            DEBUG1,
            format!(
                "skipped summarizing WAL on TLI {tli} from {} to {}",
                lsn_fmt(summary_start_lsn),
                lsn_fmt(summary_end_lsn)
            ),
        )?;
    }

    Ok(summary_end_lsn)
}

fn SummarizeDbaseRecord(
    xlogreader: &XLogReaderState<'_>,
    brtab: &mut blkreftable::BlockRefTable<'_>,
) -> PgResult<()> {
    let info = xlogreader.XLogRecGetInfo() & !XLR_INFO_MASK;
    let data = xlogreader.XLogRecGetData();

    // C sets a limit block of 0 for relfilenumber zero to mark every relation
    // in the DB OID/TS OID pair as recreated (see walsummarizer.c).
    if info == XLOG_DBASE_CREATE_FILE_COPY || info == XLOG_DBASE_CREATE_WAL_LOG {
        let db_id = u32::from_ne_bytes(data[0..4].try_into().unwrap());
        let tablespace_id = u32::from_ne_bytes(data[4..8].try_into().unwrap());
        let rlocator = RelFileLocator { spcOid: tablespace_id, dbOid: db_id, relNumber: 0 };
        brtab.set_limit_block(rlocator, MAIN_FORKNUM, 0);
    } else if info == XLOG_DBASE_DROP {
        let db_id = u32::from_ne_bytes(data[0..4].try_into().unwrap());
        let ntablespaces = i32::from_ne_bytes(data[4..8].try_into().unwrap()).max(0) as usize;
        for i in 0..ntablespaces {
            let off = 8 + 4 * i;
            let spc = u32::from_ne_bytes(data[off..off + 4].try_into().unwrap());
            let rlocator = RelFileLocator { spcOid: spc, dbOid: db_id, relNumber: 0 };
            brtab.set_limit_block(rlocator, MAIN_FORKNUM, 0);
        }
    }
    Ok(())
}

fn SummarizeSmgrRecord(
    xlogreader: &XLogReaderState<'_>,
    brtab: &mut blkreftable::BlockRefTable<'_>,
) -> PgResult<()> {
    let info = xlogreader.XLogRecGetInfo() & !XLR_INFO_MASK;
    let data = xlogreader.XLogRecGetData();

    if info == XLOG_SMGR_CREATE {
        // xl_smgr_create: RelFileLocator (12), ForkNumber (4).
        let rlocator = rlocator_at(data, 0);
        let forknum = ForkNumber::from_i32(i32::from_ne_bytes(data[12..16].try_into().unwrap()))
            .expect("valid fork number in smgr-create record");
        if forknum != FSM_FORKNUM {
            brtab.set_limit_block(rlocator, forknum, 0);
        }
    } else if info == XLOG_SMGR_TRUNCATE {
        // xl_smgr_truncate: BlockNumber (4), RelFileLocator (12), int flags (4).
        let blkno = u32::from_ne_bytes(data[0..4].try_into().unwrap());
        let rlocator = rlocator_at(data, 4);
        let flags = u32::from_ne_bytes(data[16..20].try_into().unwrap());
        if flags & SMGR_TRUNCATE_HEAP != 0 {
            brtab.set_limit_block(rlocator, MAIN_FORKNUM, blkno);
        }
        if flags & SMGR_TRUNCATE_VM != 0 {
            brtab.set_limit_block(rlocator, VISIBILITYMAP_FORKNUM, blkno);
        }
    }
    Ok(())
}

fn rlocator_at(data: &[u8], off: usize) -> RelFileLocator {
    RelFileLocator {
        spcOid: u32::from_ne_bytes(data[off..off + 4].try_into().unwrap()),
        dbOid: u32::from_ne_bytes(data[off + 4..off + 8].try_into().unwrap()),
        relNumber: u32::from_ne_bytes(data[off + 8..off + 12].try_into().unwrap()),
    }
}

fn SummarizeXactRecord(
    xlogreader: &XLogReaderState<'_>,
    brtab: &mut blkreftable::BlockRefTable<'_>,
) -> PgResult<()> {
    let info = xlogreader.XLogRecGetInfo();
    let xact_info = info & !XLR_INFO_MASK & xact::XLOG_XACT_OPMASK;
    let data = xlogreader.XLogRecGetData();

    if xact_info == xact::XLOG_XACT_COMMIT || xact_info == xact::XLOG_XACT_COMMIT_PREPARED {
        let parsed = xact::parse_commit_record(info, data)?;
        for xl in &parsed.xlocators {
            for raw in 0..=MAXFORKNUM {
                let forknum = ForkNumber::from_i32(raw).unwrap();
                if forknum != FSM_FORKNUM {
                    brtab.set_limit_block(*xl, forknum, 0);
                }
            }
        }
    } else if xact_info == xact::XLOG_XACT_ABORT || xact_info == xact::XLOG_XACT_ABORT_PREPARED {
        let parsed = xact::parse_abort_record(info, data)?;
        for xl in &parsed.xlocators {
            for raw in 0..=MAXFORKNUM {
                let forknum = ForkNumber::from_i32(raw).unwrap();
                if forknum != FSM_FORKNUM {
                    brtab.set_limit_block(*xl, forknum, 0);
                }
            }
        }
    }
    Ok(())
}

// Returns Some(new_fast_forward) when summarization must stop before this
// record, None when no special handling applies.
fn SummarizeXlogRecord(xlogreader: &XLogReaderState<'_>) -> PgResult<Option<bool>> {
    let info = xlogreader.XLogRecGetInfo() & !XLR_INFO_MASK;
    let data = xlogreader.XLogRecGetData();

    let record_wal_level: i32;
    if info == XLOG_CHECKPOINT_REDO {
        record_wal_level = i32::from_ne_bytes(data[0..4].try_into().unwrap());
    } else if info == XLOG_CHECKPOINT_SHUTDOWN {
        let ckpt = controldata_utils::CheckPoint::from_bytes(data);
        record_wal_level = ckpt.wal_level;
    } else if info == XLOG_PARAMETER_CHANGE {
        // xl_parameter_change: wal_level is the sixth int.
        record_wal_level = i32::from_ne_bytes(data[20..24].try_into().unwrap());
    } else if info == XLOG_END_OF_RECOVERY {
        // xl_end_of_recovery: TimestampTz (8), TLI (4), PrevTLI (4), wal_level.
        record_wal_level = i32::from_ne_bytes(data[16..20].try_into().unwrap());
    } else {
        return Ok(None);
    }

    Ok(Some(record_wal_level == WAL_LEVEL_MINIMAL))
}

fn summarizer_wait_for_wal() -> PgResult<()> {
    let pages_read = PAGES_READ_SINCE_LAST_SLEEP.get();
    let mut sleep_quanta = SLEEP_QUANTA.get();
    if pages_read == 0 {
        sleep_quanta = (sleep_quanta * 2).min(MAX_SLEEP_QUANTA);
    } else if pages_read > 1 {
        if pages_read > sleep_quanta - 1 {
            sleep_quanta = 1;
        } else {
            sleep_quanta -= pages_read;
        }
    }
    SLEEP_QUANTA.set(sleep_quanta);

    if pgstat_seams::pgstat_report_wal::is_installed() {
        pgstat_seams::pgstat_report_wal::call(false);
    }

    latch::WaitLatch(
        g::MyLatch(),
        WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
        sleep_quanta * MS_PER_SLEEP_QUANTUM,
        WAIT_EVENT_WAL_SUMMARIZER_WAL,
    )?;
    if let Some(l) = g::MyLatch() {
        latch::ResetLatch(l);
    }

    PAGES_READ_SINCE_LAST_SLEEP.set(0);
    Ok(())
}

fn MaybeRemoveOldWalSummaries(mcx: Mcx<'_>) -> PgResult<()> {
    let redo_pointer = GetRedoRecPtr();

    if wal_summary_keep_time() == 0 {
        return Ok(());
    }

    // Redo pointer unchanged => at most one removal pass per checkpoint cycle.
    if redo_pointer == REDO_POINTER_AT_LAST_SUMMARY_REMOVAL.get() {
        return Ok(());
    }
    REDO_POINTER_AT_LAST_SUMMARY_REMOVAL.set(redo_pointer);

    // DST P2 (contract §1.2): retention cutoff on pg_clock::wall_secs().
    // (The file-mtime duration_since below is P1 SimVfs territory — left raw.)
    let cutoff_time = pg_clock::wall_secs() - wal_summary_keep_time() as i64 * 60;

    let mut wslist = GetWalSummaries(mcx, 0, InvalidXLogRecPtr, InvalidXLogRecPtr)?;

    while !wslist.is_empty() {
        ProcessWalSummarizerInterrupts()?;

        let selected_tli = wslist[0].tli;
        let oldest_segno = XLogGetOldestSegno(selected_tli)?;
        let oldest_lsn = if oldest_segno != 0 {
            XLogSegNoOffsetToRecPtr(oldest_segno, 0, wal_segment_size())
        } else {
            InvalidXLogRecPtr
        };

        let mut rest = vec_with_capacity_in(mcx, wslist.len())?;
        for ws in wslist.drain(..) {
            ProcessWalSummarizerInterrupts()?;
            if selected_tli != ws.tli {
                rest.push(ws);
                continue;
            }
            if oldest_lsn == InvalidXLogRecPtr || ws.end_lsn <= oldest_lsn {
                RemoveWalSummaryIfOlderThan(&ws, cutoff_time)?;
            }
        }
        wslist = rest;
    }
    Ok(())
}

pub struct WalSummaryFile {
    pub start_lsn: XLogRecPtr,
    pub end_lsn: XLogRecPtr,
    pub tli: TimeLineID,
}

// walsummary.c GetWalSummaries, homed here (walsummary.c stays non-core; the
// summarizer is its only in-core consumer besides backup).
pub fn GetWalSummaries<'m>(
    mcx: Mcx<'m>,
    tli: TimeLineID,
    start_lsn: XLogRecPtr,
    end_lsn: XLogRecPtr,
) -> PgResult<PgVec<'m, WalSummaryFile>> {
    let dirname = format!("{XLOGDIR}/summaries");
    let mut result = PgVec::new_in(mcx);
    let sdir = fd::AllocateDir(&dirname)?;
    while let Some(dent) = fd::ReadDir(sdir, &dirname)? {
        let name = &dent.d_name;
        let Some((file_tli, file_start_lsn, file_end_lsn)) = parse_wal_summary_filename(name)
        else {
            continue;
        };
        if tli != 0 && tli != file_tli {
            continue;
        }
        if start_lsn != InvalidXLogRecPtr && start_lsn >= file_end_lsn {
            continue;
        }
        if end_lsn != InvalidXLogRecPtr && end_lsn <= file_start_lsn {
            continue;
        }
        result.push(WalSummaryFile {
            start_lsn: file_start_lsn,
            end_lsn: file_end_lsn,
            tli: file_tli,
        });
    }
    fd::FreeDir(sdir)?;
    Ok(result)
}

fn parse_wal_summary_filename(name: &str) -> Option<(TimeLineID, XLogRecPtr, XLogRecPtr)> {
    // IsWalSummaryFilename: 40 upper-hex chars + ".summary".
    if name.len() != 48 || !name.ends_with(".summary") {
        return None;
    }
    let hex = &name[..40];
    if !hex.bytes().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_lowercase()) {
        return None;
    }
    let f = |r: core::ops::Range<usize>| u32::from_str_radix(&hex[r], 16).unwrap();
    let tli = f(0..8);
    let start = ((f(8..16) as u64) << 32) | f(16..24) as u64;
    let end = ((f(24..32) as u64) << 32) | f(32..40) as u64;
    Some((tli, start, end))
}

pub fn RemoveWalSummaryIfOlderThan(ws: &WalSummaryFile, cutoff_time: i64) -> PgResult<()> {
    let path = format!(
        "{XLOGDIR}/summaries/{:08X}{:08X}{:08X}{:08X}{:08X}.summary",
        ws.tli,
        (ws.start_lsn >> 32) as u32,
        ws.start_lsn as u32,
        (ws.end_lsn >> 32) as u32,
        ws.end_lsn as u32
    );

    let mut md = fd::FileInfo::zeroed();
    if fd::pg_lstat(&path, &mut md) != 0 {
        let en = fd::get_errno();
        if en == libc::ENOENT {
            return Ok(());
        }
        ereport(ERROR)
            .with_saved_errno(en)
            .errcode_for_file_access()
            .errmsg(format!("could not stat file \"{path}\": %m"))
            .finish(loc("RemoveWalSummaryIfOlderThan"))?;
        unreachable!();
    }
    let mtime = if md.mtime_sec >= 0 { md.mtime_sec } else { i64::MAX };
    if mtime >= cutoff_time {
        return Ok(());
    }
    if fd::pg_unlink(&path) != 0 {
        ereport(ERROR)
            .with_saved_errno(fd::get_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not remove file \"{path}\": %m"))
            .finish(loc("RemoveWalSummaryIfOlderThan"))?;
    }
    elog(DEBUG2, format!("removing file \"{path}\""))?;
    Ok(())
}

pub fn init_seams() {
    use guc_tables::GucVarAccessors;
    guc_tables::vars::wal_summary_keep_time.install(GucVarAccessors {
        get: || WAL_SUMMARY_KEEP_TIME.load(Relaxed),
        set: |v| WAL_SUMMARY_KEEP_TIME.store(v, Relaxed),
    });
    walsummarizer_seams::wakeup_wal_summarizer::set(WakeupWalSummarizer);
    walsummarizer_seams::wait_for_wal_summarization::set(WaitForWalSummarization);
    walsummarizer_seams::get_oldest_unsummarized_lsn::set(|| {
        Ok(GetOldestUnsummarizedLSN(None, None)?.unwrap_or(InvalidXLogRecPtr))
    });
    walsummarizer_seams::get_wal_summarizer_state::set(|| {
        let s = GetWalSummarizerState()?;
        Ok((s.summarized_tli, s.summarized_lsn, s.pending_lsn, s.summarizer_pid))
    });
}
