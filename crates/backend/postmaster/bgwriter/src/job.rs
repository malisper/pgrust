//! The bgwriter JOB DRIVER (M4 increment 4, docs/design/m4-bgjobs.md
//! §3.2-§3.4): the same [`crate::bgwriter_cycle`] body the thread driver
//! runs, expressed as a [`bgjobs::BgJob`] — cycles execute on pool workers
//! under Maintenance RGs; identity, signals, config reloads, and teardown
//! live on the dispatcher thread (the job's stable TLS home).
//!
//! Split of responsibilities:
//! - DISPATCHER (startup/control/teardown): SetMyBackendType +
//!   AuxiliaryProcessMainCommon (aux PGPROC, procsignal slot pss_pid=job
//!   pid, beentry — the pg_stat_activity row), the daemon's thread-signal
//!   dispositions (dispatcher TLS — DrainThreadSignals runs them there),
//!   SIGHUP → ProcessConfigFile + overlay refresh, SIGTERM → Exit,
//!   SIGQUIT/SIGABRT/SIGKILL → crash Abandon, exit = shmem_exit(0) +
//!   announce_child_exit (the reaper's join is a lookup miss — job pids
//!   are never in CHILD_THREADS).
//! - POOL WORKER (run_cycle): binds the job envelope (pid, procno,
//!   MyProc, latch, backend type + the GUC overlay stamp) via RAII, runs
//!   the verbatim cycle body, unbinds. ProcessMainLoopInterrupts inside
//!   the body is inert on workers by construction: shutdown/reload flags
//!   are thread-local and only the dispatcher's ever get set — proc_exit
//!   is unreachable from a worker.
//!
//! HIBERNATION is a small FSM here (armed → hibernating), C-equivalent:
//! entering hibernation is its own dispatch (no body run — exactly C's
//! "first delay elapsed, no wake"), publishes the procno through
//! StrategyNotifyBgWriter so allocation-side SetLatch wakes ride the
//! ordinary latch redirect, and the next dispatch retracts it.
//!
//! SINGLETON: the crash flag and GUC overlay assume one bgwriter job per
//! process (C invariant; the checkpointer SHUTDOWN_XLOG_PENDING process-
//! static precedent). Generalizing the envelope for a second daemon
//! (walwriter) is the next migration's first task.

use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use bgjobs::{BgJob, Control, CycleOutcome, CycleReason};
use init_small::globals as g;
use types_core::{pid_t, ProcNumber, INVALID_PROC_NUMBER};
use types_error::PgError;
use types_storage::latch::{Latch, LatchHandle};

use crate::{bgwriter_cycle, BgWriterState, HIBERNATE_FACTOR};

/// Crash-fanout flag: SIGQUIT/SIGABRT/SIGKILL thread-signal renderings
/// land here (handlers run on the dispatcher during DrainThreadSignals).
static CRASH_PENDING: AtomicBool = AtomicBool::new(false);

fn note_crash() {
    CRASH_PENDING.store(true, Ordering::SeqCst);
}

/// The audited GUC set the cycle body reads (docs/design/m4-bgjobs.md
/// §3.1): captured on the dispatcher (whose TLS ProcessConfigFile keeps
/// current) and stamped into the executing worker's cells at bind.
#[derive(Clone, Copy)]
struct Overlay {
    delay_ms: i32,
    lru_maxpages: i32,
    lru_multiplier: f64,
    flush_after: i32,
    track_io_timing: bool,
}

impl Overlay {
    /// Read on the dispatcher thread (job startup + every reload).
    fn capture() -> Overlay {
        use guc_tables::vars as v;
        Overlay {
            delay_ms: v::BgWriterDelay.read(),
            lru_maxpages: v::bgwriter_lru_maxpages.read(),
            lru_multiplier: v::bgwriter_lru_multiplier.read(),
            flush_after: v::bgwriter_flush_after.read(),
            track_io_timing: v::track_io_timing.read(),
        }
    }
}

struct JobState {
    inner: BgWriterState,
    /// can_hibernate && prev_hibernate at the last body run — C's
    /// hibernate-entry condition, consumed by the next Deadline dispatch.
    armed: bool,
    /// StrategyNotifyBgWriter(procno) published; retract on next dispatch.
    hibernating: bool,
}

pub struct BgWriterJob {
    pid: pid_t,
    /// The pmchild slot StartChildProcess assigned (register_postmaster_
    /// child_active keys on it during InitAuxiliaryProcess).
    child_slot: i32,
    /// Set by startup() (aux PGPROC acquisition); INVALID before.
    procno: AtomicI32,
    state: Mutex<JobState>,
    overlay: Mutex<Overlay>,
}

impl BgWriterJob {
    pub fn new(pid: pid_t, child_slot: i32) -> BgWriterJob {
        BgWriterJob {
            pid,
            child_slot,
            procno: AtomicI32::new(INVALID_PROC_NUMBER),
            state: Mutex::new(JobState {
                inner: BgWriterState::new(),
                armed: false,
                hibernating: false,
            }),
            overlay: Mutex::new(Overlay {
                delay_ms: 200,
                lru_maxpages: 100,
                lru_multiplier: 2.0,
                flush_after: guc_tables::consts::DEFAULT_BGWRITER_FLUSH_AFTER,
                track_io_timing: false,
            }),
        }
    }

    fn procno(&self) -> ProcNumber {
        self.procno.load(Ordering::Acquire)
    }

    fn refresh_overlay(&self) {
        *self.overlay.lock().unwrap() = Overlay::capture();
    }

    fn announce(&self, exitstatus: i32) {
        if postmaster_seams::announce_child_exit::is_installed() {
            postmaster_seams::announce_child_exit::call(self.pid, exitstatus);
        }
    }
}

/// RAII envelope bind for one cycle task on a pool worker (§3.4): identity
/// TLS + the GUC overlay stamp; LIFO restore.
struct EnvelopeBind {
    prev_pid: pid_t,
    prev_procno: ProcNumber,
    prev_task_proc: ProcNumber,
    prev_latch: Option<LatchHandle>,
    prev_btype: types_core::BackendType,
    prev_guc: Overlay,
}

impl EnvelopeBind {
    fn bind(pid: pid_t, procno: ProcNumber, overlay: &Overlay) -> EnvelopeBind {
        use guc_tables::vars as v;
        let prev = EnvelopeBind {
            prev_pid: g::MyProcPid(),
            prev_procno: g::MyProcNumber(),
            prev_task_proc: lmgr_proc::bind_task_proc(procno),
            prev_latch: g::MyLatch(),
            prev_btype: miscinit::GetMyBackendType(),
            prev_guc: Overlay {
                delay_ms: v::BgWriterDelay.read(),
                lru_maxpages: v::bgwriter_lru_maxpages.read(),
                lru_multiplier: v::bgwriter_lru_multiplier.read(),
                flush_after: v::bgwriter_flush_after.read(),
                track_io_timing: v::track_io_timing.read(),
            },
        };
        g::SetMyProcPid(pid);
        g::SetMyProcNumber(procno);
        g::SetMyLatch(Some(LatchHandle::proc(procno)));
        miscinit::SetMyBackendType(types_core::BackendType::BgWriter);
        v::BgWriterDelay.write(overlay.delay_ms);
        v::bgwriter_lru_maxpages.write(overlay.lru_maxpages);
        v::bgwriter_lru_multiplier.write(overlay.lru_multiplier);
        v::bgwriter_flush_after.write(overlay.flush_after);
        v::track_io_timing.write(overlay.track_io_timing);
        prev
    }
}

impl Drop for EnvelopeBind {
    fn drop(&mut self) {
        use guc_tables::vars as v;
        v::BgWriterDelay.write(self.prev_guc.delay_ms);
        v::bgwriter_lru_maxpages.write(self.prev_guc.lru_maxpages);
        v::bgwriter_lru_multiplier.write(self.prev_guc.lru_multiplier);
        v::bgwriter_flush_after.write(self.prev_guc.flush_after);
        v::track_io_timing.write(self.prev_guc.track_io_timing);
        miscinit::SetMyBackendType(self.prev_btype);
        g::SetMyLatch(self.prev_latch);
        lmgr_proc::unbind_task_proc(self.prev_task_proc);
        g::SetMyProcNumber(self.prev_procno);
        g::SetMyProcPid(self.prev_pid);
    }
}

impl BgJob for BgWriterJob {
    fn name(&self) -> &'static str {
        "bgwriter"
    }

    fn latch(&self) -> Option<&'static Latch> {
        let procno = self.procno();
        (procno != INVALID_PROC_NUMBER)
            .then(|| &lmgr_proc::GetPGProcByNumber(procno).procLatch)
    }

    /// The daemon main's prelude, on the dispatcher thread with the job's
    /// pid bound (the dispatcher keeps this identity — it is the job's
    /// session thread that never runs the loop).
    fn startup(&self) -> Result<(), Box<PgError>> {
        // The dispatcher thread is process-lifetime and hosts SUCCESSIVE
        // job lifecycles (normal-exit relaunch; crash-abandon relaunch) —
        // reset the per-lifecycle thread state a C child gets fresh from
        // fork, then run the once-per-thread child half exactly once (the
        // wretain warm-claim split: launch_backend run_child_task).
        thread_local! {
            static CHILD_INITED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
        }
        ipc::on_exit_reset();
        miscinit::SetProcessingMode(types_core::ProcessingMode::InitProcessing);
        // Stale-identity clear: a crash-ABANDONED lifecycle leaves the
        // dispatcher TLS pointing into pre-reset shared memory (the clean
        // path clears these in AuxiliaryProcKill via teardown's
        // shmem_exit). Without this, every post-crash relaunch dies at
        // InitAuxiliaryProcess "you already exist" and the postmaster
        // crash-loops hot (observed: fleet job ...-42c3).
        let _ = lmgr_proc::bind_task_proc(INVALID_PROC_NUMBER);
        g::SetMyProcNumber(INVALID_PROC_NUMBER);
        g::SetMyLatch(None);
        let init = if CHILD_INITED.get() {
            miscinit::InitProcessGlobals(self.pid);
            Ok(())
        } else {
            miscinit::InitPostmasterChild(self.pid).map(|()| CHILD_INITED.set(true))
        };
        if let Err(e) = init {
            self.announce(1 << 8);
            return Err(e);
        }
        g::SetMyPMChildSlot(self.child_slot);
        miscinit::SetMyBackendType(types_core::BackendType::BgWriter);
        if let Err(e) = auxprocess::AuxiliaryProcessMainCommon() {
            // C fatal_exit parity: proc_exit(1) RUNS THE EXIT CALLBACKS —
            // whatever partial identity was acquired (aux PGPROC, beentry,
            // procsignal slot) must be released or every relaunch inherits
            // it ("you already exist").
            if let Err(e2) = ipc::shmem_exit(1) {
                elog::emit_error_report_for(&e2);
            }
            self.announce(1 << 8);
            return Err(e);
        }
        self.procno.store(g::MyProcNumber(), Ordering::Release);

        {
            use procsignal::ThreadSignalHandler::{Ignore, Simple};
            procsignal::pqsignal_thread(
                libc::SIGHUP,
                Simple(interrupt::SignalHandlerForConfigReload),
            );
            procsignal::pqsignal_thread(libc::SIGINT, Ignore);
            procsignal::pqsignal_thread(
                libc::SIGTERM,
                Simple(interrupt::SignalHandlerForShutdownRequest),
            );
            procsignal::pqsignal_thread(libc::SIGALRM, Ignore);
            procsignal::pqsignal_thread(libc::SIGPIPE, Ignore);
            procsignal::pqsignal_thread(libc::SIGUSR2, Ignore);
            // Crash-fanout renderings (thread daemons die by unwind; a
            // threadless job converts them into Abandon at control()).
            procsignal::pqsignal_thread(libc::SIGQUIT, Simple(note_crash));
            procsignal::pqsignal_thread(libc::SIGABRT, Simple(note_crash));
            procsignal::pqsignal_thread(libc::SIGKILL, Simple(note_crash));
        }

        {
            let mut st = self.state.lock().unwrap();
            st.inner = BgWriterState::new();
            st.inner.last_snapshot_ts = timestamp_seams::get_current_timestamp::call();
            st.inner.sync.reset_writeback_context();
        }
        self.refresh_overlay();
        Ok(())
    }

    /// Signal/reload/shutdown processing on the dispatcher.
    fn control(&self) -> Control {
        let _ = procsignal::DrainThreadSignals();
        if CRASH_PENDING.swap(false, Ordering::SeqCst) {
            // KilledBySignal announce shape: raw signo (WTERMSIG). The
            // identity is ABANDONED (shmem resets wholesale); stop
            // pointing at the doomed PGPROC.
            self.procno.store(INVALID_PROC_NUMBER, Ordering::Release);
            self.announce(libc::SIGQUIT);
            return Control::Abandon;
        }
        if procsignal_seams::proc_signal_barrier_pending::call() {
            let _ = procsignal_seams::process_proc_signal_barrier::call();
        }
        if interrupt::ConfigReloadPending() {
            interrupt::SetConfigReloadPending(false);
            if let Err(e) = guc_file_seams::process_config_file::call(types_guc::PGC_SIGHUP) {
                elog::emit_error_report_for(&e);
            }
            self.refresh_overlay();
        }
        if interrupt::ShutdownRequestPending() {
            interrupt::SetShutdownRequestPending(false);
            return Control::Exit;
        }
        Control::Continue
    }

    /// Clean exit on the dispatcher: the aux shmem-exit chain
    /// (ShutdownAuxiliaryProcess, beentry shutdown, AuxiliaryProcKill,
    /// CleanupProcSignalState) + the normal-exit announce. The reaper's
    /// join is a CHILD_THREADS lookup miss — nothing to join.
    fn teardown(&self) {
        if let Err(e) = ipc::shmem_exit(0) {
            elog::emit_error_report_for(&e);
        }
        self.procno.store(INVALID_PROC_NUMBER, Ordering::Release);
        self.announce(0); // WIFEXITED(0)
    }

    /// Hook/cycle panic: C parity — a daemon panic is a child crash. The
    /// WTERMSIG-shaped announce routes the postmaster into its ordinary
    /// crash handling (HandleChildCrash → reinit → relaunch) instead of
    /// wedging shutdown on a child that never exits.
    fn crashed(&self) {
        self.procno.store(INVALID_PROC_NUMBER, Ordering::Release);
        self.announce(libc::SIGABRT);
    }

    fn run_cycle(&self, reason: CycleReason) -> CycleOutcome {
        let overlay = *self.overlay.lock().unwrap();
        let mut st = self.state.lock().unwrap();
        let procno = self.procno();
        let _bind = EnvelopeBind::bind(self.pid, procno, &overlay);
        let delay = Duration::from_millis(overlay.delay_ms.max(1) as u64);

        if st.hibernating {
            bufmgr::StrategyNotifyBgWriter(-1);
            st.hibernating = false;
        } else if reason == CycleReason::Deadline && st.armed {
            // C's hibernate entry: the delay elapsed with no wake. No body
            // run; publish the wake procno and take the long nap.
            st.armed = false;
            st.hibernating = true;
            bufmgr::StrategyNotifyBgWriter(procno);
            return CycleOutcome::Sleep(delay * HIBERNATE_FACTOR as u32);
        }

        match bgwriter_cycle(&mut st.inner) {
            Ok(can_hibernate) => {
                st.armed = can_hibernate && st.inner.prev_hibernate;
                st.inner.prev_hibernate = can_hibernate;
                CycleOutcome::Sleep(delay)
            }
            Err(err) => {
                // The daemons' uniform error leg, minus the inline sleep
                // (the backoff is the re-arm deadline) — plus a pending-
                // stats flush so counters accumulated before the error
                // cannot strand on this worker's TLS (the next cycle may
                // run elsewhere; C's next-cycle flush assumes one thread).
                crate::abort_cleanup(&err);
                st.inner.sync.reset_writeback_context();
                waitevent_seams::pgstat_report_wait_end::call();
                if pgstat_seams::pgstat_report_bgwriter::is_installed() {
                    pgstat_seams::pgstat_report_bgwriter::call();
                }
                st.inner.prev_hibernate = false;
                st.armed = false;
                CycleOutcome::Sleep(Duration::from_secs(1))
            }
        }
    }
}
