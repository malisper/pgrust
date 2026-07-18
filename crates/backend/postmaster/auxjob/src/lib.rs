//! M4 bgjobs: the shared AUX-DAEMON JOB layer (docs/design/m4-bgjobs.md
//! §3.1-§3.4, §3.6) — everything a migrated periodic aux daemon
//! (bgwriter, walwriter, ...) needs beyond its own loop body, generalized
//! from the bgwriter migration so a SECOND concurrent job can exist (the
//! recorded "envelope swap" task, notes/m4-bgjobs.md Known deviations).
//!
//! Split of responsibilities (the bgwriter donor pattern, unchanged):
//! - DISPATCHER (startup/control/teardown/crashed): the job's stable home
//!   for identity TLS, signal drains, config reloads, exit announces.
//!   NEW HERE vs the singleton donor: the dispatcher no longer hosts ONE
//!   job's identity permanently — each job owns a [`Seat`] (the stash of
//!   every per-identity thread-local) and every hook runs under an RAII
//!   seat bind that swaps the job's identity in and back out. The seat
//!   surface (each verified live in the bgwriter chain or by audit):
//!   pid/start-time/procno/PMChildSlot/latch globals, backend type,
//!   processing mode, lmgr MY_PROC, the procsignal thread identity (slot
//!   registration + handler table — the tables DIFFER between daemons),
//!   the interrupt pending flags (without the swap, job A's control()
//!   would consume job B's SIGTERM), the ipc exit-callback stacks
//!   (without the swap, job A's teardown would run job B's aux exit
//!   chain), the resowner cells, and the beentry binding.
//! - POOL WORKER (run_cycle): binds the generic identity half
//!   ([`CycleBind`]) + the once-per-worker BaseInit, then the daemon's
//!   `run_cycle_bound` stamps its audited GUC overlay and runs the
//!   verbatim loop body once.
//!
//! ONCE-PER-THREAD vs PER-JOB (the InitFileAccess "call me only once"
//! class): InitPostmasterChild + AuxiliaryProcessMainCommon (BaseInit) run
//! for the FIRST job lifecycle on the dispatcher thread; every subsequent
//! lifecycle — same job relaunched OR a different daemon's first start —
//! takes InitProcessGlobals + AuxiliaryProcessRejoinCommon. Likewise
//! [`ensure_worker_cycle_init`]'s per-worker flag is shared across
//! daemons here: a walwriter cycle on a worker that already BaseInit'd
//! for a bgwriter cycle must not re-run BaseInit.

#![allow(clippy::result_large_err)]

use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use bgjobs::{BgJob, Control, CycleOutcome, CycleReason};
use init_small::globals as g;
use types_core::{pid_t, BackendType, ProcNumber, ProcessingMode, INVALID_PROC_NUMBER};
use types_error::PgError;
use types_storage::latch::{Latch, LatchHandle};

#[cfg(test)]
mod tests;

/// One migrated aux daemon's non-generic half: its loop body, signal
/// table, GUC overlay, and shmem publications. Everything else (identity
/// lifecycle, announces, panic/crash legs) is [`AuxJob`]'s.
pub trait AuxDaemon: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn backend_type(&self) -> BackendType;

    /// The daemon main's pqsignal_thread table (dispatcher thread, under
    /// the job's seat — the table is part of the seat). The crash
    /// renderings (SIGQUIT/SIGABRT/SIGKILL) are installed by the shell.
    fn install_signal_handlers(&self);

    /// Post-prelude init on the dispatcher under the seat, identity
    /// acquired (MyProcNumber valid): control-state reset, shmem
    /// publications (e.g. walwriterProc), overlay capture.
    fn on_started(&self);

    /// SIGHUP processed on the dispatcher (ProcessConfigFile already ran
    /// against the dispatcher's GUC store): refresh the audited overlay.
    fn on_reload(&self);

    /// One daemon loop-body iteration on a pool worker under [`CycleBind`]
    /// (identity bound, worker BaseInit done). The implementation stamps
    /// its own GUC overlay (RAII) and returns the deadline the C loop tail
    /// would have passed to WaitLatch.
    fn run_cycle_bound(&self, procno: ProcNumber, reason: CycleReason) -> CycleOutcome;

    /// The worker-side per-thread init failed before the body could run:
    /// run the daemon's abort cleanup + state reset. The shell re-arms at
    /// the daemons' uniform 1s backoff.
    fn worker_init_failed(&self, err: &PgError);
}

// ---------------------------------------------------------------------------
// The dispatcher-side identity seat
// ---------------------------------------------------------------------------

/// The stash of every per-identity thread-local while the job is NOT the
/// dispatcher thread's bound identity. All swaps are self-inverse: bind
/// and unbind are the same field-wise exchange, so the dispatcher's
/// neutral state rides in the seat while the job is bound and is restored
/// exactly on unbind (RAII, unwind-safe — hook panics are contained by
/// the bgjobs rim and must not leak one job's identity into another's
/// hook).
pub struct Seat {
    proc_pid: pid_t,
    start_time: i64,
    start_timestamp: i64,
    proc_number: ProcNumber,
    pm_child_slot: i32,
    latch: Option<LatchHandle>,
    backend_type: BackendType,
    processing_mode: ProcessingMode,
    task_proc: ProcNumber,
    psig: procsignal::ThreadSignalIdentity,
    config_reload_pending: bool,
    shutdown_request_pending: bool,
    exits: ipc::ExitCallbackLists,
    cur_owner: types_resowner::ResourceOwner,
    aux_owner: types_resowner::ResourceOwner,
    beentry: Option<&'static backend_status::PgBackendStatus>,
}

impl Seat {
    pub fn new() -> Seat {
        Seat {
            proc_pid: 0,
            start_time: 0,
            start_timestamp: 0,
            proc_number: INVALID_PROC_NUMBER,
            pm_child_slot: 0,
            latch: None,
            backend_type: BackendType::Invalid,
            processing_mode: ProcessingMode::InitProcessing,
            task_proc: INVALID_PROC_NUMBER,
            psig: procsignal::ThreadSignalIdentity::unbound(),
            config_reload_pending: false,
            shutdown_request_pending: false,
            exits: ipc::ExitCallbackLists::empty(),
            cur_owner: types_resowner::ResourceOwner::NULL,
            aux_owner: types_resowner::ResourceOwner::NULL,
            beentry: None,
        }
    }

    /// The self-inverse exchange of every seat field with the live TLS.
    fn swap(&mut self) {
        let cur = g::MyProcPid();
        g::SetMyProcPid(self.proc_pid);
        self.proc_pid = cur;

        let cur = g::MyStartTime();
        g::SetMyStartTime(self.start_time);
        self.start_time = cur;

        let cur = g::MyStartTimestamp();
        g::SetMyStartTimestamp(self.start_timestamp);
        self.start_timestamp = cur;

        let cur = g::MyProcNumber();
        g::SetMyProcNumber(self.proc_number);
        self.proc_number = cur;

        let cur = g::MyPMChildSlot();
        g::SetMyPMChildSlot(self.pm_child_slot);
        self.pm_child_slot = cur;

        let cur = g::MyLatch();
        g::SetMyLatch(self.latch);
        self.latch = cur;

        let cur = miscinit::GetMyBackendType();
        miscinit::SetMyBackendType(self.backend_type);
        self.backend_type = cur;

        let cur = miscinit::GetProcessingMode();
        miscinit::SetProcessingMode(self.processing_mode);
        self.processing_mode = cur;

        self.task_proc = lmgr_proc::bind_task_proc(self.task_proc);

        procsignal::swap_thread_signal_identity(&mut self.psig);

        let cur = interrupt::ConfigReloadPending();
        interrupt::SetConfigReloadPending(self.config_reload_pending);
        self.config_reload_pending = cur;

        let cur = interrupt::ShutdownRequestPending();
        interrupt::SetShutdownRequestPending(self.shutdown_request_pending);
        self.shutdown_request_pending = cur;

        ipc::swap_exit_callback_lists(&mut self.exits);

        let cur = resowner::CurrentResourceOwner();
        resowner::SetCurrentResourceOwner(self.cur_owner);
        self.cur_owner = cur;

        let cur = resowner::AuxProcessResourceOwner();
        resowner::SetAuxProcessResourceOwner(self.aux_owner);
        self.aux_owner = cur;

        backend_status::swap_my_beentry(&mut self.beentry);
    }

    /// Bind the seat onto the dispatcher thread (RAII; Drop unbinds).
    fn bind(&mut self) -> SeatBind<'_> {
        self.swap();
        // Wait-event attribution while bound (LWLock waits inside a hook,
        // e.g. ProcessConfigFile): point the thread's wait-event storage
        // at the bound identity's PGPROC. Startup binds with procno still
        // INVALID; InitAuxiliaryProcess sets the storage itself then.
        let procno = g::MyProcNumber();
        if procno != INVALID_PROC_NUMBER
            && waitevent_seams::pgstat_set_wait_event_storage::is_installed()
        {
            waitevent_seams::pgstat_set_wait_event_storage::call(
                &lmgr_proc::GetPGProcByNumber(procno).wait_event_info,
            );
        }
        SeatBind { seat: self }
    }
}

impl Default for Seat {
    fn default() -> Self {
        Seat::new()
    }
}

struct SeatBind<'a> {
    seat: &'a mut Seat,
}

impl Drop for SeatBind<'_> {
    fn drop(&mut self) {
        self.seat.swap();
        // Neutral dispatcher state: wait events report nowhere (the reset
        // storage), never against a job the thread is not bound to.
        if waitevent_seams::pgstat_reset_wait_event_storage::is_installed() {
            waitevent_seams::pgstat_reset_wait_event_storage::call();
        }
    }
}

// ---------------------------------------------------------------------------
// Shared dispatcher-thread statics
// ---------------------------------------------------------------------------

/// Crash-fanout flag: SIGQUIT/SIGABRT/SIGKILL thread-signal renderings land
/// here (handlers run on the dispatcher during DrainThreadSignals, under
/// the target job's seat). SHARED across jobs and safe: control() drains
/// one job's pending word and consumes this flag immediately after, on the
/// single dispatcher thread — no other job's drain can interleave.
static CRASH_PENDING: AtomicBool = AtomicBool::new(false);

fn note_crash() {
    CRASH_PENDING.store(true, Ordering::SeqCst);
}

thread_local! {
    /// The once-per-THREAD child half on the dispatcher: InitPostmasterChild
    /// + AuxiliaryProcessMainCommon (BaseInit — "call me only once"). Shared
    /// across ALL aux jobs hosted by this thread; per-job lifecycles take
    /// the InitProcessGlobals + AuxiliaryProcessRejoinCommon half.
    static THREAD_CHILD_INITED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };

    /// Once-per-worker BaseInit (the thread-infrastructure half a backend
    /// prelude provides), shared across ALL aux daemons' cycles on that
    /// worker.
    static WORKER_BASE_INITED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Once-per-worker thread infrastructure (BaseInit's half): pool workers
/// never ran a backend prelude — the first aux-job cycle on a worker
/// initializes the VFD cache, smgr, buffer access, sync and xloginsert
/// scratch (found live in the bgwriter chain: AllocateVfd "InitFileAccess
/// not called?"; fleet job ...-0f92). Needs the identity bind (BaseInit
/// asserts MyProc). The wait-event storage BaseInit points at the bound
/// PGPROC is reset afterward so waits of FOREIGN tasks later scheduled on
/// this worker are not attributed to the daemon's pg_stat_activity row.
pub fn ensure_worker_cycle_init() -> Result<(), Box<PgError>> {
    if WORKER_BASE_INITED.get() {
        return Ok(());
    }
    postinit::BaseInit()?;
    if waitevent_seams::pgstat_reset_wait_event_storage::is_installed() {
        waitevent_seams::pgstat_reset_wait_event_storage::call();
    }
    WORKER_BASE_INITED.set(true);
    Ok(())
}

// ---------------------------------------------------------------------------
// The worker-side cycle identity bind
// ---------------------------------------------------------------------------

/// RAII identity bind for one cycle task on a pool worker (§3.4): identity
/// TLS, LIFO restore. GUC overlay stamping is the daemon's own RAII inside
/// `run_cycle_bound` (the audited set differs per daemon).
pub struct CycleBind {
    prev_pid: pid_t,
    prev_procno: ProcNumber,
    prev_task_proc: ProcNumber,
    prev_latch: Option<LatchHandle>,
    prev_btype: BackendType,
    prev_cur_owner: types_resowner::ResourceOwner,
    prev_aux_owner: types_resowner::ResourceOwner,
    /// Per-cycle resource owner created in THE WORKER'S arena (resowner
    /// arenas are thread-local — a dispatcher-created owner handle is not
    /// portable across threads; found live: "stale ResourceOwner (slot out
    /// of range)", fleet job ...-261e). Released + deleted at unbind;
    /// C-equivalent because the aux owner's cycle-time content (buffer
    /// pins) is balanced within one loop body.
    cycle_owner: types_resowner::ResourceOwner,
}

impl CycleBind {
    pub fn bind(pid: pid_t, procno: ProcNumber, btype: BackendType) -> CycleBind {
        let cycle_owner =
            resowner::ResourceOwnerCreate(types_resowner::ResourceOwner::NULL, "AuxJobCycle")
                .expect("cycle resource owner");
        let prev = CycleBind {
            prev_pid: g::MyProcPid(),
            prev_procno: g::MyProcNumber(),
            prev_task_proc: lmgr_proc::bind_task_proc(procno),
            prev_latch: g::MyLatch(),
            prev_btype: miscinit::GetMyBackendType(),
            prev_cur_owner: resowner::CurrentResourceOwner(),
            prev_aux_owner: resowner::AuxProcessResourceOwner(),
            cycle_owner,
        };
        g::SetMyProcPid(pid);
        g::SetMyProcNumber(procno);
        g::SetMyLatch(Some(LatchHandle::proc(procno)));
        miscinit::SetMyBackendType(btype);
        // Buffer pins and the error leg's ReleaseAuxProcessResources route
        // through the per-cycle owner (found live: worker
        // ResourceOwnerEnlarge on the NULL owner, fleet job ...-24fb).
        resowner::SetCurrentResourceOwner(cycle_owner);
        resowner::SetAuxProcessResourceOwner(cycle_owner);
        prev
    }
}

impl Drop for CycleBind {
    fn drop(&mut self) {
        // Restore the owner cells FIRST (ResourceOwnerDelete asserts the
        // deleted owner is not current), then drain + delete the per-cycle
        // owner (a clean cycle leaves it empty — pins are balanced within
        // one body; the error leg's abort cleanup already released through
        // it).
        resowner::SetAuxProcessResourceOwner(self.prev_aux_owner);
        resowner::SetCurrentResourceOwner(self.prev_cur_owner);
        use types_resowner::ResourceReleasePhase::{
            RESOURCE_RELEASE_AFTER_LOCKS, RESOURCE_RELEASE_BEFORE_LOCKS, RESOURCE_RELEASE_LOCKS,
        };
        for phase in [
            RESOURCE_RELEASE_BEFORE_LOCKS,
            RESOURCE_RELEASE_LOCKS,
            RESOURCE_RELEASE_AFTER_LOCKS,
        ] {
            if let Err(e) = resowner::ResourceOwnerRelease(self.cycle_owner, phase, false, true) {
                elog::emit_error_report_for(&e);
            }
        }
        resowner::ResourceOwnerDelete(self.cycle_owner);
        miscinit::SetMyBackendType(self.prev_btype);
        g::SetMyLatch(self.prev_latch);
        lmgr_proc::unbind_task_proc(self.prev_task_proc);
        g::SetMyProcNumber(self.prev_procno);
        g::SetMyProcPid(self.prev_pid);
    }
}

// ---------------------------------------------------------------------------
// The generic job shell
// ---------------------------------------------------------------------------

/// A migrated aux daemon as a [`bgjobs::BgJob`]: the generic identity
/// lifecycle around an [`AuxDaemon`]'s body.
pub struct AuxJob<D: AuxDaemon> {
    pid: pid_t,
    /// The pmchild slot StartChildProcess assigned
    /// (register_postmaster_child_active keys on it during
    /// InitAuxiliaryProcess).
    child_slot: i32,
    /// Set by startup() (aux PGPROC acquisition); INVALID before.
    procno: AtomicI32,
    seat: Mutex<Seat>,
    daemon: D,
}

impl<D: AuxDaemon> AuxJob<D> {
    pub fn new(pid: pid_t, child_slot: i32, daemon: D) -> AuxJob<D> {
        AuxJob {
            pid,
            child_slot,
            procno: AtomicI32::new(INVALID_PROC_NUMBER),
            seat: Mutex::new(Seat::new()),
            daemon,
        }
    }

    pub fn daemon(&self) -> &D {
        &self.daemon
    }

    fn procno(&self) -> ProcNumber {
        self.procno.load(Ordering::Acquire)
    }

    fn announce(&self, exitstatus: i32) {
        if postmaster_seams::announce_child_exit::is_installed() {
            postmaster_seams::announce_child_exit::call(self.pid, exitstatus);
        }
    }
}

impl<D: AuxDaemon> BgJob for AuxJob<D> {
    fn name(&self) -> &'static str {
        self.daemon.name()
    }

    fn latch(&self) -> Option<&'static Latch> {
        let procno = self.procno();
        (procno != INVALID_PROC_NUMBER)
            .then(|| &lmgr_proc::GetPGProcByNumber(procno).procLatch)
    }

    /// The daemon main's prelude, on the dispatcher thread under the job's
    /// seat.
    fn startup(&self) -> Result<(), Box<PgError>> {
        let mut seat = self.seat.lock().unwrap();
        let _bound = seat.bind();
        // Per-lifecycle reset of the state a C child gets fresh from fork
        // (all under the seat, so only THIS job's state is touched — the
        // exit-list reset in particular must never see another job's
        // registrations).
        ipc::on_exit_reset();
        miscinit::SetProcessingMode(ProcessingMode::InitProcessing);
        // Stale-identity clear: a crash-ABANDONED lifecycle leaves the seat
        // pointing into pre-reset shared memory (the clean path clears
        // these in AuxiliaryProcKill via teardown's shmem_exit). Without
        // this, every post-crash relaunch dies at InitAuxiliaryProcess
        // "you already exist" and the postmaster crash-loops hot
        // (observed: fleet job ...-42c3).
        if g::MyProcNumber() != INVALID_PROC_NUMBER
            && g::MyLatch() == Some(LatchHandle::proc(g::MyProcNumber()))
        {
            // Abandoned lifecycle left the shared latch bound; restore the
            // thread-local latch through the standard helper (its asserts
            // require MyProcNumber still set — order before the clears;
            // NEVER SetMyLatch(None): SwitchToSharedLatch on the next
            // lifecycle asserts the local latch is current).
            miscinit::SwitchBackToLocalLatch();
        }
        let _ = lmgr_proc::bind_task_proc(INVALID_PROC_NUMBER);
        g::SetMyProcNumber(INVALID_PROC_NUMBER);
        // Owner cells are never cleared by release; a stale value trips
        // CreateAuxProcessResourceOwner's fresh-lifecycle asserts.
        resowner::SetCurrentResourceOwner(types_resowner::ResourceOwner::NULL);
        resowner::SetAuxProcessResourceOwner(types_resowner::ResourceOwner::NULL);
        let first_on_thread = !THREAD_CHILD_INITED.get();
        if first_on_thread {
            if let Err(e) = miscinit::InitPostmasterChild(self.pid) {
                self.announce(1 << 8);
                return Err(e);
            }
            THREAD_CHILD_INITED.set(true);
        } else {
            miscinit::InitProcessGlobals(self.pid);
            if g::MyLatch().is_none() {
                // A different daemon's FIRST lifecycle on this thread: the
                // fresh seat carries no latch. Point MyLatch at the
                // thread's (shared, allocate-once) local latch —
                // SwitchToSharedLatch inside the aux prelude asserts it is
                // current.
                miscinit::InitProcessLocalLatch();
            }
        }
        g::SetMyPMChildSlot(self.child_slot);
        miscinit::SetMyBackendType(self.daemon.backend_type());
        // First lifecycle ON THIS THREAD runs the full aux main prelude;
        // every other (same job relaunched, or another daemon's first
        // start) runs its per-lifecycle half only (BaseInit's
        // InitFileAccess etc. are once-per-thread: "call me only once").
        let prelude = if first_on_thread {
            auxprocess::AuxiliaryProcessMainCommon()
        } else {
            auxprocess::AuxiliaryProcessRejoinCommon()
        };
        if let Err(e) = prelude {
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

        self.daemon.install_signal_handlers();
        {
            use procsignal::ThreadSignalHandler::Simple;
            // Crash-fanout renderings (thread daemons die by unwind; a
            // threadless job converts them into Abandon at control()).
            procsignal::pqsignal_thread(procsignal::signums::SIGQUIT, Simple(note_crash));
            procsignal::pqsignal_thread(procsignal::signums::SIGABRT, Simple(note_crash));
            procsignal::pqsignal_thread(procsignal::signums::SIGKILL, Simple(note_crash));
        }

        self.daemon.on_started();
        Ok(())
    }

    /// Signal/reload/shutdown processing on the dispatcher, under the
    /// job's seat.
    fn control(&self) -> Control {
        let mut seat = self.seat.lock().unwrap();
        let _bound = seat.bind();
        let _ = procsignal::DrainThreadSignals();
        if CRASH_PENDING.swap(false, Ordering::SeqCst) {
            // KilledBySignal announce shape: raw signo (WTERMSIG). The
            // identity is ABANDONED (shmem resets wholesale); stop
            // pointing at the doomed PGPROC.
            self.procno.store(INVALID_PROC_NUMBER, Ordering::Release);
            self.announce(procsignal::signums::SIGQUIT);
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
            self.daemon.on_reload();
        }
        if interrupt::ShutdownRequestPending() {
            interrupt::SetShutdownRequestPending(false);
            return Control::Exit;
        }
        Control::Continue
    }

    /// Clean exit on the dispatcher under the seat: the job's aux
    /// shmem-exit chain (ShutdownAuxiliaryProcess, beentry shutdown,
    /// AuxiliaryProcKill, CleanupProcSignalState) + the normal-exit
    /// announce. The reaper's join is a CHILD_THREADS lookup miss —
    /// nothing to join.
    fn teardown(&self) {
        let mut seat = self.seat.lock().unwrap();
        let _bound = seat.bind();
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
        self.announce(procsignal::signums::SIGABRT);
    }

    fn run_cycle(&self, reason: CycleReason) -> CycleOutcome {
        let procno = self.procno();
        let _bind = CycleBind::bind(self.pid, procno, self.daemon.backend_type());
        if let Err(e) = ensure_worker_cycle_init() {
            self.daemon.worker_init_failed(&e);
            return CycleOutcome::Sleep(Duration::from_secs(1));
        }
        self.daemon.run_cycle_bound(procno, reason)
    }
}
