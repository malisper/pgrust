//! The walwriter JOB DRIVER (M4, docs/design/m4-bgjobs.md §4 row 2): the
//! same [`crate::walwriter_cycle`] body the thread driver runs, expressed
//! as an [`auxjob::AuxDaemon`] — cycles execute on pool workers under
//! Maintenance RGs; identity, signals, config reloads, and teardown live
//! on the dispatcher thread under the job's seat.
//!
//! What is walwriter's own here:
//! - The audited GUC overlay the cycle body reads (docs/design §3.1):
//!   wal_writer_delay + wal_writer_flush_after (XLogBackgroundFlush
//!   pacing), track_wal_io_timing + track_io_timing (pgstat WAL/IO flush
//!   timing), fsync + wal_sync_method (the XLogWrite durability leg —
//!   wal_sync_method stamps through the assign-hook semantics so a changed
//!   method closes the worker's open WAL segment first).
//! - HIBERNATION is the C timeout stretch (×25) + the shared-memory
//!   sleeping flag, both computed INSIDE the shared cycle body from
//!   per-job state — no dispatcher-side FSM (unlike bgwriter's
//!   hibernate-entry dispatch). The async-commit wake
//!   (XLogSetAsyncXactLSN → SetLatch(walwriterProc latch)) rides the
//!   dispatcher latch redirect with no caller change.
//! - walwriterProc publication at startup (C loop-prologue equivalent);
//!   left stale at exit exactly as C leaves it (teardown releases the
//!   PGPROC through the same aux exit chain C's proc_exit runs).
//! - The error leg force-flushes pending WAL stats so counters accumulated
//!   before the error cannot strand on this worker's TLS (the next cycle
//!   may run elsewhere; C's next-cycle flush assumes one thread).

use std::sync::atomic::Ordering::Relaxed;
use std::sync::Mutex;
use std::time::Duration;

use bgjobs::{CycleOutcome, CycleReason};
use init_small::globals as g;
use types_core::{pid_t, ProcNumber};
use types_error::PgError;

use crate::{walwriter_cycle, WalWriterState, LOOPS_UNTIL_HIBERNATE};

/// The walwriter job: the generic aux-job shell around [`WalWriterDaemon`].
pub type WalWriterJob = auxjob::AuxJob<WalWriterDaemon>;

pub fn new_walwriter_job(pid: pid_t, child_slot: i32) -> WalWriterJob {
    auxjob::AuxJob::new(pid, child_slot, WalWriterDaemon::new())
}

/// The audited GUC set the cycle body reads (docs/design/m4-bgjobs.md
/// §3.1): captured on the dispatcher (whose TLS ProcessConfigFile keeps
/// current) and stamped into the executing worker's cells at cycle entry.
#[derive(Clone, Copy)]
struct Overlay {
    delay_ms: i32,
    flush_after: i32,
    track_wal_io_timing: bool,
    track_io_timing: bool,
    fsync: bool,
    wal_sync_method: i32,
}

impl Overlay {
    /// Read on the dispatcher thread (job startup + every reload).
    fn capture() -> Overlay {
        use guc_tables::vars as v;
        Overlay {
            delay_ms: v::WalWriterDelay.read(),
            flush_after: v::WalWriterFlushAfter.read(),
            track_wal_io_timing: v::track_wal_io_timing.read(),
            track_io_timing: v::track_io_timing.read(),
            fsync: g::enableFsync(),
            wal_sync_method: v::wal_sync_method.read(),
        }
    }
}

/// RAII overlay stamp on the executing worker (LIFO restore).
struct OverlayStamp {
    prev: Overlay,
}

impl OverlayStamp {
    fn stamp(overlay: &Overlay) -> OverlayStamp {
        let prev = Overlay::capture();
        Self::write(overlay);
        OverlayStamp { prev }
    }

    fn write(o: &Overlay) {
        use guc_tables::vars as v;
        v::WalWriterDelay.write(o.delay_ms);
        v::WalWriterFlushAfter.write(o.flush_after);
        v::track_wal_io_timing.write(o.track_wal_io_timing);
        v::track_io_timing.write(o.track_io_timing);
        g::set_enableFsync(o.fsync);
        // Assign-hook semantics: a changed method fsyncs + closes this
        // worker's open WAL segment before the cell write.
        transam_xlog::stamp_wal_sync_method(o.wal_sync_method);
    }
}

impl Drop for OverlayStamp {
    fn drop(&mut self) {
        Self::write(&self.prev);
    }
}

pub struct WalWriterDaemon {
    state: Mutex<WalWriterState>,
    overlay: Mutex<Overlay>,
}

impl WalWriterDaemon {
    fn new() -> WalWriterDaemon {
        WalWriterDaemon {
            state: Mutex::new(WalWriterState::new()),
            overlay: Mutex::new(Overlay {
                delay_ms: 200,
                flush_after: crate::DEFAULT_WAL_WRITER_FLUSH_AFTER,
                track_wal_io_timing: false,
                track_io_timing: false,
                fsync: true,
                wal_sync_method: 0,
            }),
        }
    }

    fn refresh_overlay(&self) {
        *self.overlay.lock().unwrap() = Overlay::capture();
    }
}

impl auxjob::AuxDaemon for WalWriterDaemon {
    fn name(&self) -> &'static str {
        "walwriter"
    }

    fn backend_type(&self) -> types_core::BackendType {
        types_core::BackendType::WalWriter
    }

    fn install_signal_handlers(&self) {
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

    /// C WalWriterMain's loop prologue (walwriter.c:206-216), on the
    /// dispatcher under the seat with the aux identity acquired: reset
    /// hibernation, clear the shared sleeping flag, advertise our proc
    /// number for async-commit wakes.
    fn on_started(&self) {
        {
            let mut st = self.state.lock().unwrap();
            *st = WalWriterState::new();
        }
        transam_xlog::SetWalWriterSleeping(false);
        lmgr_proc::ProcGlobal().walwriterProc.store(g::MyProcNumber(), Relaxed);
        self.refresh_overlay();
    }

    fn on_reload(&self) {
        self.refresh_overlay();
    }

    fn worker_init_failed(&self, err: &PgError) {
        crate::abort_cleanup(err);
        let mut st = self.state.lock().unwrap();
        st.left_till_hibernate = LOOPS_UNTIL_HIBERNATE;
        st.hibernating = false;
        transam_xlog::SetWalWriterSleeping(false);
    }

    fn run_cycle_bound(&self, _procno: ProcNumber, _reason: CycleReason) -> CycleOutcome {
        let overlay = *self.overlay.lock().unwrap();
        let _stamp = OverlayStamp::stamp(&overlay);
        let mut st = self.state.lock().unwrap();
        match walwriter_cycle(&mut st) {
            Ok(timeout_ms) => CycleOutcome::Sleep(Duration::from_millis(timeout_ms.max(1) as u64)),
            Err(err) => {
                // The daemons' uniform error leg, minus the inline sleep
                // (the backoff is the re-arm deadline) — plus the pending-
                // WAL-stats force flush (module doc).
                crate::abort_cleanup(&err);
                if pgstat_seams::pgstat_report_wal::is_installed() {
                    pgstat_seams::pgstat_report_wal::call(true);
                }
                st.left_till_hibernate = LOOPS_UNTIL_HIBERNATE;
                st.hibernating = false;
                transam_xlog::SetWalWriterSleeping(false);
                CycleOutcome::Sleep(Duration::from_secs(1))
            }
        }
    }
}
