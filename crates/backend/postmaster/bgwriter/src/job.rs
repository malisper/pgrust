//! The bgwriter JOB DRIVER (M4 increment 4, docs/design/m4-bgjobs.md
//! §3.2-§3.4): the same [`crate::bgwriter_cycle`] body the thread driver
//! runs, expressed as an [`auxjob::AuxDaemon`] — cycles execute on pool
//! workers under Maintenance RGs; identity, signals, config reloads, and
//! teardown live on the dispatcher thread under the job's SEAT (the
//! multi-job envelope swap — auxjob owns the generic lifecycle; this
//! module owns only what is bgwriter's: the audited GUC overlay, the
//! hibernation FSM, and the loop body).
//!
//! HIBERNATION is a small FSM here (armed → hibernating), C-equivalent:
//! entering hibernation is its own dispatch (no body run — exactly C's
//! "first delay elapsed, no wake"), publishes the procno through
//! StrategyNotifyBgWriter so allocation-side SetLatch wakes ride the
//! ordinary latch redirect, and the next dispatch retracts it.

use std::sync::Mutex;
use std::time::Duration;

use bgjobs::{CycleOutcome, CycleReason};
use types_core::{pid_t, ProcNumber};
use types_error::PgError;

use crate::{bgwriter_cycle, BgWriterState, HIBERNATE_FACTOR};

/// The bgwriter job: the generic aux-job shell around [`BgWriterDaemon`].
pub type BgWriterJob = auxjob::AuxJob<BgWriterDaemon>;

pub fn new_bgwriter_job(pid: pid_t, child_slot: i32) -> BgWriterJob {
    auxjob::AuxJob::new(pid, child_slot, BgWriterDaemon::new())
}

/// The audited GUC set the cycle body reads (docs/design/m4-bgjobs.md
/// §3.1): captured on the dispatcher (whose TLS ProcessConfigFile keeps
/// current) and stamped into the executing worker's cells at cycle entry.
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
        v::BgWriterDelay.write(o.delay_ms);
        v::bgwriter_lru_maxpages.write(o.lru_maxpages);
        v::bgwriter_lru_multiplier.write(o.lru_multiplier);
        v::bgwriter_flush_after.write(o.flush_after);
        v::track_io_timing.write(o.track_io_timing);
    }
}

impl Drop for OverlayStamp {
    fn drop(&mut self) {
        Self::write(&self.prev);
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

pub struct BgWriterDaemon {
    state: Mutex<JobState>,
    overlay: Mutex<Overlay>,
}

impl BgWriterDaemon {
    fn new() -> BgWriterDaemon {
        BgWriterDaemon {
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

    fn refresh_overlay(&self) {
        *self.overlay.lock().unwrap() = Overlay::capture();
    }
}

impl auxjob::AuxDaemon for BgWriterDaemon {
    fn name(&self) -> &'static str {
        "bgwriter"
    }

    fn backend_type(&self) -> types_core::BackendType {
        types_core::BackendType::BgWriter
    }

    fn install_signal_handlers(&self) {
        use procsignal::ThreadSignalHandler::{Ignore, Simple};
        procsignal::pqsignal_thread(procsignal::signums::SIGHUP, Simple(interrupt::SignalHandlerForConfigReload));
        procsignal::pqsignal_thread(procsignal::signums::SIGINT, Ignore);
        procsignal::pqsignal_thread(
            procsignal::signums::SIGTERM,
            Simple(interrupt::SignalHandlerForShutdownRequest),
        );
        procsignal::pqsignal_thread(procsignal::signums::SIGALRM, Ignore);
        procsignal::pqsignal_thread(procsignal::signums::SIGPIPE, Ignore);
        procsignal::pqsignal_thread(procsignal::signums::SIGUSR2, Ignore);
    }

    fn on_started(&self) {
        let mut st = self.state.lock().unwrap();
        st.inner = BgWriterState::new();
        st.inner.last_snapshot_ts = timestamp_seams::get_current_timestamp::call();
        st.inner.sync.reset_writeback_context();
        st.armed = false;
        st.hibernating = false;
        drop(st);
        self.refresh_overlay();
    }

    fn on_reload(&self) {
        self.refresh_overlay();
    }

    fn worker_init_failed(&self, err: &PgError) {
        crate::abort_cleanup(err);
        let mut st = self.state.lock().unwrap();
        st.inner.prev_hibernate = false;
        st.armed = false;
    }

    fn run_cycle_bound(&self, procno: ProcNumber, reason: CycleReason) -> CycleOutcome {
        let overlay = *self.overlay.lock().unwrap();
        let _stamp = OverlayStamp::stamp(&overlay);
        let mut st = self.state.lock().unwrap();
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
