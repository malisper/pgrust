//! Worker pool driver: `cores + K` threads running the worker loop against
//! one [`Runtime`].
//!
//! Layering: this module owns only the LOOP and (for tests) std-thread
//! spawning + join. Production spawning goes through launch_backend's
//! rtpool glue (synthetic pids via the guarded reserve_child_pid counter,
//! wpool-style warm prelude, standard child stack size) which calls
//! [`WorkerPool::spawn_with`] with its own spawner. Runtime workers are
//! EXECUTORS, not sessions: no PGPROC, no bgworker slot, no reaper entry,
//! and — deliberately — NO wakeup-registry entry (they park on the
//! runtime's eventcount, not a latch), so the pid-collision/registry-hijack
//! wedge class (notes/pardistinct-contention-fix.md) has no surface here.
//! Any FUTURE latch use by pool threads must follow the rekey discipline
//! (waiteventset_seams::rekey_wakeup_registry) — documented, not needed.

use std::io;
use std::sync::Arc;
use std::thread::JoinHandle;

use crate::sched::Step;
use crate::Runtime;

/// The worker loop. Permit discipline: a permit is held across the whole
/// scheduling step (superset of task execution — satisfies "any
/// task-executing thread holds one"), released before parking so an idle
/// worker never caps a busy one; a task that blocks inside the step may
/// donate the permit to a standby through the §2.8 seams (crate::io).
/// Park discipline: the epoch is captured BEFORE the step, so a publish
/// between a failed pick and the park is never lost.
///
/// §2.9 uring duties: ring created at loop start / torn down at loop exit,
/// and a non-blocking boundary reap after EVERY step — completions run
/// (IoToken unpark-all) and issuer pins drop before the worker runs its
/// next task or parks.
pub fn worker_loop(rt: &Arc<Runtime>, worker: usize) {
    let mut local = rt.worker_local(worker);
    let ring = crate::io::worker_enter(rt);
    rt.register_worker_ring(worker, ring);
    // Register this thread for the spill blocking-section facade (M3.5
    // §6.1): spill code inside task bodies may donate this thread's permit
    // to a standby for the duration of a blocking syscall region.
    // SAFETY: the permit semaphore lives in `rt`, which outlives this loop;
    // blocking_io_section is only reachable from task bodies inside
    // worker_step, where the permit is held.
    let _blocking_reg = unsafe { crate::blocking::PermitThreadReg::new(rt.execution_permits()) };
    if crate::sched::step_v2() {
        // STEP-V2 (agg192-contention, 48xl finding #1): permit held ACROSS
        // steps (released around parks and at exit) — the per-step global
        // semaphore acquire/release was a shared-line hot spot at high DOP —
        // and Retry parks after a bounded spin instead of spinning the whole
        // invalidated-slot window (publish/completion wake_all; the epoch is
        // captured before the failing step, so the park is lost-wakeup-free).
        // `PGRUST_RUNTIME_STEP_V2=0` restores the loop below.
        let mut held = false;
        let mut retries = 0u32;
        // GL-STMTTASK-2 change 4 (PGRUST_POOL_WAKE_SPINNER, default OFF):
        // this worker's directed-wake slot + search-phase accounting. The
        // spinner mark spans wake→claim (cleared at worker_step's claim
        // points, consumed by the directed park); submissions elide their
        // notify while any worker is in that window (ParkLot::wake_work).
        let spinner_mode = rt.wake_spinner_armed();
        let parker = std::sync::Arc::new(pgsync::WorkerParker::new());
        if spinner_mode {
            rt.spin_enter_worker(&mut local);
        }
        loop {
            let epoch = rt.park_epoch();
            if !held {
                // Flip fix: the loop-top acquire can BLOCK (permits held
                // by inline statements / suspended bodies) — a blocked
                // thread is not a searcher; drop the mark for the wait and
                // re-arm after (submissions during the wait then wake a
                // parked worker instead of eliding against us).
                if spinner_mode {
                    rt.spin_abandon_worker(&mut local);
                }
                rt.execution_permits().acquire();
                crate::io::note_permit(true);
                held = true;
                if spinner_mode && !local.spinning() {
                    rt.spin_enter_worker(&mut local);
                }
            }
            let step = rt.worker_step(&mut local);
            // Task boundary (§2.9): drain this worker's CQEs non-blockingly.
            crate::io::boundary_reap();
            match step {
                Step::Ran => {
                    retries = 0;
                    // Post-task: the worker is searching again (its next
                    // step re-scans) — re-arm the spinner mark so the
                    // between-tasks window elides submission wakes too.
                    if spinner_mode && !local.spinning() {
                        rt.spin_enter_worker(&mut local);
                    }
                }
                Step::Retry => {
                    local.drive.steps_retry += 1;
                    retries += 1;
                    if retries >= crate::sched::RETRY_PARK_AFTER {
                        retries = 0;
                        crate::io::note_permit(false);
                        rt.execution_permits().release();
                        held = false;
                        if spinner_mode {
                            rt.park_worker_directed(epoch, &parker, &mut local);
                        } else {
                            rt.park(epoch);
                        }
                    } else {
                        std::thread::yield_now();
                    }
                }
                Step::Idle => {
                    retries = 0;
                    crate::io::note_permit(false);
                    rt.execution_permits().release();
                    held = false;
                    if spinner_mode {
                        rt.park_worker_directed(epoch, &parker, &mut local);
                    } else {
                        rt.park(epoch);
                    }
                }
                Step::Stop => break,
            }
        }
        if held {
            crate::io::note_permit(false);
            rt.execution_permits().release();
        }
    } else {
        loop {
            let epoch = rt.park_epoch();
            rt.execution_permits().acquire();
            crate::io::note_permit(true);
            let step = rt.worker_step(&mut local);
            crate::io::note_permit(false);
            rt.execution_permits().release();
            // Task boundary (§2.9): drain this worker's CQEs non-blockingly.
            crate::io::boundary_reap();
            match step {
                Step::Ran => {}
                // Retry: the coordinator is mid-invalidation (slot word
                // invalid, active bit not yet cleared) — a transient window;
                // yield rather than burn the permit path (and the loom models
                // require the no-lone-progress hint on exactly this edge).
                Step::Retry => std::thread::yield_now(),
                Step::Idle => rt.park(epoch),
                Step::Stop => break,
            }
        }
    }
    rt.register_worker_ring(worker, None);
    crate::io::worker_exit();
}

/// Handle to a spawned pool (join-able; tests and clean shutdown).
pub struct WorkerPool {
    rt: Arc<Runtime>,
    handles: Vec<JoinHandle<()>>,
}

impl WorkerPool {
    /// Spawn `nthreads = workers + standbys` loop threads with plain std
    /// threads (tests; production uses [`WorkerPool::spawn_with`] via
    /// launch_backend::rtpool).
    pub fn spawn_std(rt: Arc<Runtime>) -> io::Result<WorkerPool> {
        Self::spawn_with(rt, |ordinal, body| {
            std::thread::Builder::new()
                .name(format!("pgrust-rtworker-{ordinal}"))
                .spawn(body)
        })
    }

    /// Spawn through a caller-provided spawner (launch_backend's rtpool
    /// passes one that reserves synthetic pids and applies the child
    /// prelude). The spawner returns a JoinHandle so shutdown can join.
    pub fn spawn_with(
        rt: Arc<Runtime>,
        mut spawn: impl FnMut(usize, Box<dyn FnOnce() + Send>) -> io::Result<JoinHandle<()>>,
    ) -> io::Result<WorkerPool> {
        let n = rt.nthreads();
        let mut handles = Vec::with_capacity(n);
        for ordinal in 0..n {
            let rt2 = Arc::clone(&rt);
            handles.push(spawn(ordinal, Box::new(move || worker_loop(&rt2, ordinal)))?);
        }
        Ok(WorkerPool { rt, handles })
    }

    pub fn runtime(&self) -> &Arc<Runtime> {
        &self.rt
    }

    /// Stop and join all workers (tests / clean shutdown; the production
    /// pool is process-lifetime and never calls this).
    pub fn shutdown(mut self) {
        self.rt.request_stop();
        for h in self.handles.drain(..) {
            let _ = h.join();
        }
    }
}
