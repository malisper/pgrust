//! §2.9 uring duties of the pool worker loop (M1 lane C wiring):
//!
//! - RING LIFECYCLE: each worker creates its own io_uring at loop start
//!   (`aio_seams::uring_worker_ring_init` — owner-submits-only; the ring is
//!   marked boundary-reaped so WaitIO waiters may park on IoTokens) and
//!   tears it down at loop exit. The ring id registers with the runtime
//!   worker struct ([`crate::Runtime::worker_ring`]).
//! - BOUNDARY REAPING: at every task boundary the loop calls
//!   `uring_boundary_reap` — a non-blocking CQE drain; completions run
//!   (bufmgr io_wref clear + TerminateBufferIO + IoToken complete → Waiter
//!   unpark-all) and collected issuer pins drop. Completions ride the
//!   existing ~1-2ms task cadence.
//! - IoGuard SEAMS: this module installs `aio_seams::io_permit_release` /
//!   `io_permit_reacquire` — the §2.8 declared-blocking-section hooks. A
//!   pool worker that blocks inside a task (first caller: aio_uring's
//!   genuinely-pending `uring_buf_read_wait` paths, AFTER the peek-complete
//!   elision) releases its execution permit so a standby absorbs the core,
//!   and reacquires it when the wait ends. Capacity moves; the task stays
//!   on its thread with its pin-board entry and finalization-marker
//!   obligations intact (see [`crate::sync::Semaphore::io_section`], the
//!   in-crate RAII form of the same discipline — the seam pair is its
//!   split enter/exit shape, split because the release and the reacquire
//!   happen on opposite sides of a foreign blocking wait).
//!
//! Inert by default: this module only runs on pool worker threads, the pool
//! only spawns under `PGRUST_RUNTIME=1`, and every seam call is
//! `is_installed`-guarded (aio_uring absent — e.g. unit tests, non-Linux —
//! degrades to no-ops).

use std::cell::Cell;
use std::sync::Arc;

use crate::Runtime;

thread_local! {
    /// The runtime whose worker loop runs on this thread (pool workers
    /// only; None everywhere else). Holds the Arc so the semaphore the
    /// permit seams touch can never dangle, even for test pools.
    static WORKER_RT: std::cell::RefCell<Option<Arc<Runtime>>> =
        const { std::cell::RefCell::new(None) };
    /// This worker currently holds an execution permit (set by the loop
    /// around worker_step; flipped by the permit seams inside a declared
    /// blocking section).
    static PERMIT_HELD: Cell<bool> = const { Cell::new(false) };
    /// Inside a declared blocking section (released, not yet reacquired).
    /// Guards against nested release (one permit, one release).
    static IN_IO_SECTION: Cell<bool> = const { Cell::new(false) };
}

/// Worker-loop entry: install the permit seams (process-wide, once), bind
/// this thread to `rt`, and create this worker's ring. Returns the ring id
/// for registration with the runtime worker struct (None: uring
/// unavailable or aio_uring not linked).
pub(crate) fn worker_enter(rt: &Arc<Runtime>) -> Option<u32> {
    static INSTALL: crate::sync::Once = crate::sync::Once::new();
    INSTALL.call_once(|| {
        aio_seams::io_permit_release::set(io_permit_release);
        aio_seams::io_permit_reacquire::set(io_permit_reacquire);
    });
    WORKER_RT.with(|c| *c.borrow_mut() = Some(Arc::clone(rt)));
    if !aio_seams::uring_worker_ring_init::is_installed() {
        return None;
    }
    let id = aio_seams::uring_worker_ring_init::call();
    u32::try_from(id).ok()
}

/// Worker-loop exit: tear down this worker's ring (waits out in-flight
/// DMA; completions run, IoTokens complete) and unbind the runtime.
pub(crate) fn worker_exit() {
    if aio_seams::uring_worker_ring_teardown::is_installed() {
        aio_seams::uring_worker_ring_teardown::call();
    }
    WORKER_RT.with(|c| *c.borrow_mut() = None);
    debug_assert!(!IN_IO_SECTION.get(), "worker exited inside a blocking section");
    PERMIT_HELD.set(false);
}

/// Loop bookkeeping: the worker acquired (true) / is about to release
/// (false) its execution permit. A task that entered a declared blocking
/// section through the seams restores the flag on reacquire, so the flag is
/// always accurate at the loop's own release point.
/// GL-STMTTASK-2 quantum yield: true while THIS thread is inside a
/// declared blocking section (permit already donated) — the governor's
/// double-release guard. One TLS load.
pub(crate) fn in_io_section() -> bool {
    IN_IO_SECTION.get()
}

pub(crate) fn note_permit(held: bool) {
    debug_assert!(
        !IN_IO_SECTION.get(),
        "permit bookkeeping while inside a blocking section"
    );
    PERMIT_HELD.set(held);
}

/// §2.9 boundary duty: non-blocking drain of this worker's CQEs.
pub(crate) fn boundary_reap() {
    if aio_seams::uring_boundary_reap::is_installed() {
        aio_seams::uring_boundary_reap::call();
    }
}

/// Seam impl: enter a declared blocking section. True only when this thread
/// is a pool worker holding an execution permit and not already inside one
/// — the caller must then call [`io_permit_reacquire`] exactly once.
fn io_permit_release() -> bool {
    if !PERMIT_HELD.get() || IN_IO_SECTION.get() {
        return false;
    }
    let released = WORKER_RT.with(|c| match &*c.borrow() {
        Some(rt) => {
            // Grant follows permit (§2.8 × ledger composition): donate the
            // width grant FIRST so a standby woken by the permit release
            // finds the slot joinable (no-op under knob OFF / off-task).
            crate::sched::ledger_donate_current();
            rt.execution_permits().release();
            true
        }
        None => false,
    });
    if released {
        PERMIT_HELD.set(false);
        IN_IO_SECTION.set(true);
    }
    released
}

/// Seam impl: exit the declared blocking section — reacquire the permit as
/// an ordinary contender (no priority; condvar wake order).
fn io_permit_reacquire() {
    assert!(
        IN_IO_SECTION.get(),
        "io_permit_reacquire without a matching release"
    );
    WORKER_RT.with(|c| {
        c.borrow()
            .as_ref()
            .expect("blocking section outlived its worker loop")
            .execution_permits()
            .acquire();
    });
    // Grant follows permit: retake the width grant only once the permit is
    // back (transient over-target resolves via Yield; no-op under knob OFF).
    crate::sched::ledger_restore_current();
    IN_IO_SECTION.set(false);
    PERMIT_HELD.set(true);
}
