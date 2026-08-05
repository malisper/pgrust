//! Declared-blocking-section FACADE for spill I/O (M3.5 inc-1; design
//! authority docs/design/m3.5-spill.md §6.1, redesign §2.8).
//!
//! [`crate::sync::Semaphore::io_section`] has a hard caller contract: the
//! calling thread must actually HOLD an execution permit (it releases
//! unconditionally; a permit-less call would inflate the count). Pool
//! workers hold one across the whole scheduling step (pool.rs); the phase-1
//! pinned-regime helpers and the leader hold NONE. This facade lets the
//! spill substrate bracket every spill syscall region unconditionally:
//!
//! - on a REGISTERED pool-worker thread the section is ARMED — the permit
//!   is donated for the region (a standby absorbs the core) and reacquired
//!   on drop, exactly `io_section`;
//! - on every other thread it is a NO-OP.
//!
//! The concurrency semantics are entirely `io_section`'s; the facade adds a
//! thread-local registration gate. Loom coverage: tests/loom.rs
//! `facade_standby_absorption` (the §2.8 standby-absorption model driven
//! through this facade). Call sites live in the spill substrate (the
//! spillset crate); registration belongs to the pool's worker loop (and to
//! the loom pool-loop emulation).

use std::cell::Cell;
use std::marker::PhantomData;
use std::ptr;

use crate::sync::Semaphore;

thread_local! {
    /// Non-null exactly while a [`PermitThreadReg`] for this thread lives.
    static PERMIT_SEM: Cell<*const Semaphore> = const { Cell::new(ptr::null()) };
}

/// Registration guard: marks the current thread as a permit-holding pool
/// worker for the guard's lifetime. Runtime-internal surface: created by
/// the pool worker loop and by loom pool-loop emulations — operators never
/// register threads.
pub struct PermitThreadReg {
    prev: *const Semaphore,
    _not_send: PhantomData<*const ()>,
}

impl PermitThreadReg {
    /// # Safety
    ///
    /// The caller guarantees that `sem` outlives BOTH this guard and every
    /// [`BlockingIoSection`] entered on this thread while it is registered,
    /// and that [`blocking_io_section`] is only called at points where the
    /// thread actually holds one of `sem`'s permits. The pool loop
    /// satisfies both: the runtime (owning the semaphore) outlives the
    /// loop, and the permit is held across the whole `worker_step`, inside
    /// which all task-body spill I/O runs.
    pub unsafe fn new(sem: &Semaphore) -> PermitThreadReg {
        let prev = PERMIT_SEM.with(|c| c.replace(sem as *const Semaphore));
        PermitThreadReg { prev, _not_send: PhantomData }
    }
}

impl Drop for PermitThreadReg {
    fn drop(&mut self) {
        PERMIT_SEM.with(|c| c.set(self.prev));
    }
}

/// RAII blocking section from [`blocking_io_section`]. `!Send` by
/// construction (raw pointer): the permit must be reacquired on the thread
/// that donated it.
pub struct BlockingIoSection {
    sem: *const Semaphore,
}

impl Drop for BlockingIoSection {
    fn drop(&mut self) {
        if !self.sem.is_null() {
            // SAFETY: non-null only when armed under a PermitThreadReg,
            // whose unsafe constructor's contract makes the semaphore
            // outlive every section; !Send keeps us on the donating thread.
            unsafe { (*self.sem).acquire() }
            // Grant follows permit (§2.8 × ledger composition): retake the
            // width grant only once the permit is back (no-op under knob
            // OFF / off-task).
            crate::sched::ledger_restore_current();
        }
    }
}

/// Enter a declared blocking section for spill I/O. Armed (permit donated)
/// on a registered pool-worker thread; no-op anywhere else. See the module
/// docs for the contract.
pub fn blocking_io_section() -> BlockingIoSection {
    let sem = PERMIT_SEM.with(Cell::get);
    if !sem.is_null() {
        // Grant follows permit (§2.8 × ledger composition): donate the
        // width grant FIRST so a standby woken by the permit release finds
        // the slot joinable (no-op under knob OFF / off-task).
        crate::sched::ledger_donate_current();
        // SAFETY: as in Drop.
        unsafe { (*sem).release() }
    }
    BlockingIoSection { sem }
}
