//! Deadlock detector — an idiomatic safe-Rust port of
//! `src/backend/storage/lmgr/deadlock.c`.
//!
//! This crate is the deadlock-detection and wait-queue-resolution half of the
//! lock manager. It pairs with proc.c: `ProcSleep`/`CheckDeadLock` calls
//! [`dead_lock_check`] under all the lock-table partition locks; on a hard
//! deadlock the caller raises the error built by [`dead_lock_report`]. `ProcSleep`
//! also records trivial two-way deadlocks via [`remember_simple_dead_lock`], and
//! queries [`get_blocking_auto_vacuum_pgproc`] to decide whether to cancel a
//! blocking autovacuum worker. [`init_dead_lock_checking`] runs once at backend
//! startup.
//!
//! # The shared graph: an index-handle arena
//!
//! deadlock.c walks the live shared-memory `LOCK`/`PROCLOCK`/`PGPROC` graph by
//! **address identity** and rewrites wait queues in place. Because that graph is
//! cyclic and identity-compared, it cannot be modeled by owned trees; the
//! idiomatic faithful model of shared memory is an **arena of fixed-identity
//! slots** addressed by [`types_deadlock::ProcId`] / [`types_deadlock::LockId`] /
//! [`types_deadlock::ProcLockId`]. A slot id is the exact analogue of an absolute
//! shmem address: `Copy`, identity-comparable, and stable for the slot's lifetime.
//! The arena is [`types_deadlock::LockSpace`]; the detector takes `&mut LockSpace`
//! because it runs while holding all partition locks (exclusive access).
//!
//! # What is in-crate vs. a seam
//!
//! The detector's *own* algorithm — the waits-for-graph DFS
//! (`FindLockCycle`/`Recurse`/`RecurseMember`), the constraint-expansion
//! (`ExpandConstraints`) and topological sort (`TopoSort`) that try to reorder
//! wait queues to break soft cycles, and the recursive configuration search
//! (`DeadLockCheckRecurse`/`TestConfiguration`) — is ported 1:1 in [`detector`].
//!
//! The genuine externals — `MaxBackends` (globals.c), `MyProc`/`ProcLockWakeup`
//! (proc.c), `GetLocksMethodTable`/`GetLockmodeName` (lock.c), `DescribeLockTag`
//! (lmgr.c), and the two pgstat queries used only by the report — are routed
//! through their owners' per-owner seam crates.
//!
//! # Process-local scratch (NOT shmem)
//!
//! All of deadlock.c's working storage is per-backend file-scope `static` memory,
//! allocated once from `TopMemoryContext` in `InitDeadLockChecking` (the comment
//! there is explicit it is per-backend and deliberately not inherited from the
//! postmaster). It is modeled here as a per-backend `thread_local` owned struct of
//! `Vec`s — process-local, no shared memory.

mod detector;

#[cfg(test)]
mod tests;

pub use detector::{
    dead_lock_check, dead_lock_report, get_blocking_auto_vacuum_pgproc, init_dead_lock_checking,
    remember_simple_dead_lock,
};

#[cfg(feature = "debug_deadlock")]
pub use detector::print_lock_queue;

// Re-export the vocabulary the detector returns/consumes (owned by types-deadlock).
pub use types_deadlock::{
    DeadLockState, DeadlockInfo, DeadlockReport, Edge, LockId, LockMethodData, LockSlot, LockSpace,
    ProcId, ProcLockId, ProcLockSlot, ProcSlot, WaitOrder,
};

/// Install every seam this crate owns (its inward [`backend-storage-lmgr-deadlock-seams`]
/// declarations). Called once from `seams-init::init_all()`.
pub fn init_seams() {
    deadlock_seams::init_dead_lock_checking::set(detector::init_dead_lock_checking);
    deadlock_seams::dead_lock_check::set(detector::dead_lock_check);
    deadlock_seams::get_blocking_auto_vacuum_pgproc::set(
        detector::get_blocking_auto_vacuum_pgproc,
    );
    deadlock_seams::dead_lock_report::set(detector::dead_lock_report);
    deadlock_seams::remember_simple_dead_lock::set(
        detector::remember_simple_dead_lock,
    );
}
