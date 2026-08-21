//! SpillIo — every spill-file syscall region is a DECLARED blocking
//! section (m3.5-spill.md §6.1; parallelism-redesign §2.8).
//!
//! The mechanism is `runtime::blocking_io_section()` (runtime/src/
//! blocking.rs): on a REGISTERED pool-worker thread the execution permit is
//! donated for the region (grant-follows-permit, a standby absorbs the
//! core) and reacquired on drop; on every other thread — the leader,
//! binder-bound helpers, tests — it is a no-op. Registration belongs to the
//! pool's worker loop; loom coverage is the runtime's
//! (`facade_standby_absorption`).
//!
//! THE SUBSTRATE OWNS THE CALL SITES so operators cannot forget: every
//! open/read/write/close/delete in [`crate::set`] and every unload/reload
//! in [`crate::pool`] runs inside [`io_event`]. Nothing else in this crate
//! performs I/O, and consumers get no raw-file surface to bypass it with.

/// Run one spill I/O event under a declared blocking section.
///
/// The guard is RAII: the permit (if any) is donated for exactly the
/// closure's extent — including its error paths — and reacquired before
/// `io_event` returns, so a task never migrates threads and never holds a
/// donated-away permit across an event boundary.
#[inline]
pub(crate) fn io_event<R>(f: impl FnOnce() -> R) -> R {
    let _section = runtime::blocking_io_section();
    f()
}
