//! Seam declarations for the `backend-storage-ipc-latch` unit
//! (`storage/ipc/latch.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// `SetLatch(MyLatch)`: set this backend's own process latch. `MyLatch`
    /// is the per-backend latch pointer (globals.c); the latch crate resolves
    /// it when installing, so signal-handler callers (which cannot carry a
    /// `LatchHandle`) need no parameter. Async-signal-safe and infallible in
    /// C.
    pub fn set_latch_my_latch()
);

seam_core::seam!(
    /// `SetLatch(&GetPGProcByNumber(procno)->procLatch)`: set the process latch
    /// of the backend identified by `procno`. The latch crate resolves the
    /// target PGPROC's latch when installing. Async-signal-safe and infallible
    /// in C.
    pub fn set_latch_for_procno(procno: types_core::ProcNumber)
);

seam_core::seam!(
    /// `ResetLatch(latch)`: clear the given latch. C call sites that pass
    /// `MyLatch` translate to an explicit handle the caller holds.
    /// Infallible in C.
    pub fn reset_latch(latch: types_storage::latch::LatchHandle)
);

seam_core::seam!(
    /// `ResetLatch(MyLatch)`: clear this backend's own process latch. Like
    /// `set_latch_my_latch`, the latch crate resolves `MyLatch` when installing.
    pub fn reset_latch_my_latch()
);

seam_core::seam!(
    /// `WaitLatch(MyLatch, wakeEvents, timeout, wait_event_info)` — wait until
    /// the latch is set, the timeout elapses, or (with `WL_EXIT_ON_PM_DEATH`)
    /// the postmaster dies. Returns the OR'd `WL_*` reasons that fired.
    pub fn wait_latch(
        wake_events: i32,
        timeout: i64,
        wait_event_info: types_core::uint32
    ) -> i32
);

seam_core::seam!(
    /// `WaitLatchOrSocket(MyLatch, wakeEvents, sock, timeout, wait_event_info)`
    /// — like `wait_latch` but also wakes on socket readiness.
    pub fn wait_latch_or_socket(
        wake_events: i32,
        sock: types_core::pgsocket,
        timeout: i64,
        wait_event_info: types_core::uint32
    ) -> i32
);
