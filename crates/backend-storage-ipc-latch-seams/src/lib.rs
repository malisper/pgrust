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
    /// `ResetLatch(latch)`: clear the given latch. C call sites that pass
    /// `MyLatch` translate to an explicit handle the caller holds.
    /// Infallible in C.
    pub fn reset_latch(latch: types_storage::latch::LatchHandle)
);

seam_core::seam!(
    /// `ResetLatch(MyLatch)`: clear this backend's own process latch. The
    /// my-latch counterpart of [`reset_latch`], for callers (slot-sync worker)
    /// that only ever reset `MyLatch`. Infallible in C.
    pub fn reset_latch_my_latch()
);

seam_core::seam!(
    /// `WaitLatch(MyLatch, wakeEvents, timeout, wait_event_info)`
    /// (latch.c) — wait on this backend's latch / a timeout / the configured
    /// socket+pm-death events, returning the bitmask of events that fired.
    /// `WL_EXIT_ON_PM_DEATH` makes it `proc_exit` on postmaster death rather
    /// than return, so it is infallible from the caller's view.
    pub fn wait_latch_my_latch(wake_events: u32, timeout_ms: i64, wait_event_info: u32) -> i32
);

seam_core::seam!(
    /// `kill(pid, SIGUSR1)` — signal another backend (the slot-sync worker)
    /// to wake. `Err` carries the C `ereport(WARNING)`-on-`kill`-failure path
    /// surfaced as a propagating error.
    pub fn kill_sigusr1(pid: i32) -> types_error::PgResult<()>
);
