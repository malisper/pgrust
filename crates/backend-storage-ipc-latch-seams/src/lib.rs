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
    /// counterpart to [`set_latch_my_latch`] for callers (the parallel-apply
    /// loop) that operate on `MyLatch` and hold no `LatchHandle`. Infallible
    /// in C.
    pub fn reset_latch_my_latch()
);

seam_core::seam!(
    /// `WaitLatch(MyLatch, wakeEvents, timeout, wait_event_info)`
    /// (storage/ipc/latch.c): sleep until one of `wake_events` fires or
    /// `timeout` (ms) elapses, waiting on this backend's own `MyLatch`.
    /// Returns the bitmask of events that fired. `wait_event_info` is the
    /// `uint32` wait-event id (class base | index, pgstat's scheme). C does
    /// not `ereport` from `WaitLatch` itself (a `WL_EXIT_ON_PM_DEATH` exit is
    /// `proc_exit`, noreturn), so this is infallible.
    pub fn wait_latch_my_latch(wake_events: i32, timeout: i64, wait_event_info: u32) -> i32
);
