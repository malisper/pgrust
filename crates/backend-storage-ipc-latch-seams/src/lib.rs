//! Seam declarations for the `backend-storage-ipc-latch` unit
//! (`storage/ipc/latch.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// `SetLatch(MyLatch)`: set this backend's own process latch. `MyLatch`
    /// is the per-backend latch pointer (globals.c); the latch crate resolves
    /// it when installing, so signal-handler callers need no `Latch` handle.
    /// Async-signal-safe and infallible in C.
    pub fn set_latch_my_latch()
);

seam_core::seam!(
    /// `ResetLatch(MyLatch)`: clear this backend's own process latch before
    /// checking for work, so later `SetLatch` wakeups are not lost. `MyLatch`
    /// is resolved by the latch crate when installing (same pattern as
    /// `set_latch_my_latch`). Infallible in C.
    pub fn reset_latch_my_latch()
);
