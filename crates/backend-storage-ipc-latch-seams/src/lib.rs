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
