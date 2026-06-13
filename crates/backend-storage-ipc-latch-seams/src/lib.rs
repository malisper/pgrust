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
