//! Seam declarations for the `backend-storage-aio-aio` unit
//! (`storage/aio/aio.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `pgaio_error_cleanup()` (aio.c) — reclaim in-flight AIO handles during
    /// error recovery (the aux-process `sigsetjmp` cleanup calls it).
    /// Infallible; runs with interrupts held.
    pub fn pgaio_error_cleanup()
);
