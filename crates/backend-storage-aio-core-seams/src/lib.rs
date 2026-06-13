//! Seam declarations for the `backend-storage-aio-core` unit
//! (`storage/aio/aio.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `pgaio_error_cleanup(void)` (`storage/aio/aio.c`) — abort and clean up
    /// any in-flight AIO this backend owns, called from error-recovery paths.
    /// Infallible.
    pub fn pgaio_error_cleanup()
);
