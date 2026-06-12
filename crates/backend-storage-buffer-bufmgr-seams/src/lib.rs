//! Seam declarations for the `backend-storage-buffer-bufmgr` unit
//! (`storage/buffer/bufmgr.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

seam_core::seam!(
    /// `HoldingBufferPinThatDelaysRecovery()` — does this backend hold the
    /// buffer pin the Startup process is waiting for?
    pub fn holding_buffer_pin_that_delays_recovery() -> bool
);
