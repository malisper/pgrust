//! Seam declarations for the `backend-storage-ipc-pmsignal` unit
//! (`storage/ipc/pmsignal.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// `PostmasterIsAlive()` (`storage/pmsignal.h` / pmsignal.c) — probe
    /// whether the postmaster process is still running. The owner carries
    /// the platform split: where a postmaster-death signal exists the
    /// `postmaster_possibly_dead` fast path short-circuits the
    /// `PostmasterIsAliveInternal()` pipe probe.
    pub fn postmaster_is_alive() -> bool
);

seam_core::seam!(
    /// `SendPostmasterSignal(PMSIGNAL_BACKGROUND_WORKER_CHANGE)` (pmsignal.c)
    /// — set the shared `PMSignalFlags` slot and `kill(PostmasterPid, SIGUSR1)`
    /// so the postmaster runs `BackgroundWorkerStateChange`. Narrow seam for
    /// the single reason bgworker.c sends.
    pub fn send_postmaster_signal_bgworker_change()
);
