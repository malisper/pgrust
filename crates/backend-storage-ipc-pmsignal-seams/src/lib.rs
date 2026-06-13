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
    /// `PostmasterDeathSignalInit()` (`storage/ipc/pmsignal.c`) — arrange for a
    /// signal when the postmaster dies, if the platform supports it. `Err` on
    /// the prctl/procctl failure path (`elog(ERROR)`).
    pub fn postmaster_death_signal_init() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `fcntl(postmaster_alive_fds[POSTMASTER_FD_WATCH], F_SETFD, FD_CLOEXEC)`
    /// (`miscinit.c:162`) — keep the postmaster-death watch pipe out of
    /// exec'd subprograms. `ereport(FATAL)` on failure.
    pub fn set_postmaster_death_watch_cloexec() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `SendPostmasterSignal(PMSIGNAL_BACKGROUND_WORKER_CHANGE)` (pmsignal.c)
    /// — set the shared `PMSignalFlags` slot and `kill(PostmasterPid, SIGUSR1)`
    /// so the postmaster runs `BackgroundWorkerStateChange`. Narrow seam for
    /// the single reason bgworker.c sends.
    pub fn send_postmaster_signal_bgworker_change()
);
