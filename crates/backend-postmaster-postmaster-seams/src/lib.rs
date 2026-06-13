//! Seam declarations for the `backend-postmaster-postmaster` unit
//! (`src/backend/postmaster/postmaster.c`). The owning unit installs these
//! from its `init_seams()`; until then a call panics loudly.

seam_core::seam!(
    /// `ClosePostmasterPorts(am_syslogger)` (`postmaster.c`): in a child
    /// process, close the postmaster's listen sockets and other
    /// postmaster-only file descriptors.
    pub fn close_postmaster_ports(am_syslogger: bool)
);

seam_core::seam!(
    /// `PostmasterMarkPIDForWorkerNotify(pid)` (`postmaster.c`): scan the
    /// postmaster's active-child list for `pid`, set `bkend->bgworker_notify`,
    /// and return whether the PID was found. Called from the postmaster while
    /// reconciling new worker slots.
    pub fn postmaster_mark_pid_for_worker_notify(pid: i32) -> bool
);

seam_core::seam!(
    /// `kill(pid, SIGUSR1)` from the postmaster to a child it tracks — used by
    /// bgworker.c to wake a worker's `bgw_notify_pid` backend on a state
    /// change. The owner (postmaster.c, which forks the children) performs the
    /// actual `kill`.
    pub fn signal_child_sigusr1(pid: i32)
);

seam_core::seam!(
    /// `kill(pid, SIGTERM)` from the postmaster to a running worker, used to
    /// terminate a worker marked for shutdown.
    pub fn signal_child_sigterm(pid: i32)
);

seam_core::seam!(
    /// `MemoryContextDelete(PostmasterContext); PostmasterContext = NULL`
    /// (`postmaster.c` owns `PostmasterContext`): a freshly-forked child
    /// releases the postmaster's working context after copying its startup
    /// data out of it.
    pub fn delete_postmaster_context()
);
