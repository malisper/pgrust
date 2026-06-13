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

seam_core::seam!(
    /// `PreAuthDelay` (`postmaster.c` GUC) — seconds to sleep before
    /// authentication, a debugging aid. Pure read of backend-local GUC state.
    pub fn pre_auth_delay() -> i32
);

seam_core::seam!(
    /// `AuthenticationTimeout` (`postmaster.c` GUC) — seconds to wait for the
    /// startup packet / authentication exchange. Pure read.
    pub fn authentication_timeout() -> i32
);

seam_core::seam!(
    /// `log_hostname` (`postmaster.c` GUC) — whether to log/resolve the
    /// client's host name (vs. numeric address). Pure read.
    pub fn log_hostname() -> bool
);

// --- backend-utils-init-postinit consumers (postmaster.c) ---

seam_core::seam!(
    /// `ClientAuthInProgress` (postmaster.c global): read the flag.
    pub fn client_auth_in_progress() -> bool
);

seam_core::seam!(
    /// `ClientAuthInProgress = value` (postmaster.c global): set the flag that
    /// limits log-message visibility during authentication.
    pub fn set_client_auth_in_progress(value: bool)
);

// --- backend-storage-ipc-pmsignal consumers (postmaster death watch) ---

seam_core::seam!(
    /// `read(postmaster_alive_fds[POSTMASTER_FD_WATCH], &c, 1)`
    /// (`PostmasterIsAliveInternal`, pmsignal.c). `postmaster_alive_fds` is the
    /// death-watch pipe set up by postmaster.c (which owns those fds), so the
    /// raw non-blocking `read` lives behind this seam. Returns `(rc, errno)`:
    /// `rc < 0` with `errno == EAGAIN/EWOULDBLOCK` means the postmaster is still
    /// alive; `rc == 0` means EOF (postmaster gone); `rc > 0` is unexpected data.
    pub fn read_postmaster_death_watch() -> (isize, i32)
);

seam_core::seam!(
    /// Request a signal on parent (postmaster) death:
    /// `prctl(PR_SET_PDEATHSIG, signum)` / `procctl(PROC_PDEATHSIG_CTL, &signum)`
    /// (`PostmasterDeathSignalInit`, pmsignal.c). The platform mechanism is the
    /// OS boundary; the owner performs it. `Err` carries the C
    /// `elog(ERROR, "could not request parent death signal: %m")`.
    pub fn request_parent_death_signal(signum: i32) -> types_error::PgResult<()>
);
