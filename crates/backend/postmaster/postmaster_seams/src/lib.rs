seam_core::seam!(
    // The waitpid channel's thread-model rendering: launch_backend reports a
    // finished child thread; exitstatus uses C wait-status encoding
    // (exit code << 8; a crash unwind reports as WTERMSIG SIGABRT).
    pub fn announce_child_exit(pid: i32, exitstatus: i32)
);

seam_core::seam!(
    // kill(PostmasterPid, SIGUSR1)'s thread rendering: run the postmaster's
    // SIGUSR1 handler (pend pmsignal + set the PM latch).
    pub fn signal_postmaster_sigusr1()
);

seam_core::seam!(
    // C `PgStartTime` (timestamp.c global, written once by the postmaster).
    pub fn pg_start_time() -> i64
);
