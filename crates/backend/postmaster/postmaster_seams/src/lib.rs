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
    // kill(PostmasterPid, SIGHUP)'s thread rendering: run the postmaster's
    // SIGHUP handler (pend the reload request + set the PM latch).
    pub fn signal_postmaster_sighup()
);

seam_core::seam!(
    // C `PgStartTime` (timestamp.c global, written once by the postmaster).
    pub fn pg_start_time() -> i64
);

seam_core::seam!(
    // PgStartTime's writer arm for the no-postmaster entries: C's standalone
    // backend assigns the global directly (postgres.c:4155); the storage
    // lives with the postmaster crate here.
    pub fn set_pg_start_time(ts: i64)
);

seam_core::seam!(
    // §3.1 P-pool claim: hand a just-registered BGWORKER_CLASS_PARALLEL slot
    // to a parked standby thread, bypassing the postmaster round-trip and
    // thread spawn. Returns the standby's reserved MyProcPid, or 0 when no
    // standby is available (caller falls back to the postmaster spawn path).
    // Called under the bgworker registry lock; lock order registry -> pool ->
    // pmchild matches BackgroundWorkerStateChange's registry -> pmchild.
    pub fn parallel_pool_dispatch(slot: i32, generation: u64, dboid: types_core::Oid) -> i32
);

seam_core::seam!(
    // DROP DATABASE rider: retire parked pool standbys pinned to the dropped
    // database. Dispatch is same-db-only, so they can never be claimed again,
    // yet each holds a bgworker PGPROC — left parked they starve the
    // postmaster fallback spawn path (InitProcess freelist exhaustion) and
    // hold POPULATION at target so maintain() never replenishes fresh ones.
    pub fn parallel_pool_retire_db(dboid: types_core::Oid)
);

seam_core::seam!(
    // PERMIT-S2 F2 (sim wpool demo): postmaster-thread pool replenish —
    // spawn standbys up to target. The tcop sim corpus drives the REAL
    // wpool through this seam (a direct launch_backend dep would be a
    // package cycle through autovacuum -> commands_vacuum -> explain).
    pub fn wpool_maintain()
);

seam_core::seam!(
    // PERMIT-S2 F2 (sim wpool demo): retire every parked standby.
    pub fn wpool_flush()
);

seam_core::seam!(
    // PERMIT-S2 F2 (sim wpool demo): live standby-thread count (the demo's
    // drain probe).
    pub fn wpool_population() -> i32
);

seam_core::seam!(
    // PERMIT-S5 (sim rtpool demo): start the runtime worker pool through
    // launch_backend's (now-doored) rtpool spawn sites. Returns the live
    // pool-thread count, or -1 when PGRUST_RUNTIME disables the runtime.
    // Seam-routed for the same package-cycle reason as the wpool trio.
    pub fn rtpool_start() -> i32
);

seam_core::seam!(
    // PERMIT-S5 (sim rtpool demo): ask the pool's workers to exit their
    // loops (stop flag + wake_all). Production never calls this — the pool
    // is process-lifetime.
    pub fn rtpool_stop()
);

seam_core::seam!(
    // PERMIT-S5 (sim rtpool demo): live pool-thread count (the drain probe).
    pub fn rtpool_population() -> i32
);
