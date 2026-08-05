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

seam_core::seam!(
    // SIMVFS-SHARED (sim rtgang corpus): the standing gang's PGPROC
    // boot-reserve sizing (main_entry parity — the sim session harness must
    // call it BEFORE the ladder's InitializeMaxBackends). Seam-routed for
    // the same package-cycle reason as the wpool/rtpool trios.
    pub fn rtgang_procs_wanted() -> i32
);

seam_core::seam!(
    // SIMVFS-SHARED (sim rtgang corpus): install the standing-gang spawner
    // (serverloop parity — after shared memory exists; threads spawn at
    // first engagement).
    pub fn rtgang_install()
);

seam_core::seam!(
    // Shutdown sequencing: fence the standing gang when the state machine
    // stops backends (PM_STOP_BACKENDS / immediate shutdown) — parked gang
    // threads exit clean, new engagements refuse. Registry-invisible
    // threads must sequence OUT before the shutdown checkpoint (C parity:
    // no live children behind the postmaster's count).
    pub fn rtgang_retire()
);

seam_core::seam!(
    // Shutdown sequencing: live standing-gang thread count — the
    // PM_WAIT_BACKENDS quiescence gate. Exiting gang threads poke
    // PMSIGNAL_ADVANCE_STATE_MACHINE as the count drains.
    pub fn rtgang_live() -> i32
);

seam_core::seam!(
    // SIMCORPUS (sim parallel-query corpus): PostmasterMain parity — the
    // bgworker registry init (main_entry.rs calls it after shared memory;
    // the single-user/stdio boot ladder never does, because standalone C
    // has no bgworkers). A harness whose boot thread plays the postmaster
    // must run it before any RegisterDynamicBackgroundWorker, or the
    // leader dies at "BackgroundWorkerData accessed before
    // BackgroundWorkerShmemInit". Seam-routed (tcop cannot depend on
    // bgworker: bgworker depends on tcop).
    pub fn bgworker_shmem_init()
);

seam_core::seam!(
    // DST-MULTIBACKEND (sim pool-miss deferral corpus): the startup-exit
    // PROMOTION a harness boot thread that plays the postmaster never runs
    // (process_pm_child_exit's startup arm): claim the postmaster
    // environment (IsPostmasterEnvironment — main_entry parity; the boot
    // thread ran the STANDALONE ladder, whose identity is not-postmaster)
    // and enter PM_RUN with connections allowed + the deferred-bgworker
    // sweep requested. Without it a pool-miss registration defers to
    // "postmaster start" forever: bgworker_should_start_now gates
    // ConsistentState workers on PM_RUN, and postmaster_child_launch
    // asserts IsPostmasterEnvironment — the simcorpus §7 named boundary.
    // Call AFTER the surrogate's recovery owner (session 1) completes —
    // C's moment is the startup child's clean exit. Seam-routed for the
    // same package-cycle reason as pm_service_pending.
    pub fn pm_promote_run()
);

seam_core::seam!(
    // SIMCORPUS (sim parallel-query corpus): one postmaster SERVICE
    // quantum — the ServerLoop slice a harness boot thread needs while it
    // PLAYS the postmaster and a session runs on a child thread:
    // (1) pending child-exit announces (process_pm_child_exit: join
    // announced threads, clear bgworker slot pids,
    // ReportBackgroundWorkerExit -> the leader's BGWH_STOPPED wake —
    // without a reaper, DestroyParallelContext's
    // WaitForBackgroundWorkerShutdown waits forever);
    // (2) the PMSIGNAL_BACKGROUND_WORKER_CHANGE arm + deferred bgworker
    // starts (maybe_start_bgworkers) — a pool-miss registration defers to
    // "postmaster start", and only this arm ever starts it (found by the
    // P9 red's Gather-leader hang on NOT_YET_STARTED slots).
    // No-op when nothing is pending. Seam-routed for the same
    // package-cycle reason as the wpool/rtpool/rtgang trios (tcop cannot
    // depend on postmaster).
    pub fn pm_service_pending()
);
