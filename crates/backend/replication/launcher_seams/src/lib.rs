
seam_core::seam!(
    // ApplyLauncherRegister (replication/logical/launcher.c).
    pub fn apply_launcher_register()
);

seam_core::seam!(
    // GetLeaderApplyWorkerPid (launcher.c); InvalidPid (-1) = not a parallel
    // apply worker.
    pub fn get_leader_apply_worker_pid(pid: i32) -> i32
);

seam_core::seam!(
    // ApplyLauncherShmemInit (launcher.c), called from shmem creation and the
    // crash-cycle reset_shared.
    pub fn apply_launcher_shmem_init()
);

seam_core::seam!(
    // AtEOXact_ApplyLauncher (launcher.c); xact calls through the seam so the
    // launcher crate can depend on xact for its catalog transaction.
    pub fn at_eoxact_apply_launcher(is_commit: bool)
);
