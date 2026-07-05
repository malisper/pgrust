
seam_core::seam!(
    // ApplyLauncherRegister (replication/logical/launcher.c).
    pub fn apply_launcher_register()
);

seam_core::seam!(
    // GetLeaderApplyWorkerPid (launcher.c); InvalidPid (-1) = not a parallel
    // apply worker.
    pub fn get_leader_apply_worker_pid(pid: i32) -> i32
);
