// Portal-facing ipc.c slice (ipc_seams is concurrently owned by another port).
seam_core::seam!(
    // shmem_exit_inprogress (ipc.c) — true while proc_exit/shmem_exit runs;
    // gates AtAbort_Portals' fail-the-active-portal path under elog(FATAL).
    pub fn shmem_exit_inprogress() -> bool
);
