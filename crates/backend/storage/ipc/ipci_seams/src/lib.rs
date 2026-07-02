seam_core::seam!(
    // CreateSharedMemoryAndSemaphores (storage/ipc/ipci.c): size + create the
    // shmem segment and run every CreateOrAttachShmemStructs initializer.
    // C reads FastPathLockGroupsPerBackend as a global; the repo rule passes
    // the InitializeFastPathLocks result as a parameter instead.
    pub fn create_shared_memory_and_semaphores(
        fastpath_lock_groups_per_backend: i32,
    ) -> types_error::PgResult<()>
);
