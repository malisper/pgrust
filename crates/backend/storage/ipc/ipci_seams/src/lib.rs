seam_core::seam!(
    // CreateSharedMemoryAndSemaphores (storage/ipc/ipci.c): size + create the
    // shmem segment and run every CreateOrAttachShmemStructs initializer.
    pub fn create_shared_memory_and_semaphores() -> types_error::PgResult<()>
);
