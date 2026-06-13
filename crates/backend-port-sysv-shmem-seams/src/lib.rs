//! Seam declarations for the `backend-port-sysv-shmem` unit
//! (`src/backend/port/sysv_shmem.c`). The owning unit installs these from its
//! `init_seams()`; until then a call panics loudly.

seam_core::seam!(
    /// `PGSharedMemoryDetach()` (`sysv_shmem.c`): detach from the main shared
    /// memory segment(s) in a child process that does not need them.
    pub fn pg_shared_memory_detach()
);
