//! Seam declarations for the `backend-storage-ipc-ipci` unit
//! (`storage/ipc/ipci.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `CreateSharedMemoryAndSemaphores()` (ipci.c): create and initialize the
    /// main shared-memory segment and semaphores. The C path `ereport(FATAL)`s
    /// if it cannot create the segment (never a recoverable ERROR), so it is
    /// modeled infallible.
    pub fn create_shared_memory_and_semaphores()
);
