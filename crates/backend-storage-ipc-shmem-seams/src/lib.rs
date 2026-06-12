//! Seam declarations for the `backend-storage-ipc-shmem` unit
//! (`storage/ipc/shmem.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_error::PgResult;

seam_core::seam!(
    /// `ShmemInitStruct(const char *name, Size size, bool *foundPtr)` —
    /// allocate-or-attach a named structure in main shared memory. Returns
    /// the structure's address (a raw pointer because the memory is genuinely
    /// shared) and the C `*foundPtr` (true when the structure already
    /// existed). `Err` carries the `ereport(ERROR)`s for out-of-shared-memory
    /// and shmem-index failures.
    pub fn shmem_init_struct(name: &str, size: usize) -> PgResult<(*mut u8, bool)>
);
