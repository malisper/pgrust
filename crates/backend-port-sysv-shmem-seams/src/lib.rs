//! Seam declarations for the `backend-port-sysv-shmem` unit
//! (`src/backend/port/sysv_shmem.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// `PGSharedMemoryDetach()` (`sysv_shmem.c`): detach from the main shared
    /// memory segment(s) in a child process that does not need them.
    pub fn pg_shared_memory_detach()
);

seam_core::seam!(
    /// `PGSharedMemoryIsInUse(id1, id2)` (`port/sysv_shmem.c`) — is the SysV
    /// shared memory segment recorded in a stale `postmaster.pid` still
    /// attached by some live process? `Err` carries the shmctl failure.
    pub fn pg_shared_memory_is_in_use(id1: u64, id2: u64) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `GetHugePageSize(Size *hugepagesize, int *mmap_flags)` (sysv_shmem.c)
    /// — the huge-page size and the `MAP_HUGE*` mmap flag bits to use; the
    /// two C out-parameters are folded into the returned
    /// `(hugepagesize, mmap_flags)` tuple. Infallible (non-Linux builds
    /// return `(0, 0)`).
    pub fn get_huge_page_size() -> (usize, i32)
);

seam_core::seam!(
    /// `PGSharedMemoryCreate(Size size, PGShmemHeader **shim)` (sysv_shmem.c) —
    /// create the main shared-memory segment. Returns the segment header
    /// `seghdr` and sets `*shim`. Both are genuinely shared memory (raw
    /// pointers, opacity inherited). `Err` carries the `ereport(FATAL)` on
    /// failure. Owner unported; scaffolded slot.
    pub fn pg_shared_memory_create(
        size: types_core::Size,
    ) -> types_error::PgResult<(*mut types_storage::PGShmemHeader, *mut types_storage::PGShmemHeader)>
);
