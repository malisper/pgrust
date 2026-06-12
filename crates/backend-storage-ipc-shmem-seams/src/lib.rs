//! Seam declarations for the `backend-storage-ipc-shmem` unit
//! (`storage/ipc/shmem.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::Size;
use types_error::PgResult;

seam_core::seam!(
    /// `add_size(s1, s2)` (shmem.c) — overflow-checked shmem-size addition.
    /// `Err` carries the C `ereport(ERROR, "requested shared memory size
    /// overflows size_t")`.
    pub fn add_size(s1: Size, s2: Size) -> PgResult<Size>
);

seam_core::seam!(
    /// `mul_size(s1, s2)` (shmem.c) — overflow-checked shmem-size
    /// multiplication; same failure surface as `add_size`.
    pub fn mul_size(s1: Size, s2: Size) -> PgResult<Size>
);

seam_core::seam!(
    /// `SpinLockAcquire(ShmemLock)` — take the shmem-segment spinlock placed
    /// by `InitShmemAllocation` (shmem.c owns `ShmemLock`).
    pub fn shmem_lock_acquire()
);

seam_core::seam!(
    /// `SpinLockRelease(ShmemLock)`.
    pub fn shmem_lock_release()
);
