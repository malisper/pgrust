//! Seam declarations for the `backend-storage-ipc-shmem` unit
//! (`storage/ipc/shmem.c`).
//!
//! The owning crate `backend-storage-ipc-shmem` installs these from its
//! `init_seams()`.

use types_core::Size;
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

seam_core::seam!(
    /// `InitShmemAccess(PGShmemHeader *seghdr)` (shmem.c) — record the main
    /// shared-memory segment so `ShmemAlloc`/`ShmemInitStruct` can carve from
    /// it. The header is genuinely shared memory (raw pointer, opacity
    /// inherited). Owner unported; scaffolded slot.
    pub fn init_shmem_access(seghdr: *mut types_storage::PGShmemHeader)
);

seam_core::seam!(
    /// `InitShmemAllocation()` (shmem.c) — set up the shmem allocation
    /// mechanism (places `ShmemLock`). Owner unported; scaffolded slot.
    pub fn init_shmem_allocation()
);

seam_core::seam!(
    /// `InitShmemIndex()` (shmem.c) — create the `ShmemIndex` hashtable used to
    /// find named shmem structures. `Err` carries the out-of-shmem
    /// `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn init_shmem_index() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ShmemAllocUnlocked(Size size)` (shmem.c) — allocate a max-aligned
    /// chunk from the main shared-memory segment without taking `ShmemLock`.
    /// Used only for allocations that must happen before `ShmemLock` is ready
    /// (e.g. `PGReserveSemaphores`' `sharedSemas` array). Returns the raw
    /// address (genuinely shared memory, opacity inherited). `Err` carries the
    /// C `ereport(WARNING)`/NULL-return out-of-shmem path folded into a fatal.
    pub fn shmem_alloc_unlocked(size: Size) -> PgResult<*mut u8>
);
