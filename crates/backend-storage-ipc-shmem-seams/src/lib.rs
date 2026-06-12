//! Seam declarations for the `backend-storage-ipc-shmem` unit
//! (`storage/ipc/shmem.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// `mul_size(s1, s2)` (shmem.c) — checked Size multiplication; the C
    /// `ereport(ERROR, "requested shared memory size overflows size_t")`
    /// is the `Err`.
    pub fn mul_size(s1: usize, s2: usize) -> types_error::PgResult<usize>
);

seam_core::seam!(
    /// `add_size(s1, s2)` (shmem.c) — checked Size addition; the C
    /// `ereport(ERROR, "requested shared memory size overflows size_t")`
    /// is the `Err`.
    pub fn add_size(s1: usize, s2: usize) -> types_error::PgResult<usize>
);
