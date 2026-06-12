//! Seam declarations for the `backend-access-transam-parallel` unit
//! (`access/transam/parallel.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `IsParallelWorker()` (`access/parallel.h`):
    /// `(ParallelWorkerNumber >= 0)`; `ParallelWorkerNumber` is owned by
    /// `parallel.c`.
    pub fn is_parallel_worker() -> bool
);
