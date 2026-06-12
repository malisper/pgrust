//! Seam declarations for the `backend-partitioning-core` unit (here:
//! `partitioning/partdesc.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `DestroyPartitionDirectory(pdir)` (partdesc.c): release the partition
    /// directory's relcache reference counts. The directory crosses as the
    /// type-erased payload of the executor's `Opaque` handle; the owner
    /// downcasts (loud panic on mismatch) and consumes it.
    pub fn destroy_partition_directory(pdir: std::boxed::Box<dyn std::any::Any>)
);
