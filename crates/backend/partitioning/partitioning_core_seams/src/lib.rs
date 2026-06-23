//! Seam declarations for the `backend-partitioning-core` unit (here:
//! `partitioning/partdesc.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgBox};
use types_error::PgResult;
use nodes::partition::PartitionDescData;
use nodes::Opaque;
use rel::Relation;

seam_core::seam!(
    /// `DestroyPartitionDirectory(pdir)` (partdesc.c): release the partition
    /// directory's relcache reference counts. The directory crosses as the
    /// type-erased payload of the executor's `Opaque` handle; the owner
    /// downcasts (loud panic on mismatch) and consumes it.
    pub fn destroy_partition_directory(pdir: std::boxed::Box<dyn std::any::Any>)
);

seam_core::seam!(
    /// `CreatePartitionDirectory(mcxt, omit_detached)` (partdesc.c): create a
    /// partition directory that caches `PartitionDesc`s for the lifetime of an
    /// executor run, allocating its bookkeeping in `mcx` (C: the passed
    /// context). The directory crosses back as the partdesc-owned `Opaque`
    /// handle the EState stores in `es_partition_directory`. Fallible on OOM.
    pub fn create_partition_directory<'mcx>(
        mcx: Mcx<'mcx>,
        omit_detached: bool,
    ) -> PgResult<Opaque>
);

seam_core::seam!(
    /// `PartitionDirectoryLookup(pdir, rel)` (partdesc.c): return the
    /// `PartitionDesc` for `rel`, building it (and pinning the relcache entry)
    /// on first lookup. The result is held live for the executor run; the owned
    /// model returns it allocated in `mcx`. `pdir` is the partdesc-owned
    /// directory handle (downcast from `Opaque`). Fallible on the relcache
    /// build's `ereport(ERROR)`s and OOM.
    pub fn partition_directory_lookup<'mcx>(
        mcx: Mcx<'mcx>,
        pdir: &mut Opaque,
        rel: Relation<'mcx>,
    ) -> PgResult<PgBox<'mcx, PartitionDescData<'mcx>>>
);
