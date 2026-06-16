//! Seam declarations for the `storage/file/sharedfileset.c` unit: the
//! shared-temp-file directory protocol layered over a [`SharedFileSet`]
//! (a [`FileSet`](types_storage::fileset::FileSet) plus refcount bookkeeping).
//! `nodeHashjoin.c`'s parallel DSM hooks drive `SharedFileSetInit` (leader),
//! `SharedFileSetAttach` (worker) and `SharedFileSetDeleteAll` (rescan).
//!
//! `sharedfileset.c` is not yet ported (it is part of the `backend-storage-file`
//! unit, where only `buffile.c`/`fd.c` have landed); until it does, a call here
//! panics loudly. The owning unit installs these from its `init_seams()`.

#![allow(non_snake_case)]

use types_execparallel::DsmSegmentHandle;
use types_storage::fileset::SharedFileSet;

seam_core::seam!(
    /// `void SharedFileSetInit(SharedFileSet *fileset, dsm_segment *seg)`
    /// (sharedfileset.c) — initialize a `SharedFileSet` keyed by the creating
    /// PID + a per-PID counter, registering an on-detach cleanup callback on
    /// `seg` (or, when `seg` is the implicit-NULL session-lifetime case, an
    /// on-proc-exit callback). The leader calls this once before launching
    /// workers. Fallible: the nested `FileSetInit` (`PrepareTempTablespaces`)
    /// and the `on_dsm_detach` callback registration both allocate / `ereport`.
    pub fn SharedFileSetInit(
        fileset: &mut SharedFileSet,
        seg: DsmSegmentHandle,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `void SharedFileSetAttach(SharedFileSet *fileset, dsm_segment *seg)`
    /// (sharedfileset.c) — attach to a `SharedFileSet` created by
    /// `SharedFileSetInit`, bumping the reference count under the fileset's
    /// spinlock and registering the matching on-detach cleanup callback. Each
    /// worker calls this. Fallible: explicit `ereport(ERROR)` when the set is
    /// already destroyed, plus the `on_dsm_detach` callback registration OOM.
    pub fn SharedFileSetAttach(
        fileset: &mut SharedFileSet,
        seg: DsmSegmentHandle,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `void SharedFileSetDeleteAll(SharedFileSet *fileset)` (sharedfileset.c) —
    /// delete all the temporary directories (and the files they contain) created
    /// for `fileset` across its tablespaces. Fallible: the nested
    /// `FileSetDeleteAll` deletes temp dirs (IO can fail).
    pub fn SharedFileSetDeleteAll(fileset: &mut SharedFileSet) -> types_error::PgResult<()>
);
