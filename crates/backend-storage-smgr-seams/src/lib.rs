//! Seam declarations for the `backend-storage-smgr` unit
//! (`storage/smgr/smgr.c` + `md.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_error::PgResult;
use types_storage::RelFileLocator;

seam_core::seam!(
    /// `AtEOXact_SMgr()` — close transient SMgrRelation objects.
    pub fn at_eoxact_smgr()
);

seam_core::seam!(
    /// `DropRelationFiles(delrels, ndelrels, isRedo)` (md.c) — physically drop
    /// relation files during replay/commit application.
    pub fn drop_relation_files(delrels: &[RelFileLocator], is_redo: bool) -> PgResult<()>
);
