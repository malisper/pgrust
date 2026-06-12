//! Seam declarations for the `backend-storage-lmgr-proc` unit
//! (`storage/lmgr/proc.c`: the `MyProc` PGPROC entry).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::Oid;

seam_core::seam!(
    /// `MyProc->tempNamespaceId = nspid` (namespace.c writes the field; the
    /// PGPROC storage belongs to proc.c). Plain shared-memory field write.
    pub fn set_my_proc_temp_namespace_id(nspid: Oid)
);
