//! Seam declarations for the `backend-storage-ipc-procarray` unit
//! (`storage/ipc/procarray.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::{Oid, ProcNumber};

seam_core::seam!(
    /// `ProcNumberGetProc(procNumber)` projected to the two PGPROC fields
    /// `checkTempNamespaceStatus` reads: `Some((proc->databaseId,
    /// proc->tempNamespaceId))`, or `None` when the slot is empty (backend
    /// not alive). Shared-memory read; cannot `ereport`.
    pub fn proc_status(proc_number: ProcNumber) -> Option<(Oid, Oid)>
);
