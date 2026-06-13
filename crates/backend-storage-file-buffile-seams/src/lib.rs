//! Seam declarations for the `backend-storage-file-buffile` unit
//! (`storage/file/buffile.c`): the buffered virtual temp-file surface, trimmed
//! to what the hash-join nodes call. The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

#![allow(non_snake_case)]

use types_execparallel::BufFileHandle;

seam_core::seam!(
    /// `BufFileClose(file)` (buffile.c): flush and close a buffered temp file,
    /// deleting its backing segments. Consumes the handle. Flushing can fail
    /// with an I/O `ereport(ERROR)`, so the call is fallible.
    pub fn BufFileClose(file: BufFileHandle) -> types_error::PgResult<()>
);
