//! Seam declarations for the `backend-storage-smgr-md` unit
//! (`storage/smgr/md.c`), limited to the `syncsw[SYNC_HANDLER_MD]` vtable
//! callbacks `sync.c` dispatches to.
//!
//! `md.c` is the only handler that defines all three of `sync_syncfiletag`,
//! `sync_unlinkfiletag`, and `sync_filetagmatches`. The owning unit installs
//! these from its `init_seams()` when it lands; until then a call panics
//! loudly.

use types_error::PgResult;
use types_storage::sync::{FileTag, FileTagOpResult};

seam_core::seam!(
    /// `mdsyncfiletag(const FileTag *ftag, char *path)` (md.c) — fsync the
    /// file the tag names, returning the `0`/`<0` code, the resolved path, and
    /// the saved `errno` on failure. `Err` carries an `ereport(ERROR)` from the
    /// VFD layer (e.g. could-not-open).
    pub fn mdsyncfiletag(ftag: FileTag) -> PgResult<FileTagOpResult>
);

seam_core::seam!(
    /// `mdunlinkfiletag(const FileTag *ftag, char *path)` (md.c) — unlink the
    /// file the tag names, returning the code/path/`errno` triple.
    pub fn mdunlinkfiletag(ftag: FileTag) -> PgResult<FileTagOpResult>
);

seam_core::seam!(
    /// `mdfiletagmatches(const FileTag *ftag, const FileTag *candidate)`
    /// (md.c) — does `candidate` belong to the set selected by the filter tag
    /// `ftag` (e.g. all segments of a dropped database; matches on `dbOid`)?
    pub fn mdfiletagmatches(ftag: FileTag, candidate: FileTag) -> PgResult<bool>
);
