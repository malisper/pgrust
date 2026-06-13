//! Seam declarations for the `backend-replication-syncrep` unit
//! (`replication/syncrep.c`).
//!
//! Installed by the owning unit's `init_seams()` when it lands; until then a
//! call panics loudly.

use types_core::XLogRecPtr;
use types_error::PgResult;

seam_core::seam!(
    /// `SyncRepWaitForLSN(lsn, commit)` (syncrep.c): block until `lsn` is
    /// confirmed by enough synchronous standbys (or synchronous replication is
    /// off / we are not allowed to wait). `commit` distinguishes a commit wait
    /// (which respects `synchronous_commit`) from a non-commit wait. The wait
    /// can be interrupted by `ProcDiePending`, surfaced on `Err`.
    pub fn sync_rep_wait_for_lsn(lsn: XLogRecPtr, commit: bool) -> PgResult<()>
);
