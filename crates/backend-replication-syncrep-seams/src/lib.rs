//! Seam declarations for the `backend-replication-syncrep` unit
//! (`replication/syncrep.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::XLogRecPtr;
use types_error::PgResult;

seam_core::seam!(
    /// `SyncRepWaitForLSN(lsn, commit)` — wait for synchronous replication.
    /// Interrupt paths emit WARNINGs; cancellation can `ereport(ERROR)`.
    pub fn sync_rep_wait_for_lsn(lsn: XLogRecPtr, commit: bool) -> PgResult<()>
);
