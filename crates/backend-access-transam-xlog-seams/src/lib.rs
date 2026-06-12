//! Seam declarations for the `backend-access-transam-xlog` unit
//! (`access/transam/xlog.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::XLogRecPtr;
use types_wal::WalLevel;

seam_core::seam!(
    /// `XLogSetAsyncXactLSN(asyncXactLSN)` — mark the LSN as to-be-synced and
    /// nudge the WAL writer.
    pub fn xlog_set_async_xact_lsn(async_xact_lsn: XLogRecPtr)
);

seam_core::seam!(
    /// `wal_level` (xlog.c GUC).
    pub fn wal_level() -> WalLevel
);

seam_core::seam!(
    /// `InRecovery` (xlog.c global) — true in the startup process during
    /// crash/archive recovery.
    pub fn in_recovery() -> bool
);
