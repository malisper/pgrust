//! Seam declarations for the `backend-replication-syncrep` unit
//! (`replication/syncrep.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

extern crate alloc;

use alloc::vec::Vec;

use types_core::XLogRecPtr;
use types_error::PgResult;

seam_core::seam!(
    /// `SyncRepWaitForLSN(lsn, commit)` — wait for synchronous replication.
    /// Interrupt paths emit WARNINGs; cancellation can `ereport(ERROR)`.
    pub fn sync_rep_wait_for_lsn(lsn: XLogRecPtr, commit: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `SyncRepCleanupAtProcExit()` (syncrep.c) — remove this backend from the
    /// sync-rep wait queue during proc teardown (`ProcKill`). Infallible.
    pub fn sync_rep_cleanup_at_proc_exit()
);

seam_core::seam!(
    /// `SyncRepUpdateSyncStandbysDefined()` (syncrep.c) — recompute and publish
    /// in shmem whether synchronous standbys are configured, after a
    /// `synchronous_standby_names` change. Can `ereport` on a parse error.
    pub fn sync_rep_update_sync_standbys_defined() -> PgResult<()>
);

seam_core::seam!(
    /// `SyncRepInitConfig()` (syncrep.c) — re-parse `synchronous_standby_names`
    /// and set this walsender's `MyWalSnd->sync_standby_priority`.
    pub fn sync_rep_init_config()
);

seam_core::seam!(
    /// `SyncRepGetCandidateStandbys(&standbys)` (syncrep.c) — the currently
    /// active synchronous standbys. Returned as `(walsnd_index, pid)` pairs (the
    /// only fields `pg_stat_get_wal_senders` matches on). Infallible (a shared
    /// read under SyncRepLock).
    pub fn sync_rep_get_candidate_standbys() -> Vec<(i32, i32)>
);

seam_core::seam!(
    /// `SyncRepConfigIsPriority()` — `SyncRepConfig->syncrep_method ==
    /// SYNC_REP_PRIORITY` (vs quorum). Read of the parsed sync-rep config.
    pub fn sync_rep_config_is_priority() -> bool
);
