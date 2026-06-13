//! Seam declarations for the `backend-storage-sync` unit
//! (`storage/sync/sync.c`).
//!
//! `sync.c`'s public entry points are reached across dependency cycles by the
//! checkpointer (`InitSync`/`SyncPreCheckpoint`/`SyncPostCheckpoint`/
//! `ProcessSyncRequests`), by `postinit` (`InitSync`), and by the smgr/md and
//! SLRU subsystems plus `bufmgr` (`RegisterSyncRequest`). The owning unit
//! installs these from its `init_seams()`; until then a call panics loudly.
//!
//! `sync.c`'s tracking state (`pendingOps` / `pendingUnlinks` / the cycle
//! counters / `sync_in_progress`) is checkpointer-process-local in C — a set of
//! file-static globals. The port keeps it in a `thread_local!` backend-global
//! (the AGENTS.md backend-global-state rule), so these seams carry no state
//! handle, exactly like the C globals their call sites reference.

use types_error::PgResult;
use types_sync::{FileTag, SyncRequestType};

seam_core::seam!(
    /// `InitSync(void)` (sync.c) — create the pending-operations table iff this
    /// process tracks sync requests (`!IsUnderPostmaster ||
    /// AmCheckpointerProcess()`). The caller passes that decision as
    /// `create_pending_ops` (read off its own environment), avoiding an
    /// ambient-global getter seam.
    pub fn init_sync(create_pending_ops: bool)
);

seam_core::seam!(
    /// `SyncPreCheckpoint(void)` (sync.c) — pre-checkpoint absorb + bump the
    /// checkpoint cycle counter. Absorbs (allocates), so `Err` carries the
    /// checkpointer-queue `ereport(ERROR)`s.
    pub fn sync_pre_checkpoint() -> PgResult<()>
);

seam_core::seam!(
    /// `SyncPostCheckpoint(void)` (sync.c) — unlink files that can now be safely
    /// removed. `Err` carries fsync/unlink-path `ereport`s above WARNING.
    pub fn sync_post_checkpoint() -> PgResult<()>
);

seam_core::seam!(
    /// `ProcessSyncRequests(void)` (sync.c) — process queued fsync requests
    /// during a checkpoint. Takes the `enableFsync` / `log_checkpoints` GUC
    /// values as parameters (the caller reads them off its own GUC state, not an
    /// ambient getter). `Err` carries the `data_sync_elevel(ERROR)` fsync
    /// failures and the "cannot sync without a pendingOps table" `elog(ERROR)`.
    pub fn process_sync_requests(enable_fsync: bool, log_checkpoints: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `RegisterSyncRequest(ftag, type, retryOnError)` (sync.c) — register the
    /// request locally (standalone/startup: fsync state is local) or forward it
    /// to the checkpointer. Returns whether the request was accepted; `Err`
    /// carries the forward-path `ereport`s and any local OOM.
    pub fn register_sync_request(
        ftag: FileTag,
        request_type: SyncRequestType,
        retry_on_error: bool,
    ) -> PgResult<bool>
);
