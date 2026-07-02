use types_error::PgResult;
use types_storage::sync::{FileTag, SyncRequestType};

seam_core::seam!(
    pub fn register_sync_request(
        ftag: FileTag,
        req_type: SyncRequestType,
        retry_on_error: bool,
    ) -> PgResult<bool>
);

seam_core::seam!(
    // InitSync (sync.c); reads IsUnderPostmaster/AmCheckpointerProcess itself.
    pub fn init_sync() -> PgResult<()>
);

seam_core::seam!(
    // SyncPreCheckpoint() (sync.c); absorbs (which allocates) before the
    // cycle bump, so it carries C's ereport surface.
    pub fn sync_pre_checkpoint() -> PgResult<()>
);

seam_core::seam!(
    // SyncPostCheckpoint() (sync.c).
    pub fn sync_post_checkpoint() -> PgResult<()>
);

seam_core::seam!(
    // ProcessSyncRequests() (sync.c).
    pub fn process_sync_requests() -> PgResult<()>
);

seam_core::seam!(
    // AbsorbSyncRequests() (checkpointer.c home in C; fronted here with sync).
    pub fn absorb_sync_requests() -> PgResult<()>
);
