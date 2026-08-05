seam_core::seam!(
    // RequestCheckpoint(flags) (checkpointer.c).
    pub fn request_checkpoint(flags: i32) -> types_error::PgResult<()>
);

use types_storage::sync::{FileTag, SyncRequestType};

seam_core::seam!(
    // ForwardSyncRequest(ftag, type) (checkpointer.c): backend -> checkpointer
    // shmem queue; false = queue full. Compaction allocates, hence PgResult.
    pub fn forward_sync_request(ftag: FileTag, req_type: SyncRequestType) -> types_error::PgResult<bool>
);

seam_core::seam!(
    // CheckpointWriteDelay(flags, progress) (checkpointer.c).
    pub fn checkpoint_write_delay(flags: i32, progress: f64) -> types_error::PgResult<()>
);
