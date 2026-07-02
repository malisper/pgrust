use types_error::PgResult;
use types_storage::sync::{FileTag, SyncRequestType};

seam_core::seam!(
    pub fn register_sync_request(
        ftag: FileTag,
        req_type: SyncRequestType,
        retry_on_error: bool,
    ) -> PgResult<bool>
);
