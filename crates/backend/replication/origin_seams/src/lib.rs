use types_core::{RepOriginId, TimestampTz, XLogRecPtr};
use types_error::PgResult;

seam_core::seam!(
    // replorigin_session_origin (origin.c global).
    pub fn replorigin_session_origin() -> RepOriginId
);

seam_core::seam!(
    pub fn replorigin_session_origin_lsn() -> XLogRecPtr
);

seam_core::seam!(
    pub fn replorigin_session_origin_timestamp() -> TimestampTz
);

seam_core::seam!(
    pub fn set_replorigin_session_origin_timestamp(ts: TimestampTz)
);

seam_core::seam!(
    pub fn replorigin_session_advance(remote_commit: XLogRecPtr, local_commit: XLogRecPtr) -> PgResult<()>
);

seam_core::seam!(
    pub fn replorigin_advance(
        node: RepOriginId,
        remote_commit: XLogRecPtr,
        local_commit: XLogRecPtr,
        go_backward: bool,
        wal_log: bool,
    ) -> PgResult<()>
);
