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

seam_core::seam!(
    // StartupReplicationOrigin() (origin.c).
    pub fn startup_replication_origin() -> PgResult<()>
);

seam_core::seam!(
    // CheckPointReplicationOrigin() (origin.c).
    pub fn check_point_replication_origin() -> PgResult<()>
);

seam_core::seam!(
    // replorigin_redo (origin.c) — the REPLORIGIN rmgr rm_redo callback;
    // rmgr's table row calls through this seam.
    pub fn replorigin_redo(record: &mut xlogreader_seams::XLogReaderState) -> PgResult<()>
);
