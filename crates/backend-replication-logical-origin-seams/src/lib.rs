//! Seam declarations for the `backend-replication-logical-origin` unit
//! (`replication/logical/origin.c`, incl. its per-backend
//! `replorigin_session_*` globals). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::{RepOriginId, TimestampTz, XLogRecPtr};
use types_error::PgResult;

seam_core::seam!(
    /// Read `replorigin_session_origin`.
    pub fn replorigin_session_origin() -> RepOriginId
);

seam_core::seam!(
    /// Read `replorigin_session_origin_lsn`.
    pub fn replorigin_session_origin_lsn() -> XLogRecPtr
);

seam_core::seam!(
    /// Read `replorigin_session_origin_timestamp`.
    pub fn replorigin_session_origin_timestamp() -> TimestampTz
);

seam_core::seam!(
    /// Write `replorigin_session_origin_timestamp` (RecordTransactionCommit
    /// fills it in when no origin timestamp was provided).
    pub fn set_replorigin_session_origin_timestamp(timestamp: TimestampTz)
);

seam_core::seam!(
    /// `replorigin_session_advance(remote_commit, local_commit)` — move this
    /// session's origin LSNs forward.
    pub fn replorigin_session_advance(
        remote_commit: XLogRecPtr,
        local_commit: XLogRecPtr,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `replorigin_advance(node, remote_commit, local_commit, go_backward,
    /// wal_log)` — redo-side origin progress update.
    pub fn replorigin_advance(
        node: RepOriginId,
        remote_commit: XLogRecPtr,
        local_commit: XLogRecPtr,
        go_backward: bool,
        wal_log: bool,
    ) -> PgResult<()>
);
