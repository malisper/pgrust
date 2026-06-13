//! Seam declarations for the `backend-access-transam-timeline` unit
//! (`access/transam/timeline.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::TimeLineID;

seam_core::seam!(
    /// `existsTimeLineHistory(tli)` — whether a timeline-history file exists.
    pub fn exists_timeline_history(tli: TimeLineID) -> bool
);

seam_core::seam!(
    /// `writeTimeLineHistoryFile(tli, content, len)` — write the streamed
    /// history file to `pg_wal`; `ereport(ERROR)` on I/O failure.
    pub fn write_timeline_history_file(
        tli: TimeLineID,
        content: Vec<u8>
    ) -> types_error::PgResult<()>
);
