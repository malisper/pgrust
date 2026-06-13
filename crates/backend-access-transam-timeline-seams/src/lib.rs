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

/// Result of `tliSwitchPoint(tli, history, &nextTLI)` — the LSN at which the
/// timeline ends (`InvalidXLogRecPtr` for the current one) and the next
/// timeline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimelineSwitch {
    /// `tliSwitchPoint` return — the LSN where `tli` ends.
    pub valid_until: types_core::XLogRecPtr,
    /// `*nextTLI` out-param — the timeline `tli` switches into.
    pub next_tli: TimeLineID,
}

seam_core::seam!(
    /// `tliOfPointInHistory(ptr, readTimeLineHistory(curr_tli))`
    /// (timeline.c) — the timeline of the last LSN on the segment containing
    /// `ptr`, looked up in the history of `curr_tli`. Reads the
    /// timeline-history file; `ereport(ERROR)` on a malformed file, carried
    /// on `Err`. The history list is read and freed inside the owner.
    pub fn tli_of_point_in_history(
        curr_tli: TimeLineID,
        ptr: types_core::XLogRecPtr,
    ) -> types_error::PgResult<TimeLineID>
);

seam_core::seam!(
    /// `tliSwitchPoint(tli, readTimeLineHistory(curr_tli), &nextTLI)`
    /// (timeline.c) — the switch point of `tli` within the history of
    /// `curr_tli`, plus the next timeline. `ereport(ERROR)` on a malformed
    /// history file, carried on `Err`.
    pub fn tli_switch_point(
        curr_tli: TimeLineID,
        tli: TimeLineID,
    ) -> types_error::PgResult<TimelineSwitch>
);
