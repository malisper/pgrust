//! Seam declarations for the `backend-access-transam-timeline` unit
//! (`access/transam/timeline.c`). The owning unit installs these from its
//! `init_seams()`; a cyclic caller depends on this crate and calls through it.
//!
//! `archive_recovery_requested` and `xlog_archiving_active` are the
//! `ArchiveRecoveryRequested` / `XLogArchivingActive()` per-backend globals
//! (owned by xlogrecovery / xlog); per AGENTS.md they are passed in as explicit
//! parameters rather than read through an ambient getter seam.

use types_core::{TimeLineHistoryEntry, TimeLineID, XLogRecPtr};

seam_core::seam!(
    /// `readTimeLineHistory(targetTLI)` — return the component TLIs of
    /// `targetTLI`, newest first, allocated in `mcx`.
    pub fn read_time_line_history<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        target_tli: TimeLineID,
        archive_recovery_requested: bool,
    ) -> types_error::PgResult<mcx::PgVec<'mcx, TimeLineHistoryEntry>>
);

seam_core::seam!(
    /// `existsTimeLineHistory(probeTLI)`.
    pub fn exists_time_line_history<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        probe_tli: TimeLineID,
        archive_recovery_requested: bool,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `findNewestTimeLine(startTLI)`.
    pub fn find_newest_time_line<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        start_tli: TimeLineID,
        archive_recovery_requested: bool,
    ) -> types_error::PgResult<TimeLineID>
);

seam_core::seam!(
    /// `writeTimeLineHistory(newTLI, parentTLI, switchpoint, reason)`.
    pub fn write_time_line_history<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        new_tli: TimeLineID,
        parent_tli: TimeLineID,
        switchpoint: XLogRecPtr,
        reason: &str,
        archive_recovery_requested: bool,
        xlog_archiving_active: bool,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `writeTimeLineHistoryFile(tli, content, size)`.
    pub fn write_time_line_history_file(
        tli: TimeLineID,
        content: &[u8],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `restoreTimeLineHistoryFiles(begin, end)`.
    pub fn restore_time_line_history_files<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        begin: TimeLineID,
        end: TimeLineID,
        archive_recovery_requested: bool,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `tliInHistory(tli, expectedTLEs)` — pure lookup over a history list.
    pub fn tli_in_history(tli: TimeLineID, expected_tles: &[TimeLineHistoryEntry]) -> bool
);

seam_core::seam!(
    /// `tliOfPointInHistory(ptr, history)`.
    pub fn tli_of_point_in_history(
        ptr: XLogRecPtr,
        history: &[TimeLineHistoryEntry],
    ) -> types_error::PgResult<TimeLineID>
);

seam_core::seam!(
    /// `tliSwitchPoint(tli, history, *nextTLI)` — returns
    /// `(switchpoint, nextTLI)`.
    pub fn tli_switch_point(
        tli: TimeLineID,
        history: &[TimeLineHistoryEntry],
    ) -> types_error::PgResult<(XLogRecPtr, TimeLineID)>
);
