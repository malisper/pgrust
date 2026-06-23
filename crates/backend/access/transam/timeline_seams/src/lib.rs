//! Seam declarations for the `backend-access-transam-timeline` unit
//! (`access/transam/timeline.c`): timeline-history file reading/writing, the
//! existence/newest probes, and the in-memory switch-point lookups.
//!
//! The owning unit installs these from its `init_seams()`; a cyclic caller
//! depends on this crate and calls through it.
//!
//! `archive_recovery_requested` and `xlog_archiving_active` are the
//! `ArchiveRecoveryRequested` / `XLogArchivingActive()` per-backend globals
//! (owned by xlogrecovery / xlog); per AGENTS.md they are passed in as explicit
//! parameters rather than read through an ambient getter seam.

use mcx::{Mcx, PgVec};
use types_core::{TimeLineID, XLogRecPtr};
use types_error::PgResult;
use wal::TimeLineHistoryEntry;

seam_core::seam!(
    /// `readTimeLineHistory(targetTLI)` (timeline.c) — read and parse the
    /// timeline-history file for `target_tli`, returning the entries newest
    /// first (the C `List *` built head-insert). The list and its cells are
    /// palloc'd in the caller's current context, so the port allocates in
    /// `mcx`; `Err` carries the file-read `ereport(ERROR)` and OOM.
    pub fn read_timeline_history<'mcx>(
        mcx: Mcx<'mcx>,
        target_tli: TimeLineID,
    ) -> PgResult<PgVec<'mcx, TimeLineHistoryEntry>>
);

seam_core::seam!(
    /// `existsTimeLineHistory(probeTLI)` (timeline.c) — whether a
    /// timeline-history file exists for `probe_tli`. `ereport(FATAL)` on a
    /// non-`ENOENT` open failure, hence `PgResult`.
    pub fn exists_timeline_history<'mcx>(
        mcx: Mcx<'mcx>,
        probe_tli: TimeLineID,
        archive_recovery_requested: bool,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `findNewestTimeLine(startTLI)` (timeline.c) — the newest existing
    /// timeline, assuming `start_tli` exists. Propagates the probe's `FATAL`.
    pub fn find_newest_timeline<'mcx>(
        mcx: Mcx<'mcx>,
        start_tli: TimeLineID,
        archive_recovery_requested: bool,
    ) -> PgResult<TimeLineID>
);

seam_core::seam!(
    /// `writeTimeLineHistory(newTLI, parentTLI, switchpoint, reason)`
    /// (timeline.c) — assemble the new history file from the parent and the
    /// split line, fsync, and durably rename into place. `ereport(ERROR)` on
    /// any I/O failure.
    pub fn write_timeline_history<'mcx>(
        mcx: Mcx<'mcx>,
        new_tli: TimeLineID,
        parent_tli: TimeLineID,
        switchpoint: XLogRecPtr,
        reason: &str,
        archive_recovery_requested: bool,
        xlog_archiving_active: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `writeTimeLineHistoryFile(tli, content, len)` (timeline.c) — write the
    /// streamed history file to `pg_wal`; `ereport(ERROR)` on I/O failure.
    pub fn write_timeline_history_file(
        tli: TimeLineID,
        content: &[u8],
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `restoreTimeLineHistoryFiles(begin, end)` (timeline.c) — copy the
    /// history files in `[begin, end)` from archive to `pg_wal`.
    pub fn restore_timeline_history_files<'mcx>(
        mcx: Mcx<'mcx>,
        begin: TimeLineID,
        end: TimeLineID,
        archive_recovery_requested: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `tliInHistory(tli, expectedTLEs)` (timeline.c) — pure lookup over a
    /// history list: whether `tli` appears in `expected_tles`.
    pub fn tli_in_history(tli: TimeLineID, expected_tles: &[TimeLineHistoryEntry]) -> bool
);

seam_core::seam!(
    /// `tliOfPointInHistory(ptr, history)` (timeline.c) — the timeline of the
    /// last LSN on the segment containing `ptr`, looked up in the already-read
    /// `history` (newest first, as returned by `read_timeline_history`).
    pub fn tli_of_point_in_history(
        ptr: XLogRecPtr,
        history: &[TimeLineHistoryEntry],
    ) -> PgResult<TimeLineID>
);

seam_core::seam!(
    /// `tliSwitchPoint(tli, history, *nextTLI)` (timeline.c) — find the LSN at
    /// which `tli` ended; returns `(switchpoint, next_tli)` (`next_tli`
    /// mirrors the non-NULL `*nextTLI` out-param, the timeline `tli` switched
    /// to). `elog(ERROR)` if `tli` is not found in `history`.
    pub fn tli_switch_point(
        tli: TimeLineID,
        history: &[TimeLineHistoryEntry],
    ) -> PgResult<(XLogRecPtr, TimeLineID)>
);

seam_core::seam!(
    /// `TLHistoryFileName(fname, tli)` (xlog_internal.h) — the canonical
    /// timeline-history file *name* (`"%08X.history"`).
    pub fn tl_history_file_name(tli: TimeLineID) -> String
);

seam_core::seam!(
    /// `TLHistoryFilePath(path, tli)` (xlog_internal.h) — the canonical
    /// timeline-history file *path* (`XLOGDIR "/%08X.history"`).
    pub fn tl_history_file_path(tli: TimeLineID) -> String
);
