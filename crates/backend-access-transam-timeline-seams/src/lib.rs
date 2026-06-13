//! Seam declarations for the `backend-access-transam-timeline` unit
//! (`access/transam/timeline.c`): timeline-history file reading and the
//! switch-point lookup.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgVec};
use types_core::{TimeLineID, XLogRecPtr};
use types_error::PgResult;
use types_wal::TimeLineHistoryEntry;

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
    /// `tliSwitchPoint(tli, history, *nextTLI)` (timeline.c) — find the LSN at
    /// which `tli` ended; returns `(switchpoint, next_tli)` (`next_tli`
    /// mirrors the non-NULL `*nextTLI` out-param, the timeline `tli` switched
    /// to). `elog(ERROR)` if `tli` is not found in `history`.
    pub fn tli_switch_point(
        tli: TimeLineID,
        history: &[TimeLineHistoryEntry],
    ) -> PgResult<(XLogRecPtr, TimeLineID)>
);
