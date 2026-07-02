use mcx::{Mcx, PgVec};
use types_core::{TimeLineID, XLogRecPtr};
use types_error::PgResult;

// TimeLineHistoryEntry (access/timeline.h).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimeLineHistoryEntry {
    pub tli: TimeLineID,
    pub begin: XLogRecPtr,
    pub end: XLogRecPtr,
}

seam_core::seam!(
    // readTimeLineHistory(targetTLI) (timeline.c); newest entry first.
    pub fn read_timeline_history<'mcx>(
        mcx: Mcx<'mcx>,
        target_tli: TimeLineID,
    ) -> PgResult<PgVec<'mcx, TimeLineHistoryEntry>>
);

seam_core::seam!(
    // tliOfPointInHistory(ptr, history) (timeline.c).
    pub fn tli_of_point_in_history(ptr: XLogRecPtr, history: &[TimeLineHistoryEntry]) -> TimeLineID
);

seam_core::seam!(
    // tliSwitchPoint(tli, history, *nextTLI) (timeline.c); returns
    // (switchpoint, nextTLI).
    pub fn tli_switch_point(
        tli: TimeLineID,
        history: &[TimeLineHistoryEntry],
    ) -> (XLogRecPtr, TimeLineID)
);
