use mcx::{Mcx, PgVec};
use types_core::{TimeLineID, XLogRecPtr};
use types_error::PgResult;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimeLineHistoryEntry {
    pub tli: TimeLineID,
    pub begin: XLogRecPtr,
    pub end: XLogRecPtr,
}

seam_core::seam!(
    pub fn read_timeline_history<'mcx>(
        mcx: Mcx<'mcx>,
        target_tli: TimeLineID,
    ) -> PgResult<PgVec<'mcx, TimeLineHistoryEntry>>
);

seam_core::seam!(
    pub fn tli_of_point_in_history(
        ptr: XLogRecPtr,
        history: &[TimeLineHistoryEntry],
    ) -> PgResult<TimeLineID>
);

seam_core::seam!(
    // Returns (switchpoint, nextTLI).
    pub fn tli_switch_point(
        tli: TimeLineID,
        history: &[TimeLineHistoryEntry],
    ) -> PgResult<(XLogRecPtr, TimeLineID)>
);

seam_core::seam!(
    pub fn restore_timeline_history_files(begin: TimeLineID, end: TimeLineID) -> PgResult<()>
);

seam_core::seam!(
    pub fn find_newest_timeline(start_tli: TimeLineID) -> PgResult<TimeLineID>
);

seam_core::seam!(
    pub fn write_timeline_history<'a>(
        new_tli: TimeLineID,
        parent_tli: TimeLineID,
        switchpoint: XLogRecPtr,
        reason: &'a str,
    ) -> PgResult<()>
);

seam_core::seam!(
    // existsTimeLineHistory(probeTLI) (timeline.c).
    pub fn exists_timeline_history(probe_tli: TimeLineID) -> PgResult<bool>
);
