//! Seam declarations for the `backend-utils-activity-xact` unit
//! (`utils/activity/pgstat_xact.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use ::mcx::{Mcx, PgVec};
use ::types_error::PgResult;
use ::wal::XlXactStatsItem;

seam_core::seam!(
    /// `AtEOXact_PgStat(isCommit, parallel)` — apply/discard transactional
    /// stats state at top-level transaction end.
    pub fn at_eoxact_pgstat(is_commit: bool, parallel: bool)
);

seam_core::seam!(
    /// `AtEOSubXact_PgStat(isCommit, nestDepth)`.
    pub fn at_eosubxact_pgstat(is_commit: bool, nest_depth: i32)
);

seam_core::seam!(
    /// `AtPrepare_PgStat()` — record transactional stats in the 2PC state.
    pub fn at_prepare_pgstat() -> PgResult<()>
);

seam_core::seam!(
    /// `PostPrepare_PgStat()`.
    pub fn post_prepare_pgstat()
);

seam_core::seam!(
    /// `pgstat_get_transactional_drops(isCommit, &items)` — stats objects this
    /// transaction drops; allocated in `mcx` (C: palloc).
    pub fn pgstat_get_transactional_drops<'mcx>(
        mcx: Mcx<'mcx>,
        is_commit: bool,
    ) -> PgResult<PgVec<'mcx, XlXactStatsItem>>
);

seam_core::seam!(
    /// `pgstat_execute_transactional_drops(ndrops, items, is_redo)`.
    pub fn pgstat_execute_transactional_drops(
        items: &[XlXactStatsItem],
        is_redo: bool,
    ) -> PgResult<()>
);
