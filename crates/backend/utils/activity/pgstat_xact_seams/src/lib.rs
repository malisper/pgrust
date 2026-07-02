use mcx::{Mcx, PgVec};
use types_core::xact::XlXactStatsItem;
use types_error::PgResult;

seam_core::seam!(
    // pgstat_get_transactional_drops(isCommit, &items) (pgstat_xact.c).
    pub fn pgstat_get_transactional_drops<'mcx>(
        mcx: Mcx<'mcx>,
        is_commit: bool,
    ) -> PgResult<PgVec<'mcx, XlXactStatsItem>>
);

seam_core::seam!(
    pub fn at_eoxact_pgstat(is_commit: bool, is_parallel_worker: bool)
);

seam_core::seam!(
    pub fn at_eosubxact_pgstat(is_commit: bool, nest_depth: i32)
);

seam_core::seam!(
    pub fn at_prepare_pgstat() -> PgResult<()>
);

seam_core::seam!(
    pub fn post_prepare_pgstat()
);

seam_core::seam!(
    pub fn pgstat_execute_transactional_drops<'a>(
        items: &'a [XlXactStatsItem],
        is_redo: bool,
    ) -> PgResult<()>
);
