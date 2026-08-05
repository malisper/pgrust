use types_core::{BlockNumber, Size};
use types_error::PgResult;
use types_rel::RelationData;

seam_core::seam!(
    // GetPageWithFreeSpace (freespace.c); InvalidBlockNumber = FSM knows nothing.
    pub fn get_page_with_free_space<'a, 'mcx>(
        rel: &'a RelationData<'mcx>,
        space_needed: Size,
    ) -> PgResult<BlockNumber>
);

seam_core::seam!(
    pub fn record_and_get_page_with_free_space<'a, 'mcx>(
        rel: &'a RelationData<'mcx>,
        old_page: BlockNumber,
        old_space_avail: Size,
        space_needed: Size,
    ) -> PgResult<BlockNumber>
);

seam_core::seam!(
    pub fn record_page_with_free_space<'a, 'mcx>(
        rel: &'a RelationData<'mcx>,
        heap_blk: BlockNumber,
        space_avail: Size,
    ) -> PgResult<()>
);

seam_core::seam!(
    // FreeSpaceMapVacuumRange (freespace.c): end is exclusive.
    pub fn free_space_map_vacuum_range<'a, 'mcx>(
        rel: &'a RelationData<'mcx>,
        start: BlockNumber,
        end: BlockNumber,
    ) -> PgResult<()>
);
