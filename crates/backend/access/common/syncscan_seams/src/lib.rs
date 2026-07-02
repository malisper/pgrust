use types_core::BlockNumber;
use types_error::PgResult;
use types_rel::RelationData;

seam_core::seam!(
    pub fn ss_get_location<'a, 'mcx>(
        rel: &'a RelationData<'mcx>,
        relnblocks: BlockNumber,
    ) -> PgResult<BlockNumber>
);

seam_core::seam!(
    pub fn ss_report_location<'a, 'mcx>(
        rel: &'a RelationData<'mcx>,
        location: BlockNumber,
    ) -> PgResult<()>
);
