use types_core::BlockNumber;
use types_error::PgResult;
use types_rel::RelationData;

seam_core::seam!(
    pub fn vac_update_relstats(
        relation: &RelationData<'_>,
        num_pages: BlockNumber,
        num_tuples: f64,
        num_all_visible_pages: BlockNumber,
        num_all_frozen_pages: BlockNumber,
        hasindex: bool,
        in_outer_xact: bool,
    ) -> PgResult<()>
);
