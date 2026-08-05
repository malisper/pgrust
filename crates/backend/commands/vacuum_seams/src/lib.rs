use types_core::BlockNumber;
use types_error::PgResult;
use types_rel::RelationData;

// Cost-based vacuum delay for index AMs: nbtree cannot depend on
// commands_vacuum (which depends on nbtree), so its per-page delay points
// reach vacuum_delay_point through this seam.
seam_core::seam!(
    pub fn vacuum_delay_point(is_analyze: bool) -> PgResult<()>
);

seam_core::seam!(
    // Returns C's (*frozenxid_updated, *minmulti_updated) out-params: whether
    // relfrozenxid / relminmxid were actually advanced (vacuum.c).
    pub fn vac_update_relstats(
        relation: &RelationData<'_>,
        num_pages: BlockNumber,
        num_tuples: f64,
        num_all_visible_pages: BlockNumber,
        num_all_frozen_pages: BlockNumber,
        hasindex: bool,
        frozenxid: types_core::TransactionId,
        minmulti: types_core::MultiXactId,
        in_outer_xact: bool,
    ) -> PgResult<(bool, bool)>
);
