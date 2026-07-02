use mcx::{Mcx, PgVec};
use types_core::{AttrNumber, Oid};
use types_error::PgResult;
use types_portal::PortalData;

// A TargetEntry projected to the fields row-description senders read.
#[derive(Clone, Copy, Debug, Default)]
pub struct TargetEntrySummary {
    pub resjunk: bool,
    pub resorigtbl: Oid,
    pub resorigcol: AttrNumber,
}

seam_core::seam!(
    // FetchPortalTargetList (pquery.c); NIL comes back as an empty vec.
    pub fn fetch_portal_target_list<'a, 'mcx>(
        mcx: Mcx<'mcx>,
        portal: &'a PortalData<'a>,
    ) -> PgResult<PgVec<'mcx, TargetEntrySummary>>
);
