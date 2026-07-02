use mcx::Mcx;
use rel_vocab::RangeVar;
use types_core::Oid;
use types_error::PgResult;
use types_rel::{Relation, LOCKMODE};

seam_core::seam!(
    pub fn relation_open<'mcx>(
        mcx: Mcx<'mcx>,
        relation_id: Oid,
        lockmode: LOCKMODE,
    ) -> PgResult<Relation<'mcx>>
);

// Ok(None) on the try_/missing_ok flavors is the C NULL for a missing relation.
seam_core::seam!(
    pub fn try_relation_open<'mcx>(
        mcx: Mcx<'mcx>,
        relation_id: Oid,
        lockmode: LOCKMODE,
    ) -> PgResult<Option<Relation<'mcx>>>
);

seam_core::seam!(
    pub fn relation_openrv<'mcx>(
        mcx: Mcx<'mcx>,
        relation: &RangeVar,
        lockmode: LOCKMODE,
    ) -> PgResult<Relation<'mcx>>
);

seam_core::seam!(
    pub fn relation_openrv_extended<'mcx>(
        mcx: Mcx<'mcx>,
        relation: &RangeVar,
        lockmode: LOCKMODE,
        missing_ok: bool,
    ) -> PgResult<Option<Relation<'mcx>>>
);
