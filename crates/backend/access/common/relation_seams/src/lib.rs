use mcx::Mcx;
use rel_vocab::{RangeVar, Relation, LOCKMODE};
use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    pub fn relation_open<'mcx>(
        mcx: Mcx<'mcx>,
        relation_id: Oid,
        lockmode: LOCKMODE,
    ) -> PgResult<Relation<'mcx>>
);

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

// Consumes the handle: refcount drop plus, when lockmode != NoLock, lock release.
seam_core::seam!(
    pub fn relation_close(relation: Relation<'_>, lockmode: LOCKMODE) -> PgResult<()>
);
