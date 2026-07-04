use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_rel::{Relation, LOCKMODE};

seam_core::seam!(
    pub fn index_open<'mcx>(
        mcx: Mcx<'mcx>,
        relation_id: Oid,
        lockmode: LOCKMODE,
    ) -> PgResult<Relation<'mcx>>
);

seam_core::seam!(
    // index_can_return (indexam.c): amcanreturn dispatch; opens the index
    // with AccessShareLock as C's amutils fallback does.
    pub fn index_can_return(mcx: Mcx<'_>, index_oid: Oid, attno: i32) -> PgResult<bool>
);

seam_core::seam!(
    pub fn try_index_open<'mcx>(
        mcx: Mcx<'mcx>,
        relation_id: Oid,
        lockmode: LOCKMODE,
    ) -> PgResult<Option<Relation<'mcx>>>
);
