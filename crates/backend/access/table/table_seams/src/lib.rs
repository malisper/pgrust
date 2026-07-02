use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_rel::{Relation, LOCKMODE};

seam_core::seam!(
    pub fn table_open<'mcx>(
        mcx: Mcx<'mcx>,
        relation_id: Oid,
        lockmode: LOCKMODE,
    ) -> PgResult<Relation<'mcx>>
);

seam_core::seam!(
    pub fn try_table_open<'mcx>(
        mcx: Mcx<'mcx>,
        relation_id: Oid,
        lockmode: LOCKMODE,
    ) -> PgResult<Option<Relation<'mcx>>>
);
