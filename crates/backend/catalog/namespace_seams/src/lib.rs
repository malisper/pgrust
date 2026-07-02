use mcx::Mcx;
use rel_vocab::RangeVar;
use types_core::Oid;
use types_error::PgResult;
use types_rel::LOCKMODE;

seam_core::seam!(
    pub fn range_var_get_relid(
        mcx: Mcx<'_>,
        relation: &RangeVar,
        lockmode: LOCKMODE,
        missing_ok: bool,
    ) -> PgResult<Oid>
);
