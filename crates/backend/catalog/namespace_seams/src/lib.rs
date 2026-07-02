use mcx::Mcx;
use rel_vocab::RangeVar;
use types_core::Oid;
use types_error::PgResult;
use types_rel::LOCKMODE;

// RangeVarGetRelid: namespace search + lock; with missing_ok, InvalidOid is
// the C not-found return (other causes still ereport). mcx hosts the lookup's
// transient catalog copies.
seam_core::seam!(
    pub fn range_var_get_relid(
        mcx: Mcx<'_>,
        relation: &RangeVar,
        lockmode: LOCKMODE,
        missing_ok: bool,
    ) -> PgResult<Oid>
);
