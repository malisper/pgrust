use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    // doDeletion's REWRITE_RELATION_ID arm (catalog_dependency <- rewrite_define cycle).
    pub fn remove_rewrite_rule_by_id(mcx: Mcx<'_>, rule_oid: Oid) -> PgResult<()>
);
