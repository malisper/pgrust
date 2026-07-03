use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    // RemoveRewriteRuleById (rewriteRemove.c); catalog_dependency's doDeletion
    // cannot dep rewrite_define (it deps catalog_dependency).
    pub fn remove_rewrite_rule_by_id(mcx: Mcx<'_>, rule_oid: Oid) -> PgResult<()>
);
