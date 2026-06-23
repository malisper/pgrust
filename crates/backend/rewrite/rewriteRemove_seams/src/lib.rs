//! Seam declarations for the `backend-rewrite-rewriteRemove` unit
//! (`rewrite/rewriteRemove.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use ::types_core::primitive::Oid;
use ::types_error::PgResult;

seam_core::seam!(
    /// `RemoveRewriteRuleById(ruleOid)` (rewrite/rewriteRemove.c): the per-class
    /// `OCLASS_REWRITE` drop handler dependency.c's `doDeletion` invokes for a
    /// `pg_rewrite` object. Removes the rewrite rule's catalog row. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn RemoveRewriteRuleById(ruleOid: Oid) -> PgResult<()>
);
