//! Seam declarations for the `backend-rewrite-rewriteSupport` unit
//! (`rewrite/rewriteSupport.c`): rule-name resolution.
//!
//! The owning unit (rewriteSupport.c, not yet ported) installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `get_rewrite_oid(relid, rulename, missing_ok)` (rewriteSupport.c): the
    /// OID of the named rewrite rule on relation `relid`, or `InvalidOid` with
    /// `missing_ok = true`. With `missing_ok = false` a miss raises
    /// `ERRCODE_UNDEFINED_OBJECT` (`Err`). Used by `get_object_address`'s
    /// `OBJECT_RULE` arm.
    pub fn get_rewrite_oid(relid: Oid, rulename: &str, missing_ok: bool) -> PgResult<Oid>
);
