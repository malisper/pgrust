//! Seam declarations for the `backend-catalog-pg-attrdef` unit
//! (`catalog/pg_attrdef.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use types_core::primitive::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `RemoveAttrDefaultById(attrdefId)` (catalog/pg_attrdef.c): the per-class
    /// `OCLASS_DEFAULT` drop handler dependency.c's `doDeletion` invokes for a
    /// `pg_attrdef` object. Removes the column-default catalog row and clears
    /// the owning column's `atthasdef`. Can `ereport(ERROR)`, carried on `Err`.
    pub fn RemoveAttrDefaultById(attrdefId: Oid) -> PgResult<()>
);
