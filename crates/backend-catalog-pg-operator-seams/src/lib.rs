//! Seam declarations for the `backend-catalog-pg-operator` unit
//! (`catalog/pg_operator.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use types_core::primitive::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `RemoveOperatorById(operOid)` (catalog/pg_operator.c): the per-class
    /// `OCLASS_OPERATOR` drop handler dependency.c's `doDeletion` invokes for a
    /// `pg_operator` object. Removes the operator's catalog row. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn RemoveOperatorById(operOid: Oid) -> PgResult<()>
);
