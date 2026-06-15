//! Seam declarations for the `backend-catalog-pg-inherits` unit
//! (`catalog/pg_inherits.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

#![allow(non_snake_case)]

use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::PgResult;
use types_storage::lock::LOCKMODE;

seam_core::seam!(
    /// `find_all_inheritors(parentrelId, lockmode, NULL)` (pg_inherits.c) —
    /// all inheritor OIDs (CLUSTER passes `NoLock`).
    pub fn find_all_inheritors<'mcx>(
        mcx: Mcx<'mcx>,
        parent_rel_id: Oid,
        lockmode: LOCKMODE,
    ) -> PgResult<PgVec<'mcx, Oid>>
);

seam_core::seam!(
    /// `typeInheritsFrom(subclassTypeId, superclassTypeId)` (pg_inherits.c) —
    /// whether the relation type `subclassTypeId` is an inheritance child of
    /// the relation type `superclassTypeId` (walking the pg_inherits graph).
    pub fn type_inherits_from(
        subclass_type_id: Oid,
        superclass_type_id: Oid,
    ) -> PgResult<bool>
);
