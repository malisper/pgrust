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

seam_core::seam!(
    /// `StoreSingleInheritance(relationId, parentOid, seqNumber)`
    /// (pg_inherits.c): insert a single `pg_inherits` row recording that
    /// `relationId` inherits from `parentOid` at ordinal `seqNumber`. Called by
    /// `index_create` (catalog/index.c) to link a partition index to its parent
    /// index. `Err` carries the catalog-insert `ereport(ERROR)`s.
    pub fn store_single_inheritance(
        relation_id: Oid,
        parent_oid: Oid,
        seq_number: i32,
    ) -> PgResult<()>
);
