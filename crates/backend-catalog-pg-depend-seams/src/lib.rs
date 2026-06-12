//! Seam declarations for the `backend-catalog-pg-depend` unit
//! (`catalog/pg_depend.c`), for callers that would cycle with the catalog
//! layer (commands, dependency.c, ...).
//!
//! `backend-catalog-pg-depend` installs every one of these from its
//! `init_seams()`.
//!
//! Signature mapping (same as the owning crate): C `long` record counts are
//! `i64`; C `List *` of OIDs is `PgVec<'mcx, Oid>` allocated in the caller's
//! `mcx`; `sequenceIsOwned`'s bool + out-params are `Option<(Oid, i32)>`;
//! the catalog `deptype` byte is `i8`.

#![allow(non_snake_case)]

use mcx::{Mcx, PgVec};
use types_catalog::catalog_dependency::{DependencyType, ObjectAddress};
use types_core::primitive::{AttrNumber, Oid};
use types_error::PgResult;

seam_core::seam!(
    /// `recordDependencyOn(depender, referenced, behavior)`.
    pub fn recordDependencyOn(
        mcx: Mcx<'_>,
        depender: &ObjectAddress,
        referenced: &ObjectAddress,
        behavior: DependencyType,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `recordMultipleDependencies(depender, referenced, nreferenced,
    /// behavior)` — the slice carries the C array + count.
    pub fn recordMultipleDependencies(
        mcx: Mcx<'_>,
        depender: &ObjectAddress,
        referenced: &[ObjectAddress],
        behavior: DependencyType,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `recordDependencyOnCurrentExtension(object, isReplace)`.
    pub fn recordDependencyOnCurrentExtension(
        mcx: Mcx<'_>,
        object: &ObjectAddress,
        isReplace: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `checkMembershipInCurrentExtension(object)`.
    pub fn checkMembershipInCurrentExtension(
        mcx: Mcx<'_>,
        object: &ObjectAddress,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `deleteDependencyRecordsFor(classId, objectId, skipExtensionDeps)`.
    pub fn deleteDependencyRecordsFor(
        classId: Oid,
        objectId: Oid,
        skipExtensionDeps: bool,
    ) -> PgResult<i64>
);

seam_core::seam!(
    /// `deleteDependencyRecordsForClass(classId, objectId, refclassId,
    /// deptype)`.
    pub fn deleteDependencyRecordsForClass(
        classId: Oid,
        objectId: Oid,
        refclassId: Oid,
        deptype: i8,
    ) -> PgResult<i64>
);

seam_core::seam!(
    /// `deleteDependencyRecordsForSpecific(classId, objectId, deptype,
    /// refclassId, refobjectId)`.
    pub fn deleteDependencyRecordsForSpecific(
        classId: Oid,
        objectId: Oid,
        deptype: i8,
        refclassId: Oid,
        refobjectId: Oid,
    ) -> PgResult<i64>
);

seam_core::seam!(
    /// `changeDependencyFor(classId, objectId, refClassId, oldRefObjectId,
    /// newRefObjectId)`.
    pub fn changeDependencyFor(
        mcx: Mcx<'_>,
        classId: Oid,
        objectId: Oid,
        refClassId: Oid,
        oldRefObjectId: Oid,
        newRefObjectId: Oid,
    ) -> PgResult<i64>
);

seam_core::seam!(
    /// `changeDependenciesOf(classId, oldObjectId, newObjectId)`.
    pub fn changeDependenciesOf(
        classId: Oid,
        oldObjectId: Oid,
        newObjectId: Oid,
    ) -> PgResult<i64>
);

seam_core::seam!(
    /// `changeDependenciesOn(refClassId, oldRefObjectId, newRefObjectId)`.
    pub fn changeDependenciesOn(
        mcx: Mcx<'_>,
        refClassId: Oid,
        oldRefObjectId: Oid,
        newRefObjectId: Oid,
    ) -> PgResult<i64>
);

seam_core::seam!(
    /// `getExtensionOfObject(classId, objectId)`.
    pub fn getExtensionOfObject(classId: Oid, objectId: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `getAutoExtensionsOfObject(classId, objectId)`.
    pub fn getAutoExtensionsOfObject<'mcx>(
        mcx: Mcx<'mcx>,
        classId: Oid,
        objectId: Oid,
    ) -> PgResult<PgVec<'mcx, Oid>>
);

seam_core::seam!(
    /// `getExtensionType(extensionOid, typname)`.
    pub fn getExtensionType(
        mcx: Mcx<'_>,
        extensionOid: Oid,
        typname: &str,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `sequenceIsOwned(seqId, deptype, &tableId, &colId)` — `Some((tableId,
    /// colId))` is the C `true` + out-params.
    pub fn sequenceIsOwned(seqId: Oid, deptype: i8) -> PgResult<Option<(Oid, i32)>>
);

seam_core::seam!(
    /// `getOwnedSequences(relid)`.
    pub fn getOwnedSequences<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
    ) -> PgResult<PgVec<'mcx, Oid>>
);

seam_core::seam!(
    /// `getIdentitySequence(rel, attnum, missing_ok)` — the caller's open
    /// `Relation` crosses as `&RelationData`.
    pub fn getIdentitySequence(
        mcx: Mcx<'_>,
        rel: &types_rel::rel::RelationData,
        attnum: AttrNumber,
        missing_ok: bool,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_index_constraint(indexId)`.
    pub fn get_index_constraint(indexId: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_index_ref_constraints(indexId)`.
    pub fn get_index_ref_constraints<'mcx>(
        mcx: Mcx<'mcx>,
        indexId: Oid,
    ) -> PgResult<PgVec<'mcx, Oid>>
);
