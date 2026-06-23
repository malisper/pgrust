//! Seam declarations for the `backend-catalog-pg-shdepend` unit
//! (`catalog/pg_shdepend.c`): the public entry points other catalog/command
//! units call across a dependency cycle (GRANT/REVOKE, ALTER OWNER, CREATE
//! DATABASE, DROP DATABASE, DROP/REASSIGN OWNED, generic object drop).
//!
//! `backend-catalog-pg-shdepend` installs every one of these from its own
//! `init_seams()`. The allocating entry points take `Mcx<'mcx>` and carry the
//! caller's lifetime on their allocated outputs.

#![allow(non_snake_case)]

use mcx::{Mcx, PgString, PgVec};
use ::types_catalog::catalog_dependency::ObjectAddress;
use ::types_catalog::catalog_shdepend::SharedDependencyType;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::nodes::parsenodes::DropBehavior;

seam_core::seam!(
    /// `recordSharedDependencyOn(depender, referenced, deptype)`.
    pub fn recordSharedDependencyOn(
        depender: &ObjectAddress,
        referenced: &ObjectAddress,
        deptype: SharedDependencyType,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `recordDependencyOnOwner(classId, objectId, owner)`.
    pub fn recordDependencyOnOwner(class_id: Oid, object_id: Oid, owner: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `changeDependencyOnOwner(classId, objectId, newOwnerId)`.
    pub fn changeDependencyOnOwner(
        class_id: Oid,
        object_id: Oid,
        new_owner_id: Oid,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `recordDependencyOnTablespace(classId, objectId, tablespace)`.
    pub fn recordDependencyOnTablespace(
        class_id: Oid,
        object_id: Oid,
        tablespace: Oid,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `changeDependencyOnTablespace(classId, objectId, newTablespaceId)`.
    pub fn changeDependencyOnTablespace(
        class_id: Oid,
        object_id: Oid,
        new_tablespace_id: Oid,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `updateAclDependencies(classId, objectId, objsubId, ownerId,
    /// noldmembers, oldmembers, nnewmembers, newmembers)`. The role-id arrays
    /// (sorted, de-duped) cross as owned `PgVec`s, which the routine consumes.
    pub fn updateAclDependencies<'mcx>(
        mcx: Mcx<'mcx>,
        class_id: Oid,
        object_id: Oid,
        objsub_id: i32,
        owner_id: Oid,
        oldmembers: PgVec<'mcx, Oid>,
        newmembers: PgVec<'mcx, Oid>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `updateInitAclDependencies(classId, objectId, objsubId, noldmembers,
    /// oldmembers, nnewmembers, newmembers)`.
    pub fn updateInitAclDependencies<'mcx>(
        mcx: Mcx<'mcx>,
        class_id: Oid,
        object_id: Oid,
        objsub_id: i32,
        oldmembers: PgVec<'mcx, Oid>,
        newmembers: PgVec<'mcx, Oid>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `checkSharedDependencies(classId, objectId, &detail_msg,
    /// &detail_log_msg)`: returns `(has_deps, detail, detail_log)` — the two
    /// `char **` out-params are `Option<PgString>` (the C NULL when no
    /// dependents are found). Strings are allocated in `mcx`.
    pub fn checkSharedDependencies<'mcx>(
        mcx: Mcx<'mcx>,
        class_id: Oid,
        object_id: Oid,
    ) -> PgResult<(bool, Option<PgString<'mcx>>, Option<PgString<'mcx>>)>
);

seam_core::seam!(
    /// `copyTemplateDependencies(templateDbId, newDbId)`.
    pub fn copyTemplateDependencies(template_db_id: Oid, new_db_id: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `dropDatabaseDependencies(databaseId)`.
    pub fn dropDatabaseDependencies(database_id: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `deleteSharedDependencyRecordsFor(classId, objectId, objectSubId)`.
    pub fn deleteSharedDependencyRecordsFor(
        class_id: Oid,
        object_id: Oid,
        object_sub_id: i32,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `shdepLockAndCheckObject(classId, objectId)`.
    pub fn shdepLockAndCheckObject(class_id: Oid, object_id: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `shdepDropOwned(roleids, behavior)`. The role-id list crosses as a
    /// slice (the C `List *`).
    pub fn shdepDropOwned(roleids: &[Oid], behavior: DropBehavior) -> PgResult<()>
);

seam_core::seam!(
    /// `shdepReassignOwned(roleids, newrole)`.
    pub fn shdepReassignOwned(roleids: &[Oid], newrole: Oid) -> PgResult<()>
);
