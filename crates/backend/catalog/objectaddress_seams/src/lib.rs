//! Seam declarations for the `backend-catalog-objectaddress` unit
//! (`catalog/objectaddress.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use mcx::{Mcx, PgString};
use types_catalog::catalog_dependency::ObjectAddress;
use types_core::Oid;
use types_error::PgResult;
use ::nodes::parsenodes::ObjectType;
use parsenodes::Node;
use rel::Relation;
use types_storage::lock::LOCKMODE;

/// `get_object_address(objtype, object, &relation, lockmode, missing_ok)`
/// (objectaddress.c) result: the resolved [`ObjectAddress`] plus, for the
/// relation-based object kinds, the relation it opened (`relation` out-param;
/// `None` for non-relation objects, where C leaves `relation == NULL`). The
/// caller closes the relation (`relation_close(rel, NoLock)`) once done.
pub struct ResolvedObjectAddress<'mcx> {
    pub address: ObjectAddress,
    pub relation: Option<Relation<'mcx>>,
}

seam_core::seam!(
    /// `get_object_address(objtype, object, &relp, lockmode, missing_ok)`
    /// (objectaddress.c) — resolve the parser representation behind `object`
    /// to an [`ObjectAddress`], taking `lockmode` on the target to guard
    /// against concurrent modification, and returning whatever relation it
    /// opened (the C `*relp` out-parameter): for relation-member objects the
    /// containing relation is opened so the caller can release the relcache
    /// reference while keeping the lock; for other object types it is `None`.
    ///
    /// `missing_ok` mirrors the C `bool missing_ok`: when `true` a missing
    /// object yields a [`ResolvedObjectAddress`] whose `address.objectId` is
    /// `InvalidOid` rather than an error; when `false` (the default for most
    /// callers) a vanished object raises (`Err`). Any other catalog failure is
    /// carried on `Err` regardless. `mcx` anchors the lifetime of the opened
    /// relation (the relcache arena the caller can later release).
    pub fn get_object_address<'mcx>(
        mcx: Mcx<'mcx>,
        objtype: ObjectType,
        object: &Node,
        lockmode: LOCKMODE,
        missing_ok: bool,
    ) -> PgResult<ResolvedObjectAddress<'mcx>>
);

seam_core::seam!(
    /// `get_object_namespace(&address)` (objectaddress.c): the OID of the
    /// schema containing the object, or `InvalidOid` for an object that is not
    /// schema-qualified. Catalog lookups can `ereport(ERROR)`, carried on
    /// `Err`.
    pub fn get_object_namespace(address: &ObjectAddress) -> PgResult<Oid>
);

seam_core::seam!(
    /// `check_object_ownership(roleid, objtype, address, object, relation)`
    /// (objectaddress.c): verify that `roleid` owns (or otherwise may drop)
    /// the object, raising `ERRCODE_INSUFFICIENT_PRIVILEGE` /
    /// `ACLCHECK_NOT_OWNER` → `ereport(ERROR)` otherwise (carried on `Err`).
    /// `relation` is the open relation alias for relation-member objects, else
    /// `None`.
    pub fn check_object_ownership<'mcx>(
        roleid: types_core::Oid,
        objtype: ObjectType,
        address: ObjectAddress,
        object: &Node,
        relation: Option<&Relation<'mcx>>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `getObjectDescription(object, missing_ok)` (objectaddress.c): a
    /// human-readable description of the object, palloc'd in the caller's
    /// current context (here: `mcx`). Returns `Ok(None)` for the C NULL —
    /// which arises either when `missing_ok = true` and the object vanished,
    /// or when the per-class format function returns an empty buffer (an
    /// object dropped concurrently). With `missing_ok = false` a vanished
    /// object raises (`Err`); the description machinery's catalog lookups can
    /// `ereport(ERROR)` too. `Err` includes OOM from the copy.
    pub fn get_object_description<'mcx>(
        mcx: Mcx<'mcx>,
        object: &ObjectAddress,
        missing_ok: bool,
    ) -> PgResult<Option<PgString<'mcx>>>
);

seam_core::seam!(
    /// `get_relkind_objtype(relkind)` (objectaddress.c): map a `pg_class`
    /// relkind to the `ObjectType` used in error messages. Total mapping
    /// (unknown relkinds return `OBJECT_TABLE`); cannot `ereport`.
    pub fn get_relkind_objtype(relkind: u8) -> ObjectType
);

seam_core::seam!(
    /// `get_object_catcache_oid(classId)` (objectaddress.c) — the syscache id for
    /// looking up a row in catalog `classId` by its OID, or `-1` when the catalog
    /// has no by-OID syscache (use the systable path then).
    pub fn get_object_catcache_oid(class_id: Oid) -> PgResult<i32>
);

seam_core::seam!(
    /// `get_object_attnum_oid(classId)` (objectaddress.c) — the attribute number
    /// of the OID column in catalog `classId`.
    pub fn get_object_attnum_oid(class_id: Oid) -> PgResult<i16>
);

seam_core::seam!(
    /// `get_object_oid_index(classId)` (objectaddress.c) — the OID index relation
    /// for catalog `classId` (used to drive a `systable` scan).
    pub fn get_object_oid_index(class_id: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_object_class_descr(classId)` (objectaddress.c) — a human-readable
    /// description of catalog `classId`, used in error messages.
    pub fn get_object_class_descr(class_id: Oid) -> PgResult<&'static str>
);

seam_core::seam!(
    /// The descriptive-field computation `EventTriggerSQLDropAddObject`
    /// (`event_trigger.c`) performs for a dropped object: the
    /// `obtain_object_name_namespace` schema/name + temp-namespace filter
    /// (including the `NamespaceRelationId` / `AttrDefaultRelationId` /
    /// `TriggerRelationId` / `PolicyRelationId` special cases),
    /// `getObjectIdentityParts` and `getObjectTypeDescription`. Owned by
    /// objectaddress because the `ObjectProperty` table and identity/type
    /// machinery live there; `event_trigger.c`'s caller only owns the
    /// `currentEventTriggerState->SQLDropList` it appends the result to.
    ///
    /// Returns `info.report == false` for an object in another backend's temp
    /// namespace (the C early `return` that records nothing).
    pub fn event_trigger_describe_dropped_object<'mcx>(
        mcx: Mcx<'mcx>,
        object: &ObjectAddress,
    ) -> PgResult<types_catalog::pg_event_trigger::SqlDropObjectInfo>
);

seam_core::seam!(
    /// The per-command descriptive-field computation
    /// `pg_event_trigger_ddl_commands` (`event_trigger.c`) performs for one
    /// `CollectedCommand` whose address is an ordinary object:
    /// `getObjectIdentity(addr, true)`, `getObjectTypeDescription(addr, true)`,
    /// and the namespace lookup. Owned by objectaddress (the identity / type /
    /// `ObjectProperty` machinery lives there); `event_trigger.c`'s caller only
    /// owns the `currentEventTriggerState->commandList` it iterates.
    ///
    /// `Ok(None)` mirrors the C `if (identity == NULL) continue;` (object
    /// dropped in the same command). Otherwise `(identity, type, schema)` where
    /// `schema` is `None` for a schema-less object class.
    pub fn event_trigger_describe_command_object<'mcx>(
        mcx: Mcx<'mcx>,
        object: &ObjectAddress,
    ) -> PgResult<Option<(String, String, Option<String>)>>
);
