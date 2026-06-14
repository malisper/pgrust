//! Seam declarations for the `backend-catalog-objectaddress` unit
//! (`catalog/objectaddress.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgString};
use types_catalog::catalog_dependency::ObjectAddress;
use types_core::Oid;
use types_error::PgResult;
use types_nodes::parsenodes::ObjectType;
use types_parsenodes::Node;
use types_rel::Relation;
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
