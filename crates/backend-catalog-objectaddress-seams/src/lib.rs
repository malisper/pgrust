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

seam_core::seam!(
    /// `get_object_address(objtype, object, &relp, lockmode, missing_ok)`
    /// (objectaddress.c): resolve a (possibly schema-qualified) object
    /// reference to its [`ObjectAddress`], taking `lockmode` on it (and on any
    /// containing relation). For relation-member objects the containing
    /// relation is opened and returned (the C `*relp` out-parameter) so the
    /// caller can release the relcache reference while keeping the lock; for
    /// other object types it is `None`.
    ///
    /// With `missing_ok = true` a missing object yields an [`ObjectAddress`]
    /// whose `objectId` is `InvalidOid` rather than an error; with
    /// `missing_ok = false` (or any other catalog failure) the error is
    /// carried on `Err`. `mcx` anchors the lifetime of the opened relation
    /// (the relcache arena the caller can later release).
    pub fn get_object_address<'r>(
        mcx: Mcx<'r>,
        object_type: ObjectType,
        object: &Node,
        lockmode: LOCKMODE,
        missing_ok: bool,
    ) -> PgResult<(ObjectAddress, Option<Relation<'r>>)>
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
    /// the object, raising `ERRCODE_INSUFFICIENT_PRIVILEGE` otherwise (carried
    /// on `Err`). `relation` is the open relation alias for relation-member
    /// objects, else `None`.
    pub fn check_object_ownership<'r>(
        roleid: Oid,
        objtype: ObjectType,
        address: ObjectAddress,
        object: &Node,
        relation: Option<&Relation<'r>>,
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
