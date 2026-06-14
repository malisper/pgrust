//! Seam declarations for the `backend-catalog-objectaddress` unit
//! (`catalog/objectaddress.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgString};
use types_catalog::catalog_dependency::ObjectAddress;
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
    /// `get_object_address(objtype, object, &relation, lockmode, false)`
    /// (objectaddress.c) — resolve the parser representation behind `object`
    /// to an `ObjectAddress`, taking `lockmode` on the target to guard against
    /// concurrent modification, and returning whatever relation it opened.
    /// `ereport(ERROR)`s if the object does not exist.
    pub fn get_object_address<'mcx>(
        mcx: Mcx<'mcx>,
        objtype: ObjectType,
        object: &Node,
        lockmode: LOCKMODE,
    ) -> PgResult<ResolvedObjectAddress<'mcx>>
);

seam_core::seam!(
    /// `check_object_ownership(roleid, objtype, address, object, relation)`
    /// (objectaddress.c) — require ownership of the target object; errors
    /// (`ACLCHECK_NOT_OWNER` → `ereport(ERROR)`) if `roleid` does not own it.
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
