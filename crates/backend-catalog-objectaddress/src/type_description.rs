//! F2 — `getObjectTypeDescription` and the relation/constraint/procedure
//! type-disambiguation helpers (objectaddress.c 4497-4823).
//!
//! Builds on the F0 `ObjectProperty[]` tables and
//! [`crate::resolve::get_catalog_object_by_oid`]. Bodies scaffolded as
//! mirror-and-panic.

use mcx::{Mcx, PgString};
use types_core::Oid;
use types_catalog::catalog_dependency::ObjectAddress;
use types_error::PgResult;

/// `getObjectTypeDescription(const ObjectAddress *object, bool missing_ok)`
/// (objectaddress.c 4497): the catalog-class "type" string (e.g. "table",
/// "view column", "operator of access method"). `Ok(None)` mirrors the C NULL
/// for a vanished object under `missing_ok`.
pub fn get_object_type_description<'mcx>(
    _mcx: Mcx<'mcx>,
    _object: &ObjectAddress,
    _missing_ok: bool,
) -> PgResult<Option<PgString<'mcx>>> {
    panic!("decomp: getObjectTypeDescription not yet filled")
}

/// `getRelationTypeDescription(StringInfo buffer, Oid relid, int32
/// objectSubId, bool missing_ok)` (objectaddress.c 4687): decode the relkind
/// (and column subid) into the relation-type string.
pub fn get_relation_type_description<'mcx>(
    _mcx: Mcx<'mcx>,
    _buffer: &mut String,
    _relid: Oid,
    _object_sub_id: i32,
    _missing_ok: bool,
) -> PgResult<()> {
    panic!("decomp: getRelationTypeDescription not yet filled")
}

/// `getConstraintTypeDescription(StringInfo buffer, Oid constroid, bool
/// missing_ok)` (objectaddress.c 4750): table- vs domain-constraint
/// disambiguation.
pub fn get_constraint_type_description<'mcx>(
    _mcx: Mcx<'mcx>,
    _buffer: &mut String,
    _constroid: Oid,
    _missing_ok: bool,
) -> PgResult<()> {
    panic!("decomp: getConstraintTypeDescription not yet filled")
}

/// `getProcedureTypeDescription(StringInfo buffer, Oid procid, bool
/// missing_ok)` (objectaddress.c 4787): prokind → function/procedure/aggregate
/// type string.
pub fn get_procedure_type_description<'mcx>(
    _mcx: Mcx<'mcx>,
    _buffer: &mut String,
    _procid: Oid,
    _missing_ok: bool,
) -> PgResult<()> {
    panic!("decomp: getProcedureTypeDescription not yet filled")
}
