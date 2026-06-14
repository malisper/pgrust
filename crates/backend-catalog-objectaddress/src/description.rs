//! F1 — `getObjectDescription` and friends (objectaddress.c 2912-4219).
//!
//! The ~41 catalog-class description arms assemble a human-readable
//! description into a `StringInfo`, threading catalog opens through
//! [`crate::resolve::get_catalog_object_by_oid`]. Per-owner sub-seams
//! (`format_type_be` / `format_procedure` / `format_operator` / …)
//! mirror-and-panic into their owners' `-seams` crates until landed. Bodies
//! are scaffolded as mirror-and-panic; this provides the real body the F0
//! `get_object_description` seam install routes to.

use mcx::{Mcx, PgString};
use types_core::Oid;
use types_catalog::catalog_dependency::ObjectAddress;
use types_error::PgResult;

/// `getObjectDescription(const ObjectAddress *object, bool missing_ok)`
/// (objectaddress.c 2912): a human-readable description of the object,
/// palloc'd in `mcx`. `Ok(None)` mirrors the C NULL (object vanished under
/// `missing_ok`, or an empty per-class buffer). This is the body the F0
/// `get_object_description` seam install routes to.
pub fn get_object_description<'mcx>(
    _mcx: Mcx<'mcx>,
    _object: &ObjectAddress,
    _missing_ok: bool,
) -> PgResult<Option<PgString<'mcx>>> {
    panic!("decomp: getObjectDescription not yet filled")
}

/// `getObjectDescriptionOids(Oid classid, Oid objid)` (objectaddress.c 4086):
/// the description for a bare (classid, objid) pair (objectSubId 0).
pub fn get_object_description_oids<'mcx>(
    _mcx: Mcx<'mcx>,
    _classid: Oid,
    _objid: Oid,
) -> PgResult<Option<PgString<'mcx>>> {
    panic!("decomp: getObjectDescriptionOids not yet filled")
}

/// `getRelationDescription(StringInfo buffer, Oid relid, bool missing_ok)`
/// (objectaddress.c 4103): append the relation/column-flavored description to
/// `buffer`.
pub fn get_relation_description<'mcx>(
    _mcx: Mcx<'mcx>,
    _buffer: &mut String,
    _relid: Oid,
    _missing_ok: bool,
) -> PgResult<()> {
    panic!("decomp: getRelationDescription not yet filled")
}

/// `getOpFamilyDescription(StringInfo buffer, Oid opfid, bool missing_ok)`
/// (objectaddress.c 4178): append the operator-family description to `buffer`.
pub fn get_op_family_description<'mcx>(
    _mcx: Mcx<'mcx>,
    _buffer: &mut String,
    _opfid: Oid,
    _missing_ok: bool,
) -> PgResult<()> {
    panic!("decomp: getOpFamilyDescription not yet filled")
}
