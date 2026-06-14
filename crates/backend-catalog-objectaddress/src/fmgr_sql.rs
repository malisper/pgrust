//! F4 — the SQL-callable leg and the text[]↔List bridges (objectaddress.c
//! 2083, 2109, 4220-4490, 6131).
//!
//! Gated on the Datum/ArrayType SQL value lane: `deconstruct_array_builtin` /
//! `construct_md_array` / `cstring_to_text` / `get_call_result_type` cross via
//! `backend-utils-adt-arrayfuncs-seams` + `backend-utils-fmgr-funcapi-seams` +
//! fmgr value primitives (mirror-and-panic into those owners where a decl is
//! missing). Depends on the F0 resolution model and the F1/F2/F3 description +
//! identity bodies. Bodies scaffolded as mirror-and-panic.
//!
//! The SQL functions are modeled at the high-level value boundary (their
//! deconstructed inputs / assembled outputs) rather than the raw `fcinfo`
//! frame; the fill stage wires the actual fmgr dispatch once the value lane
//! lands.

use mcx::{Mcx, PgString};
use types_array::ArrayType;
use types_catalog::catalog_dependency::ObjectAddress;
use types_core::Oid;
use types_error::PgResult;
use types_nodes::parsenodes::ObjectType;

/* ---------------------------------------------------------------------------
 * text[] <-> List<String> bridges (objectaddress.c 2083, 6131)
 * ------------------------------------------------------------------------- */

/// `textarray_to_strvaluelist(ArrayType *arr)` (objectaddress.c 2083):
/// `deconstruct_array_builtin(arr, TEXTOID)` then build a `List` of `String`
/// value nodes (NULL elements `ereport(ERROR)`). Modeled as a `Vec<String>`.
pub fn textarray_to_strvaluelist(_mcx: Mcx<'_>, _arr: &ArrayType) -> PgResult<Vec<String>> {
    panic!("decomp: textarray_to_strvaluelist not yet filled")
}

/// `strlist_to_textarray(List *list)` (objectaddress.c 6131): build a one-dim
/// `text[]` from the strings via `construct_md_array` (`None` ⇒ a NULL
/// element). Modeled over `&[Option<String>]`.
pub fn strlist_to_textarray<'mcx>(
    _mcx: Mcx<'mcx>,
    _list: &[Option<String>],
) -> PgResult<ArrayType> {
    panic!("decomp: strlist_to_textarray not yet filled")
}

/* ---------------------------------------------------------------------------
 * SQL-callable functions (objectaddress.c 2109, 4220-4490)
 * ------------------------------------------------------------------------- */

/// `pg_get_object_address(PG_FUNCTION_ARGS)` (objectaddress.c 2109): given a
/// type-name text, an object-name `text[]`, and an object-args `text[]`,
/// resolve to an `ObjectAddress` and return the `(classid, objid, objsubid)`
/// record. Modeled at the value boundary.
pub fn pg_get_object_address<'mcx>(
    _mcx: Mcx<'mcx>,
    _type_name: &str,
    _object_names: &[Option<String>],
    _object_args: &[Option<String>],
) -> PgResult<ObjectAddress> {
    panic!("decomp: pg_get_object_address not yet filled")
}

/// `pg_describe_object(PG_FUNCTION_ARGS)` (objectaddress.c 4220): the
/// `getObjectDescription` of a `(classid, objid, objsubid)` tuple, as text.
pub fn pg_describe_object<'mcx>(
    _mcx: Mcx<'mcx>,
    _classid: Oid,
    _objid: Oid,
    _objsubid: i32,
) -> PgResult<Option<PgString<'mcx>>> {
    panic!("decomp: pg_describe_object not yet filled")
}

/// One row of `pg_identify_object` (objectaddress.c 4248): the `(type,
/// schema, name, identity)` quadruple.
#[derive(Debug, Default)]
pub struct IdentifyObjectRow<'mcx> {
    pub type_: Option<PgString<'mcx>>,
    pub schema: Option<PgString<'mcx>>,
    pub name: Option<PgString<'mcx>>,
    pub identity: Option<PgString<'mcx>>,
}

/// `pg_identify_object(PG_FUNCTION_ARGS)` (objectaddress.c 4248): the
/// type/schema/name/identity record for a `(classid, objid, objsubid)` tuple
/// (uses `get_call_result_type` to build the result descriptor).
pub fn pg_identify_object<'mcx>(
    _mcx: Mcx<'mcx>,
    _classid: Oid,
    _objid: Oid,
    _objsubid: i32,
) -> PgResult<IdentifyObjectRow<'mcx>> {
    panic!("decomp: pg_identify_object not yet filled")
}

/// One row of `pg_identify_object_as_address` (objectaddress.c 4365): the
/// `(type, object_names text[], object_args text[])` triple.
#[derive(Debug, Default)]
pub struct IdentifyObjectAsAddressRow<'mcx> {
    pub type_: Option<PgString<'mcx>>,
    pub object_names: Vec<Option<String>>,
    pub object_args: Vec<Option<String>>,
}

/// `pg_identify_object_as_address(PG_FUNCTION_ARGS)` (objectaddress.c 4365):
/// the round-trippable `(type, names[], args[])` form of a `(classid, objid,
/// objsubid)` tuple.
pub fn pg_identify_object_as_address<'mcx>(
    _mcx: Mcx<'mcx>,
    _classid: Oid,
    _objid: Oid,
    _objsubid: i32,
) -> PgResult<IdentifyObjectAsAddressRow<'mcx>> {
    panic!("decomp: pg_identify_object_as_address not yet filled")
}

/// `pg_get_acl(PG_FUNCTION_ARGS)` (objectaddress.c 4426): the `aclitem[]` of a
/// `(classid, objid, objsubid)` object, or NULL when it has no ACL column.
pub fn pg_get_acl<'mcx>(
    _mcx: Mcx<'mcx>,
    _classid: Oid,
    _objid: Oid,
    _objsubid: i32,
) -> PgResult<Option<ArrayType>> {
    panic!("decomp: pg_get_acl not yet filled")
}

/// Quiet the unused-import lint until the fill stage references `ObjectType`
/// in the resolution path of `pg_get_object_address`.
#[allow(dead_code)]
fn _objtype_marker(_t: ObjectType) {}
