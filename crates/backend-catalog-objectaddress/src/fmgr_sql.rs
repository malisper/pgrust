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
//!
//! Fill status (F4):
//! - `pg_describe_object` — filled: pinned-OID guard + the in-crate F1
//!   `getObjectDescription`, the `cstring_to_text` step subsumed by the F1
//!   `PgString` value.
//! - `pg_identify_object_as_address` — filled: the F2 `getObjectTypeDescription`
//!   + F3 `getObjectIdentityParts` legs, returning the deconstructed
//!   name/arg `text[]` columns directly (no `strlist_to_textarray` payload
//!   needed at the value boundary).
//! - `textarray_to_strvaluelist`, `strlist_to_textarray`, `pg_get_acl` — still
//!   mirror-and-panic, genuinely gated on the array Datum value lane
//!   (`types_array::ArrayType` is header-only, no element-bytes payload).
//! - `pg_get_object_address` — mirror-and-panic, gated on the parser
//!   node-construction lane (`typeStringToTypeName`/`makeFloat`/`ObjectWithArgs`)
//!   plus the array deconstruct lane.
//! - `pg_identify_object` — mirror-and-panic, gated on the catalog-tuple read
//!   lane (`table_open`/`get_catalog_object_by_oid`/`heap_getattr` +
//!   `quote_identifier`/`get_namespace_name`).

use mcx::{Mcx, PgString};
use types_array::ArrayType;
use types_catalog::catalog_dependency::ObjectAddress;
use types_core::primitive::OidIsValid;
use types_core::Oid;
use types_error::PgResult;
use types_nodes::parsenodes::ObjectType;

use crate::description::get_object_description as f1_get_object_description;
use crate::identity::get_object_identity_parts as f3_get_object_identity_parts;
use crate::type_description::get_object_type_description as f2_get_object_type_description;

/* ---------------------------------------------------------------------------
 * text[] <-> List<String> bridges (objectaddress.c 2083, 6131)
 * ------------------------------------------------------------------------- */

/// `textarray_to_strvaluelist(ArrayType *arr)` (objectaddress.c 2083):
/// `deconstruct_array_builtin(arr, TEXTOID)` then build a `List` of `String`
/// value nodes (NULL elements `ereport(ERROR)`). Modeled as a `Vec<String>`.
pub fn textarray_to_strvaluelist(_mcx: Mcx<'_>, _arr: &ArrayType) -> PgResult<Vec<String>> {
    // Gated on the array Datum value lane: `deconstruct_array_builtin(arr,
    // TEXTOID, ...)` needs the array payload bytes, but `types_array::ArrayType`
    // models only the fixed 16-byte header — there is no element-bytes lane to
    // deconstruct here, and the `backend-utils-adt-arrayfuncs-seams`
    // deconstruct seams are keyed on a `Datum`/`&[u8]` payload, not this header
    // value. Mirror-and-panic until the SQL value lane lands.
    panic!(
        "decomp: textarray_to_strvaluelist gated on the array Datum value lane \
         (deconstruct_array_builtin needs the text[] payload bytes, absent from \
         the header-only types_array::ArrayType)"
    )
}

/// `strlist_to_textarray(List *list)` (objectaddress.c 6131): build a one-dim
/// `text[]` from the strings via `construct_md_array` (`None` ⇒ a NULL
/// element). Modeled over `&[Option<String>]`.
pub fn strlist_to_textarray<'mcx>(
    _mcx: Mcx<'mcx>,
    _list: &[Option<String>],
) -> PgResult<ArrayType> {
    // Gated on the array Datum value lane: `construct_md_array(datums, nulls, 1,
    // &j, lb, TEXTOID, -1, false, TYPALIGN_INT)` builds a real `text[]` varlena
    // with an element-bytes payload, but `types_array::ArrayType` models only
    // the fixed header — there is no payload lane to populate here. The
    // `pg_identify_object_as_address` value-boundary caller does not need this:
    // it returns the deconstructed `Vec<Option<String>>` columns directly.
    // Mirror-and-panic until the SQL value lane lands.
    panic!(
        "decomp: strlist_to_textarray gated on the array Datum value lane \
         (construct_md_array builds a text[] payload, absent from the \
         header-only types_array::ArrayType)"
    )
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
    // Gated on (a) the array Datum value lane — the per-type-name and per-arg
    // `deconstruct_array_builtin` decoding of `object_names` / `object_args` —
    // and (b) the parser node-construction lane: the C builds parser `Node`s
    // (`typeStringToTypeName`, `makeFloat`, `makeString`, `ObjectWithArgs`,
    // `list_make2`/`lcons`) per object type before invoking the
    // `get_object_address` seam. `read_objtype_from_string` (F0) is ready, but
    // the Node assembly + `typeStringToTypeName` (backend-parser) are unported.
    // Mirror-and-panic until those land.
    panic!(
        "decomp: pg_get_object_address gated on the parser node-construction \
         lane (typeStringToTypeName / makeFloat / ObjectWithArgs assembly) and \
         the array Datum value lane (deconstruct of object_names/object_args)"
    )
}

/// `pg_describe_object(PG_FUNCTION_ARGS)` (objectaddress.c 4220): the
/// `getObjectDescription` of a `(classid, objid, objsubid)` tuple, as text.
pub fn pg_describe_object<'mcx>(
    mcx: Mcx<'mcx>,
    classid: Oid,
    objid: Oid,
    objsubid: i32,
) -> PgResult<Option<PgString<'mcx>>> {
    // for "pinned" items in pg_depend, return null
    if !OidIsValid(classid) && !OidIsValid(objid) {
        return Ok(None);
    }

    let address = ObjectAddress {
        classId: classid,
        objectId: objid,
        objectSubId: objsubid,
    };

    // description = getObjectDescription(&address, true);
    // if (description == NULL) PG_RETURN_NULL();
    // else PG_RETURN_TEXT_P(cstring_to_text(description));
    //
    // The F1 body already returns the description as a `PgString` (a text-like
    // value) allocated in `mcx`, so the C `cstring_to_text` step is subsumed in
    // the value-boundary model; a `None` propagates as the SQL NULL.
    f1_get_object_description(mcx, &address, true)
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
    // Gated on the catalog-tuple read lane: the schema/name extraction opens the
    // object's catalog (`table_open(classId)`), fetches the object tuple via
    // `get_catalog_object_by_oid` (an F0 helper that itself mirror-and-panics
    // until the relcache/heaptuple lane lands), and reads the namespace/name
    // attributes with `heap_getattr` against the catalog tupdesc, then
    // `quote_identifier` / `get_namespace_name`. The type (F2) and identity (F3)
    // legs are ready in-crate, but the tuple-read + quoting cross-crate lanes
    // are unported. Mirror-and-panic until they land.
    panic!(
        "decomp: pg_identify_object gated on the catalog-tuple read lane \
         (table_open + get_catalog_object_by_oid + heap_getattr for schema/name, \
         plus quote_identifier / get_namespace_name)"
    )
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
    mcx: Mcx<'mcx>,
    classid: Oid,
    objid: Oid,
    objsubid: i32,
) -> PgResult<IdentifyObjectAsAddressRow<'mcx>> {
    let address = ObjectAddress {
        classId: classid,
        objectId: objid,
        objectSubId: objsubid,
    };

    // The C calls get_call_result_type() purely to assert the SQL return type
    // is a composite row; that check has no value-boundary representation here
    // (the caller already drives a 3-column record), so it is elided.

    // object type, which can never be NULL:
    //   values[0] = CStringGetTextDatum(getObjectTypeDescription(&address, true));
    // The F2 body models the never-NULL C result as `Option<PgString>`; a `None`
    // would crash the C (unconditional CStringGetTextDatum), so it is an
    // unexpected vanished-class condition — surface it as an `elog`-style error
    // rather than fabricate a value.
    let type_ = match f2_get_object_type_description(mcx, &address, true)? {
        Some(s) => Some(s),
        None => {
            return Err(types_error::PgError::error(format!(
                "could not identify object type for {classid}/{objid}/{objsubid}"
            )));
        }
    };

    // object identity:
    //   identity = getObjectIdentityParts(&address, &names, &args, true);
    //   if (identity == NULL) { nulls[1] = nulls[2] = true; }
    //   else {
    //       pfree(identity);
    //       values[1] = names ? strlist_to_textarray(names)
    //                         : construct_empty_array(TEXTOID);
    //       values[2] = args  ? strlist_to_textarray(args)
    //                         : construct_empty_array(TEXTOID);
    //   }
    //
    // The value-boundary model returns the deconstructed name/arg components
    // directly as the two `text[]` columns (an empty `Vec` standing in for both
    // the C `construct_empty_array(TEXTOID)` empty array and, when identity is
    // NULL, the SQL NULL columns — the identity string itself is dropped, as
    // the C only uses it as a NULL sentinel after `pfree`).
    let (object_names, object_args) = match f3_get_object_identity_parts(mcx, &address, true)? {
        None => (Vec::new(), Vec::new()),
        Some((_identity, parts)) => (
            parts.objname.into_iter().map(Some).collect(),
            parts.objargs.into_iter().map(Some).collect(),
        ),
    };

    Ok(IdentifyObjectAsAddressRow {
        type_,
        object_names,
        object_args,
    })
}

/// `pg_get_acl(PG_FUNCTION_ARGS)` (objectaddress.c 4426): the `aclitem[]` of a
/// `(classid, objid, objsubid)` object, or NULL when it has no ACL column.
pub fn pg_get_acl<'mcx>(
    _mcx: Mcx<'mcx>,
    _classid: Oid,
    _objid: Oid,
    _objsubid: i32,
) -> PgResult<Option<ArrayType>> {
    // Gated on the array Datum value lane: the ACL is read out of the object's
    // catalog tuple (`heap_getattr` / `SysCacheGetAttr` of the `aclitem[]`
    // column) and returned verbatim as a `Datum` (the array payload), but
    // `types_array::ArrayType` models only the fixed header — there is no
    // element-bytes lane to carry the aclitem[] payload across this boundary.
    // The catalog-tuple read lane (table_open / get_catalog_object_by_oid /
    // SearchSysCacheCopyAttNum) is likewise unported. Mirror-and-panic until the
    // SQL value lane lands.
    panic!(
        "decomp: pg_get_acl gated on the array Datum value lane (the aclitem[] \
         column Datum has no payload in the header-only types_array::ArrayType) \
         and the catalog-tuple read lane"
    )
}

/// Quiet the unused-import lint until the fill stage references `ObjectType`
/// in the resolution path of `pg_get_object_address`.
#[allow(dead_code)]
fn _objtype_marker(_t: ObjectType) {}
