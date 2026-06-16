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
//! - `pg_get_acl` — filled: resolves the catalog (pg_largeobject ->
//!   pg_largeobject_metadata) + the `aclitem[]` column attnum, then reads the
//!   raw varlena ACL `Datum` through the indexing owner's `get_acl_datum` seam
//!   (`table_open` + `get_catalog_object_by_oid` + `heap_getattr`, or
//!   `SearchSysCache2(ATTNUM)` + `SysCacheGetAttr(attacl)` for a relation
//!   attribute), returning it verbatim (`PG_RETURN_DATUM` / `PG_RETURN_NULL`).
//! - `pg_identify_object` — filled: opens the object's catalog via the
//!   `relation_open` seam, fetches the object tuple through the F0
//!   `get_catalog_object_by_oid` (genam seam, installed), reads the
//!   namespace/name attributes with `heap_getattr` (the `heap_attisnull` +
//!   `nocachegetattr` primitives) against `RelationGetDescr`, then
//!   `quote_identifier` / `get_namespace_name` for the schema/name columns and
//!   the F2 `getObjectTypeDescription` + F3 `getObjectIdentity` legs.
//! - `textarray_to_strvaluelist`, `strlist_to_textarray` — still
//!   mirror-and-panic, genuinely gated on the array Datum value lane
//!   (`types_array::ArrayType` is header-only, no element-bytes payload).
//! - `pg_get_object_address` — mirror-and-panic, gated on the parser
//!   node-construction lane (`makeFloat`/`makeString`/`ObjectWithArgs`) plus the
//!   array deconstruct lane (`textarray_to_strvaluelist`).

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
    // Gated on (a) the array Datum value lane — the C deconstructs the
    // `object_names`/`object_args` `text[]` inputs via `deconstruct_array_builtin`
    // / `textarray_to_strvaluelist` (both blocked: `types_array::ArrayType` is
    // header-only, no element-bytes payload) — and (b) the parser
    // node-construction lane: the C assembles parser `Node`s (`makeFloat`,
    // `makeString` String-value lists, `ObjectWithArgs`, `list_make2`/`lcons`)
    // per object type before invoking the `get_object_address` seam.
    // `read_objtype_from_string` (F0) and `typeStringToTypeName` (now installed)
    // are ready, but the String/Float/ObjectWithArgs Node assembly + the array
    // deconstruct lane are not. Mirror-and-panic until those land.
    panic!(
        "decomp: pg_get_object_address gated on the parser node-construction \
         lane (makeFloat / makeString / ObjectWithArgs assembly) and the array \
         Datum value lane (deconstruct of object_names/object_args via \
         textarray_to_strvaluelist)"
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
    mcx: Mcx<'mcx>,
    classid: Oid,
    objid: Oid,
    objsubid: i32,
) -> PgResult<IdentifyObjectRow<'mcx>> {
    use backend_utils_adt_ruleutils_seams as ruleutils;
    use backend_utils_cache_lsyscache_seams as lsyscache;
    use types_core::primitive::{InvalidAttrNumber, INVALID_OID};
    use types_storage::lock::AccessShareLock;

    use crate::identity::get_object_identity;
    use crate::properties::{
        get_object_attnum_name, get_object_attnum_namespace, get_object_attnum_oid,
        get_object_namensp_unique, is_objectclass_supported,
    };
    use crate::resolve::get_catalog_object_by_oid;

    let address = ObjectAddress {
        classId: classid,
        objectId: objid,
        objectSubId: objsubid,
    };

    // The C calls get_call_result_type() purely to assert the SQL return type is
    // a row type; that has no value-boundary representation here (the caller
    // already drives a 4-column record), so it is elided.

    let mut schema_oid: Oid = INVALID_OID;
    let mut objname: Option<PgString<'mcx>> = None;

    if is_objectclass_supported(address.classId) {
        // Relation catalog = table_open(address.classId, AccessShareLock);
        let catalog = backend_access_common_relation_seams::relation_open::call(
            mcx,
            address.classId,
            AccessShareLock,
        )?;

        // objtup = get_catalog_object_by_oid(catalog, get_object_attnum_oid(...),
        //                                    address.objectId);
        let objtup = get_catalog_object_by_oid(
            mcx,
            &catalog,
            get_object_attnum_oid(address.classId)?,
            address.objectId,
        )?;

        if let Some(objtup) = objtup {
            // nspAttnum = get_object_attnum_namespace(address.classId);
            let nsp_attnum = get_object_attnum_namespace(address.classId)?;
            if nsp_attnum != InvalidAttrNumber {
                // schema_oid = heap_getattr(objtup, nspAttnum,
                //                           RelationGetDescr(catalog), &isnull);
                match heap_getattr(mcx, &objtup, nsp_attnum as i32, &catalog.rd_att)? {
                    None => {
                        return Err(types_error::PgError::error(format!(
                            "invalid null namespace in object {}/{}/{}",
                            address.classId, address.objectId, address.objectSubId
                        )));
                    }
                    Some(d) => schema_oid = d.as_oid(),
                }
            }

            // We only return the object name if it can be used (together with the
            // schema name, if any) as a unique identifier.
            if get_object_namensp_unique(address.classId)? {
                // nameAttnum = get_object_attnum_name(address.classId);
                let name_attnum = get_object_attnum_name(address.classId)?;
                if name_attnum != InvalidAttrNumber {
                    // nameDatum = heap_getattr(objtup, nameAttnum,
                    //                          RelationGetDescr(catalog), &isnull);
                    match heap_getattr(mcx, &objtup, name_attnum as i32, &catalog.rd_att)? {
                        None => {
                            return Err(types_error::PgError::error(format!(
                                "invalid null name in object {}/{}/{}",
                                address.classId, address.objectId, address.objectSubId
                            )));
                        }
                        Some(d) => {
                            // objname = quote_identifier(NameStr(*DatumGetName(...)));
                            let name = datum_get_name(&d);
                            objname = Some(ruleutils::quote_identifier::call(mcx, &name)?);
                        }
                    }
                }
            }
        }

        // table_close(catalog, AccessShareLock);
        catalog.close(AccessShareLock)?;
    }

    let mut row = IdentifyObjectRow::default();

    // object type, which can never be NULL:
    //   values[0] = CStringGetTextDatum(getObjectTypeDescription(&address, true));
    // The F2 body models the never-NULL C result as `Option<PgString>`; a `None`
    // would crash the C (unconditional CStringGetTextDatum), so surface it as an
    // `elog`-style error rather than fabricate a value.
    row.type_ = match f2_get_object_type_description(mcx, &address, true)? {
        Some(s) => Some(s),
        None => {
            return Err(types_error::PgError::error(format!(
                "could not identify object type for {classid}/{objid}/{objsubid}"
            )));
        }
    };

    // Before doing anything, extract the object identity.  If the identity could
    // not be found, set all the fields except the object type to NULL.
    //   objidentity = getObjectIdentity(&address, true);
    let objidentity = get_object_identity(mcx, &address, true)?;

    // schema name
    if OidIsValid(schema_oid) && objidentity.is_some() {
        match lsyscache::get_namespace_name::call(mcx, schema_oid)? {
            Some(schema) => {
                row.schema = Some(ruleutils::quote_identifier::call(mcx, schema.as_str())?);
            }
            // get_namespace_name returns NULL for a dropped namespace; the C
            // `quote_identifier(NULL)` would crash, so a vanished namespace is
            // an `elog`-style error here.
            None => {
                return Err(types_error::PgError::error(format!(
                    "cache lookup failed for namespace {schema_oid}"
                )));
            }
        }
    }

    // object name
    if objname.is_some() && objidentity.is_some() {
        row.name = objname;
    }

    // object identity
    row.identity = objidentity;

    Ok(row)
}

/// `heap_getattr(tup, attnum, tupleDesc, &isnull)` (htup_details.h) for a
/// user attribute (`attnum > 0`): `Ok(None)` is the C `isnull == true` (with the
/// returned `Datum` being `(Datum) 0`, never read by the caller); `Ok(Some(d))`
/// carries the fetched value. Mirrors the macro's null short-circuit followed by
/// `nocachegetattr` (`fastgetattr`'s cached-offset fast path collapses into
/// `nocachegetattr`, which honours any existing `attcacheoff`).
fn heap_getattr<'mcx>(
    mcx: Mcx<'mcx>,
    formed: &types_tuple::backend_access_common_heaptuple::FormedTuple<'_>,
    attnum: i32,
    tuple_desc: &types_tuple::heaptuple::TupleDescData<'_>,
) -> PgResult<Option<types_tuple::backend_access_common_heaptuple::Datum<'mcx>>> {
    // if (att_isnull(...)) isnull = true; else value = fastgetattr(...);
    if backend_access_common_heaptuple::heap_attisnull(&formed.tuple, attnum, Some(tuple_desc)) {
        return Ok(None);
    }
    Ok(Some(backend_access_common_heaptuple::nocachegetattr(
        mcx,
        &formed.tuple,
        attnum,
        tuple_desc,
        formed.data.as_slice(),
    )?))
}

/// `NameStr(*DatumGetName(datum))` (objectaddress.c 4310): a name-typed
/// (`NAMEOID`, fixed 64-byte by-reference) column lands as the `ByRef` Datum arm
/// holding the `NameData` bytes; the name is the run up to the first NUL.
fn datum_get_name(datum: &types_tuple::backend_access_common_heaptuple::Datum<'_>) -> String {
    let bytes = datum.as_ref_bytes();
    let len = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..len]).into_owned()
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
    mcx: Mcx<'mcx>,
    classid: Oid,
    objid: Oid,
    objsubid: i32,
) -> PgResult<Option<types_tuple::backend_access_common_heaptuple::Datum<'mcx>>> {
    use crate::consts::{
        LargeObjectMetadataRelationId, LargeObjectRelationId, RelationRelationId,
    };
    use crate::properties::{get_object_attnum_acl, get_object_attnum_oid};

    // for "pinned" items in pg_depend, return null.
    if !OidIsValid(classid) && !OidIsValid(objid) {
        return Ok(None);
    }

    // for large objects, the catalog to look at is pg_largeobject_metadata.
    let catalog_id = if classid == LargeObjectRelationId {
        LargeObjectMetadataRelationId
    } else {
        classid
    };
    let anum_acl = get_object_attnum_acl(catalog_id)?;

    // return NULL if no ACL field for this catalog.
    if anum_acl == 0 {
        return Ok(None);
    }

    // If dealing with a relation's attribute (objsubid is set), the ACL is
    // retrieved from pg_attribute; otherwise from the object's own catalog row.
    let is_relation_attr = classid == RelationRelationId && objsubid != 0;
    let anum_oid = if is_relation_attr {
        0 // unused on the attribute path
    } else {
        get_object_attnum_oid(catalog_id)?
    };

    backend_catalog_indexing_seams::get_acl_datum::call(
        mcx,
        catalog_id,
        anum_oid,
        anum_acl,
        objid,
        objsubid,
        is_relation_attr,
    )
}

/// Quiet the unused-import lint until the fill stage references `ObjectType`
/// in the resolution path of `pg_get_object_address`.
#[allow(dead_code)]
fn _objtype_marker(_t: ObjectType) {}
