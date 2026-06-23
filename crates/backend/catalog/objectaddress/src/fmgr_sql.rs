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
//! - `textarray_to_strvaluelist`, `strlist_to_textarray` — filled over the
//!   array value lane: the on-disk `text[]` byte image is deconstructed
//!   (`deconstruct_text_array_nullable`) / built (`build_text_array_nullable`)
//!   through the arrayfuncs owner seams (the `array::ArrayType` header is
//!   not the carrier — the array crosses as its varlena byte image).
//! - `pg_get_object_address` — filled: `read_objtype_from_string` decodes the
//!   type, the name/args `text[]` byte images are deconstructed, the per-object
//!   `Node` tree is assembled (`makeFloat`/`makeString` value lists,
//!   `ObjectWithArgs`, `list_make2`/`lcons`, `typeStringToTypeName`), then the
//!   F0 `get_object_address` resolves it. The 3-column record assembly
//!   (`heap_form_tuple`) stays with the fcinfo caller; the value boundary
//!   returns the resolved `ObjectAddress`.

use mcx::{Mcx, PgString};
use types_catalog::catalog_dependency::ObjectAddress;
use types_core::primitive::OidIsValid;
use types_core::Oid;
use types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};
use ::nodes::parsenodes::ObjectType;
use ::nodes::parsenodes::{
    OBJECT_ACCESS_METHOD, OBJECT_AGGREGATE, OBJECT_AMOP, OBJECT_AMPROC, OBJECT_ATTRIBUTE,
    OBJECT_CAST, OBJECT_COLLATION, OBJECT_COLUMN, OBJECT_CONVERSION, OBJECT_DATABASE, OBJECT_DEFACL,
    OBJECT_DEFAULT, OBJECT_DOMAIN, OBJECT_DOMCONSTRAINT, OBJECT_EVENT_TRIGGER, OBJECT_EXTENSION,
    OBJECT_FDW, OBJECT_FOREIGN_SERVER, OBJECT_FOREIGN_TABLE, OBJECT_FUNCTION, OBJECT_INDEX,
    OBJECT_LANGUAGE, OBJECT_LARGEOBJECT, OBJECT_MATVIEW, OBJECT_OPCLASS, OBJECT_OPERATOR,
    OBJECT_OPFAMILY, OBJECT_PARAMETER_ACL, OBJECT_POLICY, OBJECT_PROCEDURE, OBJECT_PUBLICATION,
    OBJECT_PUBLICATION_NAMESPACE, OBJECT_PUBLICATION_REL, OBJECT_ROLE, OBJECT_ROUTINE, OBJECT_RULE,
    OBJECT_SCHEMA, OBJECT_SEQUENCE, OBJECT_STATISTIC_EXT, OBJECT_SUBSCRIPTION, OBJECT_TABCONSTRAINT,
    OBJECT_TABLE, OBJECT_TABLESPACE, OBJECT_TRANSFORM, OBJECT_TRIGGER, OBJECT_TSCONFIGURATION,
    OBJECT_TSDICTIONARY, OBJECT_TSPARSER, OBJECT_TSTEMPLATE, OBJECT_TYPE, OBJECT_USER_MAPPING,
    OBJECT_VIEW,
};

use crate::description::get_object_description as f1_get_object_description;
use crate::identity::get_object_identity_parts as f3_get_object_identity_parts;
use crate::type_description::get_object_type_description as f2_get_object_type_description;

/* ---------------------------------------------------------------------------
 * text[] <-> List<String> bridges (objectaddress.c 2083, 6131)
 * ------------------------------------------------------------------------- */

/// `textarray_to_strvaluelist(ArrayType *arr)` (objectaddress.c 2083):
/// `deconstruct_array_builtin(arr, TEXTOID, &elems, &nulls, &nelems)` then build
/// a `List` of `String` value nodes (`makeString(TextDatumGetCString(elems[i]))`);
/// a NULL element `ereport(ERROR)`. The owned model takes the on-disk `text[]`
/// byte image (a `Datum::ByRef` array column / `PG_GETARG_ARRAYTYPE_P` payload)
/// and returns the C `List *` of `String`s as a `Vec<String>` — the caller wraps
/// each in a `Node::String` (`makeString`).
pub fn textarray_to_strvaluelist<'mcx>(
    mcx: Mcx<'mcx>,
    arr_bytes: &[u8],
) -> PgResult<Vec<String>> {
    // deconstruct_array_builtin(arr, TEXTOID, &elems, &nulls, &nelems);
    let pairs =
        arrayfuncs_seams::deconstruct_text_array_nullable::call(mcx, arr_bytes)?;

    let mut list: Vec<String> = Vec::with_capacity(pairs.len());
    for elem in pairs.iter() {
        match elem {
            // if (nulls[i]) ereport(ERROR, ...);
            None => {
                return Err(PgError::error("name or argument lists may not contain nulls")
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
            }
            // list = lappend(list, makeString(TextDatumGetCString(elems[i])));
            Some(s) => list.push(s.as_str().to_string()),
        }
    }

    Ok(list)
}

/// `strlist_to_textarray(List *list)` (objectaddress.c 6131): build a one-dim
/// `text[]` from the strings via `construct_md_array(datums, nulls, 1, &j, lb,
/// TEXTOID, -1, false, TYPALIGN_INT)` (each `CStringGetTextDatum(name)`; a `None`
/// cell ⇒ a NULL element). The owned model takes `&[Option<String>]` and returns
/// the array varlena's raw byte image (carried on a by-reference `text[]`
/// `Datum`).
pub fn strlist_to_textarray<'mcx>(
    mcx: Mcx<'mcx>,
    list: &[Option<String>],
) -> PgResult<mcx::PgVec<'mcx, u8>> {
    // The C builds `datums`/`nulls` (`CStringGetTextDatum(name)` per non-null
    // cell, `nulls[j] = true` per NULL) then `construct_md_array(..., TEXTOID,
    // -1, false, TYPALIGN_INT)`. The arrayfuncs `build_text_array_nullable` seam
    // is exactly that array-build half over per-element `Option<text payload>`;
    // the element payload is the raw UTF-8 bytes (`CStringGetTextDatum` wraps a
    // C-string, so the payload is the string bytes, no NUL).
    let elems: Vec<Option<&[u8]>> = list
        .iter()
        .map(|c| c.as_ref().map(|s| s.as_bytes()))
        .collect();
    arrayfuncs_seams::build_text_array_nullable::call(mcx, &elems)
}

/* ---------------------------------------------------------------------------
 * SQL-callable functions (objectaddress.c 2109, 4220-4490)
 * ------------------------------------------------------------------------- */

/// `pg_get_object_address(PG_FUNCTION_ARGS)` (objectaddress.c 2109): given a
/// type-name text, an object-name `text[]`, and an object-args `text[]`,
/// resolve to an `ObjectAddress` and return the `(classid, objid, objsubid)`
/// record. Modeled at the value boundary.
pub fn pg_get_object_address<'mcx>(
    mcx: Mcx<'mcx>,
    type_name: &str,
    name_arr_bytes: &[u8],
    args_arr_bytes: &[u8],
) -> PgResult<ObjectAddress> {
    use parsenodes::{Float, Node, ObjectWithArgs, StringNode, TypeName};

    // char *ttype = TextDatumGetCString(PG_GETARG_DATUM(0));
    // ArrayType *namearr = PG_GETARG_ARRAYTYPE_P(1);
    // ArrayType *argsarr = PG_GETARG_ARRAYTYPE_P(2);
    // (The varlena text arg + the two text[] arrays cross the value boundary as
    // the decoded `&str` + their on-disk array byte images.)

    // itype = read_objtype_from_string(ttype);
    // if (itype < 0) ereport(ERROR, "unsupported object type \"%s\"");
    let itype = crate::resolve::read_objtype_from_string(type_name)?;
    if itype < 0 {
        return Err(
            PgError::error(format!("unsupported object type \"{type_name}\""))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        );
    }
    let objtype = ObjectType::from_i32(itype)
        .expect("read_objtype_from_string returns a valid non-negative ObjectType code");

    // Builds the per-object-type carriers the switch below assembles into a Node.
    let mut name: Vec<Node> = Vec::new();
    let mut typename: Option<TypeName> = None;
    let mut args: Vec<Node> = Vec::new();
    let mut objnode: Option<Node> = None;

    // Convert the text array to the representation appropriate for the given
    // object type. Most use a simple string Values list, but there are some
    // exceptions.
    if objtype == OBJECT_TYPE
        || objtype == OBJECT_DOMAIN
        || objtype == OBJECT_CAST
        || objtype == OBJECT_TRANSFORM
        || objtype == OBJECT_DOMCONSTRAINT
    {
        // deconstruct_array_builtin(namearr, TEXTOID, &elems, &nulls, &nelems);
        let elems = arrayfuncs_seams::deconstruct_text_array_nullable::call(
            mcx,
            name_arr_bytes,
        )?;
        if elems.len() != 1 {
            return Err(PgError::error("name list length must be exactly 1")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
        }
        let elem0 = match &elems[0] {
            Some(s) => s.as_str(),
            None => {
                return Err(PgError::error("name or argument lists may not contain nulls")
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
            }
        };
        // typename = typeStringToTypeName(TextDatumGetCString(elems[0]), NULL);
        typename =
            Some(parse_type_seams::type_string_to_type_name::call(elem0)?);
    } else if objtype == OBJECT_LARGEOBJECT {
        let elems = arrayfuncs_seams::deconstruct_text_array_nullable::call(
            mcx,
            name_arr_bytes,
        )?;
        if elems.len() != 1 {
            return Err(PgError::error("name list length must be exactly 1")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
        }
        let elem0 = match &elems[0] {
            Some(s) => s.as_str(),
            None => {
                return Err(PgError::error("large object OID may not be null")
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
            }
        };
        // objnode = (Node *) makeFloat(TextDatumGetCString(elems[0]));
        objnode = Some(Node::Float(Float {
            fval: Some(elem0.to_string()),
        }));
    } else {
        // name = textarray_to_strvaluelist(namearr);
        name = textarray_to_strvaluelist(mcx, name_arr_bytes)?
            .into_iter()
            .map(|s| Node::String(StringNode { sval: Some(s) }))
            .collect();
        if name.is_empty() {
            return Err(PgError::error("name list length must be at least 1")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
        }
    }

    // If args are given, decode them according to the object type.
    if objtype == OBJECT_AGGREGATE
        || objtype == OBJECT_FUNCTION
        || objtype == OBJECT_PROCEDURE
        || objtype == OBJECT_ROUTINE
        || objtype == OBJECT_OPERATOR
        || objtype == OBJECT_CAST
        || objtype == OBJECT_AMOP
        || objtype == OBJECT_AMPROC
    {
        // in these cases, the args list must be of TypeName
        let elems = arrayfuncs_seams::deconstruct_text_array_nullable::call(
            mcx,
            args_arr_bytes,
        )?;
        args = Vec::with_capacity(elems.len());
        for elem in elems.iter() {
            let s = match elem {
                Some(s) => s.as_str(),
                None => {
                    return Err(PgError::error("name or argument lists may not contain nulls")
                        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
                }
            };
            // args = lappend(args, typeStringToTypeName(..., NULL));
            args.push(Node::TypeName(
                parse_type_seams::type_string_to_type_name::call(s)?,
            ));
        }
    } else {
        // For all other object types, use string Values.
        args = textarray_to_strvaluelist(mcx, args_arr_bytes)?
            .into_iter()
            .map(|s| Node::String(StringNode { sval: Some(s) }))
            .collect();
    }

    // get_object_address is pretty sensitive to the length of its input lists;
    // check that they're what it wants.
    match objtype {
        OBJECT_PUBLICATION_NAMESPACE | OBJECT_USER_MAPPING => {
            if name.len() != 1 {
                return Err(PgError::error("name list length must be exactly 1")
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
            }
            // fall through to check args length
            if args.len() != 1 {
                return Err(PgError::error("argument list length must be exactly 1")
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
            }
        }
        OBJECT_DOMCONSTRAINT
        | OBJECT_CAST
        | OBJECT_PUBLICATION_REL
        | OBJECT_DEFACL
        | OBJECT_TRANSFORM => {
            if args.len() != 1 {
                return Err(PgError::error("argument list length must be exactly 1")
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
            }
        }
        OBJECT_OPFAMILY | OBJECT_OPCLASS => {
            if name.len() < 2 {
                return Err(PgError::error("name list length must be at least 2")
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
            }
        }
        OBJECT_AMOP | OBJECT_AMPROC => {
            if name.len() < 3 {
                return Err(PgError::error("name list length must be at least 3")
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
            }
            // fall through to check args length
            if args.len() != 2 {
                return Err(PgError::error("argument list length must be exactly 2")
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
            }
        }
        OBJECT_OPERATOR => {
            if args.len() != 2 {
                return Err(PgError::error("argument list length must be exactly 2")
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
            }
        }
        _ => {}
    }

    // Now build the Node type that get_object_address() expects for the given
    // type.
    match objtype {
        OBJECT_TABLE | OBJECT_SEQUENCE | OBJECT_VIEW | OBJECT_MATVIEW | OBJECT_INDEX
        | OBJECT_FOREIGN_TABLE | OBJECT_COLUMN | OBJECT_ATTRIBUTE | OBJECT_COLLATION
        | OBJECT_CONVERSION | OBJECT_STATISTIC_EXT | OBJECT_TSPARSER | OBJECT_TSDICTIONARY
        | OBJECT_TSTEMPLATE | OBJECT_TSCONFIGURATION | OBJECT_DEFAULT | OBJECT_POLICY
        | OBJECT_RULE | OBJECT_TRIGGER | OBJECT_TABCONSTRAINT | OBJECT_OPCLASS
        | OBJECT_OPFAMILY => {
            // objnode = (Node *) name;
            objnode = Some(Node::List(name));
        }
        OBJECT_ACCESS_METHOD
        | OBJECT_DATABASE
        | OBJECT_EVENT_TRIGGER
        | OBJECT_EXTENSION
        | OBJECT_FDW
        | OBJECT_FOREIGN_SERVER
        | OBJECT_LANGUAGE
        | OBJECT_PARAMETER_ACL
        | OBJECT_PUBLICATION
        | OBJECT_ROLE
        | OBJECT_SCHEMA
        | OBJECT_SUBSCRIPTION
        | OBJECT_TABLESPACE => {
            if name.len() != 1 {
                return Err(PgError::error("name list length must be exactly 1")
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
            }
            // objnode = linitial(name);
            objnode = Some(name.into_iter().next().expect("len == 1"));
        }
        OBJECT_TYPE | OBJECT_DOMAIN => {
            // objnode = (Node *) typename;
            objnode = Some(Node::TypeName(
                typename.expect("typename built above for OBJECT_TYPE/DOMAIN"),
            ));
        }
        OBJECT_CAST | OBJECT_DOMCONSTRAINT | OBJECT_TRANSFORM => {
            // objnode = (Node *) list_make2(typename, linitial(args));
            let tn = typename.expect("typename built above for CAST/DOMCONSTRAINT/TRANSFORM");
            let arg0 = args.into_iter().next().expect("args length == 1 checked");
            objnode = Some(Node::List(vec![Node::TypeName(tn), arg0]));
        }
        OBJECT_PUBLICATION_REL => {
            // objnode = (Node *) list_make2(name, linitial(args));
            let arg0 = args.into_iter().next().expect("args length == 1 checked");
            objnode = Some(Node::List(vec![Node::List(name), arg0]));
        }
        OBJECT_PUBLICATION_NAMESPACE | OBJECT_USER_MAPPING => {
            // objnode = (Node *) list_make2(linitial(name), linitial(args));
            let name0 = name.into_iter().next().expect("name length == 1 checked");
            let arg0 = args.into_iter().next().expect("args length == 1 checked");
            objnode = Some(Node::List(vec![name0, arg0]));
        }
        OBJECT_DEFACL => {
            // objnode = (Node *) lcons(linitial(args), name);
            let arg0 = args.into_iter().next().expect("args length == 1 checked");
            let mut list = vec![arg0];
            list.extend(name);
            objnode = Some(Node::List(list));
        }
        OBJECT_AMOP | OBJECT_AMPROC => {
            // objnode = (Node *) list_make2(name, args);
            objnode = Some(Node::List(vec![Node::List(name), Node::List(args)]));
        }
        OBJECT_FUNCTION | OBJECT_PROCEDURE | OBJECT_ROUTINE | OBJECT_AGGREGATE
        | OBJECT_OPERATOR => {
            // ObjectWithArgs *owa = makeNode(ObjectWithArgs);
            // owa->objname = name; owa->objargs = args; objnode = (Node *) owa;
            // objname is the C `List *` of String value nodes → the repo
            // ObjectWithArgs.objname is `Vec<String>`; objargs is the TypeName
            // list (Node::TypeName cells).
            let objname: Vec<String> = name
                .into_iter()
                .map(|n| match n {
                    Node::String(StringNode { sval }) => sval.unwrap_or_default(),
                    // textarray_to_strvaluelist only emits String nodes.
                    _ => unreachable!("objname list holds only String value nodes"),
                })
                .collect();
            objnode = Some(Node::ObjectWithArgs(ObjectWithArgs {
                objname,
                objargs: args,
                objfuncargs: Vec::new(),
                args_unspecified: false,
            }));
        }
        // OBJECT_LARGEOBJECT: already handled above (objnode = makeFloat(...)).
        _ => {}
    }

    // if (objnode == NULL) elog(ERROR, "unrecognized object type: %d", type);
    let objnode = match objnode {
        Some(n) => n,
        None => {
            return Err(PgError::error(format!(
                "unrecognized object type: {objtype:?}"
            )));
        }
    };

    // addr = get_object_address(type, objnode, &relation, AccessShareLock, false);
    let resolved = crate::resolve::get_object_address(
        mcx,
        objtype,
        &objnode,
        types_storage::lock::AccessShareLock,
        false,
    )?;

    // We don't need the relcache entry, thank you very much.
    if let Some(relation) = resolved.relation {
        relation.close(types_storage::lock::AccessShareLock)?;
    }

    // The C builds a 3-column record tuple here (get_call_result_type +
    // heap_form_tuple over ObjectIdGetDatum/Int32GetDatum). The value boundary
    // returns the resolved ObjectAddress directly; the fcinfo record assembly is
    // the caller's responsibility.
    Ok(resolved.address)
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
    use ruleutils_seams as ruleutils;
    use lsyscache_seams as lsyscache;
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
        let catalog = common_relation_seams::relation_open::call(
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
pub(crate) fn heap_getattr<'mcx>(
    mcx: Mcx<'mcx>,
    formed: &types_tuple::heaptuple::FormedTuple<'_>,
    attnum: i32,
    tuple_desc: &types_tuple::heaptuple::TupleDescData<'_>,
) -> PgResult<Option<types_tuple::heaptuple::Datum<'mcx>>> {
    // if (att_isnull(...)) isnull = true; else value = fastgetattr(...);
    if heaptuple::heap_attisnull(&formed.tuple, attnum, Some(tuple_desc)) {
        return Ok(None);
    }
    Ok(Some(heaptuple::nocachegetattr(
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
pub(crate) fn datum_get_name(datum: &types_tuple::heaptuple::Datum<'_>) -> String {
    let bytes = datum.as_ref_bytes();
    let len = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..len]).into_owned()
}

/// One row of `pg_identify_object_as_address` (objectaddress.c 4365): the
/// `(type, object_names text[], object_args text[])` triple.
#[derive(Debug, Default)]
pub struct IdentifyObjectAsAddressRow<'mcx> {
    pub type_: Option<PgString<'mcx>>,
    /// `None` ⇒ the SQL NULL columns the C sets when `getObjectIdentityParts`
    /// returns NULL (the object does not exist); `Some(vec)` ⇒ the `text[]`
    /// (an empty `vec` standing in for `construct_empty_array(TEXTOID)`).
    pub object_names: Option<Vec<Option<String>>>,
    pub object_args: Option<Vec<Option<String>>>,
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
    // directly as the two `text[]` columns. `None` (identity == NULL) maps to
    // the SQL NULL columns the C sets (`nulls[1] = nulls[2] = true`); a present
    // identity yields the arrays (an empty `Vec` standing in for the C
    // `construct_empty_array(TEXTOID)`). The identity string itself is dropped,
    // as the C only uses it as a NULL sentinel after `pfree`.
    let (object_names, object_args) = match f3_get_object_identity_parts(mcx, &address, true)? {
        None => (None, None),
        Some((_identity, parts)) => (
            Some(parts.objname.into_iter().map(Some).collect()),
            Some(parts.objargs.into_iter().map(Some).collect()),
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
) -> PgResult<Option<types_tuple::heaptuple::Datum<'mcx>>> {
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

    indexing_seams::get_acl_datum::call(
        mcx,
        catalog_id,
        anum_oid,
        anum_acl,
        objid,
        objsubid,
        is_relation_attr,
    )
}

