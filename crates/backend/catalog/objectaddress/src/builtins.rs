// objectaddress.c SQL-callable leg: pg_describe_object 3537,
// pg_identify_object 3839, pg_identify_object_as_address 3382,
// pg_get_object_address 3954; plus read_objtype_from_string and the
// text[] <-> string-list bridges.
use crate::identity::{getObjectIdentityParts, getObjectTypeDescription};
use crate::{description, getObjectDescription, properties, ObjectAddress};
use datum::Datum;
use fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData};
use mcx::Mcx;
use types_core::primitive::OidIsValid;
use types_core::{InvalidOid, Oid, TEXTOID};
use types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};
use types_nodes::parsenodes::{ObjectType, ObjectWithArgs};
use types_nodes::rawnodes::TypeName;
use types_nodes::{Node, NodeList};

use ObjectType::*;

// ObjectTypeMap (objectaddress.c); None = unmapped (-1).
#[rustfmt::skip]
static OBJECT_TYPE_MAP: &[(&str, Option<ObjectType>)] = &[
    ("table", Some(OBJECT_TABLE)),
    ("index", Some(OBJECT_INDEX)),
    ("sequence", Some(OBJECT_SEQUENCE)),
    ("toast table", None),
    ("view", Some(OBJECT_VIEW)),
    ("materialized view", Some(OBJECT_MATVIEW)),
    ("composite type", None),
    ("foreign table", Some(OBJECT_FOREIGN_TABLE)),
    ("table column", Some(OBJECT_COLUMN)),
    ("index column", None),
    ("sequence column", None),
    ("toast table column", None),
    ("view column", None),
    ("materialized view column", None),
    ("composite type column", None),
    ("foreign table column", Some(OBJECT_COLUMN)),
    ("aggregate", Some(OBJECT_AGGREGATE)),
    ("function", Some(OBJECT_FUNCTION)),
    ("procedure", Some(OBJECT_PROCEDURE)),
    ("type", Some(OBJECT_TYPE)),
    ("cast", Some(OBJECT_CAST)),
    ("collation", Some(OBJECT_COLLATION)),
    ("table constraint", Some(OBJECT_TABCONSTRAINT)),
    ("domain constraint", Some(OBJECT_DOMCONSTRAINT)),
    ("conversion", Some(OBJECT_CONVERSION)),
    ("default value", Some(OBJECT_DEFAULT)),
    ("language", Some(OBJECT_LANGUAGE)),
    ("large object", Some(OBJECT_LARGEOBJECT)),
    ("operator", Some(OBJECT_OPERATOR)),
    ("operator class", Some(OBJECT_OPCLASS)),
    ("operator family", Some(OBJECT_OPFAMILY)),
    ("access method", Some(OBJECT_ACCESS_METHOD)),
    ("operator of access method", Some(OBJECT_AMOP)),
    ("function of access method", Some(OBJECT_AMPROC)),
    ("rule", Some(OBJECT_RULE)),
    ("trigger", Some(OBJECT_TRIGGER)),
    ("schema", Some(OBJECT_SCHEMA)),
    ("text search parser", Some(OBJECT_TSPARSER)),
    ("text search dictionary", Some(OBJECT_TSDICTIONARY)),
    ("text search template", Some(OBJECT_TSTEMPLATE)),
    ("text search configuration", Some(OBJECT_TSCONFIGURATION)),
    ("role", Some(OBJECT_ROLE)),
    ("role membership", None),
    ("database", Some(OBJECT_DATABASE)),
    ("tablespace", Some(OBJECT_TABLESPACE)),
    ("foreign-data wrapper", Some(OBJECT_FDW)),
    ("server", Some(OBJECT_FOREIGN_SERVER)),
    ("user mapping", Some(OBJECT_USER_MAPPING)),
    ("default acl", Some(OBJECT_DEFACL)),
    ("extension", Some(OBJECT_EXTENSION)),
    ("event trigger", Some(OBJECT_EVENT_TRIGGER)),
    ("parameter ACL", Some(OBJECT_PARAMETER_ACL)),
    ("policy", Some(OBJECT_POLICY)),
    ("publication", Some(OBJECT_PUBLICATION)),
    ("publication namespace", Some(OBJECT_PUBLICATION_NAMESPACE)),
    ("publication relation", Some(OBJECT_PUBLICATION_REL)),
    ("subscription", Some(OBJECT_SUBSCRIPTION)),
    ("transform", Some(OBJECT_TRANSFORM)),
    ("statistics object", Some(OBJECT_STATISTIC_EXT)),
];

// read_objtype_from_string (objectaddress.c): unknown name errors; an
// unmapped row returns None (the caller's "unsupported object type").
pub fn read_objtype_from_string(objtype: &str) -> PgResult<Option<ObjectType>> {
    for (name, ty) in OBJECT_TYPE_MAP {
        if *name == objtype {
            return Ok(*ty);
        }
    }
    Err(param_err(format!("unrecognized object type \"{objtype}\"")))
}

#[track_caller]
#[cold]
fn param_err(msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE))
}

fn str_in<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let v = mcx::slice_in(mcx, s.as_bytes())?;
    Ok(core::str::from_utf8(v.leak()).expect("copied str stays UTF-8"))
}

fn varlena_image<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    // SAFETY: by-ref varlena argument datum; toast expansion happens below.
    unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) }
}

fn text_body(image: &[u8]) -> &[u8] {
    let p = image.as_ptr();
    // SAFETY: image spans the whole varlena.
    unsafe {
        if types_tuple::varatt::varatt_is_1b(p) {
            &image[types_tuple::varatt::VARHDRSZ_SHORT..]
        } else {
            &image[types_tuple::varatt::VARHDRSZ..]
        }
    }
}

// deconstruct_array_builtin(TEXTOID) over an argument datum, elements as
// Option<&'mcx str> (None = SQL NULL element).
fn deconstruct_text_array<'mcx>(
    mcx: Mcx<'mcx>,
    d: Datum,
) -> PgResult<Vec<Option<&'mcx str>>> {
    let img = detoast::detoast_attr(mcx, varlena_image(d))?;
    let (elems, nulls) =
        arrayfuncs::construct::deconstruct_array_builtin(mcx, &img, TEXTOID, true)?;
    let mut out = Vec::with_capacity(elems.len());
    for (i, elem) in elems.iter().enumerate() {
        if nulls[i] {
            out.push(None);
        } else {
            let body = text_body(varlena_image(*elem));
            let s = str_in(mcx, core::str::from_utf8(body).expect("text is valid UTF-8"))?;
            out.push(Some(s));
        }
    }
    Ok(out)
}

#[track_caller]
#[cold]
fn null_element_err() -> Box<PgError> {
    param_err("name or argument lists may not contain nulls".to_string())
}

// textarray_to_strvaluelist (objectaddress.c).
fn textarray_to_strvaluelist<'mcx>(mcx: Mcx<'mcx>, d: Datum) -> PgResult<Vec<Node<'mcx>>> {
    let elems = deconstruct_text_array(mcx, d)?;
    let mut out = Vec::with_capacity(elems.len());
    for elem in elems {
        let Some(s) = elem else {
            return Err(null_element_err().into());
        };
        out.push(Node::mk(mcx, types_nodes::String { sval: s })?);
    }
    Ok(out)
}

fn text_datum(mcx: Mcx<'_>, s: &str) -> PgResult<Datum> {
    Ok(fmgr::varlena_result(varlena::cstring_to_text(mcx, s.as_bytes())?))
}

// strlist_to_textarray (objectaddress.c); empty list still yields an array.
fn strlist_to_textarray<'mcx>(mcx: Mcx<'mcx>, items: &[String]) -> PgResult<Datum> {
    if items.is_empty() {
        let img = arrayfuncs::construct_empty_array(mcx, TEXTOID)?;
        return Ok(Datum::from_usize(img.leak().as_ptr() as usize));
    }
    let mut elems: Vec<Datum> = Vec::with_capacity(items.len());
    for s in items {
        elems.push(text_datum(mcx, s)?);
    }
    let img = arrayfuncs::construct_md_array(
        mcx,
        &elems,
        None,
        1,
        &[items.len() as i32],
        &[1],
        TEXTOID,
        -1,
        false,
        b'i',
    )?;
    Ok(Datum::from_usize(img.leak().as_ptr() as usize))
}

fn composite_result(
    mcx: Mcx<'_>,
    flinfo: &FmgrInfo,
    values: &[Datum],
    isnull: &[bool],
) -> PgResult<Datum> {
    let resolved = funcapi::get_call_result_type(mcx, flinfo, None)?;
    if resolved.class != funcapi::TypeFuncClass::Composite {
        return Err(Box::new(PgError::error("return type must be a row type")));
    }
    let tupdesc = resolved.result_tuple_desc.expect("composite result has tupdesc");
    let tup = heaptuple::heap_form_tuple(mcx, &tupdesc, values, isnull)?;
    let d = Datum::from_usize(tup.header_ptr() as usize);
    core::mem::forget(tup); // leak into the arming context (C palloc ownership)
    Ok(d)
}

fn fc_pg_describe_object(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let classid = fcinfo.arg_oid(0);
    let objid = fcinfo.arg_oid(1);
    let objsubid = fcinfo.arg_i32(2);
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    // Pinned pg_depend entries describe as NULL.
    if !OidIsValid(classid) && !OidIsValid(objid) {
        fcinfo.isnull = true;
        return Ok(Datum::null());
    }
    let address = ObjectAddress::sub_set(classid, objid, objsubid);
    match getObjectDescription(mcx, &address, true)? {
        Some(descr) => text_datum(mcx, &descr),
        None => {
            fcinfo.isnull = true;
            Ok(Datum::null())
        }
    }
}

fn fc_pg_identify_object(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_identify_object: resolved FmgrInfo required");
    let classid = fcinfo.arg_oid(0);
    let objid = fcinfo.arg_oid(1);
    let objsubid = fcinfo.arg_i32(2);
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let address = ObjectAddress::sub_set(classid, objid, objsubid);

    let mut schema_oid = InvalidOid;
    let mut objname: Option<String> = None;
    if properties::is_objectclass_supported(address.classId) {
        let prop = properties::get_object_property_data(address.classId);
        let row = description::scan_one_row(
            mcx,
            address.classId,
            prop.oid_index_oid,
            address.objectId,
            |tup, desc| {
                let nsp = (prop.attnum_namespace != 0).then(|| {
                    let mut isnull = false;
                    // SAFETY: attnum from the ObjectProperty row for this catalog.
                    let d = unsafe {
                        types_tuple::heap_getattr(tup, prop.attnum_namespace, desc, &mut isnull)
                    };
                    (d.as_oid(), isnull)
                });
                let name = (prop.is_nsp_name_unique && prop.attnum_name != 0).then(|| {
                    let mut isnull = false;
                    // SAFETY: attnum from the ObjectProperty row for this catalog.
                    let d = unsafe {
                        types_tuple::heap_getattr(tup, prop.attnum_name, desc, &mut isnull)
                    };
                    if isnull {
                        None
                    } else {
                        Some(description::name_from_datum(d))
                    }
                });
                (nsp, name)
            },
        )?;
        if let Some((nsp, name)) = row {
            if let Some((nspoid, isnull)) = nsp {
                if isnull {
                    return Err(Box::new(PgError::error(format!(
                        "invalid null namespace in object {}/{}/{}",
                        address.classId, address.objectId, address.objectSubId
                    ))));
                }
                schema_oid = nspoid;
            }
            match name {
                Some(Some(n)) => {
                    objname = Some(format_type::quote_identifier(&n).into_owned())
                }
                Some(None) => {
                    return Err(Box::new(PgError::error(format!(
                        "invalid null name in object {}/{}/{}",
                        address.classId, address.objectId, address.objectSubId
                    ))));
                }
                None => {}
            }
        }
    }

    let mut values = [Datum::null(); 4];
    let mut nulls = [true; 4];
    let typedesc = getObjectTypeDescription(mcx, &address, true)?
        .expect("object type description is never NULL");
    values[0] = text_datum(mcx, &typedesc)?;
    nulls[0] = false;

    let objidentity = description::getObjectIdentity(mcx, &address, true)?;

    if OidIsValid(schema_oid) && objidentity.is_some() {
        let nspname = lsyscache::misc::get_namespace_name(mcx, schema_oid)?
            .map(|n| n.as_str().to_string())
            .unwrap_or_else(|| panic!("cache lookup failed for namespace {schema_oid}"));
        values[1] = text_datum(mcx, &format_type::quote_identifier(&nspname))?;
        nulls[1] = false;
    }
    if let (Some(name), Some(_)) = (&objname, &objidentity) {
        values[2] = text_datum(mcx, name)?;
        nulls[2] = false;
    }
    if let Some(identity) = &objidentity {
        values[3] = text_datum(mcx, identity)?;
        nulls[3] = false;
    }
    composite_result(mcx, flinfo, &values, &nulls)
}

fn fc_pg_identify_object_as_address(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_identify_object_as_address: resolved FmgrInfo required");
    let classid = fcinfo.arg_oid(0);
    let objid = fcinfo.arg_oid(1);
    let objsubid = fcinfo.arg_i32(2);
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let address = ObjectAddress::sub_set(classid, objid, objsubid);

    let mut values = [Datum::null(); 3];
    let mut nulls = [true; 3];
    let typedesc = getObjectTypeDescription(mcx, &address, true)?
        .expect("object type description is never NULL");
    values[0] = text_datum(mcx, &typedesc)?;
    nulls[0] = false;

    if let Some(ident) = getObjectIdentityParts(mcx, &address, true)? {
        values[1] = strlist_to_textarray(mcx, &ident.objname)?;
        nulls[1] = false;
        values[2] = strlist_to_textarray(mcx, &ident.objargs)?;
        nulls[2] = false;
    }
    composite_result(mcx, flinfo, &values, &nulls)
}

fn fc_pg_get_object_address(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_get_object_address: resolved FmgrInfo required");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    // SAFETY: strict fn, arg 0 is a non-null text varlena.
    let ttype_b = unsafe { fcinfo.arg_varlena_packed(0)? };
    let ttype = core::str::from_utf8(ttype_b.data())
        .expect("text is valid UTF-8")
        .to_string();
    let namearr = fcinfo.arg(1);
    let argsarr = fcinfo.arg(2);

    let Some(objtype) = read_objtype_from_string(&ttype)? else {
        return Err(param_err(format!("unsupported object type \"{ttype}\"")));
    };

    let mut name: Vec<Node<'_>> = Vec::new();
    let mut typename: Option<&TypeName<'_>> = None;
    let mut objnode: Option<Node<'_>> = None;

    if matches!(
        objtype,
        OBJECT_TYPE | OBJECT_DOMAIN | OBJECT_CAST | OBJECT_TRANSFORM | OBJECT_DOMCONSTRAINT
    ) {
        let elems = deconstruct_text_array(mcx, namearr)?;
        if elems.len() != 1 {
            return Err(param_err("name list length must be exactly 1".to_string()));
        }
        let Some(s) = elems[0] else {
            return Err(null_element_err().into());
        };
        typename = Some(parse_utilcmd::typeStringToTypeName(mcx, s)?);
    } else if objtype == OBJECT_LARGEOBJECT {
        let elems = deconstruct_text_array(mcx, namearr)?;
        if elems.len() != 1 {
            return Err(param_err("name list length must be exactly 1".to_string()));
        }
        let Some(s) = elems[0] else {
            return Err(param_err("large object OID may not be null".to_string()));
        };
        objnode = Some(Node::mk(mcx, types_nodes::Float { fval: s })?);
    } else {
        name = textarray_to_strvaluelist(mcx, namearr)?;
        if name.is_empty() {
            return Err(param_err("name list length must be at least 1".to_string()));
        }
    }

    let args: Vec<Node<'_>> = if matches!(
        objtype,
        OBJECT_AGGREGATE
            | OBJECT_FUNCTION
            | OBJECT_PROCEDURE
            | OBJECT_ROUTINE
            | OBJECT_OPERATOR
            | OBJECT_CAST
            | OBJECT_AMOP
            | OBJECT_AMPROC
    ) {
        let elems = deconstruct_text_array(mcx, argsarr)?;
        let mut out = Vec::with_capacity(elems.len());
        for elem in elems {
            let Some(s) = elem else {
                return Err(null_element_err().into());
            };
            let tn = parse_utilcmd::typeStringToTypeName(mcx, s)?;
            out.push(mk_type_name_node(mcx, tn)?);
        }
        out
    } else {
        textarray_to_strvaluelist(mcx, argsarr)?
    };

    match objtype {
        OBJECT_PUBLICATION_NAMESPACE | OBJECT_USER_MAPPING => {
            if name.len() != 1 {
                return Err(param_err("name list length must be exactly 1".to_string()));
            }
            if args.len() != 1 {
                return Err(param_err("argument list length must be exactly 1".to_string()));
            }
        }
        OBJECT_DOMCONSTRAINT | OBJECT_CAST | OBJECT_PUBLICATION_REL | OBJECT_DEFACL
        | OBJECT_TRANSFORM => {
            if args.len() != 1 {
                return Err(param_err("argument list length must be exactly 1".to_string()));
            }
        }
        OBJECT_OPFAMILY | OBJECT_OPCLASS => {
            if name.len() < 2 {
                return Err(param_err("name list length must be at least 2".to_string()));
            }
        }
        OBJECT_AMOP | OBJECT_AMPROC => {
            if name.len() < 3 {
                return Err(param_err("name list length must be at least 3".to_string()));
            }
            if args.len() != 2 {
                return Err(param_err("argument list length must be exactly 2".to_string()));
            }
        }
        OBJECT_OPERATOR => {
            if args.len() != 2 {
                return Err(param_err("argument list length must be exactly 2".to_string()));
            }
        }
        _ => {}
    }

    fn list_node<'mcx>(mcx: Mcx<'mcx>, cells: &[Node<'mcx>]) -> PgResult<Node<'mcx>> {
        let list = NodeList::from_slice(mcx, cells)?;
        Node::mk(mcx, list)
    }

    match objtype {
        OBJECT_TABLE | OBJECT_SEQUENCE | OBJECT_VIEW | OBJECT_MATVIEW | OBJECT_INDEX
        | OBJECT_FOREIGN_TABLE | OBJECT_COLUMN | OBJECT_ATTRIBUTE | OBJECT_COLLATION
        | OBJECT_CONVERSION | OBJECT_STATISTIC_EXT | OBJECT_TSPARSER | OBJECT_TSDICTIONARY
        | OBJECT_TSTEMPLATE | OBJECT_TSCONFIGURATION | OBJECT_DEFAULT | OBJECT_POLICY
        | OBJECT_RULE | OBJECT_TRIGGER | OBJECT_TABCONSTRAINT | OBJECT_OPCLASS
        | OBJECT_OPFAMILY => {
            objnode = Some(list_node(mcx, &name)?);
        }
        OBJECT_ACCESS_METHOD | OBJECT_DATABASE | OBJECT_EVENT_TRIGGER | OBJECT_EXTENSION
        | OBJECT_FDW | OBJECT_FOREIGN_SERVER | OBJECT_LANGUAGE | OBJECT_PARAMETER_ACL
        | OBJECT_PUBLICATION | OBJECT_ROLE | OBJECT_SCHEMA | OBJECT_SUBSCRIPTION
        | OBJECT_TABLESPACE => {
            if name.len() != 1 {
                return Err(param_err("name list length must be exactly 1".to_string()));
            }
            objnode = Some(name[0]);
        }
        OBJECT_TYPE | OBJECT_DOMAIN => {
            objnode = Some(mk_type_name_node(mcx, typename.expect("built above"))?);
        }
        OBJECT_CAST | OBJECT_DOMCONSTRAINT | OBJECT_TRANSFORM => {
            let tn = mk_type_name_node(mcx, typename.expect("built above"))?;
            objnode = Some(list_node(mcx, &[tn, args[0]])?);
        }
        OBJECT_PUBLICATION_REL => {
            let nl = list_node(mcx, &name)?;
            objnode = Some(list_node(mcx, &[nl, args[0]])?);
        }
        OBJECT_PUBLICATION_NAMESPACE | OBJECT_USER_MAPPING => {
            objnode = Some(list_node(mcx, &[name[0], args[0]])?);
        }
        OBJECT_DEFACL => {
            let mut cells = Vec::with_capacity(name.len() + 1);
            cells.push(args[0]);
            cells.extend_from_slice(&name);
            objnode = Some(list_node(mcx, &cells)?);
        }
        OBJECT_AMOP | OBJECT_AMPROC => {
            let nl = list_node(mcx, &name)?;
            let al = list_node(mcx, &args)?;
            objnode = Some(list_node(mcx, &[nl, al])?);
        }
        OBJECT_FUNCTION | OBJECT_PROCEDURE | OBJECT_ROUTINE | OBJECT_AGGREGATE
        | OBJECT_OPERATOR => {
            let optargs: Vec<Option<Node>> = args.iter().copied().map(Some).collect();
            let owa = ObjectWithArgs {
                objname: NodeList::from_slice(mcx, &name)?,
                objargs: types_nodes::OptNodeList::from_slice(mcx, &optargs)?,
                objfuncargs: NodeList::default(),
                args_unspecified: false,
            };
            objnode = Some(Node::mk(mcx, owa)?);
        }
        // OBJECT_LARGEOBJECT already handled above.
        _ => {}
    }

    let objnode = objnode
        .unwrap_or_else(|| panic!("unrecognized object type: {objtype:?}"));

    let (addr, relation) = crate::get_object_address(
        mcx,
        objtype,
        objnode,
        types_rel::AccessShareLock,
        false,
    )?;
    if let Some(rel) = relation {
        rel.close(types_rel::AccessShareLock)?;
    }

    let flinfo = &*flinfo;
    let values = [
        Datum::from_oid(addr.classId),
        Datum::from_oid(addr.objectId),
        Datum::from_i32(addr.objectSubId),
    ];
    composite_result(mcx, flinfo, &values, &[false, false, false])
}

const LARGE_OBJECT_RELATION_ID: Oid = 2613;
const LARGE_OBJECT_METADATA_RELATION_ID: Oid = 2995;
const ATTRIBUTE_RELID_NUM_INDEX_ID: Oid = 2659;
const ANUM_PG_ATTRIBUTE_ATTACL: i32 = 22;

fn eq_key(
    attno: types_core::AttrNumber,
    func: types_core::RegProcedure,
    arg: Datum,
) -> types_scan::scankey::ScanKeyData {
    let mut key = types_scan::scankey::ScanKeyData::empty();
    key.sk_attno = attno;
    key.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
    key.sk_collation = types_core::C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(func)
        .unwrap_or_else(|e| panic!("fmgr_info({func}) failed: {e:?}"));
    key.sk_argument = arg;
    key
}

fn acl_attr_image(
    tup: &types_tuple::HeapTupleData<'_>,
    attnum: i32,
    desc: &types_tuple::TupleDescData<'_>,
) -> Option<Vec<u8>> {
    let mut isnull = false;
    // SAFETY: attnum comes from the ObjectProperty row / pg_attribute layout.
    let d = unsafe { types_tuple::heap_getattr(tup, attnum, desc, &mut isnull) };
    (!isnull).then(|| varlena_image(d).to_vec())
}

// pg_get_acl (objectaddress.c): NULL, never error, for nonexistent objects.
fn fc_pg_get_acl(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let classid = fcinfo.arg_oid(0);
    let objid = fcinfo.arg_oid(1);
    let objsubid = fcinfo.arg_i32(2);
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };

    // Pinned pg_depend entries have no ACL.
    if !OidIsValid(classid) && !OidIsValid(objid) {
        fcinfo.isnull = true;
        return Ok(Datum::null());
    }

    let catalog_id = if classid == LARGE_OBJECT_RELATION_ID {
        LARGE_OBJECT_METADATA_RELATION_ID
    } else {
        classid
    };
    if !properties::is_objectclass_supported(catalog_id) {
        return Err(Box::new(PgError::error(format!(
            "unrecognized class ID: {catalog_id}"
        ))));
    }
    let prop = properties::get_object_property_data(catalog_id);
    let anum_acl = prop.attnum_acl;
    if anum_acl == 0 {
        fcinfo.isnull = true;
        return Ok(Datum::null());
    }

    let img = if classid == types_core::RELATION_RELATION_ID && objsubid != 0 {
        let rel =
            table::table_open(mcx, types_core::ATTRIBUTE_RELATION_ID, types_rel::AccessShareLock)?;
        let keys = [
            eq_key(1, types_core::fmgr::F_OIDEQ, Datum::from_oid(objid)),
            eq_key(5, types_core::fmgr::F_INT2EQ, Datum::from_i16(objsubid as i16)),
        ];
        let mut scan = genam::systable_beginscan(
            mcx,
            &rel,
            ATTRIBUTE_RELID_NUM_INDEX_ID,
            true,
            None,
            &keys,
        )?;
        let img = genam::systable_getnext(mcx, &mut scan)?
            .and_then(|tup| acl_attr_image(tup, ANUM_PG_ATTRIBUTE_ATTACL, rel.descr()));
        genam::systable_endscan(mcx, scan)?;
        rel.close(types_rel::AccessShareLock)?;
        img
    } else {
        description::scan_one_row(mcx, catalog_id, prop.oid_index_oid, objid, |tup, desc| {
            acl_attr_image(tup, anum_acl, desc)
        })?
        .flatten()
    };

    match img {
        Some(img) => {
            let v = detoast::detoast_attr(mcx, &img)?;
            Ok(Datum::from_usize(v.leak().as_ptr() as usize))
        }
        None => {
            fcinfo.isnull = true;
            Ok(Datum::null())
        }
    }
}

fn mk_type_name_node<'mcx>(mcx: Mcx<'mcx>, tn: &TypeName<'mcx>) -> PgResult<Node<'mcx>> {
    Node::mk(
        mcx,
        TypeName {
            names: NodeList::from_slice(mcx, tn.names.as_slice())?,
            typeOid: tn.typeOid,
            setof: tn.setof,
            pct_type: tn.pct_type,
            typmods: NodeList::from_slice(mcx, tn.typmods.as_slice())?,
            typemod: tn.typemod,
            arrayBounds: NodeList::from_slice(mcx, tn.arrayBounds.as_slice())?,
            location: tn.location,
        },
    )
}

pub static OBJECTADDRESS_BUILTINS: &[FmgrBuiltin] = &[
    FmgrBuiltin {
        foid: 3382,
        name: "pg_identify_object_as_address",
        nargs: 3,
        strict: true,
        retset: false,
        func: fc_pg_identify_object_as_address,
    },
    FmgrBuiltin {
        foid: 3537,
        name: "pg_describe_object",
        nargs: 3,
        strict: true,
        retset: false,
        func: fc_pg_describe_object,
    },
    FmgrBuiltin {
        foid: 3839,
        name: "pg_identify_object",
        nargs: 3,
        strict: true,
        retset: false,
        func: fc_pg_identify_object,
    },
    FmgrBuiltin {
        foid: 3954,
        name: "pg_get_object_address",
        nargs: 3,
        strict: true,
        retset: false,
        func: fc_pg_get_object_address,
    },
    FmgrBuiltin {
        foid: 6385,
        name: "pg_get_acl",
        nargs: 3,
        strict: true,
        retset: false,
        func: fc_pg_get_acl,
    },
];
