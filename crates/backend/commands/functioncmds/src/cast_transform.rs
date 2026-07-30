// functioncmds.c CreateCast / CreateTransform / get_transform_oid.
use datum::Datum;
use elog::ereport;
use mcx::Mcx;
use types_core::{
    InvalidOid, Oid, OidIsValid, BOOLOID, INT4OID, INTERNALOID, LANGUAGE_RELATION_ID,
    TYPE_RELATION_ID,
};
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_DUPLICATE_OBJECT, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_INVALID_OBJECT_DEFINITION, ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE,
    WARNING,
};
use types_nodes::parsenodes::{
    CreateCastStmt, CreateTransformStmt, ObjectType, ObjectWithArgs, ACL_EXECUTE, ACL_USAGE,
};
use types_nodes::primnodes::CoercionContext;
use types_nodes::rawnodes::TypeName;
use types_rel::RowExclusiveLock;

use crate::err;
use pg_proc::ObjectAddress;

const TYPTYPE_COMPOSITE: i8 = b'c' as i8;
const TYPTYPE_DOMAIN: i8 = b'd' as i8;
const TYPTYPE_ENUM: i8 = b'e' as i8;
const TYPTYPE_MULTIRANGE: i8 = b'm' as i8;
const TYPTYPE_PSEUDO: i8 = b'p' as i8;
const TYPTYPE_RANGE: i8 = b'r' as i8;

const COERCION_METHOD_FUNCTION: i8 = b'f' as i8;
const COERCION_METHOD_BINARY: i8 = b'b' as i8;
const COERCION_METHOD_INOUT: i8 = b'i' as i8;

const COERCION_CODE_IMPLICIT: i8 = b'i' as i8;
const COERCION_CODE_ASSIGNMENT: i8 = b'a' as i8;
const COERCION_CODE_EXPLICIT: i8 = b'e' as i8;

const PROKIND_FUNCTION: i8 = b'f' as i8;
const PROVOLATILE_VOLATILE: i8 = b'v' as i8;

pub const TransformRelationId: Oid = 3576;
const TransformOidIndexId: Oid = 3574;
const TransformTypeLangIndexId: Oid = 3575;
const Natts_pg_transform: usize = 5;
const Anum_pg_transform_oid: i32 = 1;
const Anum_pg_transform_trffromsql: usize = 4;
const Anum_pg_transform_trftosql: usize = 5;

const PROCEDURE_RELATION_ID: Oid = 1255;

#[track_caller]
#[cold]
#[inline(never)]
fn objdef_err(msg: &str) -> Box<PgError> {
    err(msg.to_string(), ERRCODE_INVALID_OBJECT_DEFINITION)
}

// aclcheck_error_type (aclchk.c): arrays report their element type.
pub(crate) fn aclcheck_error_type(aclerr: i32, typeOid: Oid) -> PgResult<()> {
    let element_type = lsyscache::get_element_type(typeOid)?;
    let typeOid = if OidIsValid(element_type) { element_type } else { typeOid };
    aclchk::aclcheck_error(aclerr, ObjectType::OBJECT_TYPE, &format_type::format_type_be(typeOid)?)
}

fn typename_oid<'a>(mcx: Mcx<'_>, node: Option<types_nodes::Node<'a>>) -> PgResult<(Oid, &'a TypeName<'a>)> {
    let tn = node
        .expect("TypeName node")
        .as_variant::<TypeName>()
        .expect("TypeName node");
    let oid = parse_utilcmd::typenameTypeId(mcx, None, tn)?;
    Ok((oid, tn))
}

pub fn CreateCast<'mcx>(mcx: Mcx<'mcx>, stmt: &CreateCastStmt<'mcx>) -> PgResult<ObjectAddress> {
    let (sourcetypeid, source_tn) = typename_oid(mcx, stmt.sourcetype)?;
    let (targettypeid, target_tn) = typename_oid(mcx, stmt.targettype)?;
    let sourcetyptype = lsyscache::get_typtype(sourcetypeid)?;
    let targettyptype = lsyscache::get_typtype(targettypeid)?;

    if sourcetyptype == TYPTYPE_PSEUDO {
        return Err(err(
            format!(
                "source data type {} is a pseudo-type",
                commands_define::TypeNameToString(mcx, source_tn)?.as_str()
            ),
            ERRCODE_WRONG_OBJECT_TYPE,
        ));
    }
    if targettyptype == TYPTYPE_PSEUDO {
        return Err(err(
            format!(
                "target data type {} is a pseudo-type",
                commands_define::TypeNameToString(mcx, target_tn)?.as_str()
            ),
            ERRCODE_WRONG_OBJECT_TYPE,
        ));
    }

    let userid = miscinit::GetUserId();
    if !aclchk::object_ownercheck(TYPE_RELATION_ID, sourcetypeid, userid)?
        && !aclchk::object_ownercheck(TYPE_RELATION_ID, targettypeid, userid)?
    {
        return Err(err(
            format!(
                "must be owner of type {} or type {}",
                format_type::format_type_be(sourcetypeid)?,
                format_type::format_type_be(targettypeid)?
            ),
            ERRCODE_INSUFFICIENT_PRIVILEGE,
        ));
    }

    let aclresult = aclchk::object_aclcheck(TYPE_RELATION_ID, sourcetypeid, userid, ACL_USAGE)?;
    if aclresult != aclchk::ACLCHECK_OK {
        aclcheck_error_type(aclresult, sourcetypeid)?;
    }
    let aclresult = aclchk::object_aclcheck(TYPE_RELATION_ID, targettypeid, userid, ACL_USAGE)?;
    if aclresult != aclchk::ACLCHECK_OK {
        aclcheck_error_type(aclresult, targettypeid)?;
    }

    if sourcetyptype == TYPTYPE_DOMAIN {
        ereport(WARNING)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg("cast will be ignored because the source data type is a domain")
            .finish(ErrorLocation::new(file!(), line!() as i32, "CreateCast"))?;
    } else if targettyptype == TYPTYPE_DOMAIN {
        ereport(WARNING)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg("cast will be ignored because the target data type is a domain")
            .finish(ErrorLocation::new(file!(), line!() as i32, "CreateCast"))?;
    }

    let castmethod = if stmt.func.is_some() {
        COERCION_METHOD_FUNCTION
    } else if stmt.inout {
        COERCION_METHOD_INOUT
    } else {
        COERCION_METHOD_BINARY
    };

    let mut funcid = InvalidOid;
    let mut incastid = InvalidOid;
    let mut outcastid = InvalidOid;
    let mut nargs = 0i16;

    if castmethod == COERCION_METHOD_FUNCTION {
        let owa = stmt
            .func
            .expect("func node")
            .as_variant::<ObjectWithArgs>()
            .expect("func is an ObjectWithArgs");
        funcid = parse_func::LookupFuncWithArgs(ObjectType::OBJECT_FUNCTION, owa, false)?;

        let shape = syscache_seams::lookup_pg_proc_shape::call(funcid)?
            .unwrap_or_else(|| panic!("cache lookup failed for function {funcid}"));
        let (prorettype, proargtypes) = lsyscache::get_func_signature(mcx, funcid)?;
        nargs = shape.pronargs;
        if !(1..=3).contains(&nargs) {
            return Err(objdef_err("cast function must take one to three arguments"));
        }
        let (ok, cast) = coerce::IsBinaryCoercibleWithCast(sourcetypeid, proargtypes[0])?;
        if !ok {
            return Err(objdef_err(
                "argument of cast function must match or be binary-coercible from source data type",
            ));
        }
        incastid = cast;
        if nargs > 1 && proargtypes[1] != INT4OID {
            return Err(err(
                format!("second argument of cast function must be type {}", "integer"),
                ERRCODE_INVALID_OBJECT_DEFINITION,
            ));
        }
        if nargs > 2 && proargtypes[2] != BOOLOID {
            return Err(err(
                format!("third argument of cast function must be type {}", "boolean"),
                ERRCODE_INVALID_OBJECT_DEFINITION,
            ));
        }
        let (ok, cast) = coerce::IsBinaryCoercibleWithCast(prorettype, targettypeid)?;
        if !ok {
            return Err(objdef_err(
                "return data type of cast function must match or be binary-coercible to target data type",
            ));
        }
        outcastid = cast;
        if shape.prokind != PROKIND_FUNCTION {
            return Err(objdef_err("cast function must be a normal function"));
        }
        if shape.proretset {
            return Err(objdef_err("cast function must not return a set"));
        }
    }

    if castmethod == COERCION_METHOD_BINARY {
        // Erroneous binary-compatible casts can crash the backend.
        if !superuser::superuser()? {
            return Err(err(
                "must be superuser to create a cast WITHOUT FUNCTION".to_string(),
                ERRCODE_INSUFFICIENT_PRIVILEGE,
            ));
        }

        let (typ1len, typ1byval, typ1align) = lsyscache::get_typlenbyvalalign(sourcetypeid)?;
        let (typ2len, typ2byval, typ2align) = lsyscache::get_typlenbyvalalign(targettypeid)?;
        if typ1len != typ2len || typ1byval != typ2byval || typ1align != typ2align {
            return Err(objdef_err("source and target data types are not physically compatible"));
        }

        // Composite, array, range and enum types embed OIDs; never
        // binary-compatible with each other.
        if sourcetyptype == TYPTYPE_COMPOSITE || targettyptype == TYPTYPE_COMPOSITE {
            return Err(objdef_err("composite data types are not binary-compatible"));
        }
        if OidIsValid(lsyscache::get_element_type(sourcetypeid)?)
            || OidIsValid(lsyscache::get_element_type(targettypeid)?)
        {
            return Err(objdef_err("array data types are not binary-compatible"));
        }
        if sourcetyptype == TYPTYPE_RANGE
            || targettyptype == TYPTYPE_RANGE
            || sourcetyptype == TYPTYPE_MULTIRANGE
            || targettyptype == TYPTYPE_MULTIRANGE
        {
            return Err(objdef_err("range data types are not binary-compatible"));
        }
        if sourcetyptype == TYPTYPE_ENUM || targettyptype == TYPTYPE_ENUM {
            return Err(objdef_err("enum data types are not binary-compatible"));
        }
        // Domain-to-base is already allowed; the other way must go through
        // domain coercion for constraint checking.
        if sourcetyptype == TYPTYPE_DOMAIN || targettyptype == TYPTYPE_DOMAIN {
            return Err(objdef_err("domain data types must not be marked binary-compatible"));
        }
    }

    // Same source and target only for length coercion (multi-arg) functions.
    if sourcetypeid == targettypeid && nargs < 2 {
        return Err(objdef_err("source data type and target data type are the same"));
    }

    let castcontext = match stmt.context {
        CoercionContext::COERCION_IMPLICIT => COERCION_CODE_IMPLICIT,
        CoercionContext::COERCION_ASSIGNMENT => COERCION_CODE_ASSIGNMENT,
        CoercionContext::COERCION_EXPLICIT => COERCION_CODE_EXPLICIT,
        other => panic!("unrecognized CoercionContext: {}", other as u32),
    };

    pg_cast::CastCreate(
        mcx,
        sourcetypeid,
        targettypeid,
        funcid,
        incastid,
        outcastid,
        castcontext,
        castmethod,
        pg_depend::DependencyType::Normal,
    )
}

fn check_transform_function(shape: &syscache_seams::PgProcShape, argtype0: Oid) -> PgResult<()> {
    if shape.provolatile == PROVOLATILE_VOLATILE {
        return Err(objdef_err("transform function must not be volatile"));
    }
    if shape.prokind != PROKIND_FUNCTION {
        return Err(objdef_err("transform function must be a normal function"));
    }
    if shape.proretset {
        return Err(objdef_err("transform function must not return a set"));
    }
    if shape.pronargs != 1 {
        return Err(objdef_err("transform function must take one argument"));
    }
    if argtype0 != INTERNALOID {
        return Err(err(
            format!("first argument of transform function must be type {}", "internal"),
            ERRCODE_INVALID_OBJECT_DEFINITION,
        ));
    }
    Ok(())
}

fn transform_func_lookup<'mcx>(
    mcx: Mcx<'mcx>,
    node: types_nodes::Node<'mcx>,
    userid: Oid,
) -> PgResult<(Oid, Oid, syscache_seams::PgProcShape)> {
    let owa = node.as_variant::<ObjectWithArgs>().expect("transform function is an ObjectWithArgs");
    let funcid = parse_func::LookupFuncWithArgs(ObjectType::OBJECT_FUNCTION, owa, false)?;

    if !aclchk::object_ownercheck(PROCEDURE_RELATION_ID, funcid, userid)? {
        aclchk::aclcheck_error(
            aclchk::ACLCHECK_NOT_OWNER,
            ObjectType::OBJECT_FUNCTION,
            &catalog_objectaddress::NameListToString(&owa.objname),
        )?;
    }
    let aclresult = aclchk::object_aclcheck(PROCEDURE_RELATION_ID, funcid, userid, ACL_EXECUTE)?;
    if aclresult != aclchk::ACLCHECK_OK {
        aclchk::aclcheck_error(
            aclresult,
            ObjectType::OBJECT_FUNCTION,
            &catalog_objectaddress::NameListToString(&owa.objname),
        )?;
    }

    let shape = syscache_seams::lookup_pg_proc_shape::call(funcid)?
        .unwrap_or_else(|| panic!("cache lookup failed for function {funcid}"));
    let (rettype, args) = lsyscache::get_func_signature(mcx, funcid)?;
    let _ = rettype;
    let argtype0 = if args.is_empty() { InvalidOid } else { args[0] };
    Ok((funcid, argtype0, shape))
}

fn oid_key(attno: types_core::AttrNumber, oid: Oid) -> types_scan::scankey::ScanKeyData {
    let mut key = types_scan::scankey::ScanKeyData::empty();
    key.sk_attno = attno;
    key.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(oid);
    key
}

pub fn CreateTransform<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateTransformStmt<'mcx>,
) -> PgResult<ObjectAddress> {
    let (typeid, type_tn) = typename_oid(mcx, stmt.type_name)?;
    let typtype = lsyscache::get_typtype(typeid)?;
    let lang = stmt.lang.expect("transform language name");

    if typtype == TYPTYPE_PSEUDO {
        return Err(err(
            format!(
                "data type {} is a pseudo-type",
                commands_define::TypeNameToString(mcx, type_tn)?.as_str()
            ),
            ERRCODE_WRONG_OBJECT_TYPE,
        ));
    }
    if typtype == TYPTYPE_DOMAIN {
        return Err(err(
            format!(
                "data type {} is a domain",
                commands_define::TypeNameToString(mcx, type_tn)?.as_str()
            ),
            ERRCODE_WRONG_OBJECT_TYPE,
        ));
    }

    let userid = miscinit::GetUserId();
    if !aclchk::object_ownercheck(TYPE_RELATION_ID, typeid, userid)? {
        aclcheck_error_type(aclchk::ACLCHECK_NOT_OWNER, typeid)?;
    }
    let aclresult = aclchk::object_aclcheck(TYPE_RELATION_ID, typeid, userid, ACL_USAGE)?;
    if aclresult != aclchk::ACLCHECK_OK {
        aclcheck_error_type(aclresult, typeid)?;
    }

    let langid = adt_acl::get_language_oid(lang, false)?;
    let aclresult = aclchk::object_aclcheck(LANGUAGE_RELATION_ID, langid, userid, ACL_USAGE)?;
    if aclresult != aclchk::ACLCHECK_OK {
        aclchk::aclcheck_error(aclresult, ObjectType::OBJECT_LANGUAGE, lang)?;
    }

    let fromsqlfuncid = match stmt.fromsql {
        Some(node) => {
            let (funcid, argtype0, shape) = transform_func_lookup(mcx, node, userid)?;
            if shape.prorettype != INTERNALOID {
                return Err(err(
                    format!("return data type of FROM SQL function must be {}", "internal"),
                    ERRCODE_INVALID_OBJECT_DEFINITION,
                ));
            }
            check_transform_function(&shape, argtype0)?;
            funcid
        }
        None => InvalidOid,
    };
    let tosqlfuncid = match stmt.tosql {
        Some(node) => {
            let (funcid, argtype0, shape) = transform_func_lookup(mcx, node, userid)?;
            if shape.prorettype != typeid {
                return Err(objdef_err(
                    "return data type of TO SQL function must be the transform data type",
                ));
            }
            check_transform_function(&shape, argtype0)?;
            funcid
        }
        None => InvalidOid,
    };

    let relation = table::table_open(mcx, TransformRelationId, RowExclusiveLock)?;

    let mut values: [Datum; Natts_pg_transform] = [
        Datum::from_oid(InvalidOid),
        Datum::from_oid(typeid),
        Datum::from_oid(langid),
        Datum::from_oid(fromsqlfuncid),
        Datum::from_oid(tosqlfuncid),
    ];
    let nulls = [false; Natts_pg_transform];

    let keys = [oid_key(2, typeid), oid_key(3, langid)];
    let mut scan =
        genam::systable_beginscan(mcx, &relation, TransformTypeLangIndexId, true, None, &keys)?;
    let existing = match genam::systable_getnext(mcx, &mut scan)? {
        Some(oldtup) => {
            if !stmt.replace {
                return Err(err(
                    format!(
                        "transform for type {} language \"{lang}\" already exists",
                        format_type::format_type_be(typeid)?
                    ),
                    ERRCODE_DUPLICATE_OBJECT,
                ));
            }
            let mut isnull = false;
            // SAFETY: oid is the fixed first NOT NULL pg_transform column.
            let oid = unsafe {
                types_tuple::heap_getattr(oldtup, Anum_pg_transform_oid, relation.descr(), &mut isnull)
            }
            .as_oid();
            let mut replaces = [false; Natts_pg_transform];
            replaces[Anum_pg_transform_trffromsql - 1] = true;
            replaces[Anum_pg_transform_trftosql - 1] = true;
            let newtup = heaptuple::heap_modify_tuple(
                mcx,
                oldtup,
                relation.descr(),
                &values,
                &nulls,
                &replaces,
            )?;
            Some((oid, oldtup.t_self, newtup))
        }
        None => None,
    };
    genam::systable_endscan(mcx, scan)?;

    let (transformid, is_replace) = match existing {
        Some((oid, otid, mut newtup)) => {
            catalog_indexing::CatalogTupleUpdate(mcx, &relation, &otid, &mut newtup)?;
            (oid, true)
        }
        None => {
            let oid = catalog::GetNewOidWithIndex(
                mcx,
                &relation,
                TransformOidIndexId,
                Anum_pg_transform_oid as types_core::AttrNumber,
            )?;
            values[0] = Datum::from_oid(oid);
            let mut newtup = heaptuple::heap_form_tuple(mcx, relation.descr(), &values, &nulls)?;
            catalog_indexing::CatalogTupleInsert(mcx, &relation, &mut newtup)?;
            (oid, false)
        }
    };

    if is_replace {
        pg_depend::deleteDependencyRecordsFor(mcx, TransformRelationId, transformid, true)?;
    }

    let myself = ObjectAddress::set(TransformRelationId, transformid);
    let mut referenced: [ObjectAddress; 4] = [myself; 4];
    let mut n = 0;
    referenced[n] = ObjectAddress::set(LANGUAGE_RELATION_ID, langid);
    n += 1;
    referenced[n] = ObjectAddress::set(TYPE_RELATION_ID, typeid);
    n += 1;
    if OidIsValid(fromsqlfuncid) {
        referenced[n] = ObjectAddress::set(PROCEDURE_RELATION_ID, fromsqlfuncid);
        n += 1;
    }
    if OidIsValid(tosqlfuncid) {
        referenced[n] = ObjectAddress::set(PROCEDURE_RELATION_ID, tosqlfuncid);
        n += 1;
    }
    pg_depend::record_object_address_dependencies(
        mcx,
        &myself,
        &mut referenced[..n],
        pg_depend::DependencyType::Normal,
    )?;

    pg_depend::recordDependencyOnCurrentExtension(mcx, &myself, is_replace)?;

    // InvokeObjectPostCreateHook: object-access hooks are elided repo-wide.

    relation.close(RowExclusiveLock)?;

    Ok(myself)
}

pub fn get_transform_oid(mcx: Mcx<'_>, type_id: Oid, lang_id: Oid, missing_ok: bool) -> PgResult<Oid> {
    let oid = cache_syscache::GetSysCacheOid(
        cache_syscache::cacheinfo::TRFTYPELANG,
        Anum_pg_transform_oid,
        cache_syscache::SysCacheKey::Value(Datum::from_oid(type_id)),
        cache_syscache::SysCacheKey::Value(Datum::from_oid(lang_id)),
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
    )?;
    if !OidIsValid(oid) && !missing_ok {
        return Err(err(
            format!(
                "transform for type {} language \"{}\" does not exist",
                format_type::format_type_be(type_id)?,
                lsyscache::get_language_name(mcx, lang_id, false)?
                    .expect("language name")
                    .as_str()
            ),
            ERRCODE_UNDEFINED_OBJECT,
        ));
    }
    Ok(oid)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shape() -> syscache_seams::PgProcShape {
        syscache_seams::PgProcShape {
            pronamespace: 11,
            prorettype: INTERNALOID,
            provariadic: InvalidOid,
            prosupport: InvalidOid,
            prolang: 12,
            pronargs: 1,
            prokind: PROKIND_FUNCTION,
            provolatile: b'i' as i8,
            proparallel: b's' as i8,
            proretset: false,
            proisstrict: true,
            proleakproof: false,
            prosecdef: false,
            proconfig_isnull: true,
        }
    }

    #[test]
    fn transform_function_checks_match_c_order() {
        assert!(check_transform_function(&shape(), INTERNALOID).is_ok());

        let mut s = shape();
        s.provolatile = PROVOLATILE_VOLATILE;
        let e = check_transform_function(&s, INTERNALOID).unwrap_err();
        assert!(e.to_string().contains("must not be volatile"));

        let mut s = shape();
        s.prokind = b'a' as i8;
        let e = check_transform_function(&s, INTERNALOID).unwrap_err();
        assert!(e.to_string().contains("must be a normal function"));

        let mut s = shape();
        s.proretset = true;
        let e = check_transform_function(&s, INTERNALOID).unwrap_err();
        assert!(e.to_string().contains("must not return a set"));

        let mut s = shape();
        s.pronargs = 2;
        let e = check_transform_function(&s, INTERNALOID).unwrap_err();
        assert!(e.to_string().contains("must take one argument"));

        let e = check_transform_function(&shape(), INT4OID).unwrap_err();
        assert!(e.to_string().contains("first argument of transform function"));
    }

    #[test]
    fn coercion_codes_match_pg_cast_h() {
        assert_eq!(COERCION_CODE_IMPLICIT, b'i' as i8);
        assert_eq!(COERCION_CODE_ASSIGNMENT, b'a' as i8);
        assert_eq!(COERCION_CODE_EXPLICIT, b'e' as i8);
        assert_eq!(COERCION_METHOD_FUNCTION, b'f' as i8);
        assert_eq!(COERCION_METHOD_BINARY, b'b' as i8);
        assert_eq!(COERCION_METHOD_INOUT, b'i' as i8);
    }
}
