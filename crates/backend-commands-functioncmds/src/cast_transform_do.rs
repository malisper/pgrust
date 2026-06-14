//! Cast / Transform / DO family of `backend/commands/functioncmds.c`.
//!
//! `CreateCast`, `CreateTransform` (+ `check_transform_function`,
//! `check_transform_func`), `get_transform_oid`, `IsThereFunctionInNamespace`,
//! and `ExecuteDoStmt`.

use crate::keystone::{
    as_type_name, cache_lookup_failed_function, check_language_permissions, def_arg_str_val,
    def_name, error_conflicting_def_elem, errloc, name_list_to_string, OBJECT_FUNCTION,
};
use backend_commands_functioncmds_seams::{self as seam, CastFuncForm, TransformFuncForm};
use backend_utils_error::ereport;
use mcx::Mcx;
use types_acl::{ACLCHECK_NOT_OWNER, ACLCHECK_OK, ACL_EXECUTE, ACL_USAGE};
use types_catalog::catalog_dependency::ObjectAddress;
use types_cache::SysCacheKey;
use types_core::{AttrNumber, InvalidOid, Oid, OidIsValid};
use types_datum::Datum;
use types_error::{
    PgResult, ERRCODE_DUPLICATE_FUNCTION, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_INTERNAL_ERROR, ERRCODE_INVALID_OBJECT_DEFINITION, ERRCODE_SYNTAX_ERROR,
    ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE, ERROR, WARNING,
};
use types_parsenodes::{
    CoercionContext, CreateCastStmt, CreateTransformStmt, DefElem, DoStmt, InlineCodeBlock, Node,
    COERCION_CODE_ASSIGNMENT, COERCION_CODE_EXPLICIT, COERCION_CODE_IMPLICIT, COERCION_METHOD_BINARY,
    COERCION_METHOD_FUNCTION, COERCION_METHOD_INOUT, PROKIND_FUNCTION, PROVOLATILE_VOLATILE,
    TYPTYPE_COMPOSITE, TYPTYPE_DOMAIN, TYPTYPE_ENUM, TYPTYPE_MULTIRANGE, TYPTYPE_PSEUDO,
    TYPTYPE_RANGE,
};
use types_tuple::{BOOLOID, INT4OID, INTERNALOID};

// ===========================================================================
// CreateCast (functioncmds.c:1538)
// ===========================================================================

pub fn CreateCast(stmt: &CreateCastStmt) -> PgResult<ObjectAddress> {
    let sourcetype = match &stmt.sourcetype {
        Some(b) => as_type_name(b)?,
        None => {
            return Err(ereport(ERROR)
                .errmsg_internal("CreateCast: stmt->sourcetype is NULL")
                .into_error());
        }
    };
    let targettype = match &stmt.targettype {
        Some(b) => as_type_name(b)?,
        None => {
            return Err(ereport(ERROR)
                .errmsg_internal("CreateCast: stmt->targettype is NULL")
                .into_error());
        }
    };

    let sourcetypeid = seam::typename_type_id::call(sourcetype.clone())?;
    let targettypeid = seam::typename_type_id::call(targettype.clone())?;
    let sourcetyptype = seam::get_typtype::call(sourcetypeid)?;
    let targettyptype = seam::get_typtype::call(targettypeid)?;

    /* No pseudo-types allowed */
    if sourcetyptype == TYPTYPE_PSEUDO {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "source data type {} is a pseudo-type",
                seam::type_name_to_string::call(sourcetype.clone())?
            ))
            .into_error());
    }

    if targettyptype == TYPTYPE_PSEUDO {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "target data type {} is a pseudo-type",
                seam::type_name_to_string::call(targettype.clone())?
            ))
            .into_error());
    }

    /* Permission check */
    if !seam::type_ownercheck::call(sourcetypeid, seam::get_user_id::call()?)?
        && !seam::type_ownercheck::call(targettypeid, seam::get_user_id::call()?)?
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!(
                "must be owner of type {} or type {}",
                backend_utils_adt_format_type_seams::format_type_be_str::call(sourcetypeid)?,
                backend_utils_adt_format_type_seams::format_type_be_str::call(targettypeid)?
            ))
            .into_error());
    }

    let aclresult = seam::type_aclcheck::call(sourcetypeid, seam::get_user_id::call()?, ACL_USAGE)?;
    if aclresult != ACLCHECK_OK {
        seam::aclcheck_error_type::call(aclresult, sourcetypeid)?;
    }

    let aclresult = seam::type_aclcheck::call(targettypeid, seam::get_user_id::call()?, ACL_USAGE)?;
    if aclresult != ACLCHECK_OK {
        seam::aclcheck_error_type::call(aclresult, targettypeid)?;
    }

    /* Domains are allowed for historical reasons, but we warn */
    if sourcetyptype == TYPTYPE_DOMAIN {
        ereport(WARNING)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg("cast will be ignored because the source data type is a domain")
            .finish(errloc(1592, "CreateCast"))?;
    } else if targettyptype == TYPTYPE_DOMAIN {
        ereport(WARNING)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg("cast will be ignored because the target data type is a domain")
            .finish(errloc(1597, "CreateCast"))?;
    }

    /* Determine the cast method */
    let castmethod: i8 = if stmt.func.is_some() {
        COERCION_METHOD_FUNCTION
    } else if stmt.inout {
        COERCION_METHOD_INOUT
    } else {
        COERCION_METHOD_BINARY
    };

    let funcid: Oid;
    let nargs: i32;
    let mut incastid = InvalidOid;
    let mut outcastid = InvalidOid;

    if castmethod == COERCION_METHOD_FUNCTION {
        let func = match &stmt.func {
            Some(f) => (**f).clone(),
            None => {
                return Err(ereport(ERROR)
                    .errmsg_internal("CreateCast: COERCION_METHOD_FUNCTION but func is NULL")
                    .into_error());
            }
        };
        funcid = seam::lookup_func_with_args::call(OBJECT_FUNCTION, func, false)?;

        let procstruct: CastFuncForm = match seam::fetch_cast_func_form::call(funcid)? {
            Some(p) => p,
            None => {
                return Err(cache_lookup_failed_function(funcid));
            }
        };

        nargs = procstruct.pronargs as i32;
        if nargs < 1 || nargs > 3 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("cast function must take one to three arguments")
                .into_error());
        }
        let (coercible, castid) =
            seam::is_binary_coercible_with_cast::call(sourcetypeid, procstruct.proargtypes[0])?;
        incastid = castid;
        if !coercible {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("argument of cast function must match or be binary-coercible from source data type")
                .into_error());
        }
        if nargs > 1 && procstruct.proargtypes[1] != INT4OID {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg(format!(
                    "second argument of cast function must be type {}",
                    "integer"
                ))
                .into_error());
        }
        if nargs > 2 && procstruct.proargtypes[2] != BOOLOID {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg(format!(
                    "third argument of cast function must be type {}",
                    "boolean"
                ))
                .into_error());
        }
        let (coercible, castid) =
            seam::is_binary_coercible_with_cast::call(procstruct.prorettype, targettypeid)?;
        outcastid = castid;
        if !coercible {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("return data type of cast function must match or be binary-coercible to target data type")
                .into_error());
        }

        /* NOT_USED volatility check intentionally omitted, as in C */

        if procstruct.prokind != PROKIND_FUNCTION {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("cast function must be a normal function")
                .into_error());
        }
        if procstruct.proretset {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("cast function must not return a set")
                .into_error());
        }
    } else {
        funcid = InvalidOid;
        nargs = 0;
    }

    if castmethod == COERCION_METHOD_BINARY {
        /* Must be superuser to create binary-compatible casts. */
        if !seam::superuser::call()? {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg("must be superuser to create a cast WITHOUT FUNCTION")
                .into_error());
        }

        /*
         * Insist that the types match as to size, alignment, and pass-by-value
         * attributes.
         */
        let (typ1len, typ1byval, typ1align) = seam::get_typlenbyvalalign::call(sourcetypeid)?;
        let (typ2len, typ2byval, typ2align) = seam::get_typlenbyvalalign::call(targettypeid)?;
        if typ1len != typ2len || typ1byval != typ2byval || typ1align != typ2align {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("source and target data types are not physically compatible")
                .into_error());
        }

        /* Composite, array, range and enum types are never binary-compatible. */
        if sourcetyptype == TYPTYPE_COMPOSITE || targettyptype == TYPTYPE_COMPOSITE {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("composite data types are not binary-compatible")
                .into_error());
        }

        if OidIsValid(seam::get_element_type::call(sourcetypeid)?)
            || OidIsValid(seam::get_element_type::call(targettypeid)?)
        {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("array data types are not binary-compatible")
                .into_error());
        }

        if sourcetyptype == TYPTYPE_RANGE
            || targettyptype == TYPTYPE_RANGE
            || sourcetyptype == TYPTYPE_MULTIRANGE
            || targettyptype == TYPTYPE_MULTIRANGE
        {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("range data types are not binary-compatible")
                .into_error());
        }

        if sourcetyptype == TYPTYPE_ENUM || targettyptype == TYPTYPE_ENUM {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("enum data types are not binary-compatible")
                .into_error());
        }

        /* Disallow binary-compatibility casts involving domains. */
        if sourcetyptype == TYPTYPE_DOMAIN || targettyptype == TYPTYPE_DOMAIN {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("domain data types must not be marked binary-compatible")
                .into_error());
        }
    }

    /*
     * Allow source and target types to be same only for length coercion
     * functions.  We assume a multi-arg function does length coercion.
     */
    if sourcetypeid == targettypeid && nargs < 2 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg("source data type and target data type are the same")
            .into_error());
    }

    /* convert CoercionContext enum to char value for castcontext */
    let castcontext: i8 = match stmt.context {
        CoercionContext::COERCION_IMPLICIT => COERCION_CODE_IMPLICIT,
        CoercionContext::COERCION_ASSIGNMENT => COERCION_CODE_ASSIGNMENT,
        /* COERCION_PLPGSQL is intentionally not covered here */
        CoercionContext::COERCION_EXPLICIT => COERCION_CODE_EXPLICIT,
        other => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INTERNAL_ERROR)
                .errmsg_internal(format!("unrecognized CoercionContext: {}", other as i32))
                .into_error());
        }
    };

    seam::cast_create::call(
        sourcetypeid,
        targettypeid,
        funcid,
        incastid,
        outcastid,
        castcontext,
        castmethod,
    )
}

// ===========================================================================
// check_transform_function (functioncmds.c:1801)
// ===========================================================================

fn check_transform_function(procstruct: &TransformFuncForm) -> PgResult<()> {
    if procstruct.provolatile == PROVOLATILE_VOLATILE {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg("transform function must not be volatile")
            .into_error());
    }
    if procstruct.prokind != PROKIND_FUNCTION {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg("transform function must be a normal function")
            .into_error());
    }
    if procstruct.proretset {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg("transform function must not return a set")
            .into_error());
    }
    if procstruct.pronargs != 1 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg("transform function must take one argument")
            .into_error());
    }
    if procstruct.proargtype0 != INTERNALOID {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(format!(
                "first argument of transform function must be type {}",
                "internal"
            ))
            .into_error());
    }
    Ok(())
}

// ===========================================================================
// CreateTransform (functioncmds.c:1831)
// ===========================================================================

pub fn CreateTransform(stmt: &CreateTransformStmt) -> PgResult<ObjectAddress> {
    /* Get the type */
    let type_name = match &stmt.type_name {
        Some(b) => as_type_name(b)?,
        None => {
            return Err(ereport(ERROR)
                .errmsg_internal("CreateTransform: stmt->type_name is NULL")
                .into_error());
        }
    };
    let typeid = seam::typename_type_id::call(type_name.clone())?;
    let typtype = seam::get_typtype::call(typeid)?;

    if typtype == TYPTYPE_PSEUDO {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "data type {} is a pseudo-type",
                seam::type_name_to_string::call(type_name.clone())?
            ))
            .into_error());
    }

    if typtype == TYPTYPE_DOMAIN {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "data type {} is a domain",
                seam::type_name_to_string::call(type_name.clone())?
            ))
            .into_error());
    }

    if !seam::type_ownercheck::call(typeid, seam::get_user_id::call()?)? {
        seam::aclcheck_error_type::call(ACLCHECK_NOT_OWNER, typeid)?;
    }

    let aclresult = seam::type_aclcheck::call(typeid, seam::get_user_id::call()?, ACL_USAGE)?;
    if aclresult != ACLCHECK_OK {
        seam::aclcheck_error_type::call(aclresult, typeid)?;
    }

    /* Get the language */
    let lang = stmt.lang.clone().unwrap_or_default();
    let langid = seam::get_language_oid::call(lang.clone(), false)?;

    let aclresult = seam::language_aclcheck::call(langid, seam::get_user_id::call()?, ACL_USAGE)?;
    if aclresult != ACLCHECK_OK {
        seam::aclcheck_error_language::call(aclresult, lang.clone())?;
    }

    /* Get the functions */
    let fromsqlfuncid = check_transform_func(&stmt.fromsql, typeid, true)?;
    let tosqlfuncid = check_transform_func(&stmt.tosql, typeid, false)?;

    /*
     * Ready to go — the pg_transform insert/update, dependency rebuild,
     * extension dependency, and post-create hook are raw catalog tuple I/O.
     */
    seam::create_transform_tuple::call(
        typeid,
        langid,
        fromsqlfuncid,
        tosqlfuncid,
        stmt.replace,
        lang,
    )
}

/// The per-direction function lookup + permission + signature check of
/// `CreateTransform` (functioncmds.c:1880-1953). `is_from` selects the FROM-SQL
/// return-type rule (`internal`) vs the TO-SQL rule (the transform data type).
fn check_transform_func(func: &Option<Box<Node>>, typeid: Oid, is_from: bool) -> PgResult<Oid> {
    let func = match func {
        Some(f) => (**f).clone(),
        None => return Ok(InvalidOid),
    };

    /* `func` is an `ObjectWithArgs`; objname is its qualified name list. */
    let objname: Vec<String> = match func.as_objectwithargs() {
        Some(owa) => owa.objname.clone(),
        _ => {
            return Err(ereport(ERROR)
                .errmsg_internal("CreateTransform: fromsql/tosql is not an ObjectWithArgs")
                .into_error());
        }
    };

    let funcid = seam::lookup_func_with_args::call(OBJECT_FUNCTION, func, false)?;

    if !seam::proc_ownercheck::call(funcid, seam::get_user_id::call()?)? {
        seam::aclcheck_error_function::call(ACLCHECK_NOT_OWNER, name_list_to_string(&objname))?;
    }

    let aclresult = seam::proc_aclcheck::call(funcid, seam::get_user_id::call()?, ACL_EXECUTE)?;
    if aclresult != ACLCHECK_OK {
        seam::aclcheck_error_function::call(aclresult, name_list_to_string(&objname))?;
    }

    let procstruct = match seam::fetch_transform_func_form::call(funcid)? {
        Some(p) => p,
        None => return Err(cache_lookup_failed_function(funcid)),
    };

    if is_from {
        if procstruct.prorettype != INTERNALOID {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg(format!(
                    "return data type of FROM SQL function must be {}",
                    "internal"
                ))
                .into_error());
        }
    } else if procstruct.prorettype != typeid {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg("return data type of TO SQL function must be the transform data type")
            .into_error());
    }
    check_transform_function(&procstruct)?;

    Ok(funcid)
}

// ===========================================================================
// get_transform_oid (functioncmds.c:2036)
// ===========================================================================

/// `Anum_pg_transform_oid` = 1 (`catalog/pg_transform_d.h`).
const Anum_pg_transform_oid: AttrNumber = 1;

/// `get_transform_oid(type_id, lang_id, missing_ok)` (functioncmds.c:2036).
///
/// The lookup core is `GetSysCacheOid2(TRFTYPELANG, Anum_pg_transform_oid,
/// ObjectIdGetDatum(type_id), ObjectIdGetDatum(lang_id))` over the live
/// syscache; only the error-message helpers cross seams.
pub fn get_transform_oid(
    mcx: Mcx<'_>,
    type_id: Oid,
    lang_id: Oid,
    missing_ok: bool,
) -> PgResult<Oid> {
    let oid = backend_utils_cache_syscache::GetSysCacheOid(
        mcx,
        backend_utils_cache_syscache::TRFTYPELANG,
        Anum_pg_transform_oid,
        SysCacheKey::Value(Datum::from_oid(type_id)),
        SysCacheKey::Value(Datum::from_oid(lang_id)),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )?;
    if !OidIsValid(oid) && !missing_ok {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!(
                "transform for type {} language \"{}\" does not exist",
                backend_utils_adt_format_type_seams::format_type_be_str::call(type_id)?,
                seam::get_language_name::call(lang_id)?
            ))
            .into_error());
    }
    Ok(oid)
}

// ===========================================================================
// IsThereFunctionInNamespace (functioncmds.c:2060)
// ===========================================================================

pub fn IsThereFunctionInNamespace(
    proname: &str,
    pronargs: i32,
    proargtypes: &[Oid],
    nsp_oid: Oid,
) -> PgResult<()> {
    /* check for duplicate name (more friendly than unique-index failure) */
    if seam::function_exists_in_namespace::call(proname.to_string(), proargtypes.to_vec(), nsp_oid)?
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_FUNCTION)
            .errmsg(format!(
                "function {} already exists in schema \"{}\"",
                seam::funcname_signature_string::call(
                    proname.to_string(),
                    pronargs,
                    proargtypes.to_vec()
                )?,
                seam::get_namespace_name::call(nsp_oid)?.unwrap_or_default()
            ))
            .into_error());
    }
    Ok(())
}

// ===========================================================================
// ExecuteDoStmt (functioncmds.c:2083)
// ===========================================================================

pub fn ExecuteDoStmt(stmt: &DoStmt, atomic: bool) -> PgResult<()> {
    let mut codeblock = InlineCodeBlock {
        source_text: None,
        langOid: InvalidOid,
        langIsTrusted: false,
        atomic: false,
    };
    let mut as_item: Option<DefElem> = None;
    let mut language_item: Option<DefElem> = None;

    /* Process options we got from gram.y */
    for arg in &stmt.args {
        let defel = match arg.as_defelem() {
            Some(d) => d,
            _ => {
                return Err(ereport(ERROR)
                    .errmsg_internal("ExecuteDoStmt: arg is not a DefElem")
                    .into_error());
            }
        };
        let defname = def_name(defel);

        if defname == "as" {
            if as_item.is_some() {
                return Err(error_conflicting_def_elem(defel));
            }
            as_item = Some(defel.clone());
        } else if defname == "language" {
            if language_item.is_some() {
                return Err(error_conflicting_def_elem(defel));
            }
            language_item = Some(defel.clone());
        } else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INTERNAL_ERROR)
                .errmsg_internal(format!("option \"{defname}\" not recognized"))
                .into_error());
        }
    }

    if let Some(as_item) = &as_item {
        codeblock.source_text = Some(def_arg_str_val(as_item)?);
    } else {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("no inline code specified")
            .into_error());
    }

    /* if LANGUAGE option wasn't specified, use the default */
    let language = if let Some(language_item) = &language_item {
        def_arg_str_val(language_item)?
    } else {
        "plpgsql".to_string()
    };

    /* Look up the language and validate permissions */
    let language_struct = match seam::lookup_language_by_name::call(language.clone())? {
        Some(ls) => ls,
        None => {
            let mut b = ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("language \"{language}\" does not exist"));
            if seam::extension_file_exists::call(language.clone())? {
                b = b.errhint("Use CREATE EXTENSION to load the language into the database.");
            }
            return Err(b.into_error());
        }
    };

    codeblock.langOid = language_struct.oid;
    codeblock.langIsTrusted = language_struct.lanpltrusted;
    codeblock.atomic = atomic;

    check_language_permissions(&language_struct)?;

    /* get the handler function's OID */
    let laninline = language_struct.laninline;
    if !OidIsValid(laninline) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!(
                "language \"{}\" does not support inline code execution",
                language_struct.lanname
            ))
            .into_error());
    }

    /* execute the inline handler */
    seam::execute_inline_handler::call(laninline, codeblock)
}
