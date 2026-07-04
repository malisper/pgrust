// functioncmds.c CREATE FUNCTION/PROCEDURE lane. Loud: inline SQL bodies
// (BEGIN ATOMIC / RETURN), parameter defaults, TABLE parameter mode,
// WINDOW/TRANSFORM/SUPPORT/SET options, languages beyond sql+internal,
// shell types, %TYPE / typmod / array-bound TypeNames, ALTER/DROP FUNCTION,
// CREATE CAST/TRANSFORM, DO, CALL.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use mcx::Mcx;
use pg_proc::{
    ClanguageId, ProcedureCreateArgs, INTERNALlanguageId, PROKIND_FUNCTION, PROKIND_PROCEDURE,
    PROPARALLEL_RESTRICTED, PROPARALLEL_SAFE, PROPARALLEL_UNSAFE, PROVOLATILE_IMMUTABLE,
    PROVOLATILE_STABLE, PROVOLATILE_VOLATILE, SQLlanguageId,
};
use types_core::{
    InvalidOid, Oid, ANYARRAYOID, ANYCOMPATIBLEARRAYOID, ANYOID, LANGUAGE_RELATION_ID,
    NAMESPACE_RELATION_ID, RECORDOID, TYPE_RELATION_ID, VOIDOID,
};
use types_error::{
    PgError, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_FUNCTION_DEFINITION,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_OBJECT, ERROR,
};
use types_nodes::parsenodes::{
    CreateFunctionStmt, DefElem, FunctionParameter, FunctionParameterMode, ObjectType,
};
use types_nodes::rawnodes::TypeName;
use types_nodes::Node;

pub use pg_proc::ObjectAddress;

const Anum_pg_language_oid: i32 = 1;
const Anum_pg_language_lanpltrusted: i32 = 5;
const Anum_pg_language_laninline: i32 = 7;
const Anum_pg_language_lanvalidator: i32 = 8;

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: functioncmds {what}")
}

#[cold]
#[inline(never)]
fn err(msg: String, sqlstate: types_error::SqlState) -> Box<PgError> {
    Box::new(PgError::new(ERROR, msg).with_sqlstate(sqlstate))
}

#[cold]
#[inline(never)]
fn conflicting_options() -> Box<PgError> {
    err("conflicting or redundant options".to_string(), ERRCODE_SYNTAX_ERROR)
}

#[cold]
#[inline(never)]
fn invalid_procedure_attribute() -> Box<PgError> {
    err(
        "invalid attribute in procedure definition".to_string(),
        ERRCODE_INVALID_FUNCTION_DEFINITION,
    )
}

struct FunctionAttrs<'mcx> {
    as_clause: Option<&'mcx DefElem<'mcx>>,
    language: Option<&'mcx str>,
    volatility: i8,
    strict: bool,
    security: bool,
    leakproof: bool,
    procost: f32,
    prorows: f32,
    parallel: i8,
}

fn defel_bool(defel: &DefElem<'_>) -> bool {
    defel
        .arg
        .and_then(|n| n.as_boolean())
        .unwrap_or_else(|| panic!("DefElem \"{}\": expected Boolean", defel.defname.unwrap_or("")))
        .boolval
}

fn defel_str<'mcx>(defel: &DefElem<'mcx>) -> &'mcx str {
    defel
        .arg
        .and_then(|n| n.as_string())
        .unwrap_or_else(|| panic!("DefElem \"{}\": expected String", defel.defname.unwrap_or("")))
        .sval
}

// defGetNumeric (define.c) over the NumericOnly shapes (Integer | Float).
fn defel_numeric(defel: &DefElem<'_>) -> PgResult<f32> {
    let arg = defel.arg.expect("DefElem numeric arg");
    if let Some(i) = arg.as_integer() {
        return Ok(i.ival as f32);
    }
    if let Some(f) = arg.as_float() {
        return f.fval.parse::<f32>().map_err(|_| {
            err(
                format!("{} requires a numeric value", defel.defname.unwrap_or("")),
                ERRCODE_SYNTAX_ERROR,
            )
        });
    }
    Err(err(
        format!("{} requires a numeric value", defel.defname.unwrap_or("")),
        ERRCODE_SYNTAX_ERROR,
    ))
}

fn interpret_func_volatility(defel: &DefElem<'_>) -> i8 {
    match defel_str(defel) {
        "immutable" => PROVOLATILE_IMMUTABLE,
        "stable" => PROVOLATILE_STABLE,
        "volatile" => PROVOLATILE_VOLATILE,
        other => panic!("invalid volatility \"{other}\""),
    }
}

fn interpret_func_parallel(defel: &DefElem<'_>) -> PgResult<i8> {
    match defel_str(defel) {
        "safe" => Ok(PROPARALLEL_SAFE),
        "unsafe" => Ok(PROPARALLEL_UNSAFE),
        "restricted" => Ok(PROPARALLEL_RESTRICTED),
        _ => Err(err(
            "parameter \"parallel\" must be SAFE, RESTRICTED, or UNSAFE".to_string(),
            ERRCODE_SYNTAX_ERROR,
        )),
    }
}

// compute_function_attributes + compute_common_attribute (functioncmds.c).
fn compute_function_attributes<'mcx>(
    stmt: &CreateFunctionStmt<'mcx>,
) -> PgResult<FunctionAttrs<'mcx>> {
    let mut as_item: Option<&'mcx DefElem<'mcx>> = None;
    let mut language_item: Option<&'mcx DefElem<'mcx>> = None;
    let mut volatility_item: Option<&'mcx DefElem<'mcx>> = None;
    let mut strict_item: Option<&'mcx DefElem<'mcx>> = None;
    let mut security_item: Option<&'mcx DefElem<'mcx>> = None;
    let mut leakproof_item: Option<&'mcx DefElem<'mcx>> = None;
    let mut cost_item: Option<&'mcx DefElem<'mcx>> = None;
    let mut rows_item: Option<&'mcx DefElem<'mcx>> = None;
    let mut parallel_item: Option<&'mcx DefElem<'mcx>> = None;

    let is_procedure = stmt.is_procedure;
    for option in stmt.options.iter() {
        let defel = option.as_def_elem().expect("createfunc_opt_list holds DefElems");
        let name = defel.defname.unwrap_or("");
        // compute_common_attribute rejects these before the conflict check.
        if is_procedure
            && matches!(
                name,
                "window" | "volatility" | "strict" | "leakproof" | "cost" | "rows" | "support"
                    | "parallel"
            )
        {
            return Err(invalid_procedure_attribute());
        }
        let slot: &mut Option<&'mcx DefElem<'mcx>> = match name {
            "as" => &mut as_item,
            "language" => &mut language_item,
            "transform" => unported("TRANSFORM option"),
            "window" => unported("WINDOW option"),
            "volatility" => &mut volatility_item,
            "strict" => &mut strict_item,
            "security" => &mut security_item,
            "leakproof" => &mut leakproof_item,
            "set" => unported("SET option (proconfig)"),
            "cost" => &mut cost_item,
            "rows" => &mut rows_item,
            "support" => unported("SUPPORT option"),
            "parallel" => &mut parallel_item,
            other => panic!("option \"{other}\" not recognized"),
        };
        if slot.is_some() {
            return Err(conflicting_options());
        }
        *slot = Some(defel);
    }

    let procost = match cost_item {
        Some(d) => {
            let v = defel_numeric(d)?;
            if v <= 0.0 {
                return Err(err(
                    "COST must be positive".to_string(),
                    ERRCODE_INVALID_PARAMETER_VALUE,
                ));
            }
            v
        }
        None => -1.0,
    };
    let prorows = match rows_item {
        Some(d) => {
            let v = defel_numeric(d)?;
            if v <= 0.0 {
                return Err(err(
                    "ROWS must be positive".to_string(),
                    ERRCODE_INVALID_PARAMETER_VALUE,
                ));
            }
            v
        }
        None => -1.0,
    };

    Ok(FunctionAttrs {
        as_clause: as_item,
        language: language_item.map(defel_str),
        volatility: volatility_item.map_or(PROVOLATILE_VOLATILE, interpret_func_volatility),
        strict: strict_item.map(defel_bool).unwrap_or(false),
        security: match security_item.map(defel_bool) {
            // Accepting DEFINER here would silently run as the caller —
            // fmgr_security_definer is unported.
            Some(true) => unported("SECURITY DEFINER (fmgr_security_definer)"),
            v => v.unwrap_or(false),
        },
        leakproof: leakproof_item.map(defel_bool).unwrap_or(false),
        procost,
        prorows,
        parallel: match parallel_item {
            Some(d) => interpret_func_parallel(d)?,
            None => PROPARALLEL_UNSAFE,
        },
    })
}

// LookupTypeName/typenameTypeId (parse_type.c) for function signatures:
// setof rides on the TypeName; shell types and decorated names are loud.
fn resolve_type_name<'mcx>(mcx: Mcx<'mcx>, tn: &TypeName<'_>) -> PgResult<Oid> {
    let (typoid, typname) = resolve_type_oid(mcx, tn)?;
    if typoid == InvalidOid {
        return Err(err(
            format!("type \"{typname}\" does not exist"),
            ERRCODE_UNDEFINED_OBJECT,
        ));
    }
    check_defined_and_acl(typoid)?;
    Ok(typoid)
}

fn resolve_type_oid<'mcx, 'a>(mcx: Mcx<'mcx>, tn: &TypeName<'a>) -> PgResult<(Oid, &'a str)> {
    if tn.pct_type {
        unported("%TYPE references");
    }
    if !tn.typmods.is_nil() || tn.typemod != -1 {
        unported("type modifiers on function signature types");
    }
    if !tn.arrayBounds.is_nil() {
        unported("array-bound TypeNames on function signatures");
    }
    if tn.typeOid != InvalidOid {
        unported("pre-resolved TypeName.typeOid");
    }

    let mut names: [&str; 4] = [""; 4];
    let nnames = tn.names.len();
    if nnames == 0 || nnames > 3 {
        unported("improper TypeName names length");
    }
    for (i, n) in tn.names.iter().enumerate() {
        names[i] = n.as_string().expect("TypeName names").sval;
    }
    let (schemaname, typname) = catalog_namespace::DeconstructQualifiedName(&names[..nnames])?;

    let typoid = match schemaname {
        Some(schemaname) => {
            let namespace_id = catalog_namespace::LookupExplicitNamespace(schemaname, false)?;
            syscache_seams::lookup_pg_type_oid_by_name::call(typname, namespace_id)?
        }
        None => {
            let mut found = InvalidOid;
            for &namespace_id in catalog_namespace::fetch_search_path(mcx, true)?.iter() {
                found = syscache_seams::lookup_pg_type_oid_by_name::call(typname, namespace_id)?;
                if found != InvalidOid {
                    break;
                }
            }
            found
        }
    };
    Ok((typoid, typname))
}

fn check_defined_and_acl(typoid: Oid) -> PgResult<()> {
    match syscache_seams::pg_type_isdefined::call(typoid)? {
        Some(true) => {}
        _ => unported("shell types in function signatures"),
    }
    let aclresult = aclchk::object_aclcheck(
        TYPE_RELATION_ID,
        typoid,
        miscinit::GetUserId(),
        types_nodes::parsenodes::ACL_USAGE,
    )?;
    if aclresult != aclchk::ACLCHECK_OK {
        unported("aclcheck_error_type (type USAGE denied)");
    }
    Ok(())
}

// compute_return_type (functioncmds.c); shell-type creation is loud.
fn compute_return_type<'mcx>(
    mcx: Mcx<'mcx>,
    returnType: &TypeName<'_>,
    languageOid: Oid,
) -> PgResult<(Oid, bool)> {
    let (rettype, typname) = resolve_type_oid(mcx, returnType)?;
    if rettype == InvalidOid {
        // C makes a shell type here for internal/C-language I/O functions.
        if languageOid == INTERNALlanguageId || languageOid == ClanguageId {
            unported("shell type creation for I/O function return types (TypeShellMake)");
        }
        return Err(err(
            format!("type \"{typname}\" does not exist"),
            ERRCODE_UNDEFINED_OBJECT,
        ));
    }
    check_defined_and_acl(rettype)?;
    Ok((rettype, returnType.setof))
}

struct ParameterList<'mcx> {
    in_types: mcx::PgVec<'mcx, Oid>,
    all_types: mcx::PgVec<'mcx, Oid>,
    param_modes: mcx::PgVec<'mcx, i8>,
    names: mcx::PgVec<'mcx, &'mcx str>,
    have_names: bool,
    have_out_or_variadic: bool,
    required_result_type: Oid,
}

// interpret_function_parameter_list (functioncmds.c); DEFAULT expressions
// are loud.
fn interpret_function_parameter_list<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateFunctionStmt<'mcx>,
) -> PgResult<ParameterList<'mcx>> {
    use FunctionParameterMode::*;
    let is_procedure = stmt.is_procedure;
    let n = stmt.parameters.len();
    let mut in_types: mcx::PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, n)?;
    let mut all_types: mcx::PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, n)?;
    let mut param_modes: mcx::PgVec<'mcx, i8> = mcx::vec_with_capacity_in(mcx, n)?;
    let mut names: mcx::PgVec<'mcx, &'mcx str> = mcx::vec_with_capacity_in(mcx, n)?;
    let mut have_names = false;
    let mut out_count = 0usize;
    let mut var_count = 0usize;
    let mut required_result_type = InvalidOid;

    for p in stmt.parameters.iter() {
        let fp: &FunctionParameter<'mcx> = p
            .as_function_parameter()
            .expect("func_args_with_defaults holds FunctionParameters");
        let fpmode = match fp.mode {
            FUNC_PARAM_DEFAULT => FUNC_PARAM_IN,
            m => m,
        };
        if fp.defexpr.is_some() {
            unported("parameter DEFAULT expressions");
        }
        let tn_node: Node<'mcx> = fp.argType.expect("FunctionParameter.argType");
        let tn = tn_node.as_variant::<TypeName>().expect("argType is a TypeName");
        let toid = resolve_type_name(mcx, tn)?;
        if tn.setof {
            let msg = if is_procedure {
                "procedures cannot accept set arguments"
            } else {
                "functions cannot accept set arguments"
            };
            return Err(err(msg.to_string(), ERRCODE_INVALID_FUNCTION_DEFINITION));
        }

        if matches!(fpmode, FUNC_PARAM_IN | FUNC_PARAM_INOUT | FUNC_PARAM_VARIADIC) {
            if var_count > 0 {
                return Err(err(
                    "VARIADIC parameter must be the last input parameter".to_string(),
                    ERRCODE_INVALID_FUNCTION_DEFINITION,
                ));
            }
            in_types.push(toid);
        }

        if fpmode != FUNC_PARAM_IN && fpmode != FUNC_PARAM_VARIADIC {
            if is_procedure {
                // OUT-after-VARIADIC is disallowed only for procedures: it
                // would cause confusion in a CALL statement.
                if var_count > 0 {
                    return Err(err(
                        "VARIADIC parameter must be the last parameter".to_string(),
                        ERRCODE_INVALID_FUNCTION_DEFINITION,
                    ));
                }
                required_result_type = RECORDOID;
            } else if out_count == 0 {
                required_result_type = toid;
            }
            out_count += 1;
        }

        if fpmode == FUNC_PARAM_VARIADIC {
            var_count += 1;
            match toid {
                ANYARRAYOID | ANYCOMPATIBLEARRAYOID | ANYOID => {}
                _ => {
                    if lsyscache::get_element_type(toid)? == InvalidOid {
                        return Err(err(
                            "VARIADIC parameter must be an array".to_string(),
                            ERRCODE_INVALID_FUNCTION_DEFINITION,
                        ));
                    }
                }
            }
        }

        all_types.push(toid);
        param_modes.push(fpmode as i8);

        let name = fp.name.unwrap_or("");
        if !name.is_empty() {
            let is_in = |m: i8| m == FUNC_PARAM_IN as i8 || m == FUNC_PARAM_VARIADIC as i8;
            let is_out = |m: i8| m == FUNC_PARAM_OUT as i8 || m == FUNC_PARAM_TABLE as i8;
            for (j, &pn) in names.iter().enumerate() {
                let prevmode = param_modes[j];
                // Pure in doesn't conflict with pure out.
                if is_in(fpmode as i8) && is_out(prevmode) {
                    continue;
                }
                if is_in(prevmode) && is_out(fpmode as i8) {
                    continue;
                }
                if !pn.is_empty() && pn == name {
                    return Err(err(
                        format!("parameter name \"{name}\" used more than once"),
                        ERRCODE_INVALID_FUNCTION_DEFINITION,
                    ));
                }
            }
            have_names = true;
        }
        names.push(name);
    }

    let have_out_or_variadic = out_count > 0 || var_count > 0;
    if have_out_or_variadic && out_count > 1 {
        required_result_type = RECORDOID;
    }
    Ok(ParameterList {
        in_types,
        all_types,
        param_modes,
        names,
        have_names,
        have_out_or_variadic,
        required_result_type,
    })
}

struct AsClause<'a> {
    prosrc: &'a str,
    probin: Option<&'a str>,
}

// interpret_AS_clause (functioncmds.c); sql_body and C-language are loud.
fn interpret_AS_clause<'a>(
    languageOid: Oid,
    languageName: &str,
    funcname: &'a str,
    as_clause: Option<&'a DefElem<'a>>,
    sql_body: Option<Node<'a>>,
) -> PgResult<AsClause<'a>> {
    if sql_body.is_some() {
        unported("inline SQL function body (BEGIN ATOMIC / RETURN)");
    }
    let Some(as_item) = as_clause else {
        return Err(err(
            "no function body specified".to_string(),
            ERRCODE_INVALID_FUNCTION_DEFINITION,
        ));
    };
    let items = as_item.arg.expect("AS DefElem arg").as_list().expect("func_as is a List");
    if languageOid == ClanguageId {
        // File name in probin, link symbol in prosrc; omitted or "-" symbol
        // substitutes the function name.
        let mut it = items.iter();
        let probin = it
            .next()
            .and_then(|n| n.as_string())
            .expect("func_as items are Strings")
            .sval;
        let prosrc = match it.next() {
            None => funcname,
            Some(n) => {
                let s = n.as_string().expect("func_as items are Strings").sval;
                if s == "-" {
                    funcname
                } else {
                    s
                }
            }
        };
        return Ok(AsClause { prosrc, probin: Some(probin) });
    }
    if items.len() != 1 {
        return Err(err(
            format!("only one AS item needed for language \"{languageName}\""),
            ERRCODE_INVALID_FUNCTION_DEFINITION,
        ));
    }
    let mut prosrc = items
        .iter()
        .next()
        .and_then(|n| n.as_string())
        .expect("func_as items are Strings")
        .sval;
    if languageOid == INTERNALlanguageId && prosrc.is_empty() {
        prosrc = funcname;
    }
    Ok(AsClause { prosrc, probin: None })
}

// QualifiedNameGetCreationNamespace (namespace.c) via the RangeVar walk.
fn qualified_name_get_creation_namespace<'mcx>(
    mcx: Mcx<'mcx>,
    funcname: &types_nodes::NodeList<'mcx>,
) -> PgResult<(Oid, &'mcx str)> {
    let mut names: [&str; 4] = [""; 4];
    let nnames = funcname.len();
    if nnames == 0 || nnames > 3 {
        unported("improper qualified function name");
    }
    for (i, n) in funcname.iter().enumerate() {
        names[i] = n.as_string().expect("func_name holds Strings").sval;
    }
    let (schemaname, objname) = catalog_namespace::DeconstructQualifiedName(&names[..nnames])?;
    let rv = rel_vocab::RangeVar {
        catalogname: None,
        schemaname,
        relname: objname,
        inh: true,
        relpersistence: b'p',
        location: -1,
    };
    let nsid = catalog_namespace::RangeVarGetCreationNamespace(mcx, &rv)?;
    Ok((nsid, objname))
}

// CreateFunction (functioncmds.c).
pub fn CreateFunction<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateFunctionStmt<'mcx>,
) -> PgResult<ObjectAddress> {
    let (namespaceId, funcname) = qualified_name_get_creation_namespace(mcx, &stmt.funcname)?;

    let aclresult = aclchk::object_aclcheck(
        NAMESPACE_RELATION_ID,
        namespaceId,
        miscinit::GetUserId(),
        types_nodes::parsenodes::ACL_CREATE,
    )?;
    if aclresult != aclchk::ACLCHECK_OK {
        let nspname = lsyscache::get_namespace_name(mcx, namespaceId)?
            .map(|s| s.to_string())
            .unwrap_or_default();
        aclchk_seams::aclcheck_error::call(aclresult, ObjectType::OBJECT_SCHEMA as i32, &nspname)?;
    }

    let attrs = compute_function_attributes(stmt)?;

    let language = match attrs.language {
        Some(l) => l,
        None => {
            if stmt.sql_body.is_some() {
                "sql"
            } else {
                return Err(err(
                    "no language specified".to_string(),
                    ERRCODE_INVALID_FUNCTION_DEFINITION,
                ));
            }
        }
    };

    let Some(lang_tuple) = cache_syscache::SearchSysCache1(
        cache_syscache::cacheinfo::LANGNAME,
        cache_syscache::SysCacheKey::Str(language),
    )?
    else {
        return Err(err(
            format!("language \"{language}\" does not exist"),
            ERRCODE_UNDEFINED_OBJECT,
        ));
    };
    let languageOid = cache_syscache::SysCacheGetAttrNotNull(
        cache_syscache::cacheinfo::LANGNAME,
        &lang_tuple,
        Anum_pg_language_oid,
    )?
    .as_oid();
    let lanpltrusted = cache_syscache::SysCacheGetAttrNotNull(
        cache_syscache::cacheinfo::LANGNAME,
        &lang_tuple,
        Anum_pg_language_lanpltrusted,
    )?
    .as_bool();
    let languageValidator = cache_syscache::SysCacheGetAttrNotNull(
        cache_syscache::cacheinfo::LANGNAME,
        &lang_tuple,
        Anum_pg_language_lanvalidator,
    )?
    .as_oid();
    cache_syscache::ReleaseSysCache(lang_tuple);

    if languageOid != SQLlanguageId && languageOid != INTERNALlanguageId
        && languageOid != ClanguageId && language != "plpgsql"
    {
        unported("languages beyond sql, internal, c and plpgsql");
    }

    if lanpltrusted {
        let aclresult = aclchk::object_aclcheck(
            LANGUAGE_RELATION_ID,
            languageOid,
            miscinit::GetUserId(),
            types_nodes::parsenodes::ACL_USAGE,
        )?;
        if aclresult != aclchk::ACLCHECK_OK {
            aclchk_seams::aclcheck_error::call(
                aclresult,
                ObjectType::OBJECT_LANGUAGE as i32,
                language,
            )?;
        }
    } else if !superuser::superuser()? {
        aclchk_seams::aclcheck_error::call(
            aclchk::ACLCHECK_NO_PRIV,
            ObjectType::OBJECT_LANGUAGE as i32,
            language,
        )?;
    }

    if attrs.leakproof && !superuser::superuser()? {
        return Err(err(
            "only superuser can define a leakproof function".to_string(),
            ERRCODE_INSUFFICIENT_PRIVILEGE,
        ));
    }

    let params = interpret_function_parameter_list(mcx, stmt)?;

    let (prorettype, returnsSet) = if stmt.is_procedure {
        debug_assert!(stmt.returnType.is_none());
        let rt = if params.required_result_type != InvalidOid {
            params.required_result_type
        } else {
            VOIDOID
        };
        (rt, false)
    } else if let Some(rt) = stmt.returnType {
        let tn = rt.as_variant::<TypeName>().expect("returnType is a TypeName");
        let (prorettype, returnsSet) = compute_return_type(mcx, tn, languageOid)?;
        if params.required_result_type != InvalidOid && prorettype != params.required_result_type {
            return Err(err(
                format!(
                    "function result type must be {} because of OUT parameters",
                    format_type::format_type_be(params.required_result_type)?
                ),
                ERRCODE_INVALID_FUNCTION_DEFINITION,
            ));
        }
        (prorettype, returnsSet)
    } else if params.required_result_type != InvalidOid {
        (params.required_result_type, false)
    } else {
        return Err(err(
            "function result type must be specified".to_string(),
            ERRCODE_INVALID_FUNCTION_DEFINITION,
        ));
    };

    let as_parsed =
        interpret_AS_clause(languageOid, language, funcname, attrs.as_clause, stmt.sql_body)?;

    let procost = if attrs.procost < 0.0 {
        if languageOid == INTERNALlanguageId || languageOid == ClanguageId {
            1.0
        } else {
            100.0
        }
    } else {
        attrs.procost
    };
    let prorows = if attrs.prorows < 0.0 {
        if returnsSet {
            1000.0
        } else {
            0.0
        }
    } else if !returnsSet {
        return Err(err(
            "ROWS is not applicable when function does not return a set".to_string(),
            ERRCODE_INVALID_PARAMETER_VALUE,
        ));
    } else {
        attrs.prorows
    };

    pg_proc::ProcedureCreate(
        mcx,
        &ProcedureCreateArgs {
            procedureName: funcname,
            procNamespace: namespaceId,
            replace: stmt.replace,
            returnsSet,
            returnType: prorettype,
            proowner: miscinit::GetUserId(),
            languageObjectId: languageOid,
            languageValidator,
            prosrc: as_parsed.prosrc,
            probin: as_parsed.probin,
            prokind: if stmt.is_procedure { PROKIND_PROCEDURE } else { PROKIND_FUNCTION },
            security_definer: attrs.security,
            isLeakProof: attrs.leakproof,
            isStrict: attrs.strict,
            volatility: attrs.volatility,
            parallel: attrs.parallel,
            parameterTypes: &params.in_types,
            allParameterTypes: if params.have_out_or_variadic {
                Some(&params.all_types)
            } else {
                None
            },
            parameterModes: if params.have_out_or_variadic {
                Some(&params.param_modes)
            } else {
                None
            },
            parameterNames: if params.have_names { Some(&params.names) } else { None },
            procost,
            prorows,
        },
    )
}

// Guts of function deletion (functioncmds.c RemoveFunctionById); aggregates
// and per-function pgstat drop are loud/no-op until their lanes land.
pub fn RemoveFunctionById<'mcx>(mcx: Mcx<'mcx>, funcOid: Oid) -> PgResult<()> {
    const Anum_pg_proc_prokind: i32 = 21;
    const PROKIND_AGGREGATE: i8 = b'a' as i8;

    let relation = table::table_open(
        mcx,
        types_core::PROCEDURE_RELATION_ID,
        types_rel::RowExclusiveLock,
    )?;
    let mut key = types_scan::scankey::ScanKeyData::empty();
    key.sk_attno = 1;
    key.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = datum::Datum::from_oid(funcOid);
    let mut scan =
        genam::systable_beginscan(mcx, &relation, pg_proc::ProcedureOidIndexId, true, None, &[key])?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for function {funcOid}"));
    let tid = tup.t_self;
    let mut isnull = false;
    // SAFETY: prokind is a fixed NOT NULL pg_proc column.
    let prokind = unsafe {
        types_tuple::heap_getattr(tup, Anum_pg_proc_prokind, relation.descr(), &mut isnull)
    }
    .as_i8();
    catalog_indexing::CatalogTupleDelete(&relation, &tid)?;
    genam::systable_endscan(mcx, scan)?;
    relation.close(types_rel::RowExclusiveLock)?;
    // pgstat_drop_function: skipped (per-function stats unported).
    if prokind == PROKIND_AGGREGATE {
        unported("RemoveFunctionById: pg_aggregate tuple deletion (aggregate DDL lane)");
    }
    Ok(())
}

// ExecuteDoStmt (functioncmds.c:2084).
pub fn ExecuteDoStmt<'mcx>(
    stmt: &types_nodes::parsenodes::DoStmt<'mcx>,
    atomic: bool,
) -> PgResult<()> {
    let mut as_item: Option<&DefElem<'mcx>> = None;
    let mut language_item: Option<&DefElem<'mcx>> = None;
    for option in stmt.args.iter() {
        let defel = option.as_def_elem().expect("dostmt_opt_list holds DefElems");
        let slot = match defel.defname.unwrap_or("") {
            "as" => &mut as_item,
            "language" => &mut language_item,
            other => panic!("option \"{other}\" not recognized"),
        };
        if slot.is_some() {
            return Err(conflicting_options());
        }
        *slot = Some(defel);
    }

    let Some(as_item) = as_item else {
        return Err(err(
            "no inline code specified".to_string(),
            ERRCODE_SYNTAX_ERROR,
        ));
    };
    let source_text = defel_str(as_item);
    let language = language_item.map(defel_str).unwrap_or("plpgsql");

    let Some(lang_tuple) = cache_syscache::SearchSysCache1(
        cache_syscache::cacheinfo::LANGNAME,
        cache_syscache::SysCacheKey::Str(language),
    )?
    else {
        let mut e = PgError::new(ERROR, format!("language \"{language}\" does not exist"))
            .with_sqlstate(ERRCODE_UNDEFINED_OBJECT);
        if extension::extension_file_exists(language)? {
            e.hint = Some("Use CREATE EXTENSION to load the language into the database.".to_string());
        }
        return Err(Box::new(e));
    };
    let lang_oid = cache_syscache::SysCacheGetAttrNotNull(
        cache_syscache::cacheinfo::LANGNAME,
        &lang_tuple,
        Anum_pg_language_oid,
    )?
    .as_oid();
    let lanpltrusted = cache_syscache::SysCacheGetAttrNotNull(
        cache_syscache::cacheinfo::LANGNAME,
        &lang_tuple,
        Anum_pg_language_lanpltrusted,
    )?
    .as_bool();
    let laninline = cache_syscache::SysCacheGetAttrNotNull(
        cache_syscache::cacheinfo::LANGNAME,
        &lang_tuple,
        Anum_pg_language_laninline,
    )?
    .as_oid();
    cache_syscache::ReleaseSysCache(lang_tuple);

    if lanpltrusted {
        let aclresult = aclchk::object_aclcheck(
            LANGUAGE_RELATION_ID,
            lang_oid,
            miscinit::GetUserId(),
            types_nodes::parsenodes::ACL_USAGE,
        )?;
        if aclresult != aclchk::ACLCHECK_OK {
            aclchk_seams::aclcheck_error::call(
                aclresult,
                ObjectType::OBJECT_LANGUAGE as i32,
                language,
            )?;
        }
    } else if !superuser::superuser()? {
        aclchk_seams::aclcheck_error::call(
            aclchk::ACLCHECK_NO_PRIV,
            ObjectType::OBJECT_LANGUAGE as i32,
            language,
        )?;
    }

    if !types_core::OidIsValid(laninline) {
        return Err(err(
            format!("language \"{language}\" does not support inline code execution"),
            types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
        ));
    }

    let codeblock = types_nodes::parsenodes::InlineCodeBlock {
        source_text,
        lang_oid,
        lang_is_trusted: lanpltrusted,
        atomic,
    };
    let mut flinfo = fmgr_core::fmgr_info(laninline)?;
    types_fmgr::function_call1_coll(
        &mut flinfo,
        types_core::InvalidOid,
        datum::Datum::from_usize(&codeblock as *const _ as usize),
    )?;
    Ok(())
}
