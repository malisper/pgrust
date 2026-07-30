// functioncmds.c CREATE FUNCTION/PROCEDURE lane. Loud: inline SQL bodies
// (BEGIN ATOMIC / RETURN), parameter defaults, TABLE parameter mode,
// TRANSFORM/SUPPORT options, languages beyond sql+internal+C+plpgsql,
// %TYPE / typmod TypeNames, DROP FUNCTION, DO.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

mod cast_transform;
pub use cast_transform::{get_transform_oid, CreateCast, CreateTransform};

use mcx::Mcx;
use pg_proc::{
    ClanguageId, ProcedureCreateArgs, INTERNALlanguageId, PROKIND_FUNCTION, PROKIND_PROCEDURE,
    PROKIND_WINDOW,
    PROPARALLEL_RESTRICTED, PROPARALLEL_SAFE, PROPARALLEL_UNSAFE, PROVOLATILE_IMMUTABLE,
    PROVOLATILE_STABLE, PROVOLATILE_VOLATILE, SQLlanguageId,
};
use types_core::{
    AttrNumber, InvalidOid, Oid, ANYARRAYOID, ANYCOMPATIBLEARRAYOID, ANYOID, FUNC_MAX_ARGS,
    LANGUAGE_RELATION_ID, NAMESPACE_RELATION_ID, PROCEDURE_RELATION_ID, RECORDOID,
    TYPE_RELATION_ID, VOIDOID,
};
use types_error::{
    PgError, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_FUNCTION_DEFINITION,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_SYNTAX_ERROR, ERRCODE_TOO_MANY_ARGUMENTS,
    ERRCODE_UNDEFINED_OBJECT, ERROR,
};
use types_nodes::parsenodes::{
    AlterFunctionStmt, CreateFunctionStmt, DefElem, FunctionParameter, FunctionParameterMode,
    ObjectType, VariableSetKind, VariableSetStmt, ACL_EXECUTE,
};
use types_nodes::rawnodes::{CallStmt, TypeName};
use types_nodes::Node;
use types_portal::ParamListHandle;

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
pub(crate) fn err(msg: String, sqlstate: types_error::SqlState) -> Box<PgError> {
    Box::new(PgError::new(ERROR, msg).with_sqlstate(sqlstate))
}

#[track_caller]
#[cold]
#[inline(never)]
fn err_at(
    pstate: &parser_small1::ParseState<'_, '_>,
    location: types_core::ParseLoc,
    msg: String,
    sqlstate: types_error::SqlState,
) -> Box<PgError> {
    let pos = parser_small1::parser_errposition(pstate, location, mbutils::GetDatabaseEncoding());
    Box::new(PgError::new(ERROR, msg).with_sqlstate(sqlstate).with_cursor_position(pos))
}

#[track_caller]
#[cold]
#[inline(never)]
fn conflicting_options() -> Box<PgError> {
    err("conflicting or redundant options".to_string(), ERRCODE_SYNTAX_ERROR)
}

#[track_caller]
#[cold]
#[inline(never)]
fn invalid_procedure_attribute(source_text: &str, location: types_core::ParseLoc) -> Box<PgError> {
    let pos = parser_small1::parser_errposition_source(
        Some(source_text.as_bytes()),
        location,
        mbutils::GetDatabaseEncoding(),
    );
    Box::new(
        PgError::new(ERROR, "invalid attribute in procedure definition".to_string())
            .with_sqlstate(ERRCODE_INVALID_FUNCTION_DEFINITION)
            .with_cursor_position(pos),
    )
}

struct FunctionAttrs<'mcx> {
    as_clause: Option<&'mcx DefElem<'mcx>>,
    language: Option<&'mcx str>,
    windowfunc: bool,
    volatility: i8,
    strict: bool,
    security: bool,
    leakproof: bool,
    proconfig: Option<Vec<String>>,
    procost: f32,
    prorows: f32,
    support: Oid,
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

// interpret_func_support (functioncmds.c): support functions always take one
// INTERNAL argument and return INTERNAL; superuser-only (privilege on the
// support function itself is moot since only superuser may name one).
fn interpret_func_support(mcx: Mcx<'_>, defel: &DefElem<'_>) -> PgResult<Oid> {
    let proc_name = commands_define::defGetQualifiedName(mcx, defel)?;
    let arg_types = [types_core::INTERNALOID];
    let proc_oid = parse_func::LookupFuncName(proc_name, 1, &arg_types, false)?;
    if lsyscache::get_func_rettype(proc_oid)? != types_core::INTERNALOID {
        return Err(err(
            format!(
                "support function {} must return type internal",
                catalog_objectaddress::NameListToString(proc_name)
            ),
            types_error::ERRCODE_INVALID_OBJECT_DEFINITION,
        ));
    }
    if !superuser::superuser()? {
        return Err(err(
            "must be superuser to specify a support function".to_string(),
            ERRCODE_INSUFFICIENT_PRIVILEGE,
        ));
    }
    Ok(proc_oid)
}

// update_proconfig_value (functioncmds.c:660): None <=> C's NULL array.
fn update_proconfig_value(
    mut a: Option<Vec<String>>,
    set_items: &[&VariableSetStmt<'_>],
) -> PgResult<Option<Vec<String>>> {
    for sstmt in set_items {
        if sstmt.kind == VariableSetKind::VAR_RESET_ALL {
            a = None;
        } else {
            let name = sstmt.name.unwrap_or("");
            a = match guc_funcs::ExtractSetVariableArgs(sstmt)? {
                Some(value) => {
                    Some(guc::GUCArrayAdd(a.as_deref().unwrap_or(&[]), name, &value)?)
                }
                None => guc::GUCArrayDelete(a.as_deref().unwrap_or(&[]), name)?,
            };
        }
    }
    Ok(a)
}

fn set_item_of<'mcx>(defel: &DefElem<'mcx>) -> &'mcx VariableSetStmt<'mcx> {
    defel
        .arg
        .and_then(|n| n.as_variant::<VariableSetStmt>())
        .expect("SET option holds a VariableSetStmt")
}

// compute_function_attributes + compute_common_attribute (functioncmds.c).
fn compute_function_attributes<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateFunctionStmt<'mcx>,
    source_text: &str,
) -> PgResult<FunctionAttrs<'mcx>> {
    let mut as_item: Option<&'mcx DefElem<'mcx>> = None;
    let mut language_item: Option<&'mcx DefElem<'mcx>> = None;
    let mut volatility_item: Option<&'mcx DefElem<'mcx>> = None;
    let mut strict_item: Option<&'mcx DefElem<'mcx>> = None;
    let mut security_item: Option<&'mcx DefElem<'mcx>> = None;
    let mut leakproof_item: Option<&'mcx DefElem<'mcx>> = None;
    let mut cost_item: Option<&'mcx DefElem<'mcx>> = None;
    let mut rows_item: Option<&'mcx DefElem<'mcx>> = None;
    let mut support_item: Option<&'mcx DefElem<'mcx>> = None;
    let mut parallel_item: Option<&'mcx DefElem<'mcx>> = None;
    let mut windowfunc_item: Option<&'mcx DefElem<'mcx>> = None;
    let mut set_items: Vec<&VariableSetStmt<'_>> = Vec::new();

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
            return Err(invalid_procedure_attribute(source_text, defel.location));
        }
        // C appends SET items; multiple SET clauses never conflict.
        if name == "set" {
            set_items.push(set_item_of(defel));
            continue;
        }
        let slot: &mut Option<&'mcx DefElem<'mcx>> = match name {
            "as" => &mut as_item,
            "language" => &mut language_item,
            // unported: TRANSFORM option (pg_transform lane)
            "transform" => {
                return Err(err(
                    "CREATE FUNCTION ... TRANSFORM is not supported yet".to_string(),
                    types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                ))
            }
            "window" => &mut windowfunc_item,
            "volatility" => &mut volatility_item,
            "strict" => &mut strict_item,
            "security" => &mut security_item,
            "leakproof" => &mut leakproof_item,
            "cost" => &mut cost_item,
            "rows" => &mut rows_item,
            "support" => &mut support_item,
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

    let proconfig = if set_items.is_empty() {
        None
    } else {
        update_proconfig_value(None, &set_items)?
    };

    Ok(FunctionAttrs {
        as_clause: as_item,
        language: language_item.map(defel_str),
        windowfunc: windowfunc_item.map(defel_bool).unwrap_or(false),
        volatility: volatility_item.map_or(PROVOLATILE_VOLATILE, interpret_func_volatility),
        strict: strict_item.map(defel_bool).unwrap_or(false),
        security: security_item.map(defel_bool).unwrap_or(false),
        leakproof: leakproof_item.map(defel_bool).unwrap_or(false),
        proconfig,
        procost,
        prorows,
        support: match support_item {
            Some(d) => interpret_func_support(mcx, d)?,
            None => InvalidOid,
        },
        parallel: match parallel_item {
            Some(d) => interpret_func_parallel(d)?,
            None => PROPARALLEL_UNSAFE,
        },
    })
}

// LookupTypeName/typenameTypeId (parse_type.c) for function signatures:
// setof rides on the TypeName; shell types and decorated names are loud.
fn resolve_type_name<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &parser_small1::ParseState<'_, 'mcx>,
    tn: &TypeName<'_>,
    languageOid: Oid,
) -> PgResult<Oid> {
    let (typoid, typname) = resolve_type_oid(mcx, tn)?;
    if typoid == InvalidOid {
        return Err(err(
            format!("type \"{typname}\" does not exist"),
            ERRCODE_UNDEFINED_OBJECT,
        ));
    }
    shell_type_check(mcx, Some(pstate), tn, typoid, languageOid, false)?;
    check_defined_and_acl(typoid)?;
    Ok(typoid)
}

fn resolve_type_oid<'mcx, 'a>(mcx: Mcx<'mcx>, tn: &TypeName<'a>) -> PgResult<(Oid, &'a str)> {
    if tn.pct_type {
        return resolve_pct_type(mcx, tn);
    }
    if tn.typeOid != InvalidOid {
        unported("pre-resolved TypeName.typeOid");
    }

    // C DeconstructQualifiedName's default arm raises the improper-qualified-name
    // error itself for 0 or >3 parts; collect every part so it can.
    let names: Vec<&str> =
        tn.names.iter().map(|n| n.as_string().expect("TypeName names").sval).collect();
    let (schemaname, typname) = catalog_namespace::DeconstructQualifiedName(&names)?;

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
    // LookupTypeNameExtended (parse_type.c): an array reference yields the
    // array type of the base.
    let typoid = if typoid != InvalidOid && !tn.arrayBounds.is_nil() {
        lsyscache::get_array_type(typoid)?
    } else {
        typoid
    };
    // C typenameTypeId: the typmod is validated by typenameTypeMod, then
    // discarded — function signatures store bare type OIDs.
    if typoid != InvalidOid && !tn.typmods.is_nil() {
        parse_utilcmd::typenameTypeMod(mcx, None, tn, typoid)?;
    }
    Ok((typoid, typname))
}

// LookupTypeNameExtended's %TYPE arm (parse_type.c): the type of an existing
// relation column, plus the intentionally unpositioned conversion NOTICE.
fn resolve_pct_type<'mcx, 'a>(mcx: Mcx<'mcx>, tn: &TypeName<'a>) -> PgResult<(Oid, &'a str)> {
    let nnames = tn.names.len();
    let mut names: [&str; 4] = [""; 4];
    if (1..=4).contains(&nnames) {
        for (i, n) in tn.names.iter().enumerate() {
            names[i] = n.as_string().expect("TypeName names").sval;
        }
    }
    let (catalogname, schemaname, relname, field) = match nnames {
        2 => (None, None, names[0], names[1]),
        3 => (None, Some(names[0]), names[1], names[2]),
        4 => (Some(names[0]), Some(names[1]), names[2], names[3]),
        n => {
            let which = if n < 2 { "too few" } else { "too many" };
            let joined = tn
                .names
                .iter()
                .map(|x| x.as_string().expect("TypeName names").sval)
                .collect::<Vec<_>>()
                .join(".");
            return Err(err(
                format!("improper %TYPE reference ({which} dotted names): {joined}"),
                ERRCODE_SYNTAX_ERROR,
            ));
        }
    };
    let rv = rel_vocab::RangeVar {
        catalogname,
        schemaname,
        relname,
        inh: true,
        relpersistence: b'p',
        location: tn.location,
    };
    let relid = namespace_seams::range_var_get_relid::call(mcx, &rv, types_rel::NoLock, false)?;
    let attnum = lsyscache::get_attnum(relid, field)?;
    if attnum == 0 {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("column \"{field}\" of relation \"{relname}\" does not exist"),
            )
            .with_sqlstate(types_error::ERRCODE_UNDEFINED_COLUMN),
        ));
    }
    let typoid = lsyscache::get_atttype(relid, attnum)?;
    debug_assert!(tn.arrayBounds.is_nil());
    elog::ereport(types_error::NOTICE)
        .errmsg(format!(
            "type reference {} converted to {}",
            catalog_objectaddress::TypeNameToString(tn),
            format_type::format_type_be(typoid)?
        ))
        .finish(types_error::ErrorLocation::new(file!(), line!() as i32, "LookupTypeNameExtended"))?;
    Ok((typoid, field))
}

fn check_defined_and_acl(typoid: Oid) -> PgResult<()> {
    let aclresult = aclchk::object_aclcheck(
        TYPE_RELATION_ID,
        typoid,
        miscinit::GetUserId(),
        types_nodes::parsenodes::ACL_USAGE,
    )?;
    if aclresult != aclchk::ACLCHECK_OK {
        cast_transform::aclcheck_error_type(aclresult, typoid)?;
    }
    Ok(())
}

fn shell_type_check<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: Option<&parser_small1::ParseState<'_, 'mcx>>,
    tn: &TypeName<'_>,
    typoid: Oid,
    languageOid: Oid,
    is_return: bool,
) -> PgResult<()> {
    if syscache_seams::pg_type_isdefined::call(typoid)?.unwrap_or(false) {
        return Ok(());
    }
    let name = commands_define::TypeNameToString(mcx, tn)?;
    let pos = pstate.map_or(0, |ps| {
        parser_small1::parser_errposition(ps, tn.location, mbutils::GetDatabaseEncoding())
    });
    if languageOid == SQLlanguageId {
        let verb = if is_return { "return" } else { "accept" };
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("SQL function cannot {verb} shell type {}", name.as_str()),
            )
            .with_sqlstate(ERRCODE_INVALID_FUNCTION_DEFINITION)
            .with_cursor_position(pos),
        ));
    }
    let what = if is_return { "return type" } else { "argument type" };
    elog::ereport(types_error::NOTICE)
        .errcode(types_error::ERRCODE_WRONG_OBJECT_TYPE)
        .errmsg(format!("{what} {} is only a shell", name.as_str()))
        .errposition(pos)
        .finish(types_error::ErrorLocation::new(file!(), line!() as i32, "CreateFunction"))
}

// compute_return_type (functioncmds.c) incl. shell-type creation for
// internal/C-language I/O functions.
fn compute_return_type<'mcx>(
    mcx: Mcx<'mcx>,
    returnType: &TypeName<'_>,
    languageOid: Oid,
) -> PgResult<(Oid, bool)> {
    let (mut rettype, _typname) = resolve_type_oid(mcx, returnType)?;
    if rettype != InvalidOid {
        shell_type_check(mcx, None, returnType, rettype, languageOid, true)?;
    } else {
        let typnam = commands_define::TypeNameToString(mcx, returnType)?;
        // C: only C-coded functions can be I/O functions; anything else is a
        // typo, not a shell request.
        if languageOid != INTERNALlanguageId && languageOid != ClanguageId {
            return Err(err(
                format!("type \"{}\" does not exist", typnam.as_str()),
                ERRCODE_UNDEFINED_OBJECT,
            ));
        }
        if !returnType.typmods.is_nil() {
            return Err(err(
                format!(
                    "type modifier cannot be specified for shell type \"{}\"",
                    typnam.as_str()
                ),
                ERRCODE_SYNTAX_ERROR,
            ));
        }
        elog::ereport(types_error::NOTICE)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("type \"{}\" is not yet defined", typnam.as_str()))
            .errdetail("Creating a shell type definition.")
            .finish(types_error::ErrorLocation::new(file!(), line!() as i32, "CreateFunction"))?;
        let mut buf = [""; 4];
        let nnames = returnType.names.len();
        assert!((1..=3).contains(&nnames), "improper qualified name");
        for (i, n) in returnType.names.iter().enumerate() {
            buf[i] = n.as_string().expect("TypeName names").sval;
        }
        let (namespaceId, typname_last) =
            catalog_namespace::QualifiedNameGetCreationNamespace(mcx, &buf[..nnames])?;
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
            aclchk_seams::aclcheck_error::call(
                aclresult,
                ObjectType::OBJECT_SCHEMA as i32,
                &nspname,
            )?;
        }
        let address =
            pg_type::TypeShellMake(mcx, typname_last, namespaceId, miscinit::GetUserId())?;
        rettype = address.objectId;
    }
    check_defined_and_acl(rettype)?;
    Ok((rettype, returnType.setof))
}

pub struct ParameterList<'mcx> {
    pub in_types: mcx::PgVec<'mcx, Oid>,
    pub all_types: mcx::PgVec<'mcx, Oid>,
    pub param_modes: mcx::PgVec<'mcx, i8>,
    pub names: mcx::PgVec<'mcx, &'mcx str>,
    pub have_names: bool,
    pub have_out_or_variadic: bool,
    pub variadic_arg_type: Oid,
    pub required_result_type: Oid,
    pub parameter_defaults: types_nodes::NodeList<'mcx>,
}

// interpret_function_parameter_list (functioncmds.c); shared with
// aggregatecmds exactly as in C.
pub fn interpret_function_parameter_list<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut parser_small1::ParseState<'_, 'mcx>,
    parameters: &types_nodes::NodeList<'mcx>,
    languageOid: Oid,
    objtype: ObjectType,
) -> PgResult<ParameterList<'mcx>> {
    use FunctionParameterMode::*;
    let is_procedure = objtype == ObjectType::OBJECT_PROCEDURE;
    let n = parameters.len();
    let mut in_types: mcx::PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, n)?;
    let mut all_types: mcx::PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, n)?;
    let mut param_modes: mcx::PgVec<'mcx, i8> = mcx::vec_with_capacity_in(mcx, n)?;
    let mut names: mcx::PgVec<'mcx, &'mcx str> = mcx::vec_with_capacity_in(mcx, n)?;
    let mut have_names = false;
    let mut out_count = 0usize;
    let mut var_count = 0usize;
    let mut variadic_arg_type = InvalidOid;
    let mut required_result_type = InvalidOid;
    let mut parameter_defaults = types_nodes::NodeList::nil();
    let mut have_defaults = false;

    for p in parameters.iter() {
        let fp: &FunctionParameter<'mcx> = p
            .as_function_parameter()
            .expect("func_args_with_defaults holds FunctionParameters");
        let fpmode = match fp.mode {
            FUNC_PARAM_DEFAULT => FUNC_PARAM_IN,
            m => m,
        };
        let tn_node: Node<'mcx> = fp.argType.expect("FunctionParameter.argType");
        let tn = tn_node.as_variant::<TypeName>().expect("argType is a TypeName");
        let toid = resolve_type_name(mcx, pstate, tn, languageOid)?;
        if tn.setof {
            let msg = match objtype {
                ObjectType::OBJECT_AGGREGATE => "aggregates cannot accept set arguments",
                ObjectType::OBJECT_PROCEDURE => "procedures cannot accept set arguments",
                _ => "functions cannot accept set arguments",
            };
            return Err(err_at(
                pstate,
                fp.location,
                msg.to_string(),
                ERRCODE_INVALID_FUNCTION_DEFINITION,
            ));
        }

        if matches!(fpmode, FUNC_PARAM_IN | FUNC_PARAM_INOUT | FUNC_PARAM_VARIADIC) {
            if var_count > 0 {
                return Err(err_at(
                    pstate,
                    fp.location,
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
                    return Err(err_at(
                        pstate,
                        fp.location,
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
            variadic_arg_type = toid;
            var_count += 1;
            match toid {
                ANYARRAYOID | ANYCOMPATIBLEARRAYOID | ANYOID => {}
                _ => {
                    if lsyscache::get_element_type(toid)? == InvalidOid {
                        return Err(err_at(
                            pstate,
                            fp.location,
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
                    return Err(err_at(
                        pstate,
                        fp.location,
                        format!("parameter name \"{name}\" used more than once"),
                        ERRCODE_INVALID_FUNCTION_DEFINITION,
                    ));
                }
            }
            have_names = true;
        }
        names.push(name);

        // functioncmds.c:409-467: cook input-parameter defaults; later
        // input (and, for procedures, OUT) parameters must keep having them.
        let isinput = matches!(fpmode, FUNC_PARAM_IN | FUNC_PARAM_INOUT | FUNC_PARAM_VARIADIC);
        if let Some(defexpr) = fp.defexpr {
            if !isinput {
                return Err(Box::new(
                    (*err(
                        "only input parameters can have default values".to_string(),
                        ERRCODE_INVALID_FUNCTION_DEFINITION,
                    ))
                    .with_cursor_position(parser_small1::parser_errposition(
                        pstate,
                        fp.location,
                        mbutils::GetDatabaseEncoding(),
                    )),
                ));
            }
            let def = parse_expr::transformExpr(
                mcx,
                pstate,
                defexpr,
                parser_small1::ParseExprKind::EXPR_KIND_FUNCTION_DEFAULT,
            )?;
            let def = coerce::coerce_to_specific_type(
                mcx,
                pstate,
                def,
                parse_expr::expr_type(def),
                nodes_core::node_funcs::expr_location(def),
                toid,
                "DEFAULT",
            )?;
            parse_collate::assign_expr_collations(mcx, pstate, def)?;
            if !pstate.p_rtable.is_nil() || var_seams::contain_var_clause::call(def) {
                return Err(Box::new(
                    (*err(
                        "cannot use table references in parameter default value".to_string(),
                        types_error::ERRCODE_INVALID_COLUMN_REFERENCE,
                    ))
                    .with_cursor_position(parser_small1::parser_errposition(
                        pstate,
                        fp.location,
                        mbutils::GetDatabaseEncoding(),
                    )),
                ));
            }
            parameter_defaults.lappend(mcx, def)?;
            have_defaults = true;
        } else {
            if isinput && have_defaults {
                return Err(Box::new(
                    (*err(
                        "input parameters after one with a default value must also have defaults"
                            .to_string(),
                        ERRCODE_INVALID_FUNCTION_DEFINITION,
                    ))
                    .with_cursor_position(parser_small1::parser_errposition(
                        pstate,
                        fp.location,
                        mbutils::GetDatabaseEncoding(),
                    )),
                ));
            }
            if is_procedure && have_defaults {
                return Err(Box::new(
                    (*err(
                        "procedure OUT parameters cannot appear after one with a default value"
                            .to_string(),
                        ERRCODE_INVALID_FUNCTION_DEFINITION,
                    ))
                    .with_cursor_position(parser_small1::parser_errposition(
                        pstate,
                        fp.location,
                        mbutils::GetDatabaseEncoding(),
                    )),
                ));
            }
        }
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
        variadic_arg_type,
        required_result_type,
        parameter_defaults,
    })
}

struct AsClause<'mcx> {
    prosrc: &'mcx str,
    probin: Option<&'mcx str>,
    sql_body: Option<Node<'mcx>>,
}

// interpret_AS_clause (functioncmds.c:865-1020).
fn interpret_AS_clause<'mcx>(
    mcx: Mcx<'mcx>,
    languageOid: Oid,
    languageName: &str,
    funcname: &'mcx str,
    as_clause: Option<&'mcx DefElem<'mcx>>,
    sql_body_in: Option<Node<'mcx>>,
    parameterTypes: &[Oid],
    inParameterNames: &[&str],
    queryString: &str,
) -> PgResult<AsClause<'mcx>> {
    if sql_body_in.is_none() && as_clause.is_none() {
        return Err(err(
            "no function body specified".to_string(),
            ERRCODE_INVALID_FUNCTION_DEFINITION,
        ));
    }
    if sql_body_in.is_some() && as_clause.is_some() {
        return Err(err(
            "duplicate function body specified".to_string(),
            ERRCODE_INVALID_FUNCTION_DEFINITION,
        ));
    }
    if sql_body_in.is_some() && languageOid != SQLlanguageId {
        return Err(err(
            "inline SQL function body only valid for language SQL".to_string(),
            ERRCODE_INVALID_FUNCTION_DEFINITION,
        ));
    }
    if let Some(sql_body_in) = sql_body_in {
        return interpret_sql_body(
            mcx,
            funcname,
            sql_body_in,
            parameterTypes,
            inParameterNames,
            queryString,
        );
    }
    let as_item = as_clause.expect("checked above");
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
        return Ok(AsClause { prosrc, probin: Some(probin), sql_body: None });
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
    Ok(AsClause { prosrc, probin: None, sql_body: None })
}

// interpret_AS_clause sql_body branch (functioncmds.c:910-990): parse-analyze
// each statement under the SQL-function parameter hooks and hand back the
// querytree(s) for prosqlbody; prosrc becomes the empty string.
fn interpret_sql_body<'mcx>(
    mcx: Mcx<'mcx>,
    funcname: &'mcx str,
    sql_body_in: Node<'mcx>,
    parameterTypes: &[Oid],
    inParameterNames: &[&str],
    queryString: &str,
) -> PgResult<AsClause<'mcx>> {
    for &t in parameterTypes {
        if coerce::IsPolymorphicType(t) {
            return Err(err(
                "SQL function with unquoted function body cannot have polymorphic arguments"
                    .to_string(),
                ERRCODE_INVALID_FUNCTION_DEFINITION,
            ));
        }
    }
    // C indexes the all-parameter name list by input-parameter position.
    let argnames = &inParameterNames[..parameterTypes.len()];

    let mut transform = |stmt: Node<'mcx>| -> PgResult<types_nodes::parsenodes::Query<'mcx>> {
        let q = analyze_seams::transform_stmt_sql_fn::call(
            mcx,
            stmt,
            queryString,
            funcname,
            parameterTypes,
            argnames,
        )?;
        if q.commandType == types_nodes::nodes_enums::CmdType::CMD_UTILITY {
            let tag = utility_seams::create_command_tag::call(
                q.utilityStmt.expect("CMD_UTILITY Query has utilityStmt"),
            );
            return Err(err(
                format!(
                    "{} is not yet supported in unquoted SQL function body",
                    cmdtag::GetCommandTagName(tag)
                ),
                types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
            ));
        }
        Ok(q)
    };

    let sql_body = if let Some(outer) = sql_body_in.as_list() {
        // BEGIN ATOMIC: a single-item list wrapping the statement list.
        let stmts = outer.nth(0).as_list().expect("routine body wraps a stmt List");
        let mut transformed = types_nodes::NodeList::nil();
        for stmt in stmts.iter() {
            transformed.lappend(mcx, Node::mk(mcx, transform(stmt)?)?)?;
        }
        let inner = Node::mk_list(mcx, transformed)?;
        Node::mk_list(mcx, types_nodes::NodeList::make1(mcx, inner)?)?
    } else {
        Node::mk(mcx, transform(sql_body_in)?)?
    };

    Ok(AsClause { prosrc: "", probin: None, sql_body: Some(sql_body) })
}

// QualifiedNameGetCreationNamespace (namespace.c) via the RangeVar walk.
fn qualified_name_get_creation_namespace<'mcx>(
    mcx: Mcx<'mcx>,
    funcname: &types_nodes::NodeList<'mcx>,
) -> PgResult<(Oid, &'mcx str)> {
    // C DeconstructQualifiedName's default arm raises the improper-qualified-name
    // error itself for 0 or >3 parts; collect every part so it can.
    let names: Vec<&str> =
        funcname.iter().map(|n| n.as_string().expect("func_name holds Strings").sval).collect();
    let (schemaname, objname) = catalog_namespace::DeconstructQualifiedName(&names)?;
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
    pstate: &mut parser_small1::ParseState<'_, 'mcx>,
    stmt: &CreateFunctionStmt<'mcx>,
    source_text: &str,
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

    let attrs = compute_function_attributes(mcx, stmt, source_text)?;

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
        // unported: languages beyond sql, internal, c and plpgsql
        return Err(err(
            format!("language \"{language}\" is not supported yet"),
            types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
        ));
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

    let objtype = if stmt.is_procedure {
        ObjectType::OBJECT_PROCEDURE
    } else {
        ObjectType::OBJECT_FUNCTION
    };
    let params =
        interpret_function_parameter_list(mcx, pstate, &stmt.parameters, languageOid, objtype)?;

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

    let as_parsed = interpret_AS_clause(
        mcx,
        languageOid,
        language,
        funcname,
        attrs.as_clause,
        stmt.sql_body,
        &params.in_types,
        &params.names,
        source_text,
    )?;

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

    // pg_proc.proargdefaults stores the nodeToString image of the cooked
    // defaults List (functioncmds.c passes the List; serialization happens
    // here because pg_proc sits below outfuncs).
    let argdefaults_str = if params.parameter_defaults.is_nil() {
        None
    } else {
        Some(outfuncs::nodeToString(
            mcx,
            types_nodes::Node::mk_list(mcx, params.parameter_defaults.clone_in(mcx)?)?,
        )?)
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
            prosqlbody: as_parsed.sql_body,
            prokind: if stmt.is_procedure {
                PROKIND_PROCEDURE
            } else if attrs.windowfunc {
                PROKIND_WINDOW
            } else {
                PROKIND_FUNCTION
            },
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
            proconfig: attrs.proconfig.as_deref(),
            procost,
            prorows,
            prosupport: attrs.support,
            parameterDefaults: argdefaults_str.as_deref(),
            numDefaults: params.parameter_defaults.len() as i16,
        },
    )
}

// proconfig text[] image -> owned "name=value" entries (pg_db_role_setting
// setconfig_entries precedent).
fn proconfig_entries(mcx: Mcx<'_>, d: datum::Datum) -> PgResult<Vec<String>> {
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null varlena attr datum addresses in-tuple bytes; the
    // length is read from its own header before slicing.
    let raw = unsafe {
        let len = types_tuple::varatt::varsize_any(p);
        core::slice::from_raw_parts(p, len)
    };
    let image = detoast_seams::detoast_attr::call(mcx, raw)?;
    let elems = datum::array_build::deconstruct_array_image(mcx, &image, -1, false, b'i')?;
    let mut out = Vec::with_capacity(elems.len());
    for e in elems.iter() {
        let ep = e.as_usize() as *const u8;
        // SAFETY: by-ref text element datum inside the detoasted image.
        let text =
            unsafe { core::slice::from_raw_parts(ep, types_tuple::varatt::varsize_any(ep)) };
        let payload = varlena::open_image(mcx, text)?;
        out.push(String::from_utf8_lossy(payload.as_bytes()).into_owned());
    }
    Ok(out)
}

fn entries_to_text_array<'mcx>(
    mcx: Mcx<'mcx>,
    entries: &[String],
) -> PgResult<mcx::PgVec<'mcx, u8>> {
    let mut texts = Vec::with_capacity(entries.len());
    let mut elems: mcx::PgVec<'mcx, datum::Datum> = mcx::vec_with_capacity_in(mcx, entries.len())?;
    for e in entries {
        texts.push(varlena::cstring_to_text(mcx, e.as_bytes())?);
    }
    for t in texts.iter() {
        elems.push(datum::Datum::from_usize(t.as_bytes().as_ptr() as usize));
    }
    datum::array_build::construct_array_image(mcx, &elems, types_core::TEXTOID, -1, false, b'i')
}

// AlterFunction (functioncmds.c:1361). SUPPORT stays loud
// (interpret_func_support + dependency swap).
pub fn AlterFunction<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterFunctionStmt<'mcx>,
    source_text: &str,
) -> PgResult<ObjectAddress> {
    use pg_proc::{
        Anum_pg_proc_procost, Anum_pg_proc_proconfig, Anum_pg_proc_proisstrict,
        Anum_pg_proc_prokind, Anum_pg_proc_proleakproof, Anum_pg_proc_proparallel,
        Anum_pg_proc_proretset, Anum_pg_proc_prorows, Anum_pg_proc_prosecdef,
        Anum_pg_proc_prosupport, Anum_pg_proc_provolatile, Natts_pg_proc, PROKIND_AGGREGATE,
    };

    let func = stmt.func.expect("AlterFunctionStmt.func");
    let rel = table::table_open(mcx, PROCEDURE_RELATION_ID, types_rel::RowExclusiveLock)?;
    let funcOid = parse_func::LookupFuncWithArgs(stmt.objtype, func, false)?;
    let address = ObjectAddress::set(PROCEDURE_RELATION_ID, funcOid);

    let tup = cache_syscache::SearchSysCacheCopy(
        mcx,
        cache_syscache::cacheinfo::PROCOID,
        cache_syscache::SysCacheKey::Value(datum::Datum::from_oid(funcOid)),
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
    )?
    .unwrap_or_else(|| panic!("cache lookup failed for function {funcOid}"));
    let t = tup.as_tuple();
    let desc = rel.descr();
    let getattr = |attnum: usize| -> (datum::Datum, bool) {
        let mut isnull = false;
        // SAFETY: attnum is a valid pg_proc column under the relation's
        // descriptor; `t` is a heap-copied tuple owned by this call.
        let d = unsafe { types_tuple::heap_getattr(t, attnum as i32, desc, &mut isnull) };
        (d, isnull)
    };

    if !aclchk::object_ownercheck(PROCEDURE_RELATION_ID, funcOid, miscinit::GetUserId())? {
        aclchk::aclcheck_error(
            aclchk::ACLCHECK_NOT_OWNER,
            stmt.objtype,
            &catalog_objectaddress::NameListToString(&func.objname),
        )?;
    }

    let prokind = getattr(Anum_pg_proc_prokind).0.as_i8();
    if prokind == PROKIND_AGGREGATE {
        return Err(err(
            format!(
                "\"{}\" is an aggregate function",
                catalog_objectaddress::NameListToString(&func.objname)
            ),
            types_error::ERRCODE_WRONG_OBJECT_TYPE,
        ));
    }
    let is_procedure = prokind == PROKIND_PROCEDURE;

    let mut volatility_item: Option<&DefElem<'mcx>> = None;
    let mut strict_item: Option<&DefElem<'mcx>> = None;
    let mut security_def_item: Option<&DefElem<'mcx>> = None;
    let mut leakproof_item: Option<&DefElem<'mcx>> = None;
    let mut cost_item: Option<&DefElem<'mcx>> = None;
    let mut rows_item: Option<&DefElem<'mcx>> = None;
    let mut support_item: Option<&DefElem<'mcx>> = None;
    let mut parallel_item: Option<&DefElem<'mcx>> = None;
    let mut set_items: Vec<&VariableSetStmt<'_>> = Vec::new();

    for action in stmt.actions.iter() {
        let defel = action.as_def_elem().expect("alterfunc_opt_list holds DefElems");
        let name = defel.defname.unwrap_or("");
        // compute_common_attribute rejects these before the conflict check.
        if is_procedure
            && matches!(
                name,
                "volatility" | "strict" | "leakproof" | "cost" | "rows" | "support" | "parallel"
            )
        {
            return Err(invalid_procedure_attribute(source_text, defel.location));
        }
        if name == "set" {
            set_items.push(set_item_of(defel));
            continue;
        }
        let slot: &mut Option<&DefElem<'mcx>> = match name {
            "volatility" => &mut volatility_item,
            "strict" => &mut strict_item,
            "security" => &mut security_def_item,
            "leakproof" => &mut leakproof_item,
            "cost" => &mut cost_item,
            "rows" => &mut rows_item,
            "support" => &mut support_item,
            "parallel" => &mut parallel_item,
            other => panic!("option \"{other}\" not recognized"),
        };
        if slot.is_some() {
            return Err(conflicting_options());
        }
        *slot = Some(defel);
    }

    let mut values = [datum::Datum::null(); Natts_pg_proc];
    let mut repl_null = [false; Natts_pg_proc];
    let mut repl_repl = [false; Natts_pg_proc];
    let set = |values: &mut [datum::Datum],
               repl_repl: &mut [bool],
               attnum: usize,
               d: datum::Datum| {
        values[attnum - 1] = d;
        repl_repl[attnum - 1] = true;
    };

    if let Some(d) = volatility_item {
        set(
            &mut values,
            &mut repl_repl,
            Anum_pg_proc_provolatile,
            datum::Datum::from_char(interpret_func_volatility(d)),
        );
    }
    if let Some(d) = strict_item {
        set(
            &mut values,
            &mut repl_repl,
            Anum_pg_proc_proisstrict,
            datum::Datum::from_bool(defel_bool(d)),
        );
    }
    if let Some(d) = security_def_item {
        set(
            &mut values,
            &mut repl_repl,
            Anum_pg_proc_prosecdef,
            datum::Datum::from_bool(defel_bool(d)),
        );
    }
    if let Some(d) = leakproof_item {
        let v = defel_bool(d);
        if v && !superuser::superuser()? {
            return Err(err(
                "only superuser can define a leakproof function".to_string(),
                ERRCODE_INSUFFICIENT_PRIVILEGE,
            ));
        }
        set(
            &mut values,
            &mut repl_repl,
            Anum_pg_proc_proleakproof,
            datum::Datum::from_bool(v),
        );
    }
    if let Some(d) = cost_item {
        let v = defel_numeric(d)?;
        if v <= 0.0 {
            return Err(err(
                "COST must be positive".to_string(),
                ERRCODE_INVALID_PARAMETER_VALUE,
            ));
        }
        set(&mut values, &mut repl_repl, Anum_pg_proc_procost, datum::Datum::from_f32(v));
    }
    if let Some(d) = rows_item {
        let v = defel_numeric(d)?;
        if v <= 0.0 {
            return Err(err(
                "ROWS must be positive".to_string(),
                ERRCODE_INVALID_PARAMETER_VALUE,
            ));
        }
        if !getattr(Anum_pg_proc_proretset).0.as_bool() {
            return Err(err(
                "ROWS is not applicable when function does not return a set".to_string(),
                ERRCODE_INVALID_PARAMETER_VALUE,
            ));
        }
        set(&mut values, &mut repl_repl, Anum_pg_proc_prorows, datum::Datum::from_f32(v));
    }
    if let Some(d) = support_item {
        // interpret_func_support handles the privilege check.
        let newsupport = interpret_func_support(mcx, d)?;
        let old_support = getattr(Anum_pg_proc_prosupport).0.as_oid();
        if types_core::OidIsValid(old_support) {
            if pg_depend::changeDependencyFor(
                mcx,
                PROCEDURE_RELATION_ID,
                funcOid,
                PROCEDURE_RELATION_ID,
                old_support,
                newsupport,
            )? != 1
            {
                panic!("could not change support dependency for function {funcOid}");
            }
        } else {
            pg_depend::recordDependencyOn(
                mcx,
                &address,
                &pg_proc::ObjectAddress::set(PROCEDURE_RELATION_ID, newsupport),
                pg_depend::DependencyType::Normal,
            )?;
        }
        set(
            &mut values,
            &mut repl_repl,
            Anum_pg_proc_prosupport,
            datum::Datum::from_oid(newsupport),
        );
    }
    if let Some(d) = parallel_item {
        set(
            &mut values,
            &mut repl_repl,
            Anum_pg_proc_proparallel,
            datum::Datum::from_char(interpret_func_parallel(d)?),
        );
    }
    let proconfig_image;
    if !set_items.is_empty() {
        let (d, isnull) = getattr(Anum_pg_proc_proconfig);
        let old = if isnull { None } else { Some(proconfig_entries(mcx, d)?) };
        let new = update_proconfig_value(old, &set_items)?;
        repl_repl[Anum_pg_proc_proconfig - 1] = true;
        match new {
            Some(entries) => {
                proconfig_image = entries_to_text_array(mcx, &entries)?;
                values[Anum_pg_proc_proconfig - 1] =
                    datum::Datum::from_usize(proconfig_image.as_ptr() as usize);
            }
            None => repl_null[Anum_pg_proc_proconfig - 1] = true,
        }
    }

    let mut newtup = heaptuple::heap_modify_tuple(mcx, t, desc, &values, &repl_null, &repl_repl)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &rel, &t.t_self, &mut newtup)?;

    // C: InvokeObjectPostAlterHook — object_access_hook surface is absent by
    // design in this port.
    rel.close(types_rel::NoLock)?;

    Ok(address)
}

// Guts of function deletion (functioncmds.c RemoveFunctionById).
pub fn RemoveFunctionById<'mcx>(mcx: Mcx<'mcx>, funcOid: Oid) -> PgResult<()> {
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
        types_tuple::heap_getattr(
            tup,
            pg_proc::Anum_pg_proc_prokind as i32,
            relation.descr(),
            &mut isnull,
        )
    }
    .as_i8();
    catalog_indexing::CatalogTupleDelete(&relation, &tid)?;
    genam::systable_endscan(mcx, scan)?;
    relation.close(types_rel::RowExclusiveLock)?;
    pgstat::function::pgstat_drop_function(funcOid);
    if prokind == PROKIND_AGGREGATE {
        let aggrel = table::table_open(
            mcx,
            types_core::AGGREGATE_RELATION_ID,
            types_rel::RowExclusiveLock,
        )?;
        let mut key = types_scan::scankey::ScanKeyData::empty();
        key.sk_attno = 1;
        key.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
        key.sk_collation = 0;
        key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
            .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
        key.sk_argument = datum::Datum::from_oid(funcOid);
        let mut scan = genam::systable_beginscan(
            mcx,
            &aggrel,
            types_core::AGGREGATE_FNOID_INDEX_ID,
            true,
            None,
            &[key],
        )?;
        let tup = genam::systable_getnext(mcx, &mut scan)?.unwrap_or_else(|| {
            panic!("cache lookup failed for pg_aggregate tuple for function {funcOid}")
        });
        let tid = tup.t_self;
        catalog_indexing::CatalogTupleDelete(&aggrel, &tid)?;
        genam::systable_endscan(mcx, scan)?;
        aggrel.close(types_rel::RowExclusiveLock)?;
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

#[track_caller]
#[cold]
#[inline(never)]
fn func_lookup_failed(funcid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!("cache lookup failed for function {funcid}")))
}

#[track_caller]
#[cold]
#[inline(never)]
fn proc_lookup_failed(funcid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!("cache lookup failed for procedure {funcid}")))
}

pub fn ExecuteCallStmt<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CallStmt<'mcx>,
    params: ParamListHandle,
    atomic: bool,
    dest: &mut tcop_dest::DestReceiver<'mcx>,
) -> PgResult<()> {
    let fexpr = stmt.funcexpr.expect("CALL: analyzed CallStmt holds a FuncExpr");

    let aclresult = aclchk::object_aclcheck(
        PROCEDURE_RELATION_ID,
        fexpr.funcid,
        miscinit::GetUserId(),
        ACL_EXECUTE,
    )?;
    if aclresult != aclchk::ACLCHECK_OK {
        let funcname = lsyscache::get_func_name(mcx, fexpr.funcid)?;
        aclchk_seams::aclcheck_error::call(
            aclresult,
            ObjectType::OBJECT_PROCEDURE as i32,
            funcname.as_ref().map_or("", |s| s.as_str()),
        )?;
    }

    let shape = syscache_seams::lookup_pg_proc_shape::call(fexpr.funcid)?
        .ok_or_else(|| func_lookup_failed(fexpr.funcid))?;
    // C: proconfig or SECURITY DEFINER forbid transaction control inside.
    let mut callcontext =
        types_fmgr::CallContext::new(atomic || !shape.proconfig_isnull || shape.prosecdef);

    let nargs = fexpr.args.len();
    if nargs > FUNC_MAX_ARGS {
        return Err(err(
            format!("cannot pass more than {FUNC_MAX_ARGS} arguments to a procedure"),
            ERRCODE_TOO_MANY_ARGUMENTS,
        ));
    }

    // InvokeFunctionExecuteHook: no hook surface exists (repo-wide).
    let mut flinfo = fmgr_seams::fmgr_info::call(fexpr.funcid)?;
    // C fmgr_info_set_expr(fexpr): sql_functions resolves RECORD result
    // shapes through fn_expr.
    let fexpr_node = types_nodes::Node::mk(
        mcx,
        types_nodes::primnodes::FuncExpr {
            funcid: fexpr.funcid,
            funcresulttype: fexpr.funcresulttype,
            funcretset: fexpr.funcretset,
            funcvariadic: fexpr.funcvariadic,
            funcformat: fexpr.funcformat,
            funccollid: fexpr.funccollid,
            inputcollid: fexpr.inputcollid,
            args: fexpr.args.clone_in(mcx)?,
            location: fexpr.location,
        },
    )?;
    flinfo.fn_expr = Some(execexpr::erase_fn_expr(mcx, fexpr_node)?);
    let mut fcinfo = types_fmgr::LocalFcinfo::<FUNC_MAX_ARGS>::fresh(fexpr.inputcollid);
    fcinfo.init(nargs as i16, fexpr.inputcollid, callcontext.fm_node_ptr(), None);
    // SAFETY: mcx is the portal context; it outlives the call and its result reads.
    unsafe { fcinfo.set_result_mcx(mcx) };

    let extern_params = if params.is_null() {
        None
    } else {
        // SAFETY: the portal that registered the handle outlives this utility call.
        Some(unsafe { types_portal::params::resolve(params) })
    };
    let bind = execexpr::ParamBind { extern_params, exec_vals: None, n_exec: 0 };

    if !atomic {
        let snap = snapmgr::GetTransactionSnapshot()?;
        snapmgr::PushActiveSnapshot(&snap)?;
    }

    let mut slots = execexpr::EvalSlots::default();
    for (i, arg) in fexpr.args.iter().enumerate() {
        let mut state = execexpr::exec_init_expr(mcx, Some(arg), bind)?.expect("arg is Some");
        state.arm_result_mcx(mcx);
        fcinfo.args[i] = execexpr::exec_eval_expr(&mut state, &mut slots)?;
    }

    if !atomic {
        snapmgr::PopActiveSnapshot()?;
    }

    // C: pgstat_init_function_usage's `pgstat_track_functions <= fn_stats`
    // early-out, hoisted to the caller as the crate's API requires.
    let fcu = if flinfo.fn_stats < types_fmgr::TRACK_FUNC_ALL
        && pgstat::function::pgstat_track_functions() > flinfo.fn_stats as i32
    {
        Some(pgstat::function::pgstat_init_function_usage(flinfo.fn_oid)?)
    } else {
        None
    };
    let retval = flinfo.invoke(&mut fcinfo)?;
    if let Some(fcu) = &fcu {
        pgstat::function::pgstat_end_function_usage(fcu, true);
    }

    if fexpr.funcresulttype == VOIDOID {
    } else if fexpr.funcresulttype == RECORDOID {
        if fcinfo.isnull {
            return Err(Box::new(PgError::error(
                "procedure returned null record".to_string(),
            )));
        }

        pquery_seams::ensure_portal_snapshot_exists::call()?;

        let p = retval.as_usize() as *const u8;
        // SAFETY: non-null record datum — a live varlena-headed composite image.
        let total = unsafe { types_tuple::varatt::varsize_any(p) };
        // SAFETY: `total` readable bytes at p, per the datum contract.
        let raw = unsafe { core::slice::from_raw_parts(p, total) };
        let rec = detoast_seams::detoast_attr::call(mcx, raw)?;
        // SAFETY: detoasted composite image; header prefix is in bounds.
        let hdr = unsafe { &*(rec.as_ptr() as *const types_tuple::HeapTupleHeaderData) };
        let retdesc =
            typcache_seams::lookup_rowtype_tupdesc_copy::call(mcx, hdr.type_id(), hdr.typmod())?;
        // SAFETY: MAXALIGN'd detoasted image of datum_length() == rec.len() bytes.
        let tuple = unsafe {
            types_tuple::HeapTupleData::from_raw_parts(
                rec.as_ptr(),
                hdr.datum_length(),
                types_tuple::ItemPointerData::invalid(),
                InvalidOid,
            )
        };
        let natts = retdesc.natts as usize;
        let mut values = mcx::vec_from_elem_in(mcx, datum::Datum::null(), natts);
        let mut nulls = mcx::vec_from_elem_in(mcx, true, natts);
        types_tuple::heap_deform_tuple(&tuple, &retdesc, &mut values, &mut nulls);

        let mut tstate =
            exectuples_output::begin_tup_output_tupdesc(mcx, dest, std::rc::Rc::new(retdesc))?;
        exectuples_output::do_tup_output(&mut tstate, mcx, &values, &nulls)?;
        exectuples_output::end_tup_output(tstate)?;
    } else {
        return Err(Box::new(PgError::error(format!(
            "unexpected result type for procedure: {}",
            fexpr.funcresulttype
        ))));
    }

    Ok(())
}

pub fn CallStmtResultDesc<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CallStmt<'_>,
) -> PgResult<Option<types_tuple::TupleDescData<'mcx>>> {
    let fexpr = stmt.funcexpr.expect("CALL: analyzed CallStmt holds a FuncExpr");

    let shape = syscache_seams::lookup_pg_proc_shape::call(fexpr.funcid)?
        .ok_or_else(|| proc_lookup_failed(fexpr.funcid))?;
    let Some(mut desc) = funcapi::build_function_result_tupdesc_t(mcx, fexpr.funcid, &shape)?
    else {
        return Ok(None);
    };

    // C: keep the declared column names but take each type from the
    // transformed outarg (polymorphic cases); typmod -1, default collation.
    debug_assert_eq!(desc.natts as usize, stmt.outargs.len());
    for (i, outarg) in stmt.outargs.iter().enumerate() {
        let name = core::str::from_utf8(desc.attrs[i].attname.name_str())
            .expect("attname is UTF-8")
            .to_string();
        tupdesc::TupleDescInitEntry(
            &mut desc,
            (i + 1) as AttrNumber,
            Some(&name),
            nodes_core::node_funcs::expr_type(outarg),
            -1,
            0,
        )?;
    }
    Ok(Some(desc))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mcx::MemoryContext;

    fn return_stmt<'mcx>(mcx: Mcx<'mcx>) -> Node<'mcx> {
        Node::build::<types_nodes::parsenodes::ReturnStmt>(mcx).unwrap().seal()
    }

    fn as_defel<'mcx>(mcx: Mcx<'mcx>) -> &'mcx DefElem<'mcx> {
        let mut d = Node::build::<DefElem>(mcx).unwrap();
        d.defname = Some("as");
        let sealed = d.seal();
        sealed.as_variant::<DefElem>().unwrap()
    }

    #[test]
    fn interpret_as_clause_body_checks_match_c_order() {
        let cx = MemoryContext::new("interpret_AS_clause test");
        let mcx = cx.mcx();

        // functioncmds.c:873-876
        let e = interpret_AS_clause(mcx, SQLlanguageId, "sql", "f", None, None, &[], &[], "")
            .err()
            .unwrap();
        assert!(e.to_string().contains("no function body specified"));

        // functioncmds.c:878-881
        let e = interpret_AS_clause(
            mcx,
            SQLlanguageId,
            "sql",
            "f",
            Some(as_defel(mcx)),
            Some(return_stmt(mcx)),
            &[],
            &[],
            "",
        )
        .err()
        .unwrap();
        assert!(e.to_string().contains("duplicate function body specified"));

        // functioncmds.c:883-886
        let e = interpret_AS_clause(
            mcx,
            ClanguageId,
            "c",
            "f",
            None,
            Some(return_stmt(mcx)),
            &[],
            &[],
            "",
        )
        .err()
        .unwrap();
        assert!(e.to_string().contains("inline SQL function body only valid for language SQL"));
    }

    #[test]
    fn sql_body_rejects_polymorphic_arguments() {
        let cx = MemoryContext::new("interpret_AS_clause polymorphic test");
        let mcx = cx.mcx();

        // functioncmds.c:926-929
        let e = interpret_AS_clause(
            mcx,
            SQLlanguageId,
            "sql",
            "f",
            None,
            Some(return_stmt(mcx)),
            &[types_core::catalog::ANYELEMENTOID],
            &["a"],
            "",
        )
        .err()
        .unwrap();
        assert!(e.to_string().contains(
            "SQL function with unquoted function body cannot have polymorphic arguments"
        ));
    }
}
