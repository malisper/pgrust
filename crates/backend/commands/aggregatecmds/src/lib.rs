//! aggregatecmds.c — DefineAggregate (CREATE [OR REPLACE] AGGREGATE).
#![allow(non_snake_case, non_upper_case_globals)]

use elog::ereport;
use mcx::Mcx;
use pg_aggregate::{
    AggregateCreate, AggregateCreateArgs, AGGKIND_HYPOTHETICAL, AGGKIND_NORMAL,
    AGGKIND_ORDERED_SET, AGGMODIFY_READ_ONLY, AGGMODIFY_READ_WRITE, AGGMODIFY_SHAREABLE,
};
use pg_depend::ObjectAddress;
use pg_proc::{PROPARALLEL_RESTRICTED, PROPARALLEL_SAFE, PROPARALLEL_UNSAFE};
use types_core::{
    InvalidOid, Oid, ANYARRAYOID, ANYCOMPATIBLEARRAYOID, ANYCOMPATIBLEMULTIRANGEOID,
    ANYCOMPATIBLENONARRAYOID, ANYCOMPATIBLEOID, ANYCOMPATIBLERANGEOID, ANYELEMENTOID, ANYENUMOID,
    ANYMULTIRANGEOID, ANYNONARRAYOID, ANYRANGEOID, INTERNALOID, NAMESPACE_RELATION_ID,
};
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_INVALID_FUNCTION_DEFINITION, ERRCODE_SYNTAX_ERROR,
    WARNING,
};
use types_nodes::parsenodes::{DefElem, ObjectType};
use types_nodes::rawnodes::TypeName;
use types_nodes::NodeList;

const TYPTYPE_PSEUDO: i8 = b'p' as i8;

#[track_caller]
#[cold]
fn err(sqlstate: types_error::SqlState, msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(sqlstate))
}

#[track_caller]
#[cold]
fn def_err(msg: &str) -> Box<PgError> {
    err(ERRCODE_INVALID_FUNCTION_DEFINITION, msg.to_string())
}

fn IsPolymorphicType(typid: Oid) -> bool {
    matches!(
        typid,
        ANYELEMENTOID
            | ANYARRAYOID
            | ANYNONARRAYOID
            | ANYENUMOID
            | ANYRANGEOID
            | ANYMULTIRANGEOID
            | ANYCOMPATIBLEOID
            | ANYCOMPATIBLEARRAYOID
            | ANYCOMPATIBLENONARRAYOID
            | ANYCOMPATIBLERANGEOID
            | ANYCOMPATIBLEMULTIRANGEOID
    )
}

// DefineAggregate (aggregatecmds.c). "oldstyle" is the pre-8.2 form where the
// input type comes from a BASETYPE element; otherwise args is a pair whose
// first element is the FunctionParameter list and whose second is an Integer
// with the number of direct args (-1 unless ordered-set).
#[allow(clippy::too_many_arguments)]
pub fn DefineAggregate<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut parser_small1::ParseState<'_, 'mcx>,
    name: &NodeList<'mcx>,
    args: &NodeList<'mcx>,
    oldstyle: bool,
    parameters: &NodeList<'mcx>,
    replace: bool,
) -> PgResult<ObjectAddress> {
    let mut aggKind = AGGKIND_NORMAL;
    let mut transfuncName: Option<&NodeList<'mcx>> = None;
    let mut finalfuncName: Option<&NodeList<'mcx>> = None;
    let mut combinefuncName: Option<&NodeList<'mcx>> = None;
    let mut serialfuncName: Option<&NodeList<'mcx>> = None;
    let mut deserialfuncName: Option<&NodeList<'mcx>> = None;
    let mut mtransfuncName: Option<&NodeList<'mcx>> = None;
    let mut minvtransfuncName: Option<&NodeList<'mcx>> = None;
    let mut mfinalfuncName: Option<&NodeList<'mcx>> = None;
    let mut finalfuncExtraArgs = false;
    let mut mfinalfuncExtraArgs = false;
    let mut finalfuncModify: i8 = 0;
    let mut mfinalfuncModify: i8 = 0;
    let mut sortoperatorName: Option<&NodeList<'mcx>> = None;
    let mut baseType: Option<&TypeName<'mcx>> = None;
    let mut transType: Option<&TypeName<'mcx>> = None;
    let mut mtransType: Option<&TypeName<'mcx>> = None;
    let mut transSpace: i32 = 0;
    let mut mtransSpace: i32 = 0;
    let mut initval: Option<&str> = None;
    let mut minitval: Option<&str> = None;
    let mut parallel: Option<&str> = None;
    let mut numDirectArgs: i32 = 0;
    let mut proparallel = PROPARALLEL_UNSAFE;

    let mut buf = [""; 4];
    let parts = name_parts(name, &mut buf);
    let (aggNamespace, aggName) = catalog_namespace::QualifiedNameGetCreationNamespace(mcx, parts)?;

    let aclresult = aclchk::object_aclcheck(
        NAMESPACE_RELATION_ID,
        aggNamespace,
        miscinit::GetUserId(),
        adt_acl::ACL_CREATE,
    )?;
    if aclresult != aclchk::ACLCHECK_OK {
        let nspname = lsyscache::get_namespace_name(mcx, aggNamespace)?;
        aclchk::aclcheck_error(
            aclresult,
            ObjectType::OBJECT_SCHEMA,
            nspname.as_ref().map(|s| s.as_str()).unwrap_or(""),
        )?;
    }

    let mut arg_list: Option<&NodeList<'mcx>> = None;
    if !oldstyle {
        debug_assert_eq!(args.len(), 2);
        numDirectArgs = args
            .nth(1)
            .as_integer()
            .expect("aggr_args pair carries an Integer second")
            .ival;
        if numDirectArgs >= 0 {
            aggKind = AGGKIND_ORDERED_SET;
        } else {
            numDirectArgs = 0;
        }
        arg_list =
            Some(args.nth(0).as_list().expect("aggr_args pair carries the arg list first"));
    }

    for n in parameters.iter() {
        let defel = n.as_def_elem().expect("CREATE AGGREGATE definition: DefElem list");
        // sfunc1/stype1/initcond1 are accepted as obsolete spellings.
        match defel.defname.unwrap_or("") {
            "sfunc" | "sfunc1" => {
                transfuncName = Some(commands_define::defGetQualifiedName(mcx, defel)?)
            }
            "finalfunc" => finalfuncName = Some(commands_define::defGetQualifiedName(mcx, defel)?),
            "combinefunc" => {
                combinefuncName = Some(commands_define::defGetQualifiedName(mcx, defel)?)
            }
            "serialfunc" => {
                serialfuncName = Some(commands_define::defGetQualifiedName(mcx, defel)?)
            }
            "deserialfunc" => {
                deserialfuncName = Some(commands_define::defGetQualifiedName(mcx, defel)?)
            }
            "msfunc" => mtransfuncName = Some(commands_define::defGetQualifiedName(mcx, defel)?),
            "minvfunc" => {
                minvtransfuncName = Some(commands_define::defGetQualifiedName(mcx, defel)?)
            }
            "mfinalfunc" => {
                mfinalfuncName = Some(commands_define::defGetQualifiedName(mcx, defel)?)
            }
            "finalfunc_extra" => finalfuncExtraArgs = commands_define::defGetBoolean(defel)?,
            "mfinalfunc_extra" => mfinalfuncExtraArgs = commands_define::defGetBoolean(defel)?,
            "finalfunc_modify" => finalfuncModify = extractModify(mcx, defel)?,
            "mfinalfunc_modify" => mfinalfuncModify = extractModify(mcx, defel)?,
            "sortop" => sortoperatorName = Some(commands_define::defGetQualifiedName(mcx, defel)?),
            "basetype" => baseType = Some(commands_define::defGetTypeName(mcx, defel)?),
            "hypothetical" => {
                if commands_define::defGetBoolean(defel)? {
                    if aggKind == AGGKIND_NORMAL {
                        return Err(def_err("only ordered-set aggregates can be hypothetical"));
                    }
                    aggKind = AGGKIND_HYPOTHETICAL;
                }
            }
            "stype" | "stype1" => transType = Some(commands_define::defGetTypeName(mcx, defel)?),
            "sspace" => transSpace = commands_define::defGetInt32(defel)?,
            "mstype" => mtransType = Some(commands_define::defGetTypeName(mcx, defel)?),
            "msspace" => mtransSpace = commands_define::defGetInt32(defel)?,
            "initcond" | "initcond1" => initval = Some(commands_define::defGetString(mcx, defel)?),
            "minitcond" => minitval = Some(commands_define::defGetString(mcx, defel)?),
            "parallel" => parallel = Some(commands_define::defGetString(mcx, defel)?),
            other => {
                ereport(WARNING)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(format!("aggregate attribute \"{other}\" not recognized"))
                    .finish(ErrorLocation::new(file!(), line!() as i32, "DefineAggregate"))?;
            }
        }
    }

    let Some(transType) = transType else {
        return Err(def_err("aggregate stype must be specified"));
    };
    let Some(transfuncName) = transfuncName else {
        return Err(def_err("aggregate sfunc must be specified"));
    };

    // mstype requires msfunc+minvfunc; without it no moving-agg option may appear.
    if mtransType.is_some() {
        if mtransfuncName.is_none() {
            return Err(def_err("aggregate msfunc must be specified when mstype is specified"));
        }
        if minvtransfuncName.is_none() {
            return Err(def_err("aggregate minvfunc must be specified when mstype is specified"));
        }
    } else {
        if mtransfuncName.is_some() {
            return Err(def_err("aggregate msfunc must not be specified without mstype"));
        }
        if minvtransfuncName.is_some() {
            return Err(def_err("aggregate minvfunc must not be specified without mstype"));
        }
        if mfinalfuncName.is_some() {
            return Err(def_err("aggregate mfinalfunc must not be specified without mstype"));
        }
        if mtransSpace != 0 {
            return Err(def_err("aggregate msspace must not be specified without mstype"));
        }
        if minitval.is_some() {
            return Err(def_err("aggregate minitcond must not be specified without mstype"));
        }
    }

    if finalfuncModify == 0 {
        finalfuncModify =
            if aggKind == AGGKIND_NORMAL { AGGMODIFY_READ_ONLY } else { AGGMODIFY_READ_WRITE };
    }
    if mfinalfuncModify == 0 {
        mfinalfuncModify =
            if aggKind == AGGKIND_NORMAL { AGGMODIFY_READ_ONLY } else { AGGMODIFY_READ_WRITE };
    }

    let mut oldstyle_types = [InvalidOid; 1];
    let parameter_types: &[Oid];
    let all_parameter_types: Option<&[Oid]>;
    let parameter_modes: Option<&[i8]>;
    let parameter_names: Option<&[&str]>;
    let variadicArgType: Oid;
    let interpreted;
    if oldstyle {
        // Old style: zero or one input; basetype ANY (case-insensitive, historically) means zero.
        let Some(baseType) = baseType else {
            return Err(def_err("aggregate input type must be specified"));
        };
        let basename = commands_define::TypeNameToString(mcx, baseType)?;
        if basename.as_str().eq_ignore_ascii_case("ANY") {
            parameter_types = &[];
        } else {
            oldstyle_types[0] = parse_utilcmd::LookupTypeNameOid(mcx, baseType)?;
            parameter_types = &oldstyle_types;
        }
        all_parameter_types = None;
        parameter_modes = None;
        parameter_names = None;
        variadicArgType = InvalidOid;
    } else {
        if baseType.is_some() {
            return Err(def_err("basetype is redundant with aggregate input type specification"));
        }
        interpreted = functioncmds::interpret_function_parameter_list(
            mcx,
            pstate,
            arg_list.expect("new-style aggr_args carries an arg list"),
            InvalidOid,
            ObjectType::OBJECT_AGGREGATE,
        )?;
        parameter_types = &interpreted.in_types;
        all_parameter_types = if interpreted.have_out_or_variadic {
            Some(&interpreted.all_types)
        } else {
            None
        };
        parameter_modes = if interpreted.have_out_or_variadic {
            Some(&interpreted.param_modes)
        } else {
            None
        };
        parameter_names = if interpreted.have_names { Some(&interpreted.names) } else { None };
        variadicArgType = interpreted.variadic_arg_type;
        // Parameter defaults and OUT params are grammar-rejected for aggregates.
        debug_assert_eq!(interpreted.required_result_type, InvalidOid);
    }

    // The transtype can't be a pseudo-type (values must be storable), except
    // polymorphic types (AggregateCreate checks) and superuser-only INTERNAL.
    let transTypeId = parse_utilcmd::LookupTypeNameOid(mcx, transType)?;
    let transTypeType = lsyscache::get_typtype(transTypeId)?;
    if transTypeType == TYPTYPE_PSEUDO && !IsPolymorphicType(transTypeId) {
        if transTypeId == INTERNALOID && superuser::superuser()? {
        } else {
            return Err(def_err(&format!(
                "aggregate transition data type cannot be {}",
                format_type::format_type_be(transTypeId)?
            )));
        }
    }

    if serialfuncName.is_some() && deserialfuncName.is_some() {
        // Serialization is only needed/allowed for transtype INTERNAL.
        if transTypeId != INTERNALOID {
            return Err(def_err(&format!(
                "serialization functions may be specified only when the aggregate transition data type is {}",
                format_type::format_type_be(INTERNALOID)?
            )));
        }
    } else if serialfuncName.is_some() || deserialfuncName.is_some() {
        return Err(def_err(
            "must specify both or neither of serialization and deserialization functions",
        ));
    }

    let mut mtransTypeId = InvalidOid;
    let mut mtransTypeType: i8 = 0;
    if let Some(mtransType) = mtransType {
        mtransTypeId = parse_utilcmd::LookupTypeNameOid(mcx, mtransType)?;
        mtransTypeType = lsyscache::get_typtype(mtransTypeId)?;
        if mtransTypeType == TYPTYPE_PSEUDO && !IsPolymorphicType(mtransTypeId) {
            if mtransTypeId == INTERNALOID && superuser::superuser()? {
            } else {
                return Err(def_err(&format!(
                    "aggregate transition data type cannot be {}",
                    format_type::format_type_be(mtransTypeId)?
                )));
            }
        }
    }

    // Initvals are stored as text; complain about bad values now, not at runtime.
    if let Some(initval) = initval {
        if transTypeType != TYPTYPE_PSEUDO {
            validate_initval(mcx, initval, transTypeId)?;
        }
    }
    if let Some(minitval) = minitval {
        if mtransTypeType != TYPTYPE_PSEUDO {
            validate_initval(mcx, minitval, mtransTypeId)?;
        }
    }

    if let Some(parallel) = parallel {
        proparallel = match parallel {
            "safe" => PROPARALLEL_SAFE,
            "restricted" => PROPARALLEL_RESTRICTED,
            "unsafe" => PROPARALLEL_UNSAFE,
            _ => {
                return Err(err(
                    ERRCODE_SYNTAX_ERROR,
                    "parameter \"parallel\" must be SAFE, RESTRICTED, or UNSAFE".to_string(),
                ));
            }
        };
    }

    AggregateCreate(
        mcx,
        &AggregateCreateArgs {
            agg_name: aggName,
            agg_namespace: aggNamespace,
            replace,
            agg_kind: aggKind,
            num_direct_args: numDirectArgs,
            parameter_types,
            all_parameter_types,
            parameter_modes,
            parameter_names,
            variadic_arg_type: variadicArgType,
            transfn_name: transfuncName,
            finalfn_name: finalfuncName,
            combinefn_name: combinefuncName,
            serialfn_name: serialfuncName,
            deserialfn_name: deserialfuncName,
            mtransfn_name: mtransfuncName,
            minvtransfn_name: minvtransfuncName,
            mfinalfn_name: mfinalfuncName,
            finalfn_extra_args: finalfuncExtraArgs,
            mfinalfn_extra_args: mfinalfuncExtraArgs,
            finalfn_modify: finalfuncModify,
            mfinalfn_modify: mfinalfuncModify,
            sortop_name: sortoperatorName,
            agg_trans_type: transTypeId,
            agg_trans_space: transSpace,
            agg_mtrans_type: mtransTypeId,
            agg_mtrans_space: mtransSpace,
            agg_initval: initval,
            agg_minitval: minitval,
            proparallel,
        },
    )
}

// OidInputFunctionCall(typinput, initval, typioparam, -1); result discarded.
fn validate_initval(mcx: Mcx<'_>, value: &str, typid: Oid) -> PgResult<()> {
    let (typinput, typioparam) = lsyscache::getTypeInputInfo(typid)?;
    let mut flinfo = fmgr_core::fmgr_info(typinput)?;
    let cstr = std::ffi::CString::new(value).expect("aggregate initcond contains an interior NUL");
    let _ = types_fmgr::input_function_call(&mut flinfo, Some(&cstr), typioparam, -1, mcx)?;
    Ok(())
}

// extractModify (aggregatecmds.c): [m]finalfunc_modify string form to the
// catalog representation.
fn extractModify(mcx: Mcx<'_>, defel: &DefElem<'_>) -> PgResult<i8> {
    match commands_define::defGetString(mcx, defel)? {
        "read_only" => Ok(AGGMODIFY_READ_ONLY),
        "shareable" => Ok(AGGMODIFY_SHAREABLE),
        "read_write" => Ok(AGGMODIFY_READ_WRITE),
        _ => Err(err(
            ERRCODE_SYNTAX_ERROR,
            format!(
                "parameter \"{}\" must be READ_ONLY, SHAREABLE, or READ_WRITE",
                defel.defname.unwrap_or("")
            ),
        )),
    }
}

fn name_parts<'a, 'mcx>(names: &NodeList<'mcx>, buf: &'a mut [&'mcx str; 4]) -> &'a [&'mcx str] {
    let n = names.len().min(buf.len());
    for (i, slot) in buf.iter_mut().enumerate().take(n) {
        *slot = names.nth(i).as_string().expect("name list holds String nodes").sval;
    }
    &buf[..n]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn polymorphic_type_oids_match_pg_type_dat() {
        for oid in [2283, 2277, 2776, 3500, 3831, 4537, 5077, 5078, 5079, 5080, 4538] {
            assert!(IsPolymorphicType(oid), "oid {oid} should be polymorphic");
        }
        assert!(!IsPolymorphicType(INTERNALOID));
        assert!(!IsPolymorphicType(23));
    }

    #[test]
    fn constants_match_pg_headers() {
        assert_eq!(TYPTYPE_PSEUDO, b'p' as i8);
        assert_eq!(INTERNALOID, 2281);
        assert_eq!(PROPARALLEL_SAFE, b's' as i8);
        assert_eq!(PROPARALLEL_RESTRICTED, b'r' as i8);
        assert_eq!(PROPARALLEL_UNSAFE, b'u' as i8);
    }
}
